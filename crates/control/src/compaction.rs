use crate::{
    ControlError, MemoryState, ObjectRef, PartitionKey, ProducerBatch, StoredSpan,
    TransactionStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionTransactionState {
    Visible,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionSourceSpan {
    pub id: i64,
    pub span: StoredSpan,
    pub transaction_state: CompactionTransactionState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub lease_id: Uuid,
    pub partition: PartitionKey,
    pub delete_retention_ms: i64,
    pub file_delete_delay_ms: i64,
    pub end_offset: i64,
    pub spans: Vec<CompactionSourceSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactedSpanDraft {
    pub source_id: i64,
    pub byte_start: u64,
    pub byte_end: u64,
    pub base_offset: i64,
    pub last_offset: i64,
    pub record_count: i32,
    pub checksum: crate::SpanChecksum,
    pub producer: Option<ProducerBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactedObject {
    pub object: ObjectRef,
    pub spans: Vec<CompactedSpanDraft>,
}

pub(crate) fn should_compact(
    spans: &[CompactionSourceSpan],
    compaction_last_offset: i64,
    min_cleanable_dirty_ratio: f64,
    max_compaction_lag_ms: i64,
    now_ms: i64,
    tombstone_recheck_due: bool,
) -> bool {
    if tombstone_recheck_due {
        return !spans.is_empty();
    }
    let dirty = spans
        .iter()
        .filter(|source| source.span.last_offset > compaction_last_offset)
        .collect::<Vec<_>>();
    let Some(oldest_dirty_ms) = dirty.iter().map(|source| source.span.timestamp_ms).min() else {
        return false;
    };
    let total_bytes = spans.iter().map(span_bytes).sum::<u128>();
    let dirty_bytes = dirty.into_iter().map(span_bytes).sum::<u128>();
    let ratio_reached = dirty_bytes as f64 >= total_bytes as f64 * min_cleanable_dirty_ratio;
    let maximum_lag_reached = oldest_dirty_ms <= now_ms.saturating_sub(max_compaction_lag_ms);
    ratio_reached || maximum_lag_reached
}

fn span_bytes(source: &CompactionSourceSpan) -> u128 {
    source
        .span
        .byte_end
        .saturating_sub(source.span.byte_start)
        .into()
}

pub(crate) async fn claim_memory(
    state: &Arc<RwLock<MemoryState>>,
    now_ms: i64,
    lease_ms: i64,
) -> Result<Option<CompactionPlan>, ControlError> {
    if lease_ms <= 0 {
        return Err(ControlError::InvalidRequest(
            "compaction lease must be positive".to_owned(),
        ));
    }
    let mut state = state.write().await;
    let mut partitions = state.partitions.keys().cloned().collect::<Vec<_>>();
    partitions
        .sort_by(|left, right| (&left.topic, left.partition).cmp(&(&right.topic, right.partition)));
    for key in partitions {
        let config = state
            .topic_configs
            .get(&key.topic)
            .cloned()
            .unwrap_or_default();
        if !config.compacts_records() {
            continue;
        }
        let partition = state
            .partitions
            .get(&key)
            .expect("partition key came from the same map")
            .clone();
        if partition
            .compaction_lease
            .is_some_and(|(_, expires_at)| expires_at > now_ms)
        {
            continue;
        }
        let cutoff_ms = now_ms.saturating_sub(config.min_compaction_lag_ms);
        let mut spans = Vec::new();
        for span in partition
            .spans
            .iter()
            .take_while(|span| span.timestamp_ms <= cutoff_ms)
        {
            let transaction_state = match span
                .transaction_id
                .and_then(|transaction_id| state.transactions.get(&transaction_id))
                .map(|transaction| transaction.status)
            {
                None => CompactionTransactionState::Visible,
                Some(TransactionStatus::Committed) => CompactionTransactionState::Committed,
                Some(TransactionStatus::Aborted) => CompactionTransactionState::Aborted,
                Some(TransactionStatus::Ongoing) => break,
            };
            spans.push(CompactionSourceSpan {
                id: span.base_offset,
                span: span.clone(),
                transaction_state,
            });
        }
        let Some(end_offset) = spans.last().map(|source| source.span.last_offset) else {
            continue;
        };
        let tombstone_recheck_due = partition
            .compaction_recheck_at_ms
            .is_some_and(|recheck_at| recheck_at <= now_ms);
        if !should_compact(
            &spans,
            partition.compaction_last_offset,
            config.min_cleanable_dirty_ratio,
            config.max_compaction_lag_ms,
            now_ms,
            tombstone_recheck_due,
        ) {
            continue;
        }
        let lease_id = Uuid::new_v4();
        state
            .partitions
            .get_mut(&key)
            .expect("partition key came from the same map")
            .compaction_lease = Some((lease_id, now_ms.saturating_add(lease_ms)));
        return Ok(Some(CompactionPlan {
            lease_id,
            partition: key,
            delete_retention_ms: config.delete_retention_ms,
            file_delete_delay_ms: config.file_delete_delay_ms,
            end_offset,
            spans,
        }));
    }
    Ok(None)
}

pub(crate) async fn commit_memory(
    state: &Arc<RwLock<MemoryState>>,
    plan: &CompactionPlan,
    objects: Vec<CompactedObject>,
    recheck_at_ms: Option<i64>,
    now_ms: i64,
) -> Result<bool, ControlError> {
    let mut state = state.write().await;
    let Some(partition) = state.partitions.get(&plan.partition) else {
        return Ok(false);
    };
    if partition
        .compaction_lease
        .is_none_or(|(lease_id, _)| lease_id != plan.lease_id)
    {
        return Ok(false);
    }
    let current = partition
        .spans
        .iter()
        .map(|span| (span.base_offset, span))
        .collect::<HashMap<_, _>>();
    if plan
        .spans
        .iter()
        .any(|source| current.get(&source.id).copied() != Some(&source.span))
    {
        state
            .partitions
            .get_mut(&plan.partition)
            .expect("partition was checked above")
            .compaction_lease = None;
        return Ok(false);
    }
    validate_replacements(plan, &objects)?;

    let mut staged = state.clone();
    let source_ids = plan
        .spans
        .iter()
        .map(|source| source.id)
        .collect::<HashSet<_>>();
    let old_object_keys = plan
        .spans
        .iter()
        .map(|source| source.span.object_key.clone())
        .collect::<HashSet<_>>();
    for object in &objects {
        if staged.objects.contains_key(&object.object.key) {
            return Err(ControlError::InvalidRequest(format!(
                "object {} is already committed",
                object.object.key
            )));
        }
        staged
            .objects
            .insert(object.object.key.clone(), object.object.clone());
        staged.staged_objects.remove(&object.object.key);
        staged.unreferenced_objects.remove(&object.object.key);
        staged.object_delete_after.remove(&object.object.key);
    }

    let partition = staged
        .partitions
        .get_mut(&plan.partition)
        .expect("partition was checked above");
    partition
        .spans
        .retain(|span| !source_ids.contains(&span.base_offset));
    for object in objects {
        for draft in object.spans {
            let source = plan
                .spans
                .iter()
                .find(|source| source.id == draft.source_id)
                .expect("replacement sources were validated");
            partition.spans.push(StoredSpan {
                partition: plan.partition.clone(),
                object_key: object.object.key.clone(),
                byte_start: draft.byte_start,
                byte_end: draft.byte_end,
                base_offset: draft.base_offset,
                last_offset: draft.last_offset,
                record_count: draft.record_count,
                timestamp_ms: source.span.timestamp_ms,
                integrity: crate::SpanIntegrity::current(draft.checksum),
                producer: draft.producer,
                transaction_id: source.span.transaction_id,
                offsets_preserved: true,
            });
        }
    }
    partition.spans.sort_by_key(|span| span.base_offset);
    if partition
        .spans
        .windows(2)
        .any(|spans| spans[0].base_offset == spans[1].base_offset)
    {
        return Err(ControlError::InvalidRequest(
            "compaction created duplicate base offsets".to_owned(),
        ));
    }
    partition.compaction_last_offset = partition.compaction_last_offset.max(plan.end_offset);
    partition.compaction_recheck_at_ms = recheck_at_ms;
    partition.compaction_lease = None;

    for object_key in old_object_keys {
        let delete_after = now_ms.saturating_add(plan.file_delete_delay_ms);
        staged
            .object_delete_after
            .entry(object_key.clone())
            .and_modify(|current| *current = (*current).max(delete_after))
            .or_insert(delete_after);
        let referenced = staged.partitions.values().any(|partition| {
            partition
                .spans
                .iter()
                .any(|span| span.object_key == object_key)
        });
        if !referenced {
            staged
                .unreferenced_objects
                .entry(object_key)
                .or_insert(now_ms);
        }
    }
    *state = staged;
    Ok(true)
}

pub(crate) async fn release_memory(
    state: &Arc<RwLock<MemoryState>>,
    partition: &PartitionKey,
    lease_id: Uuid,
) -> Result<(), ControlError> {
    let mut state = state.write().await;
    if let Some(partition) = state.partitions.get_mut(partition)
        && partition
            .compaction_lease
            .is_some_and(|(current, _)| current == lease_id)
    {
        partition.compaction_lease = None;
    }
    Ok(())
}

pub(crate) fn validate_replacements(
    plan: &CompactionPlan,
    objects: &[CompactedObject],
) -> Result<(), ControlError> {
    if plan.spans.is_empty()
        || plan.end_offset
            != plan
                .spans
                .last()
                .map(|source| source.span.last_offset)
                .unwrap_or(-1)
        || plan
            .spans
            .windows(2)
            .any(|spans| spans[0].span.base_offset >= spans[1].span.base_offset)
    {
        return Err(ControlError::InvalidRequest(
            "compaction plan has invalid source ordering".to_owned(),
        ));
    }
    let sources = plan
        .spans
        .iter()
        .map(|source| (source.id, source))
        .collect::<HashMap<_, _>>();
    let mut object_keys = HashSet::new();
    let mut replaced_sources = HashSet::new();
    let mut base_offsets = HashSet::new();
    for object in objects {
        if object.spans.is_empty() || !object_keys.insert(&object.object.key) {
            return Err(ControlError::InvalidRequest(
                "each compacted object must be unique and contain spans".to_owned(),
            ));
        }
        let mut ranges = object.spans.iter().collect::<Vec<_>>();
        ranges.sort_by_key(|span| span.byte_start);
        let mut prior_end = 0;
        for draft in ranges {
            let source = sources.get(&draft.source_id).ok_or_else(|| {
                ControlError::InvalidRequest("compaction source is not in the lease".to_owned())
            })?;
            if !replaced_sources.insert(draft.source_id)
                || !base_offsets.insert(draft.base_offset)
                || draft.record_count <= 0
                || draft.byte_start >= draft.byte_end
                || draft.byte_end > object.object.size
                || draft.byte_start < prior_end
                || draft.base_offset < source.span.base_offset
                || draft.last_offset > source.span.last_offset
                || draft.base_offset > draft.last_offset
            {
                return Err(ControlError::InvalidRequest(
                    "compacted span has invalid byte or offset bounds".to_owned(),
                ));
            }
            validate_producer(source.span.producer, draft.producer)?;
            prior_end = draft.byte_end;
        }
    }
    Ok(())
}

fn validate_producer(
    source: Option<ProducerBatch>,
    replacement: Option<ProducerBatch>,
) -> Result<(), ControlError> {
    match (source, replacement) {
        (None, None) => Ok(()),
        (Some(source), Some(replacement))
            if source.producer_id == replacement.producer_id
                && source.producer_epoch == replacement.producer_epoch =>
        {
            Ok(())
        }
        _ => Err(ControlError::InvalidRequest(
            "compaction changed producer identity".to_owned(),
        )),
    }
}
