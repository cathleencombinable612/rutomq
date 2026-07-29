use crate::batcher::PendingObjects;
use crate::object_integrity;
use crate::records::{decode_stored_records, encode_records};
use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use rutomq_control::{
    CompactedObject, CompactedSpanDraft, CompactionPlan, CompactionSourceSpan,
    CompactionTransactionState, MetadataStore, ObjectRef, ProducerBatch,
};
use rutomq_protocol::records::Record;
use rutomq_storage::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct CompactionOutcome {
    pub removed_records: u64,
    pub bytes_written: u64,
}

struct Scan {
    latest_offsets: HashMap<Bytes, i64>,
    source_records: u64,
}

pub(crate) async fn compact_plan(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    cluster_id: &str,
    pending: &PendingObjects,
    plan: &CompactionPlan,
    now_ms: i64,
    max_object_bytes: usize,
) -> Result<Option<CompactionOutcome>> {
    let scan = scan_latest_offsets(objects, plan).await?;
    let mut builder = ObjectBuilder::default();
    let mut compacted_objects = Vec::new();
    let mut pending_keys = Vec::new();
    let mut retained_records = 0u64;
    let mut recheck_at_ms = None;
    let tombstone_cutoff = now_ms.saturating_sub(plan.delete_retention_ms);

    let rewrite_result = async {
        for source in &plan.spans {
            if source.transaction_state == CompactionTransactionState::Aborted {
                continue;
            }
            let records = read_source(objects, source).await?;
            let retained = records
                .into_iter()
                .filter(|record| {
                    should_retain(
                        record,
                        source,
                        &scan.latest_offsets,
                        tombstone_cutoff,
                        plan.delete_retention_ms,
                        &mut recheck_at_ms,
                    )
                })
                .collect::<Vec<_>>();
            if retained.is_empty() {
                continue;
            }
            retained_records += retained.len() as u64;
            let producer = compacted_producer(source, &retained)?;
            let encoded = encode_records(&retained)?;
            if !builder.data.is_empty()
                && builder.data.len().saturating_add(encoded.len()) > max_object_bytes
            {
                compacted_objects.push(
                    write_object(
                        metadata,
                        objects,
                        cluster_id,
                        pending,
                        &mut pending_keys,
                        builder,
                    )
                    .await?,
                );
                builder = ObjectBuilder::default();
            }
            builder.push(source.id, &retained, producer, encoded)?;
            if builder.data.len() >= max_object_bytes {
                compacted_objects.push(
                    write_object(
                        metadata,
                        objects,
                        cluster_id,
                        pending,
                        &mut pending_keys,
                        builder,
                    )
                    .await?,
                );
                builder = ObjectBuilder::default();
            }
        }
        if !builder.data.is_empty() {
            compacted_objects.push(
                write_object(
                    metadata,
                    objects,
                    cluster_id,
                    pending,
                    &mut pending_keys,
                    builder,
                )
                .await?,
            );
        }
        let bytes_written = compacted_objects
            .iter()
            .map(|object| object.object.size)
            .sum::<u64>();
        let applied = metadata
            .commit_compaction(plan, compacted_objects, recheck_at_ms, now_ms)
            .await
            .context("commit compacted span metadata")?;
        Ok::<_, anyhow::Error>((applied, bytes_written))
    }
    .await;
    clear_pending(pending, &pending_keys);
    let (applied, bytes_written) = rewrite_result?;
    if !applied {
        return Ok(None);
    }
    Ok(Some(CompactionOutcome {
        removed_records: scan.source_records.saturating_sub(retained_records),
        bytes_written,
    }))
}

async fn scan_latest_offsets(
    objects: &Arc<dyn ObjectStore>,
    plan: &CompactionPlan,
) -> Result<Scan> {
    let mut latest_offsets = HashMap::new();
    let mut source_records = 0u64;
    for source in &plan.spans {
        if source.transaction_state == CompactionTransactionState::Aborted {
            source_records =
                source_records.saturating_add(u64::try_from(source.span.record_count).unwrap_or(0));
            continue;
        }
        let records = read_source(objects, source).await?;
        source_records = source_records.saturating_add(records.len() as u64);
        for record in records {
            if let Some(key) = record.key {
                latest_offsets.insert(key, record.offset);
            }
        }
    }
    Ok(Scan {
        latest_offsets,
        source_records,
    })
}

async fn read_source(
    objects: &Arc<dyn ObjectStore>,
    source: &CompactionSourceSpan,
) -> Result<Vec<Record>> {
    let raw = objects
        .get_range(
            &source.span.object_key,
            source.span.byte_start..source.span.byte_end,
        )
        .await
        .with_context(|| format!("read compaction source {}", source.span.object_key))?;
    object_integrity::verify(&source.span, &raw)?;
    decode_stored_records(&raw, source.span.base_offset, source.span.offsets_preserved)
}

fn should_retain(
    record: &Record,
    source: &CompactionSourceSpan,
    latest_offsets: &HashMap<Bytes, i64>,
    tombstone_cutoff: i64,
    delete_retention_ms: i64,
    recheck_at_ms: &mut Option<i64>,
) -> bool {
    let Some(key) = record.key.as_ref() else {
        return true;
    };
    if latest_offsets.get(key) != Some(&record.offset) {
        return false;
    }
    if record.value.is_some() {
        return true;
    }
    if source.span.timestamp_ms <= tombstone_cutoff {
        return false;
    }
    let expires_at = source.span.timestamp_ms.saturating_add(delete_retention_ms);
    let current = *recheck_at_ms;
    *recheck_at_ms = Some(current.map_or(expires_at, |current| current.min(expires_at)));
    true
}

fn compacted_producer(
    source: &CompactionSourceSpan,
    records: &[Record],
) -> Result<Option<ProducerBatch>> {
    let Some(original) = source.span.producer else {
        return Ok(None);
    };
    if records.iter().any(|record| {
        record.producer_id != original.producer_id
            || record.producer_epoch != original.producer_epoch
    }) {
        return Err(anyhow!("compaction source changed producer identity"));
    }
    Ok(Some(ProducerBatch {
        producer_id: original.producer_id,
        producer_epoch: original.producer_epoch,
        first_sequence: records.first().expect("records are non-empty").sequence,
        last_sequence: records.last().expect("records are non-empty").sequence,
    }))
}

#[derive(Default)]
struct ObjectBuilder {
    data: BytesMut,
    spans: Vec<CompactedSpanDraft>,
}

impl ObjectBuilder {
    fn push(
        &mut self,
        source_id: i64,
        records: &[Record],
        producer: Option<ProducerBatch>,
        encoded: Bytes,
    ) -> Result<()> {
        let byte_start = self.data.len() as u64;
        self.data.extend_from_slice(&encoded);
        self.spans.push(CompactedSpanDraft {
            source_id,
            byte_start,
            byte_end: self.data.len() as u64,
            base_offset: records.first().expect("records are non-empty").offset,
            last_offset: records.last().expect("records are non-empty").offset,
            record_count: i32::try_from(records.len())
                .map_err(|_| anyhow!("too many compacted records in one span"))?,
            checksum: object_integrity::checksum(&encoded),
            producer,
        });
        Ok(())
    }
}

async fn write_object(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    cluster_id: &str,
    pending: &PendingObjects,
    pending_keys: &mut Vec<String>,
    builder: ObjectBuilder,
) -> Result<CompactedObject> {
    let key = format!("data/{cluster_id}/compact-{}.rlog", Uuid::new_v4());
    metadata
        .stage_object(ObjectRef {
            key: key.clone(),
            size: u64::try_from(builder.data.len()).expect("object size fits in u64"),
        })
        .await
        .context("stage compacted object upload intent")?;
    pending
        .lock()
        .expect("pending object lock is not poisoned")
        .insert(key.clone());
    pending_keys.push(key.clone());
    let metadata = objects
        .put_immutable(&key, builder.data.freeze())
        .await
        .context("write compacted object")?;
    Ok(CompactedObject {
        object: ObjectRef {
            key: metadata.key,
            size: metadata.size,
        },
        spans: builder.spans,
    })
}

fn clear_pending(pending: &PendingObjects, keys: &[String]) {
    let mut pending = pending.lock().expect("pending object lock is not poisoned");
    for key in keys {
        pending.remove(key);
    }
}
