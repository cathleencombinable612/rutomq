use crate::compaction::{should_compact, validate_replacements};
use crate::{
    CURRENT_OBJECT_FORMAT_VERSION, CompactedObject, CompactionPlan, CompactionSourceSpan,
    CompactionTransactionState, ControlError, PartitionKey, ProducerBatch, StoredSpan,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub async fn claim(
    pool: &PgPool,
    now_ms: i64,
    lease_ms: i64,
) -> Result<Option<CompactionPlan>, ControlError> {
    if lease_ms <= 0 {
        return Err(ControlError::InvalidRequest(
            "compaction lease must be positive".to_owned(),
        ));
    }
    let mut transaction = pool.begin().await?;
    let partitions = sqlx::query(
        "SELECT p.topic_id, p.partition_index, p.compaction_last_offset,
                p.compaction_recheck_at_ms,
                t.name, c.delete_retention_ms, c.file_delete_delay_ms,
                c.min_compaction_lag_ms,
                c.max_compaction_lag_ms, c.min_cleanable_dirty_ratio
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         JOIN topic_configs c ON c.topic_id = p.topic_id
         WHERE c.cleanup_policy IN ('compact', 'compact,delete', 'delete,compact')
           AND (
               p.compaction_lease_id IS NULL
               OR p.compaction_lease_until_ms <= $1
           )
         ORDER BY t.name, p.partition_index
         FOR UPDATE OF p SKIP LOCKED",
    )
    .bind(now_ms)
    .fetch_all(&mut *transaction)
    .await?;
    for partition in partitions {
        let topic_id: Uuid = partition.get("topic_id");
        let partition_index: i32 = partition.get("partition_index");
        let cutoff_ms = now_ms.saturating_sub(partition.get::<i64, _>("min_compaction_lag_ms"));
        let rows = sqlx::query(
            "SELECT id, object_key, byte_start, byte_end, base_offset, last_offset,
                    record_count, timestamp_ms, txn_state, producer_id,
                    producer_epoch, first_sequence, last_sequence, transaction_id,
                    offsets_preserved, format_version, checksum
             FROM object_spans
             WHERE topic_id = $1 AND partition_index = $2
             ORDER BY base_offset",
        )
        .bind(topic_id)
        .bind(partition_index)
        .fetch_all(&mut *transaction)
        .await?;
        let topic_name: String = partition.get("name");
        let key = PartitionKey::new(&topic_name, partition_index);
        let mut spans = Vec::new();
        for row in rows {
            if row.get::<i64, _>("timestamp_ms") > cutoff_ms {
                break;
            }
            let transaction_state = transaction_state(&row)?;
            if row.get::<String, _>("txn_state") == "pending" {
                break;
            }
            spans.push(source_from_row(&row, &key, transaction_state)?);
        }
        let Some(end_offset) = spans.last().map(|source| source.span.last_offset) else {
            continue;
        };
        let tombstone_recheck_due = partition
            .get::<Option<i64>, _>("compaction_recheck_at_ms")
            .is_some_and(|recheck_at| recheck_at <= now_ms);
        if !should_compact(
            &spans,
            partition.get("compaction_last_offset"),
            partition.get("min_cleanable_dirty_ratio"),
            partition.get("max_compaction_lag_ms"),
            now_ms,
            tombstone_recheck_due,
        ) {
            continue;
        }
        let lease_id = Uuid::new_v4();
        sqlx::query(
            "UPDATE partitions
             SET compaction_lease_id = $1, compaction_lease_until_ms = $2
             WHERE topic_id = $3 AND partition_index = $4",
        )
        .bind(lease_id)
        .bind(now_ms.saturating_add(lease_ms))
        .bind(topic_id)
        .bind(partition_index)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        return Ok(Some(CompactionPlan {
            lease_id,
            partition: key,
            delete_retention_ms: partition.get("delete_retention_ms"),
            file_delete_delay_ms: partition.get("file_delete_delay_ms"),
            end_offset,
            spans,
        }));
    }
    transaction.commit().await?;
    Ok(None)
}

pub async fn commit(
    pool: &PgPool,
    plan: &CompactionPlan,
    objects: Vec<CompactedObject>,
    recheck_at_ms: Option<i64>,
    now_ms: i64,
) -> Result<bool, ControlError> {
    validate_replacements(plan, &objects)?;
    let mut transaction = pool.begin().await?;
    let partition = sqlx::query(
        "SELECT p.topic_id
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         WHERE t.name = $1 AND p.partition_index = $2
           AND p.compaction_lease_id = $3
         FOR UPDATE OF p",
    )
    .bind(&plan.partition.topic)
    .bind(plan.partition.partition)
    .bind(plan.lease_id)
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(partition) = partition else {
        transaction.rollback().await?;
        return Ok(false);
    };
    let topic_id: Uuid = partition.get("topic_id");
    let source_ids = plan
        .spans
        .iter()
        .map(|source| source.id)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT id, object_key, byte_start, byte_end, base_offset, last_offset,
                record_count, timestamp_ms, txn_state, producer_id,
                producer_epoch, first_sequence, last_sequence, transaction_id,
                offsets_preserved, format_version, checksum
         FROM object_spans
         WHERE topic_id = $1 AND partition_index = $2 AND id = ANY($3)
         ORDER BY base_offset
         FOR UPDATE",
    )
    .bind(topic_id)
    .bind(plan.partition.partition)
    .bind(&source_ids)
    .fetch_all(&mut *transaction)
    .await?;
    let current = rows
        .iter()
        .map(|row| {
            let state = transaction_state(row)?;
            let source = source_from_row(row, &plan.partition, state)?;
            Ok((source.id, source))
        })
        .collect::<Result<HashMap<_, _>, ControlError>>()?;
    let matches = current.len() == plan.spans.len()
        && plan.spans.iter().all(|expected| {
            current
                .get(&expected.id)
                .is_some_and(|row| same_source(row, expected))
        });
    if !matches {
        clear_lease(
            &mut transaction,
            topic_id,
            plan.partition.partition,
            plan.lease_id,
        )
        .await?;
        transaction.commit().await?;
        return Ok(false);
    }

    for object in &objects {
        crate::postgres_objects::lock_staged(&mut transaction, &object.object).await?;
        crate::postgres_objects::mark_committed(&mut transaction, &object.object.key).await?;
    }
    let old_object_keys = plan
        .spans
        .iter()
        .map(|source| source.span.object_key.clone())
        .collect::<HashSet<_>>();
    sqlx::query("DELETE FROM object_spans WHERE id = ANY($1)")
        .bind(&source_ids)
        .execute(&mut *transaction)
        .await?;

    let sources = plan
        .spans
        .iter()
        .map(|source| (source.id, source))
        .collect::<HashMap<_, _>>();
    for object in objects {
        for draft in object.spans {
            let source = sources
                .get(&draft.source_id)
                .expect("replacement sources were validated");
            let producer_id = draft.producer.map(|producer| producer.producer_id);
            let producer_epoch = draft.producer.map(|producer| producer.producer_epoch);
            let first_sequence = draft.producer.map(|producer| producer.first_sequence);
            let last_sequence = draft.producer.map(|producer| producer.last_sequence);
            sqlx::query(
                "INSERT INTO object_spans (
                     topic_id, partition_index, object_key, byte_start, byte_end,
                     base_offset, last_offset, record_count, timestamp_ms, txn_state,
                     producer_id, producer_epoch, first_sequence, last_sequence,
                     transaction_id, offsets_preserved, format_version, checksum
                 )
                 VALUES (
                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                     $11, $12, $13, $14, $15, TRUE, $16, $17
                 )",
            )
            .bind(topic_id)
            .bind(plan.partition.partition)
            .bind(&object.object.key)
            .bind(draft.byte_start as i64)
            .bind(draft.byte_end as i64)
            .bind(draft.base_offset)
            .bind(draft.last_offset)
            .bind(draft.record_count)
            .bind(source.span.timestamp_ms)
            .bind(transaction_state_name(source.transaction_state))
            .bind(producer_id)
            .bind(producer_epoch)
            .bind(first_sequence)
            .bind(last_sequence)
            .bind(source.span.transaction_id)
            .bind(CURRENT_OBJECT_FORMAT_VERSION)
            .bind(draft.checksum.to_vec())
            .execute(&mut *transaction)
            .await?;
        }
    }

    let old_object_keys = old_object_keys.into_iter().collect::<Vec<_>>();
    crate::postgres_objects::defer_delete(
        &mut transaction,
        &old_object_keys,
        now_ms,
        plan.file_delete_delay_ms,
    )
    .await?;
    sqlx::query(
        "UPDATE partitions
         SET compaction_last_offset = GREATEST(compaction_last_offset, $1),
             compaction_recheck_at_ms = $2,
             compaction_lease_id = NULL,
             compaction_lease_until_ms = NULL
         WHERE topic_id = $3 AND partition_index = $4
           AND compaction_lease_id = $5",
    )
    .bind(plan.end_offset)
    .bind(recheck_at_ms)
    .bind(topic_id)
    .bind(plan.partition.partition)
    .bind(plan.lease_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(true)
}

pub async fn release(
    pool: &PgPool,
    partition: &PartitionKey,
    lease_id: Uuid,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE partitions p
         SET compaction_lease_id = NULL, compaction_lease_until_ms = NULL
         FROM topics t
         WHERE t.id = p.topic_id AND t.name = $1
           AND p.partition_index = $2 AND p.compaction_lease_id = $3",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .bind(lease_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn clear_lease(
    transaction: &mut Transaction<'_, Postgres>,
    topic_id: Uuid,
    partition: i32,
    lease_id: Uuid,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE partitions
         SET compaction_lease_id = NULL, compaction_lease_until_ms = NULL
         WHERE topic_id = $1 AND partition_index = $2 AND compaction_lease_id = $3",
    )
    .bind(topic_id)
    .bind(partition)
    .bind(lease_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn source_from_row(
    row: &sqlx::postgres::PgRow,
    partition: &PartitionKey,
    transaction_state: CompactionTransactionState,
) -> Result<CompactionSourceSpan, ControlError> {
    let producer = row
        .get::<Option<i64>, _>("producer_id")
        .map(|producer_id| ProducerBatch {
            producer_id,
            producer_epoch: row.get("producer_epoch"),
            first_sequence: row.get("first_sequence"),
            last_sequence: row.get("last_sequence"),
        });
    Ok(CompactionSourceSpan {
        id: row.get("id"),
        span: StoredSpan {
            partition: partition.clone(),
            object_key: row.get("object_key"),
            byte_start: row.get::<i64, _>("byte_start") as u64,
            byte_end: row.get::<i64, _>("byte_end") as u64,
            base_offset: row.get("base_offset"),
            last_offset: row.get("last_offset"),
            record_count: row.get("record_count"),
            timestamp_ms: row.get("timestamp_ms"),
            integrity: crate::span_integrity::from_row(row)?,
            producer,
            transaction_id: row.get("transaction_id"),
            offsets_preserved: row.get("offsets_preserved"),
        },
        transaction_state,
    })
}

fn transaction_state(
    row: &sqlx::postgres::PgRow,
) -> Result<CompactionTransactionState, ControlError> {
    match row.get::<String, _>("txn_state").as_str() {
        "visible" => Ok(CompactionTransactionState::Visible),
        "committed" => Ok(CompactionTransactionState::Committed),
        "aborted" => Ok(CompactionTransactionState::Aborted),
        "pending" => Ok(CompactionTransactionState::Visible),
        other => Err(ControlError::InvalidRequest(format!(
            "unknown object span transaction state {other}"
        ))),
    }
}

fn transaction_state_name(state: CompactionTransactionState) -> &'static str {
    match state {
        CompactionTransactionState::Visible => "visible",
        CompactionTransactionState::Committed => "committed",
        CompactionTransactionState::Aborted => "aborted",
    }
}

fn same_source(left: &CompactionSourceSpan, right: &CompactionSourceSpan) -> bool {
    left.id == right.id
        && left.span == right.span
        && left.transaction_state == right.transaction_state
}
