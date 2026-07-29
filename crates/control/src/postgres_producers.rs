use super::{ActiveProducer, ControlError, PartitionKey};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub(super) async fn describe(
    pool: &PgPool,
    partition: &PartitionKey,
) -> Result<Vec<ActiveProducer>, ControlError> {
    let partition_exists = sqlx::query(
        "SELECT 1
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         WHERE t.name = $1 AND p.partition_index = $2",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .fetch_optional(pool)
    .await?
    .is_some();
    if !partition_exists {
        return Err(ControlError::PartitionNotFound {
            topic: partition.topic.clone(),
            partition: partition.partition,
        });
    }

    let rows = sqlx::query(
        "SELECT ps.producer_id, ps.producer_epoch, ps.last_sequence, ps.last_timestamp,
                COALESCE((
                    SELECT MIN(os.base_offset)
                    FROM producers p
                    JOIN object_spans os
                      ON os.transaction_id = p.current_transaction_id
                     AND os.producer_id = p.producer_id
                    WHERE p.producer_id = ps.producer_id
                      AND os.topic_id = ps.topic_id
                      AND os.partition_index = ps.partition_index
                      AND os.txn_state = 'pending'
                ), -1) AS current_transaction_start_offset
         FROM producer_sequences ps
         JOIN topics t ON t.id = ps.topic_id
         WHERE t.name = $1 AND ps.partition_index = $2
         ORDER BY ps.producer_id",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| ActiveProducer {
            producer_id: row.get("producer_id"),
            producer_epoch: row.get("producer_epoch"),
            last_sequence: row.get("last_sequence"),
            last_timestamp: row.get("last_timestamp"),
            current_transaction_start_offset: row.get("current_transaction_start_offset"),
        })
        .collect())
}

pub(super) async fn expire(
    pool: &PgPool,
    now_ms: i64,
    expiration_ms: i64,
    limit: usize,
) -> Result<u64, ControlError> {
    if expiration_ms <= 0 {
        return Err(ControlError::InvalidRequest(
            "producer id expiration must be positive".to_owned(),
        ));
    }
    if limit == 0 {
        return Ok(0);
    }
    let cutoff_ms = now_ms.saturating_sub(expiration_ms);
    let result = sqlx::query(
        "WITH candidates AS (
             SELECT ps.producer_id, ps.topic_id, ps.partition_index
             FROM producer_sequences ps
             WHERE ps.last_timestamp <= $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM object_spans os
                   WHERE os.producer_id = ps.producer_id
                     AND os.topic_id = ps.topic_id
                     AND os.partition_index = ps.partition_index
                     AND os.txn_state = 'pending'
               )
             ORDER BY ps.last_timestamp, ps.producer_id, ps.topic_id, ps.partition_index
             LIMIT $2
             FOR UPDATE OF ps SKIP LOCKED
         )
         DELETE FROM producer_sequences ps
         USING candidates c
         WHERE ps.producer_id = c.producer_id
           AND ps.topic_id = c.topic_id
           AND ps.partition_index = c.partition_index",
    )
    .bind(cutoff_ms)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn reconcile_after_log_truncation(
    transaction: &mut Transaction<'_, Postgres>,
    topic_id: uuid::Uuid,
    partition_index: i32,
) -> Result<(), ControlError> {
    sqlx::query(
        "WITH retained AS (
             SELECT ps.producer_id, ps.topic_id, ps.partition_index,
                    latest.last_sequence, latest.last_offset, latest.timestamp_ms,
                    history.history_start_offset
             FROM producer_sequences ps
             CROSS JOIN LATERAL (
                 SELECT os.last_sequence, os.last_offset, os.timestamp_ms
                 FROM object_spans os
                 WHERE os.producer_id = ps.producer_id
                   AND os.topic_id = ps.topic_id
                   AND os.partition_index = ps.partition_index
                   AND os.producer_epoch = ps.producer_epoch
                 ORDER BY os.base_offset DESC
                 LIMIT 1
             ) latest
             CROSS JOIN LATERAL (
                 SELECT MIN(recent.base_offset) AS history_start_offset
                 FROM (
                     SELECT os.base_offset
                     FROM object_spans os
                     WHERE os.producer_id = ps.producer_id
                       AND os.topic_id = ps.topic_id
                       AND os.partition_index = ps.partition_index
                       AND os.producer_epoch = ps.producer_epoch
                     ORDER BY os.base_offset DESC
                     LIMIT 5
                 ) recent
             ) history
             WHERE ps.topic_id = $1 AND ps.partition_index = $2
         )
         UPDATE producer_sequences ps
         SET last_sequence = retained.last_sequence,
             last_offset = retained.last_offset,
             last_timestamp = retained.timestamp_ms,
             history_start_offset = retained.history_start_offset,
             updated_at = now()
         FROM retained
         WHERE ps.producer_id = retained.producer_id
           AND ps.topic_id = retained.topic_id
           AND ps.partition_index = retained.partition_index",
    )
    .bind(topic_id)
    .bind(partition_index)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM producer_sequences ps
         WHERE ps.topic_id = $1 AND ps.partition_index = $2
           AND NOT EXISTS (
               SELECT 1
               FROM object_spans os
               WHERE os.producer_id = ps.producer_id
                 AND os.topic_id = ps.topic_id
                 AND os.partition_index = ps.partition_index
                 AND os.producer_epoch = ps.producer_epoch
           )",
    )
    .bind(topic_id)
    .bind(partition_index)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
