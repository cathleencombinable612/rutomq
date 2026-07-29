use crate::{ControlError, PartitionKey};
use sqlx::{PgPool, Row};

pub async fn partition_size(pool: &PgPool, partition: &PartitionKey) -> Result<i64, ControlError> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(GREATEST(s.byte_end - s.byte_start, 0)), 0)::BIGINT AS size_bytes
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         LEFT JOIN object_spans s
           ON s.topic_id = p.topic_id
          AND s.partition_index = p.partition_index
         WHERE t.name = $1 AND p.partition_index = $2
         GROUP BY p.topic_id, p.partition_index",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ControlError::PartitionNotFound {
        topic: partition.topic.clone(),
        partition: partition.partition,
    })?;
    Ok(row.get("size_bytes"))
}
