use super::{ControlError, TopicConfig, TopicInfo, topic_names};
use chrono::Utc;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const TOPIC_NAMESPACE_LOCK: i64 = 0x72_75_74_6f_6d_71;

pub(super) async fn validate_topic_creation(pool: &PgPool, name: &str) -> Result<(), ControlError> {
    topic_names::validate(name)?;
    let normalized = topic_names::normalize(name);
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM topics
         WHERE replace(name, '.', '_') = $1
         ORDER BY name",
    )
    .bind(normalized)
    .fetch_all(pool)
    .await?;
    validate_existing(name, &existing)
}

pub(super) async fn create_topic(
    pool: &PgPool,
    name: &str,
    partitions: i32,
    config: TopicConfig,
) -> Result<TopicInfo, ControlError> {
    if partitions <= 0 {
        return Err(ControlError::InvalidRequest(
            "partitions must be positive".to_owned(),
        ));
    }
    topic_names::validate(name)?;
    config.validate()?;
    let dynamic_config_names = config
        .dynamic_config_names
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(TOPIC_NAMESPACE_LOCK)
        .execute(&mut *transaction)
        .await?;
    let normalized = topic_names::normalize(name);
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM topics
         WHERE replace(name, '.', '_') = $1
         ORDER BY name",
    )
    .bind(normalized)
    .fetch_all(&mut *transaction)
    .await?;
    validate_existing(name, &existing)?;
    let id = Uuid::new_v4();
    let insert = sqlx::query("INSERT INTO topics (id, name, partition_count) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(name)
        .bind(partitions)
        .execute(&mut *transaction)
        .await;
    if let Err(error) = insert {
        if let sqlx::Error::Database(database_error) = &error
            && database_error.constraint() == Some("topics_name_key")
        {
            return Err(ControlError::TopicAlreadyExists(name.to_owned()));
        }
        return Err(error.into());
    }
    sqlx::query(
        "INSERT INTO partitions (topic_id, partition_index)
         SELECT $1, generate_series(0, $2)",
    )
    .bind(id)
    .bind(partitions - 1)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO topic_configs (
             topic_id, retention_ms, retention_bytes, cleanup_policy,
             file_delete_delay_ms, flush_messages, flush_ms,
             delete_retention_ms, min_compaction_lag_ms,
             max_compaction_lag_ms,
             min_cleanable_dirty_ratio, min_insync_replicas, max_message_bytes,
             compression_type, compression_gzip_level, compression_lz4_level,
             compression_zstd_level, message_timestamp_type,
             message_timestamp_before_max_ms, message_timestamp_after_max_ms,
             dynamic_config_names
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, $9, $10, $11, $12, $13, $14,
             $15, $16, $17, $18, $19, $20, $21
         )",
    )
    .bind(id)
    .bind(config.retention_ms)
    .bind(config.retention_bytes)
    .bind(config.cleanup_policy)
    .bind(config.file_delete_delay_ms)
    .bind(config.flush_messages)
    .bind(config.flush_ms)
    .bind(config.delete_retention_ms)
    .bind(config.min_compaction_lag_ms)
    .bind(config.max_compaction_lag_ms)
    .bind(config.min_cleanable_dirty_ratio)
    .bind(config.min_insync_replicas)
    .bind(config.max_message_bytes)
    .bind(config.compression_type)
    .bind(config.compression_gzip_level)
    .bind(config.compression_lz4_level)
    .bind(config.compression_zstd_level)
    .bind(config.message_timestamp_type)
    .bind(config.message_timestamp_before_max_ms)
    .bind(config.message_timestamp_after_max_ms)
    .bind(dynamic_config_names)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(TopicInfo {
        id,
        name: name.to_owned(),
        partitions,
    })
}

fn validate_existing(name: &str, existing: &[String]) -> Result<(), ControlError> {
    if existing.iter().any(|candidate| candidate == name) {
        return Err(ControlError::TopicAlreadyExists(name.to_owned()));
    }
    if let Some(existing) = topic_names::collision(name, existing.iter().map(String::as_str)) {
        return Err(topic_names::collision_error(name, existing));
    }
    Ok(())
}

pub(super) async fn create_partitions(
    pool: &PgPool,
    name: &str,
    new_count: i32,
) -> Result<TopicInfo, ControlError> {
    let mut transaction = pool.begin().await?;
    let row =
        sqlx::query("SELECT id, name, partition_count FROM topics WHERE name = $1 FOR UPDATE")
            .bind(name)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))?;
    let current = row.get("partition_count");
    if new_count <= current {
        return Err(ControlError::InvalidPartitionCount {
            topic: name.to_owned(),
            current,
            requested: new_count,
        });
    }
    let id = row.get("id");
    sqlx::query(
        "INSERT INTO partitions (topic_id, partition_index)
         SELECT $1, generate_series($2, $3)",
    )
    .bind(id)
    .bind(current)
    .bind(new_count - 1)
    .execute(&mut *transaction)
    .await?;
    sqlx::query("UPDATE topics SET partition_count = $2 WHERE id = $1")
        .bind(id)
        .bind(new_count)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(TopicInfo {
        id,
        name: row.get("name"),
        partitions: new_count,
    })
}

pub(super) async fn delete_topic(pool: &PgPool, name: &str) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    let topic = sqlx::query(
        "SELECT t.id, c.file_delete_delay_ms
         FROM topics t
         JOIN topic_configs c ON c.topic_id = t.id
         WHERE t.name = $1
         FOR UPDATE OF t",
    )
    .bind(name)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))?;
    let topic_id: Uuid = topic.get("id");
    delete_locked_topic(
        &mut transaction,
        topic_id,
        topic.get("file_delete_delay_ms"),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(super) async fn delete_topic_by_id(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<TopicInfo>, ControlError> {
    let mut transaction = pool.begin().await?;
    let Some(topic) = sqlx::query(
        "SELECT t.id, t.name, t.partition_count, c.file_delete_delay_ms
         FROM topics t
         JOIN topic_configs c ON c.topic_id = t.id
         WHERE t.id = $1
         FOR UPDATE OF t",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await?
    else {
        return Ok(None);
    };
    let info = TopicInfo {
        id: topic.get("id"),
        name: topic.get("name"),
        partitions: topic.get("partition_count"),
    };
    delete_locked_topic(&mut transaction, info.id, topic.get("file_delete_delay_ms")).await?;
    transaction.commit().await?;
    Ok(Some(info))
}

async fn delete_locked_topic(
    transaction: &mut Transaction<'_, Postgres>,
    topic_id: Uuid,
    file_delete_delay_ms: i64,
) -> Result<(), ControlError> {
    let object_keys = sqlx::query(
        "SELECT DISTINCT object_key
         FROM object_spans
         WHERE topic_id = $1",
    )
    .bind(topic_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| row.get::<String, _>("object_key"))
    .collect::<Vec<_>>();
    sqlx::query("DELETE FROM topics WHERE id = $1")
        .bind(topic_id)
        .execute(&mut **transaction)
        .await?;
    crate::postgres_objects::defer_delete(
        transaction,
        &object_keys,
        Utc::now().timestamp_millis(),
        file_delete_delay_ms,
    )
    .await?;
    Ok(())
}
