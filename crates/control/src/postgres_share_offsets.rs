use crate::share_records::{validate_offset_topics, validate_offset_updates};
use crate::share_state::merge_state_batches_and_completion_count;
use crate::{
    ControlError, PartitionKey, ShareOffsetDeleteResult, ShareOffsetUpdate,
    ShareOffsetUpdateResult, SharePartitionOffset, ShareStateBatch,
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub(crate) async fn describe(
    pool: &PgPool,
    group_id: &str,
    partitions: Option<&[PartitionKey]>,
) -> Result<Vec<SharePartitionOffset>, ControlError> {
    ensure_group_exists(pool, group_id).await?;
    match partitions {
        Some(partitions) => {
            let mut offsets = Vec::with_capacity(partitions.len());
            for partition in partitions {
                let row = sqlx::query(
                    "SELECT t.id AS topic_id, p.log_start_offset, p.next_offset,
                            s.start_offset,
                            COALESCE(s.delivery_complete_count, 0)
                                AS persisted_delivery_complete_count,
                            COALESCE(s.state_batches, '[]'::jsonb) AS state_batches,
                            COALESCE((
                                SELECT jsonb_agg(
                                    jsonb_build_object(
                                        'first_offset', r.record_offset,
                                        'last_offset', r.record_offset,
                                        'delivery_state', r.delivery_state,
                                        'delivery_count', r.delivery_count
                                    )
                                    ORDER BY r.record_offset
                                )
                                FROM share_record_states r
                                WHERE r.group_id = $1
                                  AND r.topic_id = t.id
                                  AND r.partition_index = p.partition_index
                            ), '[]'::jsonb) AS record_batches
                     FROM topics t
                     JOIN partitions p
                       ON p.topic_id = t.id AND p.partition_index = $3
                     LEFT JOIN share_partition_states s
                       ON s.group_id = $1
                      AND s.topic_id = t.id
                      AND s.partition_index = p.partition_index
                     WHERE t.name = $2",
                )
                .bind(group_id)
                .bind(&partition.topic)
                .bind(partition.partition)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
                offsets.push(offset_from_row(partition.clone(), &row)?);
            }
            Ok(offsets)
        }
        None => sqlx::query(
            "SELECT t.id AS topic_id, t.name, p.partition_index,
                    p.log_start_offset, p.next_offset, s.start_offset,
                    s.delivery_complete_count AS persisted_delivery_complete_count,
                    s.state_batches,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'first_offset', r.record_offset,
                                'last_offset', r.record_offset,
                                'delivery_state', r.delivery_state,
                                'delivery_count', r.delivery_count
                            )
                            ORDER BY r.record_offset
                        )
                        FROM share_record_states r
                        WHERE r.group_id = s.group_id
                          AND r.topic_id = s.topic_id
                          AND r.partition_index = s.partition_index
                    ), '[]'::jsonb) AS record_batches
             FROM share_partition_states s
             JOIN topics t ON t.id = s.topic_id
             JOIN partitions p
               ON p.topic_id = s.topic_id
              AND p.partition_index = s.partition_index
             WHERE s.group_id = $1
             ORDER BY t.name, p.partition_index",
        )
        .bind(group_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| {
            let partition =
                PartitionKey::new(row.get::<String, _>("name"), row.get("partition_index"));
            offset_from_row(partition, &row)
        })
        .collect(),
    }
}

pub(crate) async fn alter(
    pool: &PgPool,
    group_id: &str,
    updates: &[ShareOffsetUpdate],
) -> Result<Vec<ShareOffsetUpdateResult>, ControlError> {
    validate_offset_updates(group_id, updates)?;
    let mut transaction = pool.begin().await?;
    super::postgres_share_groups::lock_group(&mut transaction, group_id).await?;
    ensure_empty_group(&mut transaction, group_id, true).await?;
    let mut results = Vec::with_capacity(updates.len());
    for update in updates {
        let topic = sqlx::query("SELECT id, partition_count FROM topics WHERE name = $1")
            .bind(&update.partition.topic)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(topic) = topic else {
            results.push(update_result(update, None, false));
            continue;
        };
        let topic_id = topic.get("id");
        let partition_count: i32 = topic.get("partition_count");
        if update.partition.partition < 0 || update.partition.partition >= partition_count {
            results.push(update_result(update, Some(topic_id), false));
            continue;
        }
        super::postgres_share_records::lock_partition(
            &mut transaction,
            group_id,
            topic_id,
            update.partition.partition,
        )
        .await?;
        sqlx::query(
            "DELETE FROM share_record_states
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
        )
        .bind(group_id)
        .bind(topic_id)
        .bind(update.partition.partition)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO share_partition_states (
                 group_id, topic_id, partition_index, start_offset, state_epoch,
                 leader_epoch, delivery_complete_count, state_batches
             ) VALUES ($1, $2, $3, $4, 0, -1, 0, '[]'::jsonb)
             ON CONFLICT (group_id, topic_id, partition_index) DO UPDATE
             SET start_offset = EXCLUDED.start_offset,
                 state_epoch = share_partition_states.state_epoch + 1,
                 leader_epoch = -1,
                 delivery_complete_count = 0,
                 state_batches = '[]'::jsonb",
        )
        .bind(group_id)
        .bind(topic_id)
        .bind(update.partition.partition)
        .bind(update.start_offset)
        .execute(&mut *transaction)
        .await?;
        results.push(update_result(update, Some(topic_id), true));
    }
    transaction.commit().await?;
    Ok(results)
}

pub(crate) async fn delete(
    pool: &PgPool,
    group_id: &str,
    topics: &[String],
) -> Result<Vec<ShareOffsetDeleteResult>, ControlError> {
    validate_offset_topics(group_id, topics)?;
    let mut transaction = pool.begin().await?;
    super::postgres_share_groups::lock_group(&mut transaction, group_id).await?;
    ensure_empty_group(&mut transaction, group_id, false).await?;
    let mut results = Vec::with_capacity(topics.len());
    for topic_name in topics {
        let topic_id = sqlx::query("SELECT id FROM topics WHERE name = $1")
            .bind(topic_name)
            .fetch_optional(&mut *transaction)
            .await?
            .map(|row| row.get("id"));
        let deleted = if let Some(topic_id) = topic_id {
            sqlx::query(
                "DELETE FROM share_partition_states
                 WHERE group_id = $1 AND topic_id = $2",
            )
            .bind(group_id)
            .bind(topic_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected()
                > 0
        } else {
            false
        };
        results.push(ShareOffsetDeleteResult {
            topic: topic_name.clone(),
            topic_id,
            deleted,
        });
    }
    transaction.commit().await?;
    Ok(results)
}

async fn ensure_group_exists(pool: &PgPool, group_id: &str) -> Result<(), ControlError> {
    if group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "share group ID cannot be empty".to_owned(),
        ));
    }
    if sqlx::query("SELECT 1 FROM share_groups WHERE group_id = $1")
        .bind(group_id)
        .fetch_optional(pool)
        .await?
        .is_none()
    {
        return Err(ControlError::GroupNotFound(group_id.to_owned()));
    }
    Ok(())
}

async fn ensure_empty_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    create: bool,
) -> Result<(), ControlError> {
    let conflict = sqlx::query(
        "SELECT EXISTS (
             SELECT 1 FROM consumer_groups WHERE group_id = $1
             UNION ALL
             SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1
             UNION ALL
             SELECT 1 FROM streams_protocol_groups WHERE group_id = $1
         ) AS conflict",
    )
    .bind(group_id)
    .fetch_one(&mut **transaction)
    .await?
    .get::<bool, _>("conflict");
    if conflict {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }
    let exists = sqlx::query("SELECT 1 FROM share_groups WHERE group_id = $1 FOR UPDATE")
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
    if !exists && !create {
        return Err(ControlError::GroupNotFound(group_id.to_owned()));
    }
    if !exists {
        sqlx::query("INSERT INTO share_groups (group_id) VALUES ($1)")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    let mut group = super::postgres_share_groups::load_group(transaction, group_id)
        .await?
        .expect("share group was loaded or inserted");
    let topics = super::postgres_share_groups::load_topics(transaction).await?;
    if crate::share_groups::expire_members(&mut group, &topics, chrono::Utc::now()) {
        super::postgres_share_groups::save_group(transaction, &group).await?;
    }
    if !group.members.is_empty() {
        return Err(ControlError::NonEmptyGroup(group_id.to_owned()));
    }
    Ok(())
}

fn offset_from_row(
    partition: PartitionKey,
    row: &sqlx::postgres::PgRow,
) -> Result<SharePartitionOffset, ControlError> {
    let log_start_offset = row.get::<i64, _>("log_start_offset");
    let start_offset = row
        .get::<Option<i64>, _>("start_offset")
        .map_or(-1, |offset| offset.max(log_start_offset));
    let persisted = serde_json::from_value::<Vec<ShareStateBatch>>(row.get("state_batches"))
        .map_err(|error| ControlError::Database(sqlx::Error::Decode(Box::new(error))))?;
    let overrides = serde_json::from_value::<Vec<ShareStateBatch>>(row.get("record_batches"))
        .map_err(|error| ControlError::Database(sqlx::Error::Decode(Box::new(error))))?;
    let (_, completion_count) = merge_state_batches_and_completion_count(
        &persisted,
        &overrides,
        start_offset,
        row.get("persisted_delivery_complete_count"),
    )?;
    let high_watermark = row.get::<i64, _>("next_offset");
    let delivery_complete_count = if completion_count < 0 {
        -1
    } else {
        i64::from(completion_count).min(high_watermark.saturating_sub(start_offset).max(0))
    };
    Ok(SharePartitionOffset {
        partition,
        topic_id: row.get("topic_id"),
        start_offset,
        leader_epoch: 0,
        high_watermark,
        delivery_complete_count,
    })
}

fn update_result(
    update: &ShareOffsetUpdate,
    topic_id: Option<uuid::Uuid>,
    updated: bool,
) -> ShareOffsetUpdateResult {
    ShareOffsetUpdateResult {
        partition: update.partition.clone(),
        topic_id,
        updated,
    }
}
