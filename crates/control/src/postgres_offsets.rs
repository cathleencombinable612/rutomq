use super::{ControlError, OffsetCommit, PartitionKey, PartitionWatermarks};
use chrono::{DateTime, Utc};
use regex::Regex;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(super) async fn commit(
    pool: &PgPool,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    commit_in_transaction(&mut transaction, group_id, offsets).await?;
    transaction.commit().await?;
    Ok(())
}

pub(super) async fn commit_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    if group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "group id must not be empty".to_owned(),
        ));
    }
    let commit_timestamp_ms = Utc::now().timestamp_millis();
    let mut ordered_offsets = Vec::with_capacity(offsets.len());
    for (request_index, offset) in offsets.into_iter().enumerate() {
        if offset.offset < 0 {
            return Err(ControlError::InvalidRequest(
                "committed offset must not be negative".to_owned(),
            ));
        }
        let topic_id = sqlx::query("SELECT id FROM topics WHERE name = $1 FOR KEY SHARE")
            .bind(&offset.partition.topic)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| ControlError::TopicNotFound(offset.partition.topic.clone()))?
            .get::<Uuid, _>("id");
        let partition_exists =
            sqlx::query("SELECT 1 FROM partitions WHERE topic_id = $1 AND partition_index = $2")
                .bind(topic_id)
                .bind(offset.partition.partition)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !partition_exists {
            return Err(ControlError::PartitionNotFound {
                topic: offset.partition.topic,
                partition: offset.partition.partition,
            });
        }
        ordered_offsets.push((
            topic_id,
            offset.partition.partition,
            request_index,
            offset.offset,
            offset.leader_epoch,
            offset.metadata,
            super::offset_expire_timestamp(commit_timestamp_ms, offset.retention_time_ms)?,
        ));
    }
    ordered_offsets.sort_by_key(|(topic_id, partition, request_index, _, _, _, _)| {
        (*topic_id, *partition, *request_index)
    });
    for (topic_id, partition, _, offset, leader_epoch, metadata, expire_timestamp_ms) in
        ordered_offsets
    {
        sqlx::query(
            "INSERT INTO consumer_offsets
             (group_id, topic_id, partition_index, committed_offset,
              committed_leader_epoch, metadata, commit_timestamp_ms,
              expire_timestamp_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (group_id, topic_id, partition_index)
             DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                           committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                           metadata = EXCLUDED.metadata,
                           commit_timestamp_ms = EXCLUDED.commit_timestamp_ms,
                           expire_timestamp_ms = EXCLUDED.expire_timestamp_ms,
                           expiration_checked_at_ms = 0,
                           updated_at = now()",
        )
        .bind(group_id)
        .bind(topic_id)
        .bind(partition)
        .bind(offset)
        .bind(leader_epoch)
        .bind(metadata)
        .bind(commit_timestamp_ms)
        .bind(expire_timestamp_ms)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

pub(super) async fn partition_watermarks(
    pool: &PgPool,
    partition: &PartitionKey,
) -> Result<PartitionWatermarks, ControlError> {
    let row = sqlx::query(
        "SELECT p.next_offset, p.log_start_offset,
                COALESCE(
                    (SELECT MIN(s.base_offset) FROM object_spans s
                     WHERE s.topic_id = p.topic_id
                       AND s.partition_index = p.partition_index
                       AND s.txn_state = 'pending'),
                    p.next_offset
                ) AS last_stable_offset
         FROM partitions p JOIN topics t ON t.id = p.topic_id
         WHERE t.name = $1 AND p.partition_index = $2",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ControlError::PartitionNotFound {
        topic: partition.topic.clone(),
        partition: partition.partition,
    })?;
    Ok(PartitionWatermarks {
        high_watermark: row.get("next_offset"),
        last_stable_offset: row.get("last_stable_offset"),
        log_start_offset: row.get("log_start_offset"),
    })
}

pub(super) async fn delete_records(
    pool: &PgPool,
    partition: &PartitionKey,
    before_offset: i64,
) -> Result<i64, ControlError> {
    let mut transaction = pool.begin().await?;
    let row = sqlx::query(
        "SELECT p.topic_id, p.next_offset, p.log_start_offset,
                c.file_delete_delay_ms
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         JOIN topic_configs c ON c.topic_id = p.topic_id
         WHERE t.name = $1 AND p.partition_index = $2
         FOR UPDATE OF p",
    )
    .bind(&partition.topic)
    .bind(partition.partition)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| ControlError::PartitionNotFound {
        topic: partition.topic.clone(),
        partition: partition.partition,
    })?;
    let next_offset: i64 = row.get("next_offset");
    let current_start: i64 = row.get("log_start_offset");
    let target = if before_offset == -1 {
        next_offset
    } else {
        before_offset
    };
    if target < 0 || target > next_offset {
        return Err(ControlError::OffsetOutOfRange {
            partition: partition.clone(),
            offset: before_offset,
            start: current_start,
            end: next_offset,
        });
    }
    let topic_id: Uuid = row.get("topic_id");
    let pending_transaction_start = sqlx::query(
        "SELECT MIN(base_offset) AS base_offset
         FROM object_spans
         WHERE topic_id = $1 AND partition_index = $2 AND txn_state = 'pending'",
    )
    .bind(topic_id)
    .bind(partition.partition)
    .fetch_one(&mut *transaction)
    .await?
    .get::<Option<i64>, _>("base_offset");
    let target = pending_transaction_start.map_or(target, |pending| target.min(pending));
    let log_start_offset = current_start.max(target);
    let object_keys = sqlx::query(
        "SELECT DISTINCT object_key
         FROM object_spans
         WHERE topic_id = $1
           AND partition_index = $2
           AND last_offset < $3",
    )
    .bind(topic_id)
    .bind(partition.partition)
    .bind(log_start_offset)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| row.get::<String, _>("object_key"))
    .collect::<Vec<_>>();
    sqlx::query(
        "DELETE FROM object_spans
         WHERE topic_id = $1
           AND partition_index = $2
           AND last_offset < $3",
    )
    .bind(topic_id)
    .bind(partition.partition)
    .bind(log_start_offset)
    .execute(&mut *transaction)
    .await?;
    crate::postgres_objects::defer_delete(
        &mut transaction,
        &object_keys,
        Utc::now().timestamp_millis(),
        row.get("file_delete_delay_ms"),
    )
    .await?;
    sqlx::query(
        "UPDATE partitions SET log_start_offset = $3
         WHERE topic_id = $1 AND partition_index = $2",
    )
    .bind(topic_id)
    .bind(partition.partition)
    .bind(log_start_offset)
    .execute(&mut *transaction)
    .await?;
    crate::postgres_producers::reconcile_after_log_truncation(
        &mut transaction,
        topic_id,
        partition.partition,
    )
    .await?;
    transaction.commit().await?;
    Ok(log_start_offset)
}

pub(super) async fn delete_offsets(
    pool: &PgPool,
    group_id: &str,
    partitions: &[PartitionKey],
) -> Result<HashSet<PartitionKey>, ControlError> {
    let mut transaction = pool.begin().await?;
    let classic_exists =
        sqlx::query("SELECT 1 FROM consumer_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
    let consumer_exists =
        sqlx::query("SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
    let offsets_exist = sqlx::query("SELECT 1 FROM consumer_offsets WHERE group_id = $1 LIMIT 1")
        .bind(group_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
    if !classic_exists && !consumer_exists && !offsets_exist {
        return Err(ControlError::GroupNotFound(group_id.to_owned()));
    }

    let mut topic_ids = HashMap::new();
    for partition in partitions {
        let row = sqlx::query(
            "SELECT t.id FROM topics t
             JOIN partitions p ON p.topic_id = t.id
             WHERE t.name = $1 AND p.partition_index = $2",
        )
        .bind(&partition.topic)
        .bind(partition.partition)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| ControlError::PartitionNotFound {
            topic: partition.topic.clone(),
            partition: partition.partition,
        })?;
        topic_ids.insert(partition.clone(), row.get::<Uuid, _>("id"));
    }

    let classic_topics =
        sqlx::query("SELECT subscribed_topics FROM consumer_group_members WHERE group_id = $1")
            .bind(group_id)
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .flat_map(|row| row.get::<Vec<String>, _>("subscribed_topics"))
            .collect::<HashSet<_>>();
    let consumer_subscriptions = sqlx::query(
        "SELECT subscribed_topic_names, subscribed_topic_regex
         FROM consumer_protocol_members
         WHERE group_id = $1
           AND last_heartbeat
               + session_timeout_ms * interval '1 millisecond' > now()",
    )
    .bind(group_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| {
        let names = row
            .get::<Vec<String>, _>("subscribed_topic_names")
            .into_iter()
            .collect::<HashSet<_>>();
        let regex = row
            .get::<Option<String>, _>("subscribed_topic_regex")
            .and_then(|pattern| Regex::new(&pattern).ok());
        (names, regex)
    })
    .collect::<Vec<_>>();

    let blocked = partitions
        .iter()
        .filter(|partition| {
            classic_topics.contains(&partition.topic)
                || consumer_subscriptions.iter().any(|(names, regex)| {
                    names.contains(&partition.topic)
                        || regex
                            .as_ref()
                            .is_some_and(|regex| regex.is_match(&partition.topic))
                })
        })
        .cloned()
        .collect::<HashSet<_>>();
    let mut deletable = partitions
        .iter()
        .enumerate()
        .filter(|(_, partition)| !blocked.contains(*partition))
        .map(|(request_index, partition)| {
            (topic_ids[partition], partition.partition, request_index)
        })
        .collect::<Vec<_>>();
    deletable.sort_by_key(|(topic_id, partition, request_index)| {
        (*topic_id, *partition, *request_index)
    });
    for (topic_id, partition, _) in deletable {
        sqlx::query(
            "DELETE FROM consumer_offsets
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
        )
        .bind(group_id)
        .bind(topic_id)
        .bind(partition)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(blocked)
}

pub(super) async fn expire(
    pool: &PgPool,
    now_ms: i64,
    retention_ms: i64,
    limit: usize,
) -> Result<u64, ControlError> {
    if retention_ms < 0 {
        return Err(ControlError::InvalidRequest(
            "consumer offset retention must be non-negative".to_owned(),
        ));
    }
    if limit == 0 {
        return Ok(0);
    }
    let cutoff_ms = now_ms.saturating_sub(retention_ms);
    let candidates = sqlx::query(
        "SELECT co.group_id, co.topic_id, co.partition_index, t.name AS topic_name
         FROM consumer_offsets co
         JOIN topics t ON t.id = co.topic_id
         WHERE (co.expire_timestamp_ms IS NOT NULL
                AND co.expire_timestamp_ms <= $1)
            OR (co.expire_timestamp_ms IS NULL
                AND co.commit_timestamp_ms <= $2)
         ORDER BY co.expiration_checked_at_ms,
                  co.group_id, co.topic_id, co.partition_index
         LIMIT $3",
    )
    .bind(now_ms)
    .bind(cutoff_ms)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut transaction = pool.begin().await?;
    let mut locked_group = None::<String>;
    let mut expired = 0u64;
    let mut affected_groups = HashSet::new();
    for candidate in candidates {
        let group_id = candidate.get::<String, _>("group_id");
        if locked_group.as_deref() != Some(group_id.as_str()) {
            crate::postgres_streams_groups::lock_group(&mut transaction, &group_id).await?;
            locked_group = Some(group_id.clone());
        }
        let topic_id = candidate.get::<Uuid, _>("topic_id");
        let partition = candidate.get::<i32, _>("partition_index");
        let topic_name = candidate.get::<String, _>("topic_name");
        let Some(offset) = sqlx::query(
            "SELECT commit_timestamp_ms, expire_timestamp_ms
             FROM consumer_offsets
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
             FOR UPDATE SKIP LOCKED",
        )
        .bind(&group_id)
        .bind(topic_id)
        .bind(partition)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            continue;
        };
        if pending_transaction_offset(&mut transaction, &group_id, topic_id, partition).await? {
            mark_expiration_checked(&mut transaction, &group_id, topic_id, partition, now_ms)
                .await?;
            continue;
        }
        let commit_timestamp_ms = offset.get::<i64, _>("commit_timestamp_ms");
        let expire_timestamp_ms = offset.get::<Option<i64>, _>("expire_timestamp_ms");
        let should_expire = if let Some(expire_timestamp_ms) = expire_timestamp_ms {
            now_ms >= expire_timestamp_ms
        } else {
            now_ms.saturating_sub(commit_timestamp_ms) >= retention_ms
                && default_offset_expired(
                    &mut transaction,
                    &group_id,
                    &topic_name,
                    commit_timestamp_ms,
                    now_ms,
                    retention_ms,
                )
                .await?
        };
        if !should_expire {
            mark_expiration_checked(&mut transaction, &group_id, topic_id, partition, now_ms)
                .await?;
            continue;
        }
        let deleted = sqlx::query(
            "DELETE FROM consumer_offsets
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
        )
        .bind(&group_id)
        .bind(topic_id)
        .bind(partition)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if deleted > 0 {
            expired += deleted;
            affected_groups.insert(group_id);
        }
    }
    let mut affected_groups = affected_groups.into_iter().collect::<Vec<_>>();
    affected_groups.sort();
    for group_id in affected_groups {
        remove_expired_group_metadata(&mut transaction, &group_id, now_ms).await?;
    }
    transaction.commit().await?;
    Ok(expired)
}

async fn mark_expiration_checked(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
    now_ms: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE consumer_offsets
         SET expiration_checked_at_ms = $4
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .bind(now_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn pending_transaction_offset(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
) -> Result<bool, ControlError> {
    Ok(sqlx::query(
        "SELECT 1
         FROM transaction_offset_commits pending
         JOIN transactions txn ON txn.id = pending.transaction_id
         WHERE pending.group_id = $1
           AND pending.topic_id = $2
           AND pending.partition_index = $3
           AND txn.status = 'ongoing'
         LIMIT 1",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn default_offset_expired(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_name: &str,
    commit_timestamp_ms: i64,
    now_ms: i64,
    retention_ms: i64,
) -> Result<bool, ControlError> {
    if let Some(group) = sqlx::query(
        "SELECT protocol_type, classic_rebalance_id, classic_rebalance_pending,
                empty_since_ms
         FROM consumer_groups WHERE group_id = $1 FOR UPDATE",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?
    {
        return classic_offset_expired(
            transaction,
            group_id,
            topic_name,
            commit_timestamp_ms,
            now_ms,
            retention_ms,
            &group,
        )
        .await;
    }
    if sqlx::query("SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1 FOR UPDATE")
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some()
    {
        let members = sqlx::query(
            "SELECT member_epoch, session_timeout_ms, subscribed_topic_names,
                    subscribed_topic_regex, last_heartbeat
             FROM consumer_protocol_members WHERE group_id = $1",
        )
        .bind(group_id)
        .fetch_all(&mut **transaction)
        .await?;
        let subscribed = members.into_iter().any(|member| {
            let active =
                member.get::<i32, _>("member_epoch") == -2 || row_member_active(&member, now_ms);
            active
                && (member
                    .get::<Vec<String>, _>("subscribed_topic_names")
                    .iter()
                    .any(|topic| topic == topic_name)
                    || member
                        .get::<Option<String>, _>("subscribed_topic_regex")
                        .is_some_and(|pattern| {
                            Regex::new(&format!("^(?:{pattern})$"))
                                .is_ok_and(|regex| regex.is_match(topic_name))
                        }))
        });
        return Ok(!subscribed);
    }
    if let Some(group) =
        sqlx::query("SELECT topology FROM streams_protocol_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut **transaction)
            .await?
    {
        let active = sqlx::query(
            "SELECT session_timeout_ms, last_heartbeat
             FROM streams_protocol_members WHERE group_id = $1",
        )
        .bind(group_id)
        .fetch_all(&mut **transaction)
        .await?
        .iter()
        .any(|member| row_member_active(member, now_ms));
        if !active {
            return Ok(true);
        }
        let topology = serde_json::from_value::<crate::StreamsTopology>(
            group.get::<serde_json::Value, _>("topology"),
        )
        .map_err(|error| {
            ControlError::InvalidRequest(format!("invalid stored streams topology: {error}"))
        })?;
        let subscribed = topology.subtopologies.iter().any(|subtopology| {
            subtopology
                .source_topics
                .iter()
                .any(|topic| topic == topic_name)
                || subtopology
                    .repartition_source_topics
                    .iter()
                    .any(|topic| topic.name == topic_name)
                || subtopology.source_topic_regex.iter().any(|pattern| {
                    Regex::new(&format!("^(?:{pattern})$"))
                        .is_ok_and(|regex| regex.is_match(topic_name))
                })
        });
        return Ok(!subscribed);
    }
    Ok(now_ms.saturating_sub(commit_timestamp_ms) >= retention_ms)
}

async fn classic_offset_expired(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_name: &str,
    commit_timestamp_ms: i64,
    now_ms: i64,
    retention_ms: i64,
    group: &sqlx::postgres::PgRow,
) -> Result<bool, ControlError> {
    let rebalance_id = group.get::<Option<Uuid>, _>("classic_rebalance_id");
    let rebalance_pending = group.get::<bool, _>("classic_rebalance_pending");
    let members = sqlx::query(
        "SELECT member_id, subscribed_topics, assignment, session_timeout_ms,
                last_heartbeat, classic_joined_rebalance_id
         FROM consumer_group_members WHERE group_id = $1",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?;
    let active_members = members
        .iter()
        .filter(|member| {
            (rebalance_pending
                && member.get::<Option<Uuid>, _>("classic_joined_rebalance_id") == rebalance_id)
                || row_member_active(member, now_ms)
        })
        .collect::<Vec<_>>();
    if active_members.is_empty() {
        let empty_since_ms = group
            .get::<Option<i64>, _>("empty_since_ms")
            .or_else(|| {
                members
                    .iter()
                    .map(member_expiration_ms)
                    .max()
                    .map(|timestamp| timestamp.min(now_ms))
            })
            .unwrap_or(now_ms);
        sqlx::query(
            "UPDATE consumer_groups
             SET empty_since_ms = COALESCE(empty_since_ms, $2)
             WHERE group_id = $1",
        )
        .bind(group_id)
        .bind(empty_since_ms)
        .execute(&mut **transaction)
        .await?;
        return Ok(now_ms.saturating_sub(empty_since_ms) >= retention_ms);
    }
    if group.get::<Option<i64>, _>("empty_since_ms").is_some() {
        sqlx::query("UPDATE consumer_groups SET empty_since_ms = NULL WHERE group_id = $1")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    let stable = !rebalance_pending
        && active_members
            .iter()
            .all(|member| member.get::<Option<Vec<u8>>, _>("assignment").is_some());
    let subscribed = active_members.iter().any(|member| {
        member
            .get::<Vec<String>, _>("subscribed_topics")
            .iter()
            .any(|topic| topic == topic_name)
    });
    Ok(stable
        && group.get::<String, _>("protocol_type") == "consumer"
        && !subscribed
        && now_ms.saturating_sub(commit_timestamp_ms) >= retention_ms)
}

fn row_member_active(row: &sqlx::postgres::PgRow, now_ms: i64) -> bool {
    member_expiration_ms(row) > now_ms
}

fn member_expiration_ms(row: &sqlx::postgres::PgRow) -> i64 {
    row.get::<DateTime<Utc>, _>("last_heartbeat")
        .timestamp_millis()
        .saturating_add(i64::from(row.get::<i32, _>("session_timeout_ms")))
}

async fn remove_expired_group_metadata(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    now_ms: i64,
) -> Result<(), ControlError> {
    let offsets_remain = sqlx::query("SELECT 1 FROM consumer_offsets WHERE group_id = $1 LIMIT 1")
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
    let pending_remains = sqlx::query(
        "SELECT 1
         FROM transaction_offset_commits pending
         JOIN transactions txn ON txn.id = pending.transaction_id
         WHERE pending.group_id = $1 AND txn.status = 'ongoing'
         LIMIT 1",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if offsets_remain || pending_remains {
        return Ok(());
    }
    let classic_active = sqlx::query(
        "SELECT 1
         FROM consumer_group_members member
         JOIN consumer_groups group_state ON group_state.group_id = member.group_id
         WHERE member.group_id = $1
           AND (
               member.last_heartbeat
                   + member.session_timeout_ms * interval '1 millisecond'
                   > to_timestamp($2::double precision / 1000.0)
               OR (
                   group_state.classic_rebalance_pending
                   AND member.classic_joined_rebalance_id =
                       group_state.classic_rebalance_id
               )
           )
         LIMIT 1",
    )
    .bind(group_id)
    .bind(now_ms)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if !classic_active {
        sqlx::query("DELETE FROM consumer_groups WHERE group_id = $1")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    let consumer_active = sqlx::query(
        "SELECT 1 FROM consumer_protocol_members
         WHERE group_id = $1
           AND (
               member_epoch = -2
               OR last_heartbeat + session_timeout_ms * interval '1 millisecond'
                    > to_timestamp($2::double precision / 1000.0)
           )
         LIMIT 1",
    )
    .bind(group_id)
    .bind(now_ms)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if !consumer_active {
        sqlx::query("DELETE FROM consumer_protocol_groups WHERE group_id = $1")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    let streams_active = sqlx::query(
        "SELECT 1 FROM streams_protocol_members
         WHERE group_id = $1
           AND last_heartbeat + session_timeout_ms * interval '1 millisecond'
                 > to_timestamp($2::double precision / 1000.0)
         LIMIT 1",
    )
    .bind(group_id)
    .bind(now_ms)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if !streams_active {
        sqlx::query("DELETE FROM streams_protocol_groups WHERE group_id = $1")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}
