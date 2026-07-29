use crate::share_records::expand_acknowledgements;
use crate::share_state::{
    ACKNOWLEDGED_DELIVERY_STATE, ARCHIVED_DELIVERY_STATE, normalize_state_batches,
};
use crate::{
    ControlError, ShareAcknowledgeRecords, ShareAcknowledgementType, ShareAcquireRequest,
    ShareAcquiredRecord, ShareAutoOffsetReset, ShareFetchSession, ShareFetchSessionUpdate,
    SharePartitionState, ShareSessionPartition, ShareStateBatch,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) async fn update_session(
    pool: &PgPool,
    update: ShareFetchSessionUpdate,
) -> Result<ShareFetchSession, ControlError> {
    if update.group_id.is_empty() || update.member_id.is_empty() || update.session_epoch < -1 {
        return Err(ControlError::InvalidRequest(
            "share session group, member, or epoch is invalid".to_owned(),
        ));
    }
    let mut transaction = pool.begin().await?;
    lock_member(&mut transaction, &update.group_id, &update.member_id).await?;
    let assigned =
        assigned_partitions(&mut transaction, &update.group_id, &update.member_id).await?;
    if update
        .added
        .iter()
        .any(|partition| !assigned.contains(partition))
    {
        return Err(ControlError::InvalidRequest(
            "share session contains a partition not assigned to the member".to_owned(),
        ));
    }
    if update.session_epoch == -1 {
        sqlx::query("DELETE FROM share_fetch_sessions WHERE group_id = $1 AND member_id = $2")
            .bind(&update.group_id)
            .bind(&update.member_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        return Ok(ShareFetchSession {
            next_epoch: -1,
            partitions: Vec::new(),
        });
    }

    let next_epoch = if update.session_epoch == 0 {
        sqlx::query(
            "INSERT INTO share_fetch_sessions (group_id, member_id, session_epoch)
             VALUES ($1, $2, 1)
             ON CONFLICT (group_id, member_id) DO UPDATE SET session_epoch = 1",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM share_fetch_session_partitions
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .execute(&mut *transaction)
        .await?;
        1
    } else {
        let expected = sqlx::query(
            "SELECT session_epoch FROM share_fetch_sessions
             WHERE group_id = $1 AND member_id = $2 FOR UPDATE",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| ControlError::ShareSessionNotFound {
            group: update.group_id.clone(),
            member: update.member_id.clone(),
        })?
        .get::<i32, _>("session_epoch");
        if expected != update.session_epoch {
            return Err(ControlError::InvalidShareSessionEpoch {
                group: update.group_id,
                member: update.member_id,
                expected,
                actual: update.session_epoch,
            });
        }
        let next = next_epoch(update.session_epoch);
        sqlx::query(
            "UPDATE share_fetch_sessions SET session_epoch = $3
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .bind(next)
        .execute(&mut *transaction)
        .await?;
        next
    };

    for partition in update.forgotten {
        sqlx::query(
            "DELETE FROM share_fetch_session_partitions
             WHERE group_id = $1 AND member_id = $2
               AND topic_id = $3 AND partition_index = $4",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .bind(partition.topic_id)
        .bind(partition.partition)
        .execute(&mut *transaction)
        .await?;
    }
    for partition in update.added {
        sqlx::query(
            "INSERT INTO share_fetch_session_partitions (
                 group_id, member_id, topic_id, partition_index, partition_max_bytes
             ) VALUES ($1, $2, $3, $4, 0)
             ON CONFLICT (group_id, member_id, topic_id, partition_index) DO NOTHING",
        )
        .bind(&update.group_id)
        .bind(&update.member_id)
        .bind(partition.topic_id)
        .bind(partition.partition)
        .execute(&mut *transaction)
        .await?;
    }
    let partitions =
        session_partitions(&mut transaction, &update.group_id, &update.member_id).await?;
    transaction.commit().await?;
    Ok(ShareFetchSession {
        next_epoch,
        partitions,
    })
}

pub(crate) async fn partition_state(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    partition: &ShareSessionPartition,
    reset: ShareAutoOffsetReset,
) -> Result<SharePartitionState, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_partition(
        &mut transaction,
        group_id,
        partition.topic_id,
        partition.partition,
    )
    .await?;
    ensure_assigned(&mut transaction, group_id, member_id, partition).await?;
    let offsets = sqlx::query(
        "SELECT p.log_start_offset, p.next_offset
         FROM partitions p
         WHERE p.topic_id = $1 AND p.partition_index = $2",
    )
    .bind(partition.topic_id)
    .bind(partition.partition)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| ControlError::PartitionNotFound {
        topic: partition.topic_id.to_string(),
        partition: partition.partition,
    })?;
    let log_start_offset: i64 = offsets.get("log_start_offset");
    let initial = match reset {
        ShareAutoOffsetReset::Earliest => log_start_offset,
        ShareAutoOffsetReset::Latest => offsets.get("next_offset"),
        ShareAutoOffsetReset::Exact(offset) => offset.max(log_start_offset),
    };
    let row = sqlx::query(
        "INSERT INTO share_partition_states (
             group_id, topic_id, partition_index, start_offset,
             delivery_complete_count
         ) VALUES ($1, $2, $3, $4, 0)
         ON CONFLICT (group_id, topic_id, partition_index) DO UPDATE
         SET delivery_complete_count = CASE
                 WHEN $5 > share_partition_states.start_offset THEN 0
                 ELSE share_partition_states.delivery_complete_count
             END,
             start_offset = GREATEST(share_partition_states.start_offset, $5)
         RETURNING start_offset, state_epoch",
    )
    .bind(group_id)
    .bind(partition.topic_id)
    .bind(partition.partition)
    .bind(initial)
    .bind(log_start_offset)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(SharePartitionState {
        start_offset: row.get("start_offset"),
        state_epoch: row.get("state_epoch"),
    })
}

pub(crate) async fn existing_partition_state(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    partition: &ShareSessionPartition,
) -> Result<Option<SharePartitionState>, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_partition(
        &mut transaction,
        group_id,
        partition.topic_id,
        partition.partition,
    )
    .await?;
    ensure_assigned(&mut transaction, group_id, member_id, partition).await?;
    let row = sqlx::query(
        "UPDATE share_partition_states s
         SET start_offset = GREATEST(s.start_offset, p.log_start_offset)
         FROM partitions p
         WHERE s.group_id = $1
           AND s.topic_id = $2
           AND s.partition_index = $3
           AND p.topic_id = s.topic_id
           AND p.partition_index = s.partition_index
         RETURNING s.start_offset, s.state_epoch",
    )
    .bind(group_id)
    .bind(partition.topic_id)
    .bind(partition.partition)
    .fetch_optional(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(row.map(|row| SharePartitionState {
        start_offset: row.get("start_offset"),
        state_epoch: row.get("state_epoch"),
    }))
}

pub(crate) async fn acquire(
    pool: &PgPool,
    mut request: ShareAcquireRequest,
) -> Result<Vec<ShareAcquiredRecord>, ControlError> {
    if request.max_records == 0
        || request.max_record_locks == 0
        || request.lock_duration_ms <= 0
        || request.delivery_count_limit <= 0
    {
        return Err(ControlError::InvalidRequest(
            "share acquisition limits must be positive".to_owned(),
        ));
    }
    request.candidate_offsets.sort_unstable();
    request.candidate_offsets.dedup();
    let partition = ShareSessionPartition {
        topic_id: request.topic_id,
        partition: request.partition,
    };
    let mut transaction = pool.begin().await?;
    lock_partition(
        &mut transaction,
        &request.group_id,
        request.topic_id,
        request.partition,
    )
    .await?;
    ensure_assigned(
        &mut transaction,
        &request.group_id,
        &request.member_id,
        &partition,
    )
    .await?;
    ensure_partition_state(
        &mut transaction,
        &request.group_id,
        request.topic_id,
        request.partition,
    )
    .await?;
    sqlx::query(
        "UPDATE share_record_states
         SET record_state = CASE
                 WHEN delivery_count >= $4 THEN 'archived'
                 ELSE 'available'
             END,
             delivery_state = CASE
                 WHEN delivery_count >= $4 THEN 4
                 ELSE 0
             END,
             member_id = NULL,
             lock_expires_at = NULL
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_state = 'acquired' AND lock_expires_at <= now()",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .bind(request.delivery_count_limit)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE share_record_states
         SET record_state = 'archived', delivery_state = 4,
             member_id = NULL, lock_expires_at = NULL
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_state = 'available' AND delivery_count >= $4",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .bind(request.delivery_count_limit)
    .execute(&mut *transaction)
    .await?;
    let locked = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM share_record_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_state = 'acquired'",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .fetch_one(&mut *transaction)
    .await?;
    let capacity = i64::try_from(request.max_record_locks)
        .unwrap_or(i64::MAX)
        .saturating_sub(locked)
        .max(0);
    let limit = i64::try_from(request.max_records)
        .unwrap_or(i64::MAX)
        .min(capacity);
    let rows = sqlx::query(
        "WITH raw_offsets AS (
             SELECT DISTINCT offset_value AS record_offset
             FROM unnest($5::bigint[]) AS offset_value
         ),
         eligible AS (
             SELECT candidate.record_offset,
                    (COALESCE(
                        state.delivery_count, persisted.delivery_count, 0
                    ) + 1)::smallint AS delivery_count
             FROM raw_offsets candidate
             JOIN share_partition_states partition_state
               ON partition_state.group_id = $1
              AND partition_state.topic_id = $2
              AND partition_state.partition_index = $3
             JOIN partitions log_partition
               ON log_partition.topic_id = $2
              AND log_partition.partition_index = $3
             LEFT JOIN share_record_states state
               ON state.group_id = $1
              AND state.topic_id = $2
              AND state.partition_index = $3
              AND state.record_offset = candidate.record_offset
             LEFT JOIN LATERAL (
                 SELECT (batch ->> 'delivery_state')::smallint AS delivery_state,
                        (batch ->> 'delivery_count')::smallint AS delivery_count
                 FROM jsonb_array_elements(partition_state.state_batches) batch
                 WHERE candidate.record_offset
                       BETWEEN (batch ->> 'first_offset')::bigint
                           AND (batch ->> 'last_offset')::bigint
                 LIMIT 1
             ) persisted ON true
             WHERE candidate.record_offset >= partition_state.start_offset
               AND candidate.record_offset < log_partition.next_offset
               AND (state.record_state IS NULL OR state.record_state = 'available')
               AND COALESCE(state.delivery_state, persisted.delivery_state, 0) = 0
               AND COALESCE(state.delivery_count, persisted.delivery_count, 0) < $8
             ORDER BY candidate.record_offset
             LIMIT $6
         ),
         acquired AS (
             INSERT INTO share_record_states (
                 group_id, topic_id, partition_index, record_offset,
                 record_state, member_id, delivery_count, lock_expires_at
             )
             SELECT $1, $2, $3, record_offset, 'acquired', $4,
                    delivery_count, now() + $7 * interval '1 millisecond'
             FROM eligible
             ON CONFLICT (group_id, topic_id, partition_index, record_offset)
             DO UPDATE SET
                 record_state = 'acquired',
                 delivery_state = 0,
                 member_id = EXCLUDED.member_id,
                 delivery_count = EXCLUDED.delivery_count,
                 lock_expires_at = EXCLUDED.lock_expires_at
             WHERE share_record_states.record_state = 'available'
             RETURNING record_offset, delivery_count
         )
         SELECT record_offset, delivery_count FROM acquired ORDER BY record_offset",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .bind(&request.member_id)
    .bind(&request.candidate_offsets)
    .bind(limit)
    .bind(request.lock_duration_ms)
    .bind(request.delivery_count_limit)
    .fetch_all(&mut *transaction)
    .await?;
    if !rows.is_empty() {
        increment_state_epoch(
            &mut transaction,
            &request.group_id,
            request.topic_id,
            request.partition,
        )
        .await?;
    }
    advance_start_offset(
        &mut transaction,
        &request.group_id,
        request.topic_id,
        request.partition,
    )
    .await?;
    transaction.commit().await?;
    Ok(rows
        .into_iter()
        .map(|row| ShareAcquiredRecord {
            offset: row.get("record_offset"),
            delivery_count: row.get("delivery_count"),
        })
        .collect())
}

pub(crate) async fn acknowledge(
    pool: &PgPool,
    request: ShareAcknowledgeRecords,
) -> Result<(), ControlError> {
    if request.lock_duration_ms <= 0 || request.delivery_count_limit <= 0 {
        return Err(ControlError::InvalidRequest(
            "share acknowledgement limits must be positive".to_owned(),
        ));
    }
    let actions = expand_acknowledgements(&request.batches)?;
    let partition = ShareSessionPartition {
        topic_id: request.topic_id,
        partition: request.partition,
    };
    let mut transaction = pool.begin().await?;
    lock_partition(
        &mut transaction,
        &request.group_id,
        request.topic_id,
        request.partition,
    )
    .await?;
    ensure_assigned(
        &mut transaction,
        &request.group_id,
        &request.member_id,
        &partition,
    )
    .await?;
    ensure_partition_state(
        &mut transaction,
        &request.group_id,
        request.topic_id,
        request.partition,
    )
    .await?;
    sqlx::query(
        "UPDATE share_record_states
         SET record_state = CASE
                 WHEN delivery_count >= $4 THEN 'archived'
                 ELSE 'available'
             END,
             delivery_state = CASE
                 WHEN delivery_count >= $4 THEN 4
                 ELSE 0
             END,
             member_id = NULL,
             lock_expires_at = NULL
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_state = 'acquired' AND lock_expires_at <= now()",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .bind(request.delivery_count_limit)
    .execute(&mut *transaction)
    .await?;

    for action in [
        ShareAcknowledgementType::Gap,
        ShareAcknowledgementType::Accept,
        ShareAcknowledgementType::Release,
        ShareAcknowledgementType::Reject,
        ShareAcknowledgementType::Renew,
    ] {
        let offsets = actions
            .iter()
            .filter_map(|(offset, value)| (*value == action).then_some(*offset))
            .collect::<Vec<_>>();
        if offsets.is_empty() {
            continue;
        }
        let affected = apply_action(&mut transaction, &request, action, &offsets).await?;
        if affected != offsets.len() as u64 {
            let invalid = find_invalid_offset(&mut transaction, &request, action, &offsets).await?;
            return Err(ControlError::InvalidShareRecordState(invalid));
        }
    }
    if !actions.is_empty() {
        increment_state_epoch(
            &mut transaction,
            &request.group_id,
            request.topic_id,
            request.partition,
        )
        .await?;
        advance_start_offset(
            &mut transaction,
            &request.group_id,
            request.topic_id,
            request.partition,
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn apply_action(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ShareAcknowledgeRecords,
    action: ShareAcknowledgementType,
    offsets: &[i64],
) -> Result<u64, ControlError> {
    let result = match action {
        ShareAcknowledgementType::Gap => {
            sqlx::query(
                "INSERT INTO share_record_states (
                     group_id, topic_id, partition_index, record_offset,
                     record_state, member_id, delivery_count, lock_expires_at,
                     delivery_state
                 )
                 SELECT $1, $2, $3, offset_value, 'archived', NULL, 0, NULL, 4
                 FROM unnest($5::bigint[]) AS offset_value
                 ON CONFLICT (group_id, topic_id, partition_index, record_offset)
                 DO UPDATE SET record_state = 'archived',
                               delivery_state = 4,
                               member_id = NULL, lock_expires_at = NULL
                 WHERE share_record_states.record_state = 'archived'
                    OR (share_record_states.record_state = 'acquired'
                        AND share_record_states.member_id = $4
                        AND share_record_states.lock_expires_at > now())",
            )
            .bind(&request.group_id)
            .bind(request.topic_id)
            .bind(request.partition)
            .bind(&request.member_id)
            .bind(offsets)
            .execute(&mut **transaction)
            .await?
        }
        ShareAcknowledgementType::Accept | ShareAcknowledgementType::Reject => {
            sqlx::query(
                "UPDATE share_record_states
                 SET record_state = 'archived', delivery_state = $6,
                     member_id = NULL, lock_expires_at = NULL
                 WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
                   AND record_offset = ANY($5::bigint[])
                   AND (record_state = 'archived'
                        OR (record_state = 'acquired' AND member_id = $4
                            AND lock_expires_at > now()))",
            )
            .bind(&request.group_id)
            .bind(request.topic_id)
            .bind(request.partition)
            .bind(&request.member_id)
            .bind(offsets)
            .bind(if action == ShareAcknowledgementType::Accept {
                2i16
            } else {
                4i16
            })
            .execute(&mut **transaction)
            .await?
        }
        ShareAcknowledgementType::Release => {
            sqlx::query(
                "UPDATE share_record_states
                 SET record_state = CASE
                         WHEN delivery_count >= $6 THEN 'archived'
                         ELSE 'available'
                     END,
                     delivery_state = CASE
                         WHEN delivery_count >= $6 THEN 4
                         ELSE 0
                     END,
                     member_id = NULL,
                     lock_expires_at = NULL
                 WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
                   AND record_offset = ANY($5::bigint[])
                   AND (record_state = 'available'
                        OR (record_state = 'acquired' AND member_id = $4
                            AND lock_expires_at > now()))",
            )
            .bind(&request.group_id)
            .bind(request.topic_id)
            .bind(request.partition)
            .bind(&request.member_id)
            .bind(offsets)
            .bind(request.delivery_count_limit)
            .execute(&mut **transaction)
            .await?
        }
        ShareAcknowledgementType::Renew => {
            sqlx::query(
                "UPDATE share_record_states
                 SET lock_expires_at = now() + $6 * interval '1 millisecond'
                 WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
                   AND record_offset = ANY($5::bigint[])
                   AND record_state = 'acquired' AND member_id = $4
                   AND lock_expires_at > now()",
            )
            .bind(&request.group_id)
            .bind(request.topic_id)
            .bind(request.partition)
            .bind(&request.member_id)
            .bind(offsets)
            .bind(request.lock_duration_ms)
            .execute(&mut **transaction)
            .await?
        }
    };
    Ok(result.rows_affected())
}

async fn find_invalid_offset(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ShareAcknowledgeRecords,
    action: ShareAcknowledgementType,
    offsets: &[i64],
) -> Result<i64, ControlError> {
    let rows = sqlx::query(
        "SELECT record_offset, record_state, member_id, lock_expires_at > now() AS locked
         FROM share_record_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_offset = ANY($4::bigint[])",
    )
    .bind(&request.group_id)
    .bind(request.topic_id)
    .bind(request.partition)
    .bind(offsets)
    .fetch_all(&mut **transaction)
    .await?;
    let valid = rows
        .into_iter()
        .filter_map(|row| {
            let state: String = row.get("record_state");
            let owner: Option<String> = row.get("member_id");
            let locked: Option<bool> = row.get("locked");
            let owned = state == "acquired"
                && owner.as_deref() == Some(request.member_id.as_str())
                && locked == Some(true);
            let valid = match action {
                ShareAcknowledgementType::Gap => state == "archived" || owned,
                ShareAcknowledgementType::Accept | ShareAcknowledgementType::Reject => {
                    state == "archived" || owned
                }
                ShareAcknowledgementType::Release => state == "available" || owned,
                ShareAcknowledgementType::Renew => owned,
            };
            valid.then(|| row.get::<i64, _>("record_offset"))
        })
        .collect::<HashSet<_>>();
    Ok(offsets
        .iter()
        .copied()
        .find(|offset| !valid.contains(offset))
        .unwrap_or(offsets[0]))
}

async fn assigned_partitions(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
) -> Result<HashSet<ShareSessionPartition>, ControlError> {
    let member_exists = sqlx::query(
        "SELECT EXISTS (
             SELECT 1 FROM share_group_members WHERE group_id = $1 AND member_id = $2
         ) AS member_exists,
         EXISTS (SELECT 1 FROM share_groups WHERE group_id = $1) AS group_exists",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_one(&mut **transaction)
    .await?;
    if !member_exists.get::<bool, _>("group_exists") {
        return Err(ControlError::GroupNotFound(group_id.to_owned()));
    }
    if !member_exists.get::<bool, _>("member_exists") {
        return Err(ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: member_id.to_owned(),
        });
    }
    let rows = sqlx::query(
        "SELECT topic_id, unnest(partitions) AS partition_index
         FROM share_group_assignments
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| ShareSessionPartition {
            topic_id: row.get("topic_id"),
            partition: row.get("partition_index"),
        })
        .collect())
}

async fn ensure_assigned(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    partition: &ShareSessionPartition,
) -> Result<(), ControlError> {
    if !assigned_partitions(transaction, group_id, member_id)
        .await?
        .contains(partition)
    {
        return Err(ControlError::InvalidRequest(
            "share partition is not assigned to the member".to_owned(),
        ));
    }
    Ok(())
}

async fn ensure_partition_state(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
) -> Result<(), ControlError> {
    if sqlx::query(
        "SELECT 1 FROM share_partition_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .fetch_optional(&mut **transaction)
    .await?
    .is_none()
    {
        return Err(ControlError::InvalidRequest(
            "share partition state must be initialized before record operations".to_owned(),
        ));
    }
    Ok(())
}

async fn session_partitions(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
) -> Result<Vec<ShareSessionPartition>, ControlError> {
    Ok(sqlx::query(
        "SELECT topic_id, partition_index
         FROM share_fetch_session_partitions
         WHERE group_id = $1 AND member_id = $2
         ORDER BY topic_id, partition_index",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| ShareSessionPartition {
        topic_id: row.get("topic_id"),
        partition: row.get("partition_index"),
    })
    .collect())
}

async fn increment_state_epoch(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE share_partition_states
         SET state_epoch = state_epoch + 1
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn advance_start_offset(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
) -> Result<(), ControlError> {
    let state = sqlx::query(
        "SELECT start_offset, state_batches FROM share_partition_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .fetch_one(&mut **transaction)
    .await?;
    let current = state.get::<i64, _>("start_offset");
    let persisted = serde_json::from_value::<Vec<ShareStateBatch>>(state.get("state_batches"))
        .map_err(|error| ControlError::Database(sqlx::Error::Decode(Box::new(error))))?;
    let records = sqlx::query(
        "SELECT record_offset, delivery_state, delivery_count
         FROM share_record_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
           AND record_offset >= $4
         ORDER BY record_offset",
    )
    .bind(group_id)
    .bind(topic_id)
    .bind(partition)
    .bind(current)
    .fetch_all(&mut **transaction)
    .await?;
    let overrides = records
        .into_iter()
        .map(|row| {
            let offset = row.get("record_offset");
            ShareStateBatch {
                first_offset: offset,
                last_offset: offset,
                delivery_state: row.get::<i16, _>("delivery_state") as i8,
                delivery_count: row.get("delivery_count"),
            }
        })
        .collect::<Vec<_>>();
    let effective = normalize_state_batches(&persisted, &overrides, current)?;
    let mut next = current;
    for batch in effective {
        if batch.first_offset > next {
            break;
        }
        if batch.last_offset < next {
            continue;
        }
        if !matches!(
            batch.delivery_state,
            ACKNOWLEDGED_DELIVERY_STATE | ARCHIVED_DELIVERY_STATE
        ) {
            break;
        }
        let candidate = batch.last_offset.saturating_add(1);
        if candidate == next {
            break;
        }
        next = candidate;
    }
    if next != current {
        sqlx::query(
            "UPDATE share_partition_states
             SET start_offset = $4, delivery_complete_count = 0
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
        )
        .bind(group_id)
        .bind(topic_id)
        .bind(partition)
        .bind(next)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn lock_member(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
) -> Result<(), ControlError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 4))")
        .bind(format!("{group_id}\u{1f}{member_id}"))
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

pub(crate) async fn lock_partition(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    topic_id: Uuid,
    partition: i32,
) -> Result<(), ControlError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 5))")
        .bind(format!("{group_id}\u{1f}{topic_id}\u{1f}{partition}"))
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn next_epoch(epoch: i32) -> i32 {
    if epoch == i32::MAX { 1 } else { epoch + 1 }
}
