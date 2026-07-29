use crate::consumer_groups::{
    self, ConsumerGroupDescription, ConsumerGroupHeartbeat, ConsumerGroupHeartbeatResult,
    ConsumerGroupState, ConsumerMemberState, ConsumerTopicAssignment,
};
use crate::{
    ControlError, GroupAssignmentCompletion, GroupAssignmentTask, GroupHeartbeatOutcome,
    OffsetCommit, PartitionKey, TopicInfo,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;

pub(crate) async fn heartbeat(
    pool: &PgPool,
    heartbeat: ConsumerGroupHeartbeat,
) -> Result<ConsumerGroupHeartbeatResult, ControlError> {
    let mut transaction = pool.begin().await?;
    reject_other_protocols(&mut transaction, &heartbeat.group_id).await?;
    let joining = heartbeat.member_epoch == 0;
    if joining {
        sqlx::query(
            "INSERT INTO consumer_protocol_groups (group_id, assignor_name)
             VALUES ($1, $2) ON CONFLICT (group_id) DO NOTHING",
        )
        .bind(&heartbeat.group_id)
        .bind(
            heartbeat
                .configured_assignors
                .first()
                .cloned()
                .unwrap_or_default(),
        )
        .execute(&mut *transaction)
        .await?;
    }
    let current = load_group(&mut transaction, &heartbeat.group_id, true).await?;
    let topics = load_topics(&mut transaction).await?;
    let (group, result) =
        consumer_groups::heartbeat(current, heartbeat, &topics, chrono::Utc::now())?;
    save_group(&mut transaction, &group).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn heartbeat_deferred(
    pool: &PgPool,
    heartbeat: ConsumerGroupHeartbeat,
) -> Result<GroupHeartbeatOutcome<ConsumerGroupHeartbeatResult>, ControlError> {
    let mut transaction = pool.begin().await?;
    reject_other_protocols(&mut transaction, &heartbeat.group_id).await?;
    if heartbeat.member_epoch == 0 {
        sqlx::query(
            "INSERT INTO consumer_protocol_groups (group_id, assignor_name)
             VALUES ($1, $2) ON CONFLICT (group_id) DO NOTHING",
        )
        .bind(&heartbeat.group_id)
        .bind(
            heartbeat
                .configured_assignors
                .first()
                .cloned()
                .unwrap_or_default(),
        )
        .execute(&mut *transaction)
        .await?;
    }
    let current = load_group(&mut transaction, &heartbeat.group_id, true).await?;
    let topics = load_topics(&mut transaction).await?;
    let (group, result, assignment_task) =
        consumer_groups::heartbeat_deferred(current, heartbeat, &topics, chrono::Utc::now())?;
    save_group(&mut transaction, &group).await?;
    transaction.commit().await?;
    Ok(GroupHeartbeatOutcome {
        result,
        assignment_task,
    })
}

pub(crate) async fn complete_assignment(
    pool: &PgPool,
    task: &GroupAssignmentTask,
) -> Result<GroupAssignmentCompletion, ControlError> {
    let mut transaction = pool.begin().await?;
    super::postgres_streams_groups::lock_group(&mut transaction, &task.group_id).await?;
    let Some(mut group) = load_group(&mut transaction, &task.group_id, true).await? else {
        transaction.commit().await?;
        return Ok(GroupAssignmentCompletion::GroupNotFound);
    };
    let topics = load_topics(&mut transaction).await?;
    let completion =
        consumer_groups::complete_assignment(&mut group, &topics, task, chrono::Utc::now())?;
    if !matches!(completion, GroupAssignmentCompletion::Stale) {
        save_group(&mut transaction, &group).await?;
    }
    transaction.commit().await?;
    Ok(completion)
}

pub(crate) async fn describe(
    pool: &PgPool,
    group_ids: &[String],
) -> Result<HashMap<String, ConsumerGroupDescription>, ControlError> {
    let mut transaction = pool.begin().await?;
    let mut descriptions = HashMap::new();
    for group_id in group_ids {
        if let Some(group) = load_group(&mut transaction, group_id, false).await? {
            descriptions.insert(group_id.clone(), consumer_groups::describe(&group));
        }
    }
    transaction.commit().await?;
    Ok(descriptions)
}

pub(crate) async fn validate_member(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
) -> Result<bool, ControlError> {
    let group_exists = sqlx::query("SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1")
        .bind(group_id)
        .fetch_optional(pool)
        .await?
        .is_some();
    if !group_exists {
        return Ok(false);
    }
    let stored = sqlx::query(
        "SELECT member_epoch FROM consumer_protocol_members
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ControlError::GroupMemberNotFound {
        group: group_id.to_owned(),
        member: member_id.to_owned(),
    })?
    .get::<i32, _>("member_epoch");
    if stored != member_epoch {
        return Err(ControlError::FencedMemberEpoch {
            group: group_id.to_owned(),
            member: member_id.to_owned(),
            expected: stored,
            actual: member_epoch,
        });
    }
    Ok(true)
}

pub(crate) async fn commit_offsets(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
    offsets: Vec<OffsetCommit>,
) -> Result<Option<Vec<bool>>, ControlError> {
    let mut transaction = pool.begin().await?;
    let partitions = offsets
        .iter()
        .map(|offset| offset.partition.clone())
        .collect::<Vec<_>>();
    let Some(validity) = validate_offset_commit_in_transaction(
        &mut transaction,
        group_id,
        member_id,
        member_epoch,
        api_version,
        &partitions,
    )
    .await?
    else {
        return Ok(None);
    };
    let accepted = offsets
        .into_iter()
        .zip(&validity)
        .filter_map(|(offset, accepted)| accepted.then_some(offset))
        .collect();
    crate::postgres_offsets::commit_in_transaction(&mut transaction, group_id, accepted).await?;
    transaction.commit().await?;
    Ok(Some(validity))
}

pub(crate) async fn validate_offset_commit_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
    partitions: &[PartitionKey],
) -> Result<Option<Vec<bool>>, ControlError> {
    let Some(group) = load_group(transaction, group_id, true).await? else {
        return Ok(None);
    };
    if api_version < 9 {
        return Err(ControlError::UnsupportedVersion(
            "consumer protocol members require OffsetCommit version 9 or newer".to_owned(),
        ));
    }
    Ok(Some(consumer_groups::validate_offset_commit(
        &group,
        member_id,
        member_epoch,
        partitions,
    )?))
}

pub(crate) async fn validate_transaction_offset_commit_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    member_epoch: i32,
    partitions: &[PartitionKey],
) -> Result<Option<()>, ControlError> {
    let Some(group) = load_group(transaction, group_id, true).await? else {
        return Ok(None);
    };
    consumer_groups::validate_transaction_offset_commit(
        &group,
        member_id,
        group_instance_id,
        member_epoch,
        partitions,
    )?;
    Ok(Some(()))
}

async fn reject_other_protocols(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    super::postgres_streams_groups::lock_group(transaction, group_id).await?;
    let exists = sqlx::query(
        "SELECT
             EXISTS (
                 SELECT 1 FROM streams_protocol_groups WHERE group_id = $1
             )
             OR EXISTS (SELECT 1 FROM share_groups WHERE group_id = $1)
             AS conflict",
    )
    .bind(group_id)
    .fetch_one(&mut **transaction)
    .await?
    .get::<bool, _>("conflict");
    if exists {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }
    if super::postgres_classic_group_store::classic_group_blocks_consumer_conversion(
        transaction,
        group_id,
    )
    .await?
    {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }
    Ok(())
}

pub(crate) async fn consumer_group_blocks_classic_conversion(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<bool, ControlError> {
    let Some(mut group) = load_group(transaction, group_id, true).await? else {
        return Ok(false);
    };
    consumer_groups::expire_members(&mut group, chrono::Utc::now());
    if !group.members.is_empty() {
        return Ok(true);
    }
    sqlx::query("DELETE FROM consumer_protocol_groups WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut **transaction)
        .await?;
    Ok(false)
}

async fn load_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    for_update: bool,
) -> Result<Option<ConsumerGroupState>, ControlError> {
    let row = if for_update {
        sqlx::query(
            "SELECT group_epoch, assignment_epoch, assignment_timestamp,
                    regex_refresh_timestamp, regex_refresh_pending,
                    assignment_interval_ms, assignor_name
             FROM consumer_protocol_groups WHERE group_id = $1 FOR UPDATE",
        )
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        sqlx::query(
            "SELECT group_epoch, assignment_epoch, assignment_timestamp,
                    regex_refresh_timestamp, regex_refresh_pending,
                    assignment_interval_ms, assignor_name
             FROM consumer_protocol_groups WHERE group_id = $1",
        )
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
    };
    let Some(row) = row else {
        return Ok(None);
    };

    let mut members = sqlx::query(
        "SELECT member_id, instance_id, rack_id, member_epoch,
                previous_member_epoch,
                rebalance_timeout_ms, session_timeout_ms,
                subscribed_topic_names, subscribed_topic_regex,
                client_id, client_host, last_heartbeat
         FROM consumer_protocol_members
         WHERE group_id = $1 ORDER BY member_id",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        let member_id: String = row.get("member_id");
        (
            member_id.clone(),
            ConsumerMemberState {
                member_id,
                instance_id: row.get("instance_id"),
                rack_id: row.get("rack_id"),
                member_epoch: row.get("member_epoch"),
                previous_member_epoch: row.get("previous_member_epoch"),
                rebalance_timeout_ms: row.get("rebalance_timeout_ms"),
                session_timeout_ms: row.get("session_timeout_ms"),
                subscribed_topic_names: row.get("subscribed_topic_names"),
                subscribed_topic_regex: row.get("subscribed_topic_regex"),
                client_id: row.get("client_id"),
                client_host: row.get("client_host"),
                current_assignment: Vec::new(),
                target_assignment: Vec::new(),
                owned_assignment: Vec::new(),
                assignment_epochs: HashMap::new(),
                last_heartbeat: row.get("last_heartbeat"),
            },
        )
    })
    .collect::<HashMap<_, _>>();

    for assignment in sqlx::query(
        "SELECT a.member_id, a.assignment_kind, a.topic_id, t.name AS topic_name,
                a.partitions
         FROM consumer_protocol_assignments a
         JOIN topics t ON t.id = a.topic_id
         WHERE a.group_id = $1
         ORDER BY a.member_id, a.assignment_kind, t.name",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    {
        let member_id: String = assignment.get("member_id");
        let Some(member) = members.get_mut(&member_id) else {
            continue;
        };
        let value = ConsumerTopicAssignment {
            topic_id: assignment.get("topic_id"),
            topic_name: assignment.get("topic_name"),
            partitions: assignment.get("partitions"),
        };
        match assignment.get::<String, _>("assignment_kind").as_str() {
            "current" => member.current_assignment.push(value),
            "target" => member.target_assignment.push(value),
            "owned" => member.owned_assignment.push(value),
            _ => {
                return Err(ControlError::InvalidRequest(
                    "invalid stored consumer assignment kind".to_owned(),
                ));
            }
        }
    }

    for assignment in sqlx::query(
        "SELECT member_id, topic_id, partition_index, assignment_epoch
         FROM consumer_protocol_assignment_epochs
         WHERE group_id = $1
         ORDER BY member_id, topic_id, partition_index",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    {
        let member_id: String = assignment.get("member_id");
        if let Some(member) = members.get_mut(&member_id) {
            member.assignment_epochs.insert(
                (
                    assignment.get("topic_id"),
                    assignment.get("partition_index"),
                ),
                assignment.get("assignment_epoch"),
            );
        }
    }

    Ok(Some(ConsumerGroupState {
        group_id: group_id.to_owned(),
        group_epoch: row.get("group_epoch"),
        assignment_epoch: row.get("assignment_epoch"),
        assignment_timestamp: row.get("assignment_timestamp"),
        regex_refresh_timestamp: row.get("regex_refresh_timestamp"),
        regex_refresh_pending: row.get("regex_refresh_pending"),
        assignment_interval_ms: row.get("assignment_interval_ms"),
        assignor_name: row.get("assignor_name"),
        members,
    }))
}

async fn load_topics(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<TopicInfo>, ControlError> {
    Ok(
        sqlx::query("SELECT id, name, partition_count FROM topics ORDER BY name")
            .fetch_all(&mut **transaction)
            .await?
            .into_iter()
            .map(|row| TopicInfo {
                id: row.get("id"),
                name: row.get("name"),
                partitions: row.get("partition_count"),
            })
            .collect(),
    )
}

async fn save_group(
    transaction: &mut Transaction<'_, Postgres>,
    group: &ConsumerGroupState,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO consumer_protocol_groups
             (group_id, group_epoch, assignment_epoch, assignment_timestamp,
              regex_refresh_timestamp, regex_refresh_pending,
              assignment_interval_ms, assignor_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (group_id) DO UPDATE
         SET group_epoch = EXCLUDED.group_epoch,
             assignment_epoch = EXCLUDED.assignment_epoch,
             assignment_timestamp = EXCLUDED.assignment_timestamp,
             regex_refresh_timestamp = EXCLUDED.regex_refresh_timestamp,
             regex_refresh_pending = EXCLUDED.regex_refresh_pending,
             assignment_interval_ms = EXCLUDED.assignment_interval_ms,
             assignor_name = EXCLUDED.assignor_name,
             updated_at = now()",
    )
    .bind(&group.group_id)
    .bind(group.group_epoch)
    .bind(group.assignment_epoch)
    .bind(group.assignment_timestamp)
    .bind(group.regex_refresh_timestamp)
    .bind(group.regex_refresh_pending)
    .bind(group.assignment_interval_ms)
    .bind(&group.assignor_name)
    .execute(&mut **transaction)
    .await?;

    let member_ids = group.members.keys().cloned().collect::<Vec<_>>();
    sqlx::query(
        "DELETE FROM consumer_protocol_members
         WHERE group_id = $1 AND NOT (member_id = ANY($2::text[]))",
    )
    .bind(&group.group_id)
    .bind(&member_ids)
    .execute(&mut **transaction)
    .await?;

    let mut members = group.members.values().collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    for member in members {
        sqlx::query(
            "INSERT INTO consumer_protocol_members
                 (group_id, member_id, instance_id, rack_id, member_epoch,
                  rebalance_timeout_ms, session_timeout_ms, subscribed_topic_names,
                  subscribed_topic_regex, client_id, client_host, last_heartbeat,
                  previous_member_epoch)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (group_id, member_id) DO UPDATE
             SET instance_id = EXCLUDED.instance_id,
                 rack_id = EXCLUDED.rack_id,
                 member_epoch = EXCLUDED.member_epoch,
                 rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
                 session_timeout_ms = EXCLUDED.session_timeout_ms,
                 subscribed_topic_names = EXCLUDED.subscribed_topic_names,
                 subscribed_topic_regex = EXCLUDED.subscribed_topic_regex,
                 client_id = EXCLUDED.client_id,
                 client_host = EXCLUDED.client_host,
                 last_heartbeat = EXCLUDED.last_heartbeat,
                 previous_member_epoch = EXCLUDED.previous_member_epoch",
        )
        .bind(&group.group_id)
        .bind(&member.member_id)
        .bind(&member.instance_id)
        .bind(&member.rack_id)
        .bind(member.member_epoch)
        .bind(member.rebalance_timeout_ms)
        .bind(member.session_timeout_ms)
        .bind(&member.subscribed_topic_names)
        .bind(&member.subscribed_topic_regex)
        .bind(&member.client_id)
        .bind(&member.client_host)
        .bind(member.last_heartbeat)
        .bind(member.previous_member_epoch)
        .execute(&mut **transaction)
        .await?;
    }

    sqlx::query("DELETE FROM consumer_protocol_assignments WHERE group_id = $1")
        .bind(&group.group_id)
        .execute(&mut **transaction)
        .await?;
    sqlx::query("DELETE FROM consumer_protocol_assignment_epochs WHERE group_id = $1")
        .bind(&group.group_id)
        .execute(&mut **transaction)
        .await?;
    for member in group.members.values() {
        save_assignments(
            transaction,
            &group.group_id,
            &member.member_id,
            "current",
            &member.current_assignment,
        )
        .await?;
        save_assignments(
            transaction,
            &group.group_id,
            &member.member_id,
            "target",
            &member.target_assignment,
        )
        .await?;
        save_assignments(
            transaction,
            &group.group_id,
            &member.member_id,
            "owned",
            &member.owned_assignment,
        )
        .await?;
        let mut assignment_epochs = member.assignment_epochs.iter().collect::<Vec<_>>();
        assignment_epochs.sort_by_key(|((topic_id, partition), _)| (*topic_id, *partition));
        for ((topic_id, partition), assignment_epoch) in assignment_epochs {
            sqlx::query(
                "INSERT INTO consumer_protocol_assignment_epochs
                     (group_id, member_id, topic_id, partition_index, assignment_epoch)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(&group.group_id)
            .bind(&member.member_id)
            .bind(topic_id)
            .bind(partition)
            .bind(assignment_epoch)
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(())
}

async fn save_assignments(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    kind: &str,
    assignments: &[ConsumerTopicAssignment],
) -> Result<(), ControlError> {
    for assignment in assignments {
        sqlx::query(
            "INSERT INTO consumer_protocol_assignments
                 (group_id, member_id, assignment_kind, topic_id, partitions)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(group_id)
        .bind(member_id)
        .bind(kind)
        .bind(assignment.topic_id)
        .bind(&assignment.partitions)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}
