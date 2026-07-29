use crate::groups;
use crate::{ControlError, GroupMember, GroupProtocol};
use sqlx::{Postgres, Row, Transaction};

pub(crate) fn validate_join(
    group_id: &str,
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
    session_timeout_ms: i32,
) -> Result<(), ControlError> {
    if group_id.is_empty()
        || protocol_type.is_empty()
        || protocols.is_empty()
        || session_timeout_ms <= 0
    {
        return Err(ControlError::InvalidRequest(
            "group id, protocol type, protocols, and session timeout are required".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn reject_consumer_protocol_group(
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
    if super::postgres_consumer_groups::consumer_group_blocks_classic_conversion(
        transaction,
        group_id,
    )
    .await?
    {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }
    Ok(())
}

pub(crate) async fn classic_group_blocks_consumer_conversion(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<bool, ControlError> {
    let exists = sqlx::query("SELECT 1 FROM consumer_groups WHERE group_id = $1 FOR UPDATE")
        .bind(group_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
    if exists {
        expire_members(transaction, group_id).await?;
        let has_members =
            sqlx::query("SELECT 1 FROM consumer_group_members WHERE group_id = $1 LIMIT 1")
                .bind(group_id)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if has_members {
            return Ok(true);
        }
        sqlx::query("DELETE FROM consumer_groups WHERE group_id = $1")
            .bind(group_id)
            .execute(&mut **transaction)
            .await?;
    }
    sqlx::query("DELETE FROM classic_group_pending_members WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut **transaction)
        .await?;
    Ok(false)
}

pub(crate) async fn lock_group<'a>(
    transaction: &mut Transaction<'a, Postgres>,
    group_id: &str,
) -> Result<sqlx::postgres::PgRow, ControlError> {
    super::postgres_streams_groups::lock_group(transaction, group_id).await?;
    sqlx::query(
        "SELECT generation_id, leader_id, classic_rebalance_pending
         FROM consumer_groups WHERE group_id = $1 FOR UPDATE",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))
}

pub(crate) async fn load_members(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<Vec<GroupMember>, ControlError> {
    Ok(sqlx::query(
        "SELECT member_id, group_instance_id, protocol_name, protocol_metadata,
                protocol_names, protocol_metadata_set, subscribed_topics, client_id,
                client_host, rebalance_timeout_ms, session_timeout_ms, last_heartbeat,
                classic_joined_rebalance_id
         FROM consumer_group_members WHERE group_id = $1 ORDER BY member_id",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        let names = row.get::<Vec<String>, _>("protocol_names");
        let metadata = row.get::<Vec<Vec<u8>>, _>("protocol_metadata_set");
        GroupMember {
            member_id: row.get("member_id"),
            group_instance_id: row.get("group_instance_id"),
            protocols: names
                .into_iter()
                .zip(metadata)
                .map(|(name, metadata)| GroupProtocol { name, metadata })
                .collect(),
            protocol_name: row.get("protocol_name"),
            metadata: row.get("protocol_metadata"),
            subscribed_topics: row.get("subscribed_topics"),
            client_id: row.get("client_id"),
            client_host: row.get("client_host"),
            rebalance_timeout_ms: row.get("rebalance_timeout_ms"),
            session_timeout_ms: row.get("session_timeout_ms"),
            last_heartbeat: row.get("last_heartbeat"),
            joined_rebalance_id: row.get("classic_joined_rebalance_id"),
        }
    })
    .collect())
}

pub(crate) async fn validate_identity(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<(), ControlError> {
    if let Some(group_instance_id) = group_instance_id {
        let expected = sqlx::query(
            "SELECT member_id FROM consumer_group_members
             WHERE group_id = $1 AND group_instance_id = $2",
        )
        .bind(group_id)
        .bind(group_instance_id)
        .fetch_optional(&mut **transaction)
        .await?
        .map(|row| row.get::<String, _>("member_id"))
        .ok_or_else(|| ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: member_id.to_owned(),
        })?;
        if expected != member_id {
            return Err(ControlError::FencedInstanceId {
                group: group_id.to_owned(),
                instance_id: group_instance_id.to_owned(),
            });
        }
    }
    if !member_exists(transaction, group_id, member_id).await? {
        return Err(ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: member_id.to_owned(),
        });
    }
    Ok(())
}

async fn member_exists(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
) -> Result<bool, ControlError> {
    Ok(sqlx::query(
        "SELECT 1 FROM consumer_group_members
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

pub(crate) async fn delete_expired_pending_members(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        "DELETE FROM classic_group_pending_members
         WHERE group_id = $1 AND expires_at <= now()",
    )
    .bind(group_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn expire_members(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<u64, ControlError> {
    Ok(sqlx::query(
        "DELETE FROM consumer_group_members AS member
         USING consumer_groups AS group_state
         WHERE member.group_id = $1
           AND group_state.group_id = member.group_id
           AND NOT (
               group_state.classic_rebalance_pending
               AND member.classic_joined_rebalance_id = group_state.classic_rebalance_id
           )
           AND member.last_heartbeat
               + member.session_timeout_ms * interval '1 millisecond' <= now()",
    )
    .bind(group_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected())
}

pub(crate) async fn reap_and_rebalance(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    generation_id: i32,
    current_leader: Option<&str>,
) -> Result<i32, ControlError> {
    if expire_members(transaction, group_id).await? == 0 {
        return Ok(generation_id);
    }
    rebalance_after_removal(transaction, group_id, generation_id, current_leader).await
}

pub(crate) async fn rebalance_after_removal(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    generation_id: i32,
    current_leader: Option<&str>,
) -> Result<i32, ControlError> {
    clear_assignments(transaction, group_id).await?;
    let members = load_members(transaction, group_id).await?;
    let generation_id = generation_id + 1;
    if members.is_empty() {
        sqlx::query(
            "UPDATE consumer_groups
             SET generation_id = $2, protocol_name = NULL, leader_id = NULL,
                 empty_since_ms = COALESCE(
                     empty_since_ms,
                     FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT
                 ),
                 updated_at = now()
             WHERE group_id = $1",
        )
        .bind(group_id)
        .bind(generation_id)
        .execute(&mut **transaction)
        .await?;
        return Ok(generation_id);
    }

    let protocol_sets = members
        .iter()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    let protocol_name = groups::select_protocol(&protocol_sets)
        .expect("remaining members retain at least one common protocol");
    apply_selected_protocol(transaction, group_id, &protocol_name).await?;
    let leader = select_leader(transaction, group_id, current_leader).await?;
    sqlx::query(
        "UPDATE consumer_groups
         SET generation_id = $2, protocol_name = $3, leader_id = $4,
             empty_since_ms = NULL, updated_at = now()
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(generation_id)
    .bind(protocol_name)
    .bind(leader)
    .execute(&mut **transaction)
    .await?;
    Ok(generation_id)
}

pub(crate) async fn apply_selected_protocol(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    protocol_name: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE consumer_group_members
         SET protocol_name = $2,
             protocol_metadata = protocol_metadata_set[array_position(protocol_names, $2)]
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(protocol_name)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn clear_assignments(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    sqlx::query("UPDATE consumer_group_members SET assignment = NULL WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

pub(crate) async fn select_leader(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    current_leader: Option<&str>,
) -> Result<Option<String>, ControlError> {
    Ok(sqlx::query(
        "SELECT member_id FROM consumer_group_members
         WHERE group_id = $1
         ORDER BY CASE WHEN member_id = $2 THEN 0 ELSE 1 END, member_id
         LIMIT 1",
    )
    .bind(group_id)
    .bind(current_leader)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|row| row.get("member_id")))
}
