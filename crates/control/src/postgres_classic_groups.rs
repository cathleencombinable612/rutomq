use crate::groups;
use crate::postgres_classic_group_store::{
    apply_selected_protocol, clear_assignments, delete_expired_pending_members, expire_members,
    load_members, rebalance_after_removal, reject_consumer_protocol_group, select_leader,
    validate_join,
};
use crate::{ControlError, JoinGroupResult};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub(crate) use crate::postgres_classic_group_members::{heartbeat, leave, sync, validate_member};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join(
    pool: &PgPool,
    group_id: &str,
    requested_member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
    client: (&str, &str, &[String], i32),
    api_version: i16,
) -> Result<JoinGroupResult, ControlError> {
    let (client_id, client_host, subscribed_topics, session_timeout_ms) = client;
    validate_join(group_id, protocol_type, protocols, session_timeout_ms)?;

    let mut transaction = pool.begin().await?;
    reject_consumer_protocol_group(&mut transaction, group_id).await?;
    delete_expired_pending_members(&mut transaction, group_id).await?;

    let group = sqlx::query(
        "SELECT generation_id, protocol_type, protocol_name, leader_id
         FROM consumer_groups WHERE group_id = $1 FOR UPDATE",
    )
    .bind(group_id)
    .fetch_optional(&mut *transaction)
    .await?;
    let group_exists = group.is_some();
    let current_generation = group
        .as_ref()
        .map_or(0, |row| row.get::<i32, _>("generation_id"));
    let current_protocol_type = group
        .as_ref()
        .map(|row| row.get::<String, _>("protocol_type"));
    let current_protocol_name = group
        .as_ref()
        .and_then(|row| row.get::<Option<String>, _>("protocol_name"));
    let mut current_leader = group
        .as_ref()
        .and_then(|row| row.get::<Option<String>, _>("leader_id"));
    let expired = if group_exists {
        expire_members(&mut transaction, group_id).await?
    } else {
        0
    };
    let members = load_members(&mut transaction, group_id).await?;
    if !members.is_empty() && current_protocol_type.as_deref() != Some(protocol_type) {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }

    let incoming_protocols = groups::protocols(protocols);
    let mut initial_sets = members
        .iter()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    initial_sets.push(incoming_protocols.as_slice());
    if groups::select_protocol(&initial_sets).is_none() {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }

    if group_instance_id.is_none() && requested_member_id.is_empty() && api_version >= 4 {
        if expired > 0 {
            rebalance_after_removal(
                &mut transaction,
                group_id,
                current_generation,
                current_leader.as_deref(),
            )
            .await?;
        }
        let member_id = format!("{client_id}-{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO classic_group_pending_members (group_id, member_id, expires_at)
             VALUES ($1, $2, now() + $3 * interval '1 millisecond')",
        )
        .bind(group_id)
        .bind(&member_id)
        .bind(session_timeout_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        return Err(ControlError::MemberIdRequired { member_id });
    }

    let mapped_static_member = group_instance_id.and_then(|instance_id| {
        members
            .iter()
            .find(|member| member.group_instance_id.as_deref() == Some(instance_id))
            .map(|member| member.member_id.clone())
    });
    let existing_requested_member = !requested_member_id.is_empty()
        && members
            .iter()
            .any(|member| member.member_id == requested_member_id);
    let (member_id, replaced_member_id) = match (
        group_instance_id,
        requested_member_id.is_empty(),
        mapped_static_member,
    ) {
        (Some(_), true, existing) => (format!("{client_id}-{}", Uuid::new_v4()), existing),
        (Some(instance_id), false, Some(existing)) if existing != requested_member_id => {
            return Err(ControlError::FencedInstanceId {
                group: group_id.to_owned(),
                instance_id: instance_id.to_owned(),
            });
        }
        (Some(_), false, Some(existing)) => (existing, None),
        (Some(_), false, None) => {
            return Err(ControlError::GroupMemberNotFound {
                group: group_id.to_owned(),
                member: requested_member_id.to_owned(),
            });
        }
        (None, true, _) => (format!("{client_id}-{}", Uuid::new_v4()), None),
        (None, false, _) if existing_requested_member => (requested_member_id.to_owned(), None),
        (None, false, _) => {
            let pending = sqlx::query(
                "DELETE FROM classic_group_pending_members
                 WHERE group_id = $1 AND member_id = $2 AND expires_at > now()
                 RETURNING member_id",
            )
            .bind(group_id)
            .bind(requested_member_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if pending.is_none() {
                return Err(ControlError::GroupMemberNotFound {
                    group: group_id.to_owned(),
                    member: requested_member_id.to_owned(),
                });
            }
            (requested_member_id.to_owned(), None)
        }
    };

    if !group_exists {
        sqlx::query(
            "INSERT INTO consumer_groups
             (group_id, generation_id, protocol_type, protocol_name, leader_id)
             VALUES ($1, 0, $2, NULL, NULL)",
        )
        .bind(group_id)
        .bind(protocol_type)
        .execute(&mut *transaction)
        .await?;
    }

    let previous_member = replaced_member_id
        .as_deref()
        .or(existing_requested_member.then_some(member_id.as_str()))
        .and_then(|member_id| members.iter().find(|member| member.member_id == member_id))
        .cloned();
    let mut protocol_sets = members
        .iter()
        .filter(|member| {
            replaced_member_id.as_deref() != Some(member.member_id.as_str())
                && member.member_id != member_id
        })
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    protocol_sets.push(incoming_protocols.as_slice());
    let protocol_name = groups::select_protocol(&protocol_sets)
        .ok_or_else(|| ControlError::InconsistentGroupProtocol(group_id.to_owned()))?;
    let metadata = incoming_protocols
        .iter()
        .find(|protocol| protocol.name == protocol_name)
        .map(|protocol| protocol.metadata.clone())
        .expect("selected protocol is offered by the joining member");
    let protocol_names = incoming_protocols
        .iter()
        .map(|protocol| protocol.name.clone())
        .collect::<Vec<_>>();
    let protocol_metadata_set = incoming_protocols
        .iter()
        .map(|protocol| protocol.metadata.clone())
        .collect::<Vec<_>>();
    let identity_replaced = replaced_member_id.is_some();
    let new_member = previous_member.is_none();
    let member_changed = previous_member.as_ref().is_some_and(|member| {
        !member
            .protocols
            .iter()
            .map(|protocol| &protocol.name)
            .eq(incoming_protocols.iter().map(|protocol| &protocol.name))
            || member.subscribed_topics != subscribed_topics
    });
    let protocol_changed = current_protocol_name
        .as_deref()
        .is_some_and(|current| current != protocol_name);
    let old_leader_for_response = replaced_member_id
        .as_deref()
        .filter(|old_member_id| current_leader.as_deref() == Some(*old_member_id))
        .map(str::to_owned);

    if let Some(replaced_member_id) = &replaced_member_id {
        sqlx::query(
            "UPDATE consumer_group_members SET member_id = $3
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(group_id)
        .bind(replaced_member_id)
        .bind(&member_id)
        .execute(&mut *transaction)
        .await?;
        if current_leader.as_deref() == Some(replaced_member_id) {
            current_leader = Some(member_id.clone());
        }
    }

    sqlx::query(
        "INSERT INTO consumer_group_members
         (group_id, member_id, group_instance_id, protocol_name, protocol_metadata,
          protocol_names, protocol_metadata_set, subscribed_topics, client_id, client_host,
          rebalance_timeout_ms, session_timeout_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (group_id, member_id)
         DO UPDATE SET group_instance_id = EXCLUDED.group_instance_id,
                       protocol_name = EXCLUDED.protocol_name,
                       protocol_metadata = EXCLUDED.protocol_metadata,
                       protocol_names = EXCLUDED.protocol_names,
                       protocol_metadata_set = EXCLUDED.protocol_metadata_set,
                       subscribed_topics = EXCLUDED.subscribed_topics,
                       client_id = EXCLUDED.client_id,
                       client_host = EXCLUDED.client_host,
                       rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
                       session_timeout_ms = EXCLUDED.session_timeout_ms,
                       last_heartbeat = now()",
    )
    .bind(group_id)
    .bind(&member_id)
    .bind(group_instance_id)
    .bind(&protocol_name)
    .bind(metadata)
    .bind(&protocol_names)
    .bind(&protocol_metadata_set)
    .bind(subscribed_topics)
    .bind(client_id)
    .bind(client_host)
    .bind(session_timeout_ms)
    .bind(session_timeout_ms)
    .execute(&mut *transaction)
    .await?;

    apply_selected_protocol(&mut transaction, group_id, &protocol_name).await?;
    let rebalance =
        expired > 0 || (new_member && !identity_replaced) || member_changed || protocol_changed;
    if rebalance {
        clear_assignments(&mut transaction, group_id).await?;
    }
    let generation_id = current_generation + i32::from(rebalance);
    let leader = select_leader(&mut transaction, group_id, current_leader.as_deref())
        .await?
        .expect("joining member makes the group non-empty");
    sqlx::query(
        "UPDATE consumer_groups
         SET generation_id = $2, protocol_type = $3, protocol_name = $4,
             leader_id = $5, empty_since_ms = NULL, updated_at = now()
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(generation_id)
    .bind(protocol_type)
    .bind(&protocol_name)
    .bind(&leader)
    .execute(&mut *transaction)
    .await?;

    let members = load_members(&mut transaction, group_id).await?;
    let response_leader = old_leader_for_response
        .filter(|_| api_version < 9 && !rebalance)
        .unwrap_or_else(|| leader.clone());
    let skip_assignment =
        api_version >= 9 && identity_replaced && !rebalance && leader == member_id;
    transaction.commit().await?;
    Ok(JoinGroupResult {
        generation_id,
        protocol_type: protocol_type.to_owned(),
        protocol_name,
        leader: response_leader,
        member_id,
        members,
        skip_assignment,
        pending_rebalance: None,
        retry_after_ms: 0,
    })
}
