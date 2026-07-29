use crate::groups;
use crate::{
    ControlError, GroupMember, JoinGroupResult, MemoryMetadataStore,
    empty_consumer_group_for_classic,
};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

const RETRY_AFTER_MS: i64 = 25;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn begin_memory(
    store: &MemoryMetadataStore,
    group_id: &str,
    requested_member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
    client: (&str, &str, &[String], i32),
    rebalance_timeout_ms: i32,
    initial_rebalance_delay_ms: i32,
    max_size: i32,
    api_version: i16,
) -> Result<JoinGroupResult, ControlError> {
    let (client_id, client_host, subscribed_topics, session_timeout_ms) = client;
    validate(
        group_id,
        protocol_type,
        protocols,
        session_timeout_ms,
        rebalance_timeout_ms,
        initial_rebalance_delay_ms,
        max_size,
    )?;

    let mut state = store.state.write().await;
    if state.share_groups.contains_key(group_id) || state.streams_groups.contains_key(group_id) {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }

    let now = Utc::now();
    let replace_consumer_group = empty_consumer_group_for_classic(&state, group_id, now)?;
    state
        .pending_group_members
        .retain(|(pending_group, _), expires_at| pending_group != group_id || *expires_at > now);
    let expired = state
        .groups
        .get_mut(group_id)
        .is_some_and(|group| group.remove_expired_members(now));

    let incoming_protocols = groups::protocols(protocols);
    if let Some(group) = state.groups.get(group_id) {
        validate_protocols(group, group_id, protocol_type, &incoming_protocols)?;
    }

    if group_instance_id.is_none() && requested_member_id.is_empty() && api_version >= 4 {
        if state
            .groups
            .get(group_id)
            .is_some_and(|group| group.members.len() >= max_size as usize)
        {
            return Err(ControlError::GroupMaxSizeReached(group_id.to_owned()));
        }
        if replace_consumer_group {
            state.consumer_groups.remove(group_id);
        }
        let member_id = format!("{client_id}-{}", Uuid::new_v4());
        state.pending_group_members.insert(
            (group_id.to_owned(), member_id.clone()),
            now + Duration::milliseconds(i64::from(session_timeout_ms)),
        );
        return Err(ControlError::MemberIdRequired { member_id });
    }

    let mapped_static_member = group_instance_id.and_then(|instance_id| {
        state
            .groups
            .get(group_id)
            .and_then(|group| group.member_id_for_instance(instance_id))
    });
    let existing_requested_member = !requested_member_id.is_empty()
        && state
            .groups
            .get(group_id)
            .is_some_and(|group| group.members.contains_key(requested_member_id));
    let (member_id, replaced_member_id) = resolve_member_id(
        &mut state,
        group_id,
        requested_member_id,
        group_instance_id,
        mapped_static_member,
        existing_requested_member,
        client_id,
    )?;
    let new_member = !existing_requested_member && replaced_member_id.is_none();
    if new_member
        && state
            .groups
            .get(group_id)
            .is_some_and(|group| group.members.len() >= max_size as usize)
    {
        return Err(ControlError::GroupMaxSizeReached(group_id.to_owned()));
    }
    if group_instance_id.is_none() && !requested_member_id.is_empty() && !existing_requested_member
    {
        state
            .pending_group_members
            .remove(&(group_id.to_owned(), member_id.clone()));
    }

    if replace_consumer_group {
        state.consumer_groups.remove(group_id);
    }
    let group = state.groups.entry(group_id.to_owned()).or_default();
    let was_empty = group.members.is_empty();
    let was_stable =
        !group.rebalance_pending && !was_empty && group.assignments.len() == group.members.len();
    let was_completing = !group.rebalance_pending && !was_empty && !was_stable;
    let previous_member = replaced_member_id
        .as_deref()
        .or(existing_requested_member.then_some(member_id.as_str()))
        .and_then(|id| group.members.get(id))
        .cloned();
    let protocol_name = select_protocol(
        group,
        group_id,
        &member_id,
        replaced_member_id.as_deref(),
        &incoming_protocols,
    )?;
    let protocol_changed = !group.protocol_name.is_empty() && group.protocol_name != protocol_name;
    let member_changed = previous_member
        .as_ref()
        .is_some_and(|member| member.protocols != incoming_protocols);
    let identity_replaced = replaced_member_id.is_some();
    let old_leader = identity_replaced
        .then(|| group.leader.clone())
        .filter(|leader| replaced_member_id.as_deref() == Some(leader.as_str()));

    if let Some(replaced_member_id) = &replaced_member_id {
        group.members.remove(replaced_member_id);
        if let Some(assignment) = group.assignments.remove(replaced_member_id) {
            group.assignments.insert(member_id.clone(), assignment);
        }
        if group.leader == *replaced_member_id {
            group.leader.clone_from(&member_id);
        }
    }

    let no_rebalance = !expired
        && ((identity_replaced && was_stable && !protocol_changed)
            || (!identity_replaced
                && previous_member.is_some()
                && was_completing
                && !member_changed
                && !protocol_changed)
            || (!identity_replaced
                && previous_member.is_some()
                && was_stable
                && group.leader != member_id
                && !member_changed
                && !protocol_changed));
    let active_rebalance_id = if group.rebalance_pending {
        group.rebalance_id
    } else if no_rebalance {
        None
    } else {
        Some(Uuid::new_v4())
    };
    let metadata = incoming_protocols
        .iter()
        .find(|protocol| protocol.name == protocol_name)
        .map(|protocol| protocol.metadata.clone())
        .expect("selected protocol is offered by the joining member");
    group.protocol_type = protocol_type.to_owned();
    group.members.insert(
        member_id.clone(),
        GroupMember {
            member_id: member_id.clone(),
            group_instance_id: group_instance_id.map(str::to_owned),
            protocols: incoming_protocols,
            protocol_name: protocol_name.clone(),
            metadata,
            subscribed_topics: subscribed_topics.to_vec(),
            client_id: client_id.to_owned(),
            client_host: client_host.to_owned(),
            rebalance_timeout_ms,
            session_timeout_ms,
            last_heartbeat: now,
            joined_rebalance_id: active_rebalance_id.or_else(|| {
                previous_member
                    .as_ref()
                    .and_then(|member| member.joined_rebalance_id)
            }),
        },
    );
    group.apply_protocol(&protocol_name);

    if no_rebalance {
        if group.leader.is_empty() {
            group.leader.clone_from(&member_id);
        }
        let skip_assignment = api_version >= 9 && identity_replaced && group.leader == member_id;
        return Ok(result(
            group,
            member_id,
            old_leader
                .filter(|_| api_version < 9)
                .unwrap_or_else(|| group.leader.clone()),
            skip_assignment,
        ));
    }

    if !group.rebalance_pending {
        start_rebalance(
            group,
            active_rebalance_id.expect("new rebalance has an id"),
            now,
            was_empty,
            initial_rebalance_delay_ms,
        );
    } else if group.initial_rebalance_deadline.is_some() {
        extend_rebalance_deadline(group);
        extend_initial_delay(group, now, initial_rebalance_delay_ms);
    } else {
        extend_rebalance_deadline(group);
    }
    finish_if_ready(group, now);
    Ok(result(group, member_id, group.leader.clone(), false))
}

pub(crate) async fn poll_memory(
    store: &MemoryMetadataStore,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    rebalance_id: Uuid,
    _api_version: i16,
) -> Result<JoinGroupResult, ControlError> {
    let mut state = store.state.write().await;
    let group = state
        .groups
        .get_mut(group_id)
        .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
    group.validate_member_identity(group_id, member_id, group_instance_id)?;
    if group.rebalance_id != Some(rebalance_id) {
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    let now = Utc::now();
    group.remove_expired_members(now);
    finish_if_ready(group, now);
    Ok(result(
        group,
        member_id.to_owned(),
        group.leader.clone(),
        false,
    ))
}

pub(super) fn finish_memory_after_membership_change(group: &mut crate::MemoryGroup) {
    finish_if_ready(group, Utc::now());
}

fn validate(
    group_id: &str,
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    initial_rebalance_delay_ms: i32,
    max_size: i32,
) -> Result<(), ControlError> {
    if group_id.is_empty()
        || protocol_type.is_empty()
        || protocols.is_empty()
        || session_timeout_ms <= 0
        || rebalance_timeout_ms <= 0
        || initial_rebalance_delay_ms < 0
        || max_size <= 0
    {
        return Err(ControlError::InvalidRequest(
            "group, protocols, positive timeouts and maximum size, and a non-negative initial delay are required"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_protocols(
    group: &crate::MemoryGroup,
    group_id: &str,
    protocol_type: &str,
    incoming: &[crate::GroupProtocol],
) -> Result<(), ControlError> {
    if !group.protocol_type.is_empty() && group.protocol_type != protocol_type {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }
    let mut sets = group
        .members
        .values()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    sets.push(incoming);
    if groups::select_protocol(&sets).is_none() {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_member_id(
    state: &mut crate::MemoryState,
    group_id: &str,
    requested_member_id: &str,
    group_instance_id: Option<&str>,
    mapped_static_member: Option<String>,
    existing_requested_member: bool,
    client_id: &str,
) -> Result<(String, Option<String>), ControlError> {
    match (
        group_instance_id,
        requested_member_id.is_empty(),
        mapped_static_member,
    ) {
        (Some(_), true, existing) => Ok((format!("{client_id}-{}", Uuid::new_v4()), existing)),
        (Some(instance_id), false, Some(existing)) if existing != requested_member_id => {
            Err(ControlError::FencedInstanceId {
                group: group_id.to_owned(),
                instance_id: instance_id.to_owned(),
            })
        }
        (Some(_), false, Some(existing)) => Ok((existing, None)),
        (Some(_), false, None) => Err(ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: requested_member_id.to_owned(),
        }),
        (None, true, _) => Ok((format!("{client_id}-{}", Uuid::new_v4()), None)),
        (None, false, _) if existing_requested_member => Ok((requested_member_id.to_owned(), None)),
        (None, false, _) => {
            let pending = state
                .pending_group_members
                .contains_key(&(group_id.to_owned(), requested_member_id.to_owned()));
            if !pending {
                return Err(ControlError::GroupMemberNotFound {
                    group: group_id.to_owned(),
                    member: requested_member_id.to_owned(),
                });
            }
            Ok((requested_member_id.to_owned(), None))
        }
    }
}

fn select_protocol(
    group: &crate::MemoryGroup,
    group_id: &str,
    member_id: &str,
    replaced_member_id: Option<&str>,
    incoming: &[crate::GroupProtocol],
) -> Result<String, ControlError> {
    let mut sets = group
        .members
        .iter()
        .filter(|(existing_id, _)| {
            replaced_member_id != Some(existing_id.as_str()) && existing_id.as_str() != member_id
        })
        .map(|(_, member)| member.protocols.as_slice())
        .collect::<Vec<_>>();
    sets.push(incoming);
    groups::select_protocol(&sets)
        .ok_or_else(|| ControlError::InconsistentGroupProtocol(group_id.to_owned()))
}

fn start_rebalance(
    group: &mut crate::MemoryGroup,
    rebalance_id: Uuid,
    now: DateTime<Utc>,
    initial: bool,
    initial_delay_ms: i32,
) {
    let timeout_ms = group
        .members
        .values()
        .map(|member| member.rebalance_timeout_ms)
        .max()
        .unwrap_or(1);
    group.rebalance_id = Some(rebalance_id);
    group.rebalance_pending = true;
    group.rebalance_started_at = Some(now);
    group.rebalance_deadline = Some(now + Duration::milliseconds(i64::from(timeout_ms)));
    group.initial_rebalance_deadline =
        initial.then(|| now + Duration::milliseconds(i64::from(initial_delay_ms.min(timeout_ms))));
    group.assignments.clear();
    if group.leader.is_empty() {
        group.leader = group.members.keys().min().cloned().unwrap_or_default();
    }
}

fn extend_initial_delay(group: &mut crate::MemoryGroup, now: DateTime<Utc>, initial_delay_ms: i32) {
    let Some(deadline) = group.rebalance_deadline else {
        return;
    };
    group.initial_rebalance_deadline =
        Some((now + Duration::milliseconds(i64::from(initial_delay_ms))).min(deadline));
}

fn extend_rebalance_deadline(group: &mut crate::MemoryGroup) {
    let Some(started_at) = group.rebalance_started_at else {
        return;
    };
    let timeout_ms = group
        .members
        .values()
        .map(|member| member.rebalance_timeout_ms)
        .max()
        .unwrap_or(1);
    let deadline = started_at + Duration::milliseconds(i64::from(timeout_ms));
    group.rebalance_deadline = Some(
        group
            .rebalance_deadline
            .map_or(deadline, |current| current.max(deadline)),
    );
}

fn finish_if_ready(group: &mut crate::MemoryGroup, now: DateTime<Utc>) {
    if !group.rebalance_pending {
        return;
    }
    let rebalance_id = group.rebalance_id.expect("pending rebalance has an id");
    let all_joined = group
        .members
        .values()
        .all(|member| member.joined_rebalance_id == Some(rebalance_id));
    let initial_elapsed = group
        .initial_rebalance_deadline
        .is_none_or(|deadline| deadline <= now);
    let timed_out = group
        .rebalance_deadline
        .is_some_and(|deadline| deadline <= now);
    if !(timed_out || (all_joined && initial_elapsed)) {
        return;
    }

    if timed_out {
        let removed = group
            .members
            .iter()
            .filter(|(_, member)| {
                member.group_instance_id.is_none()
                    && member.joined_rebalance_id != Some(rebalance_id)
            })
            .map(|(member_id, _)| member_id.clone())
            .collect::<Vec<_>>();
        for member_id in removed {
            group.members.remove(&member_id);
            group.assignments.remove(&member_id);
        }
    }

    let joined_leader = group
        .members
        .get(&group.leader)
        .is_some_and(|member| member.joined_rebalance_id == Some(rebalance_id));
    if !joined_leader {
        group.leader = group
            .members
            .values()
            .filter(|member| member.joined_rebalance_id == Some(rebalance_id))
            .map(|member| member.member_id.clone())
            .min()
            .unwrap_or_default();
    }
    let sets = group
        .members
        .values()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    if let Some(protocol_name) = groups::select_protocol(&sets) {
        group.apply_protocol(&protocol_name);
    }
    group.generation_id += 1;
    group.rebalance_pending = false;
    group.rebalance_started_at = None;
    group.rebalance_deadline = None;
    group.initial_rebalance_deadline = None;
}

fn result(
    group: &crate::MemoryGroup,
    member_id: String,
    leader: String,
    skip_assignment: bool,
) -> JoinGroupResult {
    let mut members = group.members.values().cloned().collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    JoinGroupResult {
        generation_id: group.generation_id,
        protocol_type: group.protocol_type.clone(),
        protocol_name: group.protocol_name.clone(),
        leader,
        member_id,
        members,
        skip_assignment,
        pending_rebalance: group.rebalance_pending.then_some(
            group
                .rebalance_id
                .expect("pending rebalance result has an id"),
        ),
        retry_after_ms: if group.rebalance_pending {
            RETRY_AFTER_MS
        } else {
            0
        },
    }
}
