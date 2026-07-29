use crate::member_epoch;
use crate::streams_group_assignment::{
    AssignmentDelay, defer_target_assignment, endpoint_partitions, group_state, reconcile_member,
    refresh_target_assignment,
};
pub use crate::streams_group_types::*;
use crate::streams_topology::{self, ResolvedStreamsTopology};
use crate::{ControlError, TopicInfo};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

pub(crate) fn heartbeat(
    state: Option<StreamsGroupState>,
    request: StreamsGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<(StreamsGroupState, StreamsGroupHeartbeatResult), ControlError> {
    let (group, result, _) = heartbeat_inner(state, request, topics, now, false)?;
    Ok((group, result))
}

pub(crate) fn heartbeat_deferred(
    state: Option<StreamsGroupState>,
    request: StreamsGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<
    (
        StreamsGroupState,
        StreamsGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    heartbeat_inner(state, request, topics, now, true)
}

fn heartbeat_inner(
    state: Option<StreamsGroupState>,
    request: StreamsGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        StreamsGroupState,
        StreamsGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    validate_request(&request)?;
    let joining = request.member_epoch == 0;
    let incoming_topology = request.topology.clone();
    let mut group = match state {
        Some(group) => group,
        None if joining => StreamsGroupState {
            group_id: request.group_id.clone(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignment_timestamp: None,
            assignment_interval_ms: request.assignment_interval_ms,
            endpoint_information_epoch: 0,
            topology: incoming_topology
                .clone()
                .expect("joining heartbeat topology was validated"),
            statuses: Vec::new(),
            shutdown_requested: false,
            num_standby_replicas: request.num_standby_replicas,
            initial_rebalance_deadline: (request.initial_rebalance_delay_ms > 0).then(|| {
                now + Duration::milliseconds(i64::from(request.initial_rebalance_delay_ms))
            }),
            members: HashMap::new(),
        },
        None => return Err(ControlError::GroupNotFound(request.group_id)),
    };

    group.assignment_interval_ms = request.assignment_interval_ms;
    let expired = expire_members(&mut group, now);
    if request.member_epoch == -1 {
        return leave(group, &request, topics, expired, now, defer_assignment);
    }
    if request.member_epoch == -2 {
        if request.instance_id.is_some() {
            return Err(ControlError::InvalidRequest(
                "static streams group members are not supported by Kafka 4.2".to_owned(),
            ));
        }
        return leave(group, &request, topics, expired, now, defer_assignment);
    }

    let existing = group.members.get(&request.member_id).cloned();
    if !joining {
        validate_epoch(&group, existing.as_ref(), &request)?;
    }
    validate_instance_id(&group, &request)?;

    let new_member = existing.is_none();
    if new_member && group.members.len() >= request.max_size as usize {
        return Err(ControlError::GroupMaxSizeReached(request.group_id));
    }
    let topology_changed = update_topology(&mut group, incoming_topology.as_ref(), joining);
    let stale_topology = joining
        && incoming_topology
            .as_ref()
            .is_some_and(|topology| topology != &group.topology);
    let mut member = rejoining_or_existing(existing, &request, joining);
    let metadata_changed = update_member(&mut member, &request, now);
    group.members.insert(request.member_id.clone(), member);
    if request.shutdown_application {
        group.shutdown_requested = true;
    }
    let assignment_config_changed = group.num_standby_replicas != request.num_standby_replicas;
    group.num_standby_replicas = request.num_standby_replicas;
    complete_initial_delay(&mut group, now);

    let resolved = streams_topology::resolve(&group.topology, topics)?;
    group.statuses = resolved.statuses.clone();
    if group.shutdown_requested {
        group.statuses.push(StreamsGroupStatus {
            code: STREAMS_STATUS_SHUTDOWN_APPLICATION,
            detail: "the streams application was requested to shut down".to_owned(),
        });
    }
    let assignment_changed =
        expired || new_member || metadata_changed || topology_changed || assignment_config_changed;
    let (assignment_delay, assignment_task) = if defer_assignment {
        defer_target_assignment(&mut group, assignment_changed, now)
    } else {
        (
            refresh_target_assignment(&mut group, &resolved, assignment_changed, now),
            None,
        )
    };
    let assignment = reconcile_member(&mut group, &request.member_id, stale_topology, joining);
    let endpoint_information = (request.endpoint_information_epoch
        != group.endpoint_information_epoch)
        .then(|| endpoint_partitions(&group));
    let endpoint_information_epoch = group.endpoint_information_epoch;
    let mut statuses = group.statuses.clone();
    if let Some(delay) = assignment_delay {
        statuses.push(assignment_delay_status(delay));
    }
    if stale_topology {
        statuses.push(StreamsGroupStatus {
            code: STREAMS_STATUS_STALE_TOPOLOGY,
            detail: format!(
                "member topology epoch does not match initialized epoch {}",
                group.topology.epoch
            ),
        });
    }
    let member_epoch = group
        .members
        .get(&request.member_id)
        .expect("heartbeat member exists")
        .member_epoch;
    Ok((
        group,
        StreamsGroupHeartbeatResult {
            member_id: request.member_id,
            member_epoch,
            heartbeat_interval_ms: request.heartbeat_interval_ms,
            acceptable_recovery_lag: request.acceptable_recovery_lag,
            task_offset_interval_ms: request.task_offset_interval_ms,
            statuses,
            assignment,
            endpoint_information_epoch,
            partitions_by_user_endpoint: endpoint_information,
        },
        assignment_task,
    ))
}

pub(crate) fn expire_and_describe(
    group: &mut StreamsGroupState,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<(bool, StreamsGroupDescription), ControlError> {
    let expired = expire_members(group, now);
    let initial_delay_completed = complete_initial_delay(group, now);
    let resolved = streams_topology::resolve(&group.topology, topics)?;
    group.statuses = resolved.statuses.clone();
    if group.shutdown_requested {
        group.statuses.push(StreamsGroupStatus {
            code: STREAMS_STATUS_SHUTDOWN_APPLICATION,
            detail: "the streams application was requested to shut down".to_owned(),
        });
    }
    let previous_assignment_epoch = group.assignment_epoch;
    if expired || initial_delay_completed || group.group_epoch > group.assignment_epoch {
        let _ = refresh_target_assignment(group, &resolved, expired, now);
    }
    let assignment_updated = group.assignment_epoch != previous_assignment_epoch;
    Ok((
        expired || initial_delay_completed || assignment_updated,
        describe(group, &resolved),
    ))
}

pub(crate) fn describe(
    group: &StreamsGroupState,
    resolved: &ResolvedStreamsTopology,
) -> StreamsGroupDescription {
    let mut members = group
        .members
        .values()
        .map(|member| StreamsGroupMemberDescription {
            member_id: member.member_id.clone(),
            member_epoch: member.member_epoch,
            instance_id: member.instance_id.clone(),
            rack_id: member.rack_id.clone(),
            client_id: member.client_id.clone(),
            client_host: member.client_host.clone(),
            topology_epoch: member.topology_epoch,
            process_id: member.process_id.clone(),
            user_endpoint: member.user_endpoint.clone(),
            client_tags: member.client_tags.clone(),
            task_offsets: member.task_offsets.clone(),
            task_end_offsets: member.task_end_offsets.clone(),
            assignment: member.current_assignment.clone(),
            target_assignment: member.target_assignment.clone(),
        })
        .collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    StreamsGroupDescription {
        group_id: group.group_id.clone(),
        state: group_state(group, resolved.ready()).to_owned(),
        group_epoch: group.group_epoch,
        assignment_epoch: group.assignment_epoch,
        topology: resolved.topology.clone(),
        topology_ready: resolved.ready(),
        members,
    }
}

pub(crate) fn validate_member(
    group: &StreamsGroupState,
    member_id: &str,
    member_epoch: i32,
) -> Result<(), ControlError> {
    let member = group
        .members
        .get(member_id)
        .ok_or_else(|| ControlError::GroupMemberNotFound {
            group: group.group_id.clone(),
            member: member_id.to_owned(),
        })?;
    if member.member_epoch != member_epoch {
        return Err(ControlError::FencedMemberEpoch {
            group: group.group_id.clone(),
            member: member_id.to_owned(),
            expected: member.member_epoch,
            actual: member_epoch,
        });
    }
    Ok(())
}

fn validate_request(request: &StreamsGroupHeartbeat) -> Result<(), ControlError> {
    if request.group_id.is_empty() || request.member_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "streams group and member ids must not be empty".to_owned(),
        ));
    }
    if request.member_epoch < -2 {
        return Err(ControlError::InvalidRequest(format!(
            "invalid streams member epoch {}",
            request.member_epoch
        )));
    }
    if request.heartbeat_interval_ms <= 0
        || request.session_timeout_ms <= 0
        || request.max_size <= 0
        || request.assignment_interval_ms < 0
        || request.num_standby_replicas < 0
        || request.initial_rebalance_delay_ms < 0
        || request.acceptable_recovery_lag < 0
        || request.task_offset_interval_ms <= 0
    {
        return Err(ControlError::InvalidRequest(
            "streams heartbeat, session, maximum size, recovery lag, and task offset intervals are invalid"
                .to_owned(),
        ));
    }
    let joining = request.member_epoch == 0;
    if joining {
        let topology = request.topology.as_ref().ok_or_else(|| {
            ControlError::InvalidRequest("joining streams member requires a topology".to_owned())
        })?;
        if topology.subtopologies.is_empty()
            || topology
                .subtopologies
                .iter()
                .any(|subtopology| subtopology.subtopology_id.is_empty())
            || request.process_id.as_deref().is_none_or(str::is_empty)
            || request.rebalance_timeout_ms < 0
        {
            return Err(ControlError::InvalidRequest(
                "joining streams member requires subtopologies, process id, and rebalance timeout"
                    .to_owned(),
            ));
        }
        if request.owned_assignment.is_none() {
            return Err(ControlError::InvalidRequest(
                "joining streams member must report all owned task collections".to_owned(),
            ));
        }
    } else if request.topology.is_some() {
        return Err(ControlError::InvalidRequest(
            "only a joining streams member may send topology metadata".to_owned(),
        ));
    }
    Ok(())
}

fn validate_epoch(
    group: &StreamsGroupState,
    existing: Option<&StreamsMemberState>,
    request: &StreamsGroupHeartbeat,
) -> Result<(), ControlError> {
    let member = existing.ok_or_else(|| ControlError::GroupMemberNotFound {
        group: group.group_id.clone(),
        member: request.member_id.clone(),
    })?;
    let recovery_allowed = request
        .owned_assignment
        .as_ref()
        .is_some_and(|owned| assignment_is_subset(owned, &member.current_assignment));
    member_epoch::validate(
        &group.group_id,
        &request.member_id,
        member.member_epoch,
        member.previous_member_epoch,
        request.member_epoch,
        recovery_allowed,
    )
}

fn validate_instance_id(
    group: &StreamsGroupState,
    request: &StreamsGroupHeartbeat,
) -> Result<(), ControlError> {
    let Some(instance_id) = request.instance_id.as_deref() else {
        return Ok(());
    };
    if let Some(member) = group.members.values().find(|member| {
        member.instance_id.as_deref() == Some(instance_id) && member.member_id != request.member_id
    }) {
        return Err(ControlError::UnreleasedInstanceId {
            group: group.group_id.clone(),
            instance_id: instance_id.to_owned(),
            member: member.member_id.clone(),
        });
    }
    Ok(())
}

fn update_topology(
    group: &mut StreamsGroupState,
    incoming: Option<&StreamsTopology>,
    joining: bool,
) -> bool {
    let Some(incoming) = incoming else {
        return false;
    };
    if joining && group.members.is_empty() && incoming != &group.topology {
        group.topology = incoming.clone();
        group.shutdown_requested = false;
        return true;
    }
    false
}

fn rejoining_or_existing(
    existing: Option<StreamsMemberState>,
    request: &StreamsGroupHeartbeat,
    joining: bool,
) -> StreamsMemberState {
    let new_member = existing.is_none();
    let mut member = existing.unwrap_or_else(|| StreamsMemberState {
        member_id: request.member_id.clone(),
        member_epoch: 0,
        previous_member_epoch: 0,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: request.rebalance_timeout_ms,
        session_timeout_ms: request.session_timeout_ms,
        topology_epoch: request
            .topology
            .as_ref()
            .map_or(0, |topology| topology.epoch),
        process_id: String::new(),
        user_endpoint: None,
        client_tags: Vec::new(),
        task_offsets: Vec::new(),
        task_end_offsets: Vec::new(),
        client_id: String::new(),
        client_host: String::new(),
        current_assignment: StreamsTaskAssignment::default(),
        target_assignment: StreamsTaskAssignment::default(),
        owned_assignment: StreamsTaskAssignment::default(),
        last_heartbeat: Utc::now(),
    });
    if joining {
        member.topology_epoch = request
            .topology
            .as_ref()
            .expect("joining topology was validated")
            .epoch;
        if new_member {
            member.owned_assignment = request
                .owned_assignment
                .clone()
                .unwrap_or_default()
                .normalized();
        }
    }
    member
}

fn update_member(
    member: &mut StreamsMemberState,
    request: &StreamsGroupHeartbeat,
    now: DateTime<Utc>,
) -> bool {
    let previous = (
        member.instance_id.clone(),
        member.rack_id.clone(),
        member.rebalance_timeout_ms,
        member.process_id.clone(),
        member.user_endpoint.clone(),
        member.client_tags.clone(),
    );
    if let Some(instance_id) = &request.instance_id {
        member.instance_id = Some(instance_id.clone());
    }
    if let Some(rack_id) = &request.rack_id {
        member.rack_id = Some(rack_id.clone());
    }
    if request.rebalance_timeout_ms >= 0 {
        member.rebalance_timeout_ms = request.rebalance_timeout_ms;
    }
    if let Some(process_id) = &request.process_id {
        member.process_id = process_id.clone();
    }
    if let Some(endpoint) = &request.user_endpoint {
        member.user_endpoint = Some(endpoint.clone());
    }
    if let Some(tags) = &request.client_tags {
        member.client_tags = tags.clone();
        member
            .client_tags
            .sort_by(|left, right| left.key.cmp(&right.key));
    }
    if let Some(offsets) = &request.task_offsets {
        member.task_offsets = offsets.clone();
    }
    if let Some(offsets) = &request.task_end_offsets {
        member.task_end_offsets = offsets.clone();
    }
    if let Some(owned) = &request.owned_assignment {
        member.owned_assignment = owned.clone().normalized();
    }
    member.session_timeout_ms = request.session_timeout_ms;
    member.client_id = request.client_id.clone();
    member.client_host = request.client_host.clone();
    member.last_heartbeat = now;
    previous
        != (
            member.instance_id.clone(),
            member.rack_id.clone(),
            member.rebalance_timeout_ms,
            member.process_id.clone(),
            member.user_endpoint.clone(),
            member.client_tags.clone(),
        )
}

fn expire_members(group: &mut StreamsGroupState, now: DateTime<Utc>) -> bool {
    let before = group.members.len();
    group.members.retain(|_, member| {
        now.signed_duration_since(member.last_heartbeat)
            <= Duration::milliseconds(i64::from(member.session_timeout_ms))
    });
    before != group.members.len()
}

fn complete_initial_delay(group: &mut StreamsGroupState, now: DateTime<Utc>) -> bool {
    if group
        .initial_rebalance_deadline
        .is_some_and(|deadline| now >= deadline)
    {
        group.initial_rebalance_deadline = None;
        true
    } else {
        false
    }
}

fn leave(
    mut group: StreamsGroupState,
    request: &StreamsGroupHeartbeat,
    topics: &[TopicInfo],
    _expired: bool,
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        StreamsGroupState,
        StreamsGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    if group.members.remove(&request.member_id).is_none() {
        return Err(ControlError::GroupMemberNotFound {
            group: request.group_id.clone(),
            member: request.member_id.clone(),
        });
    }
    let resolved = streams_topology::resolve(&group.topology, topics)?;
    group.statuses = resolved.statuses.clone();
    let assignment_task = if defer_assignment {
        defer_target_assignment(&mut group, true, now).1
    } else {
        let _ = refresh_target_assignment(&mut group, &resolved, true, now);
        None
    };
    let endpoint_information_epoch = group.endpoint_information_epoch;
    Ok((
        group,
        StreamsGroupHeartbeatResult {
            member_id: request.member_id.clone(),
            member_epoch: request.member_epoch,
            heartbeat_interval_ms: 0,
            acceptable_recovery_lag: request.acceptable_recovery_lag,
            task_offset_interval_ms: request.task_offset_interval_ms,
            statuses: Vec::new(),
            assignment: None,
            endpoint_information_epoch,
            partitions_by_user_endpoint: None,
        },
        assignment_task,
    ))
}

pub(crate) fn complete_assignment(
    group: &mut StreamsGroupState,
    topics: &[TopicInfo],
    task: &crate::GroupAssignmentTask,
    now: DateTime<Utc>,
) -> Result<crate::GroupAssignmentCompletion, ControlError> {
    if task.protocol != crate::AssignmentProtocol::Streams
        || task.group_id != group.group_id
        || task.group_epoch != group.group_epoch
        || task.assignment_epoch != group.assignment_epoch
        || task.assignment_timestamp != group.assignment_timestamp
    {
        return Ok(crate::GroupAssignmentCompletion::Stale);
    }
    let resolved = streams_topology::resolve(&group.topology, topics)?;
    group.statuses = resolved.statuses.clone();
    if group.shutdown_requested {
        group.statuses.push(StreamsGroupStatus {
            code: STREAMS_STATUS_SHUTDOWN_APPLICATION,
            detail: "the streams application was requested to shut down".to_owned(),
        });
    }
    let previous_epoch = group.assignment_epoch;
    let _ = refresh_target_assignment(group, &resolved, false, now);
    if group.assignment_epoch == previous_epoch {
        group.assignment_timestamp = Some(now);
        Ok(crate::GroupAssignmentCompletion::Unchanged)
    } else {
        Ok(crate::GroupAssignmentCompletion::Published)
    }
}

fn assignment_delay_status(delay: AssignmentDelay) -> StreamsGroupStatus {
    let detail = match delay {
        AssignmentDelay::InitialRebalance => {
            "Assignment delayed due to the configured initial rebalance delay."
        }
        AssignmentDelay::AssignmentInterval => {
            "Assignment delayed due to the configured assignment interval."
        }
    };
    StreamsGroupStatus {
        code: STREAMS_STATUS_ASSIGNMENT_DELAYED,
        detail: detail.to_owned(),
    }
}

fn assignment_is_subset(owned: &StreamsTaskAssignment, assigned: &StreamsTaskAssignment) -> bool {
    owned
        .active_tasks
        .iter()
        .all(|task| assigned.active_tasks.contains(task))
        && owned
            .standby_tasks
            .iter()
            .all(|task| assigned.standby_tasks.contains(task))
        && owned
            .warmup_tasks
            .iter()
            .all(|task| assigned.warmup_tasks.contains(task))
}
