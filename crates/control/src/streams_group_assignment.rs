use crate::assignment_interval;
use crate::member_epoch;
use crate::streams_group_types::{
    StreamsEndpointPartitions, StreamsGroupState, StreamsTaskAssignment, StreamsTaskId,
    StreamsTopicPartitions,
};
use crate::streams_topology::ResolvedStreamsTopology;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignmentDelay {
    InitialRebalance,
    AssignmentInterval,
}

pub(crate) fn refresh_target_assignment(
    group: &mut StreamsGroupState,
    resolved: &ResolvedStreamsTopology,
    metadata_changed: bool,
    now: DateTime<Utc>,
) -> Option<AssignmentDelay> {
    if metadata_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if group.group_epoch > group.assignment_epoch {
        if group.initial_rebalance_deadline.is_some() {
            return Some(AssignmentDelay::InitialRebalance);
        }
        if !assignment_interval::can_compute(
            group.assignment_timestamp,
            group.assignment_interval_ms,
            now,
        ) {
            return Some(AssignmentDelay::AssignmentInterval);
        }
    }

    let targets = target_assignments(group, resolved);
    let target_changed = group.members.iter().any(|(member_id, member)| {
        member.target_assignment != targets.get(member_id).cloned().unwrap_or_default()
    });
    if !metadata_changed && group.group_epoch == group.assignment_epoch && target_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
        if !assignment_interval::can_compute(
            group.assignment_timestamp,
            group.assignment_interval_ms,
            now,
        ) {
            return Some(AssignmentDelay::AssignmentInterval);
        }
    }
    if group.group_epoch > group.assignment_epoch {
        group.assignment_epoch = group.group_epoch;
        group.assignment_timestamp = Some(now);
        group.endpoint_information_epoch = group.endpoint_information_epoch.saturating_add(1);
        for (member_id, member) in &mut group.members {
            member.target_assignment = targets.get(member_id).cloned().unwrap_or_default();
        }
    }
    None
}

pub(crate) fn defer_target_assignment(
    group: &mut StreamsGroupState,
    metadata_changed: bool,
    now: DateTime<Utc>,
) -> (Option<AssignmentDelay>, Option<crate::GroupAssignmentTask>) {
    if metadata_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if group.members.is_empty() {
        return (None, None);
    }
    let assignment_pending = group.group_epoch > group.assignment_epoch;
    if assignment_pending && group.initial_rebalance_deadline.is_some() {
        return (Some(AssignmentDelay::InitialRebalance), None);
    }
    if !assignment_interval::can_compute(
        group.assignment_timestamp,
        group.assignment_interval_ms,
        now,
    ) {
        return (
            assignment_pending.then_some(AssignmentDelay::AssignmentInterval),
            None,
        );
    }
    (
        None,
        Some(crate::GroupAssignmentTask {
            protocol: crate::AssignmentProtocol::Streams,
            group_id: group.group_id.clone(),
            group_epoch: group.group_epoch,
            assignment_epoch: group.assignment_epoch,
            assignment_timestamp: group.assignment_timestamp,
        }),
    )
}

fn target_assignments(
    group: &StreamsGroupState,
    resolved: &ResolvedStreamsTopology,
) -> HashMap<String, StreamsTaskAssignment> {
    let mut targets = group
        .members
        .keys()
        .map(|member_id| (member_id.clone(), StreamsTaskAssignment::default()))
        .collect::<HashMap<_, _>>();
    if !resolved.ready() || group.initial_rebalance_deadline.is_some() {
        return targets;
    }
    let mut members = group
        .members
        .values()
        .filter(|member| member.topology_epoch == group.topology.epoch)
        .map(|member| member.member_id.as_str())
        .collect::<Vec<_>>();
    members.sort_unstable();
    if members.is_empty() {
        return targets;
    }
    for (member_id, tasks) in sticky_active_tasks(group, &members, &resolved.tasks) {
        targets
            .get_mut(&member_id)
            .expect("eligible streams member exists")
            .active_tasks = tasks;
    }
    assign_standby_tasks(group, &members, resolved, &mut targets);
    targets
}

fn assign_standby_tasks(
    group: &StreamsGroupState,
    members: &[&str],
    resolved: &ResolvedStreamsTopology,
    targets: &mut HashMap<String, StreamsTaskAssignment>,
) {
    let replicas = usize::try_from(group.num_standby_replicas).unwrap_or(0);
    if replicas == 0 {
        return;
    }
    let stateful_subtopologies = resolved
        .topology
        .subtopologies
        .iter()
        .filter(|subtopology| !subtopology.state_changelog_topics.is_empty())
        .map(|subtopology| subtopology.subtopology_id.as_str())
        .collect::<HashSet<_>>();
    for task in resolved
        .tasks
        .iter()
        .filter(|task| stateful_subtopologies.contains(task.subtopology_id.as_str()))
    {
        let Some(active_owner) = members
            .iter()
            .copied()
            .find(|member| targets[*member].active_tasks.contains(task))
        else {
            continue;
        };
        let active_process = group.members[active_owner].process_id.as_str();
        let mut used_processes = HashSet::from([active_process]);
        for _ in 0..replicas {
            let candidate = members
                .iter()
                .copied()
                .filter(|member| {
                    !used_processes.contains(group.members[*member].process_id.as_str())
                })
                .min_by(|left, right| {
                    standby_preference(group, targets, left, task)
                        .cmp(&standby_preference(group, targets, right, task))
                });
            let Some(member) = candidate else {
                break;
            };
            used_processes.insert(group.members[member].process_id.as_str());
            targets
                .get_mut(member)
                .expect("eligible standby member exists")
                .standby_tasks
                .push(task.clone());
        }
    }
}

fn standby_preference(
    group: &StreamsGroupState,
    targets: &HashMap<String, StreamsTaskAssignment>,
    member_id: &str,
    task: &StreamsTaskId,
) -> (bool, usize, String) {
    let member = &group.members[member_id];
    let previously_owned = member.target_assignment.standby_tasks.contains(task)
        || member.current_assignment.standby_tasks.contains(task)
        || member.owned_assignment.standby_tasks.contains(task);
    (
        !previously_owned,
        targets[member_id].standby_tasks.len(),
        member_id.to_owned(),
    )
}

fn sticky_active_tasks(
    group: &StreamsGroupState,
    members: &[&str],
    tasks: &[StreamsTaskId],
) -> HashMap<String, Vec<StreamsTaskId>> {
    let valid = tasks.iter().cloned().collect::<BTreeSet<_>>();
    let mut previous_owner = BTreeMap::<StreamsTaskId, String>::new();
    for assignment in 0..3 {
        for member_id in members {
            let member = &group.members[*member_id];
            let previous = match assignment {
                0 => &member.target_assignment.active_tasks,
                1 => &member.current_assignment.active_tasks,
                _ => &member.owned_assignment.active_tasks,
            };
            for task in previous {
                if valid.contains(task) {
                    previous_owner
                        .entry(task.clone())
                        .or_insert_with(|| (*member_id).to_owned());
                }
            }
        }
    }

    let mut previous_counts = HashMap::<&str, usize>::new();
    for owner in previous_owner.values() {
        *previous_counts.entry(owner.as_str()).or_default() += 1;
    }
    let base = tasks.len() / members.len();
    let extra = tasks.len() % members.len();
    let mut capacity_order = members.to_vec();
    capacity_order.sort_by(|left, right| {
        previous_counts
            .get(right)
            .copied()
            .unwrap_or(0)
            .cmp(&previous_counts.get(left).copied().unwrap_or(0))
            .then(left.cmp(right))
    });
    let mut capacities = members
        .iter()
        .map(|member_id| ((*member_id).to_owned(), base))
        .collect::<HashMap<_, _>>();
    for member_id in capacity_order.into_iter().take(extra) {
        *capacities
            .get_mut(member_id)
            .expect("eligible member capacity exists") += 1;
    }

    let mut assigned = members
        .iter()
        .map(|member_id| ((*member_id).to_owned(), Vec::new()))
        .collect::<HashMap<_, Vec<StreamsTaskId>>>();
    let mut unassigned = Vec::new();
    for task in tasks {
        let Some(owner) = previous_owner.get(task) else {
            unassigned.push(task.clone());
            continue;
        };
        let owned = assigned.get_mut(owner).expect("previous owner is eligible");
        if owned.len() < capacities[owner] {
            owned.push(task.clone());
        } else {
            unassigned.push(task.clone());
        }
    }
    for task in unassigned {
        let member_id = members
            .iter()
            .copied()
            .find(|member_id| assigned[*member_id].len() < capacities[*member_id])
            .expect("streams task capacity remains");
        assigned
            .get_mut(member_id)
            .expect("eligible assignment exists")
            .push(task);
    }
    assigned
}

pub(crate) fn reconcile_member(
    group: &mut StreamsGroupState,
    member_id: &str,
    stale_topology: bool,
    force_response: bool,
) -> Option<StreamsTaskAssignment> {
    let owned_active_by_others = group
        .members
        .iter()
        .filter(|(other_id, _)| other_id.as_str() != member_id)
        .flat_map(|(_, member)| member.owned_assignment.active_tasks.iter().cloned())
        .collect::<HashSet<_>>();
    let process_id = group.members[member_id].process_id.clone();
    let owned_by_same_process = group
        .members
        .values()
        .filter(|member| member.process_id == process_id)
        .flat_map(|member| {
            member
                .owned_assignment
                .active_tasks
                .iter()
                .chain(member.owned_assignment.standby_tasks.iter())
                .cloned()
        })
        .collect::<HashSet<_>>();
    let member = group
        .members
        .get_mut(member_id)
        .expect("heartbeat member exists");
    let previous = member.current_assignment.clone();
    let target = if stale_topology {
        StreamsTaskAssignment::default()
    } else {
        member.target_assignment.clone()
    };
    let revoking = !task_set(&member.owned_assignment.active_tasks)
        .is_subset(&task_set(&target.active_tasks))
        || !task_set(&member.owned_assignment.standby_tasks)
            .is_subset(&task_set(&target.standby_tasks))
        || !task_set(&member.owned_assignment.warmup_tasks)
            .is_subset(&task_set(&target.warmup_tasks));
    let next = if revoking {
        StreamsTaskAssignment {
            active_tasks: intersection(&member.owned_assignment.active_tasks, &target.active_tasks),
            standby_tasks: intersection(
                &member.owned_assignment.standby_tasks,
                &target.standby_tasks,
            ),
            warmup_tasks: intersection(&member.owned_assignment.warmup_tasks, &target.warmup_tasks),
        }
    } else {
        StreamsTaskAssignment {
            active_tasks: target
                .active_tasks
                .iter()
                .filter(|task| !owned_active_by_others.contains(*task))
                .cloned()
                .collect(),
            standby_tasks: target
                .standby_tasks
                .iter()
                .filter(|task| {
                    member.owned_assignment.standby_tasks.contains(*task)
                        || !owned_by_same_process.contains(*task)
                })
                .cloned()
                .collect(),
            warmup_tasks: target
                .warmup_tasks
                .iter()
                .filter(|task| !owned_by_same_process.contains(*task))
                .cloned()
                .collect(),
        }
    }
    .normalized();
    if revoking {
        if previous != next {
            member.previous_member_epoch = member.member_epoch;
        }
    } else {
        member_epoch::update(
            &mut member.member_epoch,
            &mut member.previous_member_epoch,
            group.assignment_epoch,
        );
    }
    member.current_assignment = next.clone();
    let converged = member.owned_assignment == next && next == target;
    (force_response || !converged || previous != next).then_some(next)
}

pub(crate) fn endpoint_partitions(group: &StreamsGroupState) -> Vec<StreamsEndpointPartitions> {
    let changelog_by_subtopology = group
        .topology
        .subtopologies
        .iter()
        .map(|subtopology| {
            (
                subtopology.subtopology_id.as_str(),
                subtopology
                    .state_changelog_topics
                    .iter()
                    .map(|topic| topic.name.as_str())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut endpoints = Vec::new();
    for member in group.members.values() {
        let Some(endpoint) = member.user_endpoint.clone() else {
            continue;
        };
        endpoints.push(StreamsEndpointPartitions {
            endpoint,
            active_partitions: materialized_partitions(
                &member.current_assignment.active_tasks,
                &changelog_by_subtopology,
            ),
            standby_partitions: materialized_partitions(
                &member.current_assignment.standby_tasks,
                &changelog_by_subtopology,
            ),
        });
    }
    endpoints.sort_by(|left, right| {
        left.endpoint
            .host
            .cmp(&right.endpoint.host)
            .then(left.endpoint.port.cmp(&right.endpoint.port))
    });
    endpoints
}

fn materialized_partitions(
    tasks: &[StreamsTaskId],
    changelogs: &HashMap<&str, Vec<&str>>,
) -> Vec<StreamsTopicPartitions> {
    let mut by_topic = BTreeMap::<String, BTreeSet<i32>>::new();
    for task in tasks {
        for topic in changelogs
            .get(task.subtopology_id.as_str())
            .into_iter()
            .flatten()
        {
            by_topic
                .entry((*topic).to_owned())
                .or_default()
                .insert(task.partition);
        }
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| StreamsTopicPartitions {
            topic,
            partitions: partitions.into_iter().collect(),
        })
        .collect()
}

pub(crate) fn group_state(group: &StreamsGroupState, ready: bool) -> &'static str {
    if group.members.is_empty() {
        return "Empty";
    }
    if !ready {
        return "NotReady";
    }
    if group.initial_rebalance_deadline.is_some() {
        return "Assigning";
    }
    if group.group_epoch > group.assignment_epoch {
        return "Assigning";
    }
    if group.members.values().all(|member| {
        member.member_epoch == group.assignment_epoch
            && member.owned_assignment == member.current_assignment
            && member.current_assignment == member.target_assignment
    }) {
        "Stable"
    } else {
        "Reconciling"
    }
}

fn task_set(tasks: &[StreamsTaskId]) -> BTreeSet<StreamsTaskId> {
    tasks.iter().cloned().collect()
}

fn intersection(left: &[StreamsTaskId], right: &[StreamsTaskId]) -> Vec<StreamsTaskId> {
    task_set(left)
        .intersection(&task_set(right))
        .cloned()
        .collect()
}
