use crate::assignment_interval;
use crate::member_epoch;
use crate::{ControlError, TopicInfo};
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const SHARE_GROUP_ASSIGNOR: &str = "simple";

#[derive(Debug, Clone)]
pub struct ShareGroupHeartbeat {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub subscribed_topic_names: Option<Vec<String>>,
    pub client_id: String,
    pub client_host: String,
    pub heartbeat_interval_ms: i32,
    pub session_timeout_ms: i32,
    pub assignment_interval_ms: i32,
    pub max_size: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatResult {
    pub member_id: String,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<ShareTopicAssignment>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareTopicAssignment {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescription {
    pub group_id: String,
    pub state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<ShareGroupMemberDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberDescription {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assignment: Vec<ShareTopicAssignment>,
}

#[derive(Debug, Clone)]
pub(crate) struct ShareGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignment_timestamp: Option<DateTime<Utc>>,
    pub assignment_interval_ms: i32,
    pub members: HashMap<String, ShareMemberState>,
}

#[derive(Debug, Clone)]
pub(crate) struct ShareMemberState {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub session_timeout_ms: i32,
    pub subscribed_topic_names: Vec<String>,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<ShareTopicAssignment>,
    pub last_heartbeat: DateTime<Utc>,
}

impl ShareGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignment_timestamp: None,
            assignment_interval_ms: 1_000,
            members: HashMap::new(),
        }
    }

    pub fn description(&self) -> ShareGroupDescription {
        let mut members = self
            .members
            .values()
            .map(|member| ShareGroupMemberDescription {
                member_id: member.member_id.clone(),
                rack_id: member.rack_id.clone(),
                member_epoch: member.member_epoch,
                client_id: member.client_id.clone(),
                client_host: member.client_host.clone(),
                subscribed_topic_names: member.subscribed_topic_names.clone(),
                assignment: member.assignment.clone(),
            })
            .collect::<Vec<_>>();
        members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
        ShareGroupDescription {
            group_id: self.group_id.clone(),
            state: if members.is_empty() {
                "Empty".to_owned()
            } else if self.group_epoch > self.assignment_epoch {
                "Assigning".to_owned()
            } else if self
                .members
                .values()
                .any(|member| member.previous_member_epoch != member.member_epoch)
            {
                "Reconciling".to_owned()
            } else {
                "Stable".to_owned()
            },
            group_epoch: self.group_epoch,
            assignment_epoch: self.assignment_epoch,
            assignor_name: SHARE_GROUP_ASSIGNOR.to_owned(),
            members,
        }
    }
}

pub(crate) fn apply_heartbeat(
    group: &mut ShareGroupState,
    heartbeat: ShareGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<ShareGroupHeartbeatResult, ControlError> {
    let (result, _) = apply_heartbeat_inner(group, heartbeat, topics, now, false)?;
    Ok(result)
}

pub(crate) fn apply_heartbeat_deferred(
    group: &mut ShareGroupState,
    heartbeat: ShareGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<
    (
        ShareGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    apply_heartbeat_inner(group, heartbeat, topics, now, true)
}

fn apply_heartbeat_inner(
    group: &mut ShareGroupState,
    heartbeat: ShareGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        ShareGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    validate_heartbeat(&heartbeat)?;
    let mut changed = remove_expired_members(group, now);
    group.assignment_interval_ms = heartbeat.assignment_interval_ms;

    if heartbeat.member_epoch == -1 {
        changed |= group.members.remove(&heartbeat.member_id).is_some();
        let assignment_task = if defer_assignment {
            defer_target_assignment(group, changed, now)
        } else {
            maybe_update_assignment(group, topics, changed, now);
            None
        };
        return Ok((
            ShareGroupHeartbeatResult {
                member_id: heartbeat.member_id,
                member_epoch: -1,
                heartbeat_interval_ms: 0,
                assignment: None,
            },
            assignment_task,
        ));
    }

    let acknowledged_current_epoch = if heartbeat.member_epoch != 0 {
        let member = group.members.get(&heartbeat.member_id).ok_or_else(|| {
            ControlError::GroupMemberNotFound {
                group: group.group_id.clone(),
                member: heartbeat.member_id.clone(),
            }
        })?;
        member_epoch::validate(
            &group.group_id,
            &heartbeat.member_id,
            member.member_epoch,
            member.previous_member_epoch,
            heartbeat.member_epoch,
            true,
        )?;
        heartbeat.member_epoch == member.member_epoch
    } else {
        false
    };
    if heartbeat.member_epoch == 0
        && !group.members.contains_key(&heartbeat.member_id)
        && group.members.len() >= heartbeat.max_size as usize
    {
        return Err(ControlError::GroupMaxSizeReached(heartbeat.group_id));
    }

    let previous = group.members.get(&heartbeat.member_id);
    let subscriptions = heartbeat
        .subscribed_topic_names
        .clone()
        .or_else(|| previous.map(|member| member.subscribed_topic_names.clone()))
        .ok_or_else(|| {
            ControlError::InvalidRequest(
                "subscribed topic names are required when joining a share group".to_owned(),
            )
        })?;
    if subscriptions.is_empty() || subscriptions.iter().any(|topic| topic.is_empty()) {
        return Err(ControlError::InvalidRequest(
            "share group subscriptions must contain non-empty topic names".to_owned(),
        ));
    }
    let normalized = normalize_topics(subscriptions);
    changed |= previous.is_none_or(|member| {
        member.rack_id != heartbeat.rack_id
            || member.subscribed_topic_names != normalized
            || member.client_id != heartbeat.client_id
            || member.client_host != heartbeat.client_host
    });
    let prior_assignment = previous
        .map(|member| member.assignment.clone())
        .unwrap_or_default()
        .into_iter()
        .filter(|assignment| normalized.contains(&assignment.topic_name))
        .collect();
    group.members.insert(
        heartbeat.member_id.clone(),
        ShareMemberState {
            member_id: heartbeat.member_id.clone(),
            rack_id: heartbeat.rack_id,
            member_epoch: previous.map_or(0, |member| member.member_epoch),
            previous_member_epoch: previous.map_or(0, |member| {
                if acknowledged_current_epoch {
                    member.member_epoch
                } else {
                    member.previous_member_epoch
                }
            }),
            session_timeout_ms: heartbeat.session_timeout_ms,
            subscribed_topic_names: normalized,
            client_id: heartbeat.client_id,
            client_host: heartbeat.client_host,
            assignment: prior_assignment,
            last_heartbeat: now,
        },
    );
    let assignment_task = if defer_assignment {
        defer_target_assignment(group, changed, now)
    } else {
        maybe_update_assignment(group, topics, changed, now);
        None
    };
    if let Some(member) = group.members.get_mut(&heartbeat.member_id)
        && member.member_epoch == 0
    {
        member_epoch::update(
            &mut member.member_epoch,
            &mut member.previous_member_epoch,
            group.assignment_epoch,
        );
    }
    let member = group
        .members
        .get(&heartbeat.member_id)
        .expect("share member was inserted");
    Ok((
        ShareGroupHeartbeatResult {
            member_id: member.member_id.clone(),
            member_epoch: member.member_epoch,
            heartbeat_interval_ms: heartbeat.heartbeat_interval_ms,
            assignment: Some(member.assignment.clone()),
        },
        assignment_task,
    ))
}

pub(crate) fn expire_members(
    group: &mut ShareGroupState,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> bool {
    let changed = remove_expired_members(group, now);
    maybe_update_assignment(group, topics, changed, now);
    changed
}

fn remove_expired_members(group: &mut ShareGroupState, now: DateTime<Utc>) -> bool {
    let before = group.members.len();
    group.members.retain(|_, member| {
        member.last_heartbeat + Duration::milliseconds(i64::from(member.session_timeout_ms)) > now
    });
    group.members.len() != before
}

fn maybe_update_assignment(
    group: &mut ShareGroupState,
    topics: &[TopicInfo],
    metadata_changed: bool,
    now: DateTime<Utc>,
) {
    if metadata_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if !assignment_interval::can_compute(
        group.assignment_timestamp,
        group.assignment_interval_ms,
        now,
    ) {
        return;
    }
    let targets = target_assignments(group, topics);
    let target_changed = group.members.iter().any(|(member_id, member)| {
        member.assignment != targets.get(member_id).cloned().unwrap_or_default()
    });
    if group.group_epoch == group.assignment_epoch && target_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if group.group_epoch <= group.assignment_epoch {
        return;
    }
    group.assignment_epoch = group.group_epoch;
    group.assignment_timestamp = Some(now);
    for (member_id, member) in &mut group.members {
        member.assignment = targets.get(member_id).cloned().unwrap_or_default();
        member_epoch::update(
            &mut member.member_epoch,
            &mut member.previous_member_epoch,
            group.group_epoch,
        );
    }
}

fn target_assignments(
    group: &ShareGroupState,
    topics: &[TopicInfo],
) -> HashMap<String, Vec<ShareTopicAssignment>> {
    let topics = topics
        .iter()
        .map(|topic| (topic.name.as_str(), topic))
        .collect::<HashMap<_, _>>();
    group
        .members
        .iter()
        .map(|(member_id, member)| {
            (
                member_id.clone(),
                member
                    .subscribed_topic_names
                    .iter()
                    .filter_map(|name| topics.get(name.as_str()))
                    .map(|topic| ShareTopicAssignment {
                        topic_id: topic.id,
                        topic_name: topic.name.clone(),
                        partitions: (0..topic.partitions).collect(),
                    })
                    .collect(),
            )
        })
        .collect()
}

fn defer_target_assignment(
    group: &mut ShareGroupState,
    metadata_changed: bool,
    now: DateTime<Utc>,
) -> Option<crate::GroupAssignmentTask> {
    if metadata_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if group.members.is_empty()
        || !assignment_interval::can_compute(
            group.assignment_timestamp,
            group.assignment_interval_ms,
            now,
        )
    {
        return None;
    }
    Some(assignment_task(group))
}

pub(crate) fn complete_assignment(
    group: &mut ShareGroupState,
    topics: &[TopicInfo],
    task: &crate::GroupAssignmentTask,
    now: DateTime<Utc>,
) -> crate::GroupAssignmentCompletion {
    if !matches_assignment_task(group, task) {
        return crate::GroupAssignmentCompletion::Stale;
    }
    let previous_epoch = group.assignment_epoch;
    maybe_update_assignment(group, topics, false, now);
    if group.assignment_epoch == previous_epoch {
        group.assignment_timestamp = Some(now);
        crate::GroupAssignmentCompletion::Unchanged
    } else {
        crate::GroupAssignmentCompletion::Published
    }
}

fn assignment_task(group: &ShareGroupState) -> crate::GroupAssignmentTask {
    crate::GroupAssignmentTask {
        protocol: crate::AssignmentProtocol::Share,
        group_id: group.group_id.clone(),
        group_epoch: group.group_epoch,
        assignment_epoch: group.assignment_epoch,
        assignment_timestamp: group.assignment_timestamp,
    }
}

fn matches_assignment_task(group: &ShareGroupState, task: &crate::GroupAssignmentTask) -> bool {
    task.protocol == crate::AssignmentProtocol::Share
        && task.group_id == group.group_id
        && task.group_epoch == group.group_epoch
        && task.assignment_epoch == group.assignment_epoch
        && task.assignment_timestamp == group.assignment_timestamp
}

fn validate_heartbeat(heartbeat: &ShareGroupHeartbeat) -> Result<(), ControlError> {
    if heartbeat.group_id.is_empty() || heartbeat.member_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "share group and member IDs cannot be empty".to_owned(),
        ));
    }
    if heartbeat.member_epoch < -1 {
        return Err(ControlError::InvalidRequest(
            "share group member epoch must be -1, 0, or positive".to_owned(),
        ));
    }
    if heartbeat.heartbeat_interval_ms <= 0
        || heartbeat.session_timeout_ms <= 0
        || heartbeat.assignment_interval_ms < 0
        || heartbeat.max_size <= 0
    {
        return Err(ControlError::InvalidRequest(
            "share group heartbeat, session interval, and maximum size must be positive and assignment interval must be non-negative".to_owned(),
        ));
    }
    Ok(())
}

fn normalize_topics(topics: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut topics = topics
        .into_iter()
        .filter(|topic| seen.insert(topic.clone()))
        .collect::<Vec<_>>();
    topics.sort();
    topics
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat(member_id: &str, epoch: i32, topics: Option<Vec<&str>>) -> ShareGroupHeartbeat {
        ShareGroupHeartbeat {
            group_id: "workers".to_owned(),
            member_id: member_id.to_owned(),
            member_epoch: epoch,
            rack_id: None,
            subscribed_topic_names: topics
                .map(|topics| topics.into_iter().map(str::to_owned).collect()),
            client_id: member_id.to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            assignment_interval_ms: 0,
            max_size: 200,
        }
    }

    #[test]
    fn all_share_members_can_work_on_the_same_partitions() {
        let topic = TopicInfo {
            id: Uuid::new_v4(),
            name: "jobs".to_owned(),
            partitions: 2,
        };
        let now = Utc::now();
        let mut group = ShareGroupState::new("workers");
        let first = apply_heartbeat(
            &mut group,
            heartbeat("one", 0, Some(vec!["jobs"])),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        let second = apply_heartbeat(
            &mut group,
            heartbeat("two", 0, Some(vec!["jobs"])),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        assert_eq!(first.assignment.unwrap()[0].partitions, vec![0, 1]);
        assert_eq!(second.assignment.unwrap()[0].partitions, vec![0, 1]);
        assert_eq!(group.description().state, "Reconciling");

        let first_epoch = group.members["one"].member_epoch;
        apply_heartbeat(
            &mut group,
            heartbeat("one", first_epoch, None),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        assert_eq!(group.description().state, "Reconciling");

        let second_epoch = group.members["two"].member_epoch;
        apply_heartbeat(
            &mut group,
            heartbeat("two", second_epoch, None),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        assert_eq!(group.description().state, "Stable");
    }

    #[test]
    fn expired_member_releases_capacity_before_a_new_join() {
        let topic = TopicInfo {
            id: Uuid::new_v4(),
            name: "jobs".to_owned(),
            partitions: 1,
        };
        let now = Utc::now();
        let mut group = ShareGroupState::new("workers");
        let mut first = heartbeat("one", 0, Some(vec!["jobs"]));
        first.session_timeout_ms = 10;
        first.max_size = 1;
        apply_heartbeat(&mut group, first, std::slice::from_ref(&topic), now).unwrap();

        let mut second = heartbeat("two", 0, Some(vec!["jobs"]));
        second.max_size = 1;
        apply_heartbeat(
            &mut group,
            second,
            std::slice::from_ref(&topic),
            now + Duration::milliseconds(11),
        )
        .unwrap();
        assert_eq!(group.members.len(), 1);
        assert!(group.members.contains_key("two"));
    }

    #[test]
    fn stale_epochs_are_fenced_and_leave_removes_members() {
        let now = Utc::now();
        let mut group = ShareGroupState::new("workers");
        let joined = apply_heartbeat(
            &mut group,
            heartbeat("one", 0, Some(vec!["jobs"])),
            &[],
            now,
        )
        .unwrap();
        assert!(apply_heartbeat(&mut group, heartbeat("one", 99, None), &[], now).is_err());
        let left = apply_heartbeat(&mut group, heartbeat("one", -1, None), &[], now).unwrap();
        assert_eq!(left.member_epoch, -1);
        assert_eq!(joined.member_epoch, 2);
        assert!(group.members.is_empty());
    }

    #[test]
    fn assignment_updates_are_batched_until_the_interval_elapses() {
        let topic = TopicInfo {
            id: Uuid::new_v4(),
            name: "jobs".to_owned(),
            partitions: 2,
        };
        let topics = std::slice::from_ref(&topic);
        let now = Utc::now();
        let mut group = ShareGroupState::new("workers");
        let mut first_join = heartbeat("one", 0, Some(vec!["jobs"]));
        first_join.assignment_interval_ms = 1_000;
        let first = apply_heartbeat(&mut group, first_join, topics, now).unwrap();
        assert_eq!(first.member_epoch, 2);
        assert_eq!(group.assignment_timestamp, Some(now));

        let mut second_join = heartbeat("two", 0, Some(vec!["jobs"]));
        second_join.assignment_interval_ms = 1_000;
        let second = apply_heartbeat(
            &mut group,
            second_join,
            topics,
            now + Duration::milliseconds(100),
        )
        .unwrap();
        assert_eq!(group.group_epoch, 3);
        assert_eq!(group.assignment_epoch, 2);
        assert_eq!(second.member_epoch, 2);
        assert!(second.assignment.unwrap().is_empty());
        assert_eq!(group.description().state, "Assigning");

        let mut poll = heartbeat("two", second.member_epoch, None);
        poll.assignment_interval_ms = 1_000;
        let delayed = apply_heartbeat(
            &mut group,
            poll.clone(),
            topics,
            now + Duration::milliseconds(999),
        )
        .unwrap();
        assert_eq!(delayed.member_epoch, 2);
        assert_eq!(group.assignment_epoch, 2);

        let assigned = apply_heartbeat(
            &mut group,
            poll,
            topics,
            now + Duration::milliseconds(1_000),
        )
        .unwrap();
        assert_eq!(assigned.member_epoch, 3);
        assert_eq!(assigned.assignment.unwrap()[0].partitions, [0, 1]);
        assert_eq!(group.assignment_epoch, 3);
        assert_eq!(
            group.assignment_timestamp,
            Some(now + Duration::milliseconds(1_000))
        );
    }
}
