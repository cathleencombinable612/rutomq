use crate::assignment_interval;
use crate::consumer_assignor::{UNIFORM_ASSIGNOR, assign_with_regex_topics, validate_assignor};
use crate::member_epoch;
use crate::{ControlError, PartitionKey, TopicInfo};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

const REGEX_REFRESH_MIN_INTERVAL_MS: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct ConsumerGroupHeartbeat {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub subscribed_topic_names: Option<Vec<String>>,
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub configured_assignors: Vec<String>,
    pub owned_partitions: Option<Vec<ConsumerOwnedTopicPartitions>>,
    pub client_id: String,
    pub client_host: String,
    pub heartbeat_interval_ms: i32,
    pub session_timeout_ms: i32,
    pub regex_refresh_interval_ms: i32,
    pub assignment_interval_ms: i32,
    pub max_size: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerOwnedTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatResult {
    pub member_id: String,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<ConsumerTopicAssignment>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerTopicAssignment {
    pub topic_id: Uuid,
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    pub group_id: String,
    pub state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<ConsumerGroupMemberDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberDescription {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub subscribed_topic_regex: Option<String>,
    pub assignment: Vec<ConsumerTopicAssignment>,
    pub target_assignment: Vec<ConsumerTopicAssignment>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsumerGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignment_timestamp: Option<DateTime<Utc>>,
    pub regex_refresh_timestamp: Option<DateTime<Utc>>,
    pub regex_refresh_pending: bool,
    pub assignment_interval_ms: i32,
    pub assignor_name: String,
    pub members: HashMap<String, ConsumerMemberState>,
}

impl Default for ConsumerGroupState {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignment_timestamp: None,
            regex_refresh_timestamp: None,
            regex_refresh_pending: false,
            assignment_interval_ms: 1_000,
            assignor_name: UNIFORM_ASSIGNOR.to_owned(),
            members: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConsumerMemberState {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub subscribed_topic_names: Vec<String>,
    pub subscribed_topic_regex: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub current_assignment: Vec<ConsumerTopicAssignment>,
    pub target_assignment: Vec<ConsumerTopicAssignment>,
    pub owned_assignment: Vec<ConsumerTopicAssignment>,
    pub assignment_epochs: HashMap<(Uuid, i32), i32>,
    pub last_heartbeat: DateTime<Utc>,
}

impl Default for ConsumerMemberState {
    fn default() -> Self {
        Self {
            member_id: String::new(),
            instance_id: None,
            rack_id: None,
            member_epoch: 0,
            previous_member_epoch: 0,
            rebalance_timeout_ms: -1,
            session_timeout_ms: 45_000,
            subscribed_topic_names: Vec::new(),
            subscribed_topic_regex: None,
            client_id: String::new(),
            client_host: String::new(),
            current_assignment: Vec::new(),
            target_assignment: Vec::new(),
            owned_assignment: Vec::new(),
            assignment_epochs: HashMap::new(),
            last_heartbeat: Utc::now(),
        }
    }
}

pub(crate) fn heartbeat(
    state: Option<ConsumerGroupState>,
    request: ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<(ConsumerGroupState, ConsumerGroupHeartbeatResult), ControlError> {
    let (group, result, _) = heartbeat_inner(state, request, topics, now, false)?;
    Ok((group, result))
}

pub(crate) fn heartbeat_deferred(
    state: Option<ConsumerGroupState>,
    request: ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<
    (
        ConsumerGroupState,
        ConsumerGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    heartbeat_inner(state, request, topics, now, true)
}

fn heartbeat_inner(
    state: Option<ConsumerGroupState>,
    mut request: ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        ConsumerGroupState,
        ConsumerGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    validate_request(&request)?;
    if request.member_id.is_empty() {
        request.member_id = format!("rutomq-{}", Uuid::new_v4());
    }
    let joining = request.member_epoch == 0;
    let mut group = match state {
        Some(group) => group,
        None if joining => ConsumerGroupState {
            group_id: request.group_id.clone(),
            assignor_name: request.configured_assignors[0].clone(),
            ..ConsumerGroupState::default()
        },
        None => return Err(ControlError::GroupNotFound(request.group_id)),
    };

    let expired = expire_members(&mut group, now);
    group.assignment_interval_ms = request.assignment_interval_ms;
    if request.member_epoch == -1 {
        return leave(group, &request, topics, expired, now, defer_assignment);
    }
    if request.member_epoch == -2 {
        return static_leave(group, &request, topics, expired, now, defer_assignment);
    }

    let (existing, new_member, static_replacement) = if joining {
        joining_member(&mut group, &request, topics)?
    } else {
        (group.members.get(&request.member_id).cloned(), false, false)
    };
    if new_member && group.members.len() >= request.max_size as usize {
        return Err(ControlError::GroupMaxSizeReached(request.group_id));
    }
    if !joining {
        let member = existing
            .as_ref()
            .ok_or_else(|| ControlError::GroupMemberNotFound {
                group: request.group_id.clone(),
                member: request.member_id.clone(),
            })?;
        let recovery_allowed = request.owned_partitions.as_deref().is_some_and(|owned| {
            owned_set(owned).is_subset(&assignment_set(&member.current_assignment))
        });
        member_epoch::validate(
            &request.group_id,
            &request.member_id,
            member.member_epoch,
            member.previous_member_epoch,
            request.member_epoch,
            recovery_allowed,
        )?;
    }

    validate_instance_id(&group, &request)?;
    let existing_regexes = group
        .members
        .values()
        .filter_map(|member| member.subscribed_topic_regex.clone())
        .collect::<HashSet<_>>();
    let mut member = existing.expect("heartbeat member was resolved");
    let metadata_changed = update_member(&mut member, &request, topics, now)?;
    let regex_introduced = member
        .subscribed_topic_regex
        .as_ref()
        .is_some_and(|pattern| !existing_regexes.contains(pattern));
    group.members.insert(request.member_id.clone(), member);
    update_regex_refresh_state(
        &mut group,
        topics,
        regex_introduced,
        request.regex_refresh_interval_ms,
        now,
    )?;

    let assignor_changed = if let Some(assignor) = request.server_assignor.as_deref() {
        validate_assignor(assignor)?;
        let changed = group.assignor_name != assignor;
        group.assignor_name = assignor.to_owned();
        changed
    } else {
        false
    };
    let assignment_triggered = expired || new_member || metadata_changed || assignor_changed;
    let assignment_task =
        if !static_replacement || assignment_triggered || group.regex_refresh_pending {
            if defer_assignment {
                defer_target_assignment(&mut group, assignment_triggered, now)
            } else {
                refresh_target_assignment(&mut group, topics, assignment_triggered, now)?;
                None
            }
        } else {
            None
        };
    let mut result = reconcile_member(
        &mut group,
        &request.member_id,
        request.heartbeat_interval_ms,
    );
    if defer_assignment && joining && result.assignment.is_none() {
        result.assignment = Some(
            group
                .members
                .get(&request.member_id)
                .expect("joining consumer member exists")
                .current_assignment
                .clone(),
        );
    }
    Ok((group, result, assignment_task))
}

pub(crate) fn describe(group: &ConsumerGroupState) -> ConsumerGroupDescription {
    let mut members = group
        .members
        .values()
        .map(|member| ConsumerGroupMemberDescription {
            member_id: member.member_id.clone(),
            instance_id: member.instance_id.clone(),
            rack_id: member.rack_id.clone(),
            member_epoch: member.member_epoch,
            client_id: member.client_id.clone(),
            client_host: member.client_host.clone(),
            subscribed_topic_names: member.subscribed_topic_names.clone(),
            subscribed_topic_regex: member.subscribed_topic_regex.clone(),
            assignment: member.current_assignment.clone(),
            target_assignment: member.target_assignment.clone(),
        })
        .collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    let stable = group.members.values().all(|member| {
        member.member_epoch == -2
            || (member.member_epoch == group.assignment_epoch
                && assignment_set(&member.current_assignment)
                    == assignment_set(&member.target_assignment)
                && assignment_set(&member.owned_assignment)
                    == assignment_set(&member.current_assignment))
    });
    ConsumerGroupDescription {
        group_id: group.group_id.clone(),
        state: if group.members.is_empty() {
            "Empty"
        } else if group.group_epoch > group.assignment_epoch {
            "Assigning"
        } else if stable {
            "Stable"
        } else {
            "Reconciling"
        }
        .to_owned(),
        group_epoch: group.group_epoch,
        assignment_epoch: group.assignment_epoch,
        assignor_name: group.assignor_name.clone(),
        members,
    }
}

pub(crate) fn validate_member(
    group: &ConsumerGroupState,
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

pub(crate) fn validate_offset_commit(
    group: &ConsumerGroupState,
    member_id: &str,
    member_epoch: i32,
    partitions: &[PartitionKey],
) -> Result<Vec<bool>, ControlError> {
    if member_epoch < 0 && group.members.is_empty() {
        return Ok(vec![true; partitions.len()]);
    }
    let member = group
        .members
        .get(member_id)
        .ok_or_else(|| ControlError::GroupMemberNotFound {
            group: group.group_id.clone(),
            member: member_id.to_owned(),
        })?;
    if member_epoch == member.member_epoch {
        return Ok(vec![true; partitions.len()]);
    }
    if member_epoch > member.member_epoch {
        return Err(ControlError::FencedMemberEpoch {
            group: group.group_id.clone(),
            member: member_id.to_owned(),
            expected: member.member_epoch,
            actual: member_epoch,
        });
    }

    let topic_ids = member
        .current_assignment
        .iter()
        .chain(&member.target_assignment)
        .chain(&member.owned_assignment)
        .map(|assignment| (assignment.topic_name.as_str(), assignment.topic_id))
        .collect::<HashMap<_, _>>();
    Ok(partitions
        .iter()
        .map(|partition| {
            topic_ids
                .get(partition.topic.as_str())
                .and_then(|topic_id| {
                    member
                        .assignment_epochs
                        .get(&(*topic_id, partition.partition))
                })
                .is_some_and(|assignment_epoch| member_epoch >= *assignment_epoch)
        })
        .collect())
}

pub(crate) fn validate_transaction_offset_commit(
    group: &ConsumerGroupState,
    member_id: &str,
    group_instance_id: Option<&str>,
    member_epoch: i32,
    partitions: &[PartitionKey],
) -> Result<(), ControlError> {
    if member_epoch == -1 && member_id.is_empty() && group_instance_id.is_none() {
        return Ok(());
    }
    let validity = validate_offset_commit(group, member_id, member_epoch, partitions)?;
    if validity.iter().all(|valid| *valid) {
        return Ok(());
    }
    let expected = group
        .members
        .get(member_id)
        .map_or(group.assignment_epoch, |member| member.member_epoch);
    Err(ControlError::FencedMemberEpoch {
        group: group.group_id.clone(),
        member: member_id.to_owned(),
        expected,
        actual: member_epoch,
    })
}

fn validate_request(request: &ConsumerGroupHeartbeat) -> Result<(), ControlError> {
    if request.group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "consumer group id must not be empty".to_owned(),
        ));
    }
    if !matches!(request.member_epoch, -2..=i32::MAX) {
        return Err(ControlError::InvalidRequest(format!(
            "invalid consumer member epoch {}",
            request.member_epoch
        )));
    }
    if request.heartbeat_interval_ms <= 0
        || request.session_timeout_ms <= 0
        || request.regex_refresh_interval_ms < REGEX_REFRESH_MIN_INTERVAL_MS as i32
        || request.assignment_interval_ms < 0
        || request.max_size <= 0
    {
        return Err(ControlError::InvalidRequest(
            "consumer heartbeat, session timeout, and maximum size must be positive, regex refresh interval must be at least 10000 ms, and assignment interval must be non-negative".to_owned(),
        ));
    }
    let mut unique_assignors = HashSet::new();
    if request.configured_assignors.is_empty() {
        return Err(ControlError::InvalidRequest(
            "consumer group assignors must not be empty".to_owned(),
        ));
    }
    for assignor in &request.configured_assignors {
        validate_assignor(assignor)?;
        if !unique_assignors.insert(assignor.as_str()) {
            return Err(ControlError::InvalidRequest(
                "consumer group assignors must not contain duplicates".to_owned(),
            ));
        }
    }
    if let Some(assignor) = request.server_assignor.as_deref()
        && !unique_assignors.contains(assignor)
    {
        return Err(ControlError::UnsupportedConsumerAssignor(
            assignor.to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn expire_members(group: &mut ConsumerGroupState, now: DateTime<Utc>) -> bool {
    let before = group.members.len();
    group.members.retain(|_, member| {
        member.member_epoch == -2
            || now.signed_duration_since(member.last_heartbeat)
                <= Duration::milliseconds(i64::from(member.session_timeout_ms))
    });
    before != group.members.len()
}

fn leave(
    mut group: ConsumerGroupState,
    request: &ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    expired: bool,
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        ConsumerGroupState,
        ConsumerGroupHeartbeatResult,
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
    let _ = expired;
    update_regex_refresh_state(
        &mut group,
        topics,
        false,
        request.regex_refresh_interval_ms,
        now,
    )?;
    let assignment_task = if defer_assignment {
        defer_target_assignment(&mut group, true, now)
    } else {
        refresh_target_assignment(&mut group, topics, true, now)?;
        None
    };
    Ok((
        group,
        ConsumerGroupHeartbeatResult {
            member_id: request.member_id.clone(),
            member_epoch: request.member_epoch,
            heartbeat_interval_ms: 0,
            assignment: None,
        },
        assignment_task,
    ))
}

fn static_leave(
    group: ConsumerGroupState,
    request: &ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    expired: bool,
    now: DateTime<Utc>,
    defer_assignment: bool,
) -> Result<
    (
        ConsumerGroupState,
        ConsumerGroupHeartbeatResult,
        Option<crate::GroupAssignmentTask>,
    ),
    ControlError,
> {
    let Some(instance_id) = request.instance_id.as_deref() else {
        return leave(group, request, topics, expired, now, defer_assignment);
    };
    let mut group = group;
    let member = group
        .members
        .values_mut()
        .find(|member| member.instance_id.as_deref() == Some(instance_id))
        .ok_or_else(|| ControlError::GroupMemberNotFound {
            group: request.group_id.clone(),
            member: request.member_id.clone(),
        })?;
    if member.member_id != request.member_id {
        return Err(ControlError::FencedInstanceId {
            group: request.group_id.clone(),
            instance_id: instance_id.to_owned(),
        });
    }
    for assignment_epoch in member.assignment_epochs.values_mut() {
        *assignment_epoch = 0;
    }
    member.member_epoch = -2;
    Ok((
        group,
        ConsumerGroupHeartbeatResult {
            member_id: request.member_id.clone(),
            member_epoch: -2,
            heartbeat_interval_ms: 0,
            assignment: None,
        },
        None,
    ))
}

fn validate_instance_id(
    group: &ConsumerGroupState,
    request: &ConsumerGroupHeartbeat,
) -> Result<(), ControlError> {
    let Some(instance_id) = request.instance_id.as_deref() else {
        return Ok(());
    };
    if let Some(member) = group.members.values().find(|member| {
        member.instance_id.as_deref() == Some(instance_id) && member.member_id != request.member_id
    }) {
        return Err(ControlError::UnreleasedInstanceId {
            group: request.group_id.clone(),
            instance_id: instance_id.to_owned(),
            member: member.member_id.clone(),
        });
    }
    Ok(())
}

fn joining_member(
    group: &mut ConsumerGroupState,
    request: &ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
) -> Result<(Option<ConsumerMemberState>, bool, bool), ControlError> {
    if let Some(instance_id) = request.instance_id.as_deref()
        && let Some((old_member_id, mut member)) = group
            .members
            .iter()
            .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
            .map(|(member_id, member)| (member_id.clone(), member.clone()))
    {
        if member.member_epoch != -2 {
            return Err(ControlError::UnreleasedInstanceId {
                group: request.group_id.clone(),
                instance_id: instance_id.to_owned(),
                member: old_member_id,
            });
        }
        group.members.remove(&old_member_id);
        member.member_id = request.member_id.clone();
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        member.owned_assignment = normalize_owned(
            request.owned_partitions.as_deref().unwrap_or_default(),
            topics,
        );
        return Ok((Some(member), false, true));
    }

    let existing = group.members.get(&request.member_id).cloned();
    let new_member = existing.is_none();
    let mut member = existing.unwrap_or_default();
    member.member_id = request.member_id.clone();
    if new_member {
        member.owned_assignment = normalize_owned(
            request.owned_partitions.as_deref().unwrap_or_default(),
            topics,
        );
    }
    Ok((Some(member), new_member, false))
}

fn update_member(
    member: &mut ConsumerMemberState,
    request: &ConsumerGroupHeartbeat,
    topics: &[TopicInfo],
    now: DateTime<Utc>,
) -> Result<bool, ControlError> {
    let previous = (
        member.instance_id.clone(),
        member.rack_id.clone(),
        member.rebalance_timeout_ms,
        member.subscribed_topic_names.clone(),
        member.subscribed_topic_regex.clone(),
    );
    if let Some(instance_id) = request.instance_id.as_ref() {
        member.instance_id = Some(instance_id.clone());
    }
    if let Some(rack_id) = request.rack_id.as_ref() {
        member.rack_id = Some(rack_id.clone());
    }
    if request.rebalance_timeout_ms >= 0 {
        member.rebalance_timeout_ms = request.rebalance_timeout_ms;
    }
    if let Some(names) = request.subscribed_topic_names.as_ref() {
        let mut names = names.clone();
        names.sort();
        names.dedup();
        member.subscribed_topic_names = names;
    }
    if let Some(pattern) = request.subscribed_topic_regex.as_ref() {
        regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
            ControlError::InvalidRequest(format!("invalid subscribed topic regex: {error}"))
        })?;
        member.subscribed_topic_regex = Some(pattern.clone());
    }
    if member.subscribed_topic_names.is_empty() && member.subscribed_topic_regex.is_none() {
        return Err(ControlError::InvalidRequest(
            "a consumer member must subscribe to topics or a topic regex".to_owned(),
        ));
    }
    if let Some(owned) = request.owned_partitions.as_deref() {
        member.owned_assignment = normalize_owned(owned, topics);
    }
    member.session_timeout_ms = request.session_timeout_ms;
    member.client_id = request.client_id.clone();
    member.client_host = request.client_host.clone();
    member.last_heartbeat = now;
    Ok(previous
        != (
            member.instance_id.clone(),
            member.rack_id.clone(),
            member.rebalance_timeout_ms,
            member.subscribed_topic_names.clone(),
            member.subscribed_topic_regex.clone(),
        ))
}

fn update_regex_refresh_state(
    group: &mut ConsumerGroupState,
    topics: &[TopicInfo],
    regex_introduced: bool,
    refresh_interval_ms: i32,
    now: DateTime<Utc>,
) -> Result<(), ControlError> {
    let regexes = group_regexes(group)?;
    if regexes.is_empty() {
        group.regex_refresh_timestamp = None;
        group.regex_refresh_pending = false;
        return Ok(());
    }
    let elapsed_ms = group.regex_refresh_timestamp.map_or(i64::MAX, |last| {
        now.signed_duration_since(last).num_milliseconds().max(0)
    });
    if regex_introduced
        || group.regex_refresh_timestamp.is_none()
        || elapsed_ms >= i64::from(refresh_interval_ms)
        || (elapsed_ms >= REGEX_REFRESH_MIN_INTERVAL_MS
            && regex_resolution_changed(group, topics, &regexes))
    {
        group.regex_refresh_pending = true;
    }
    Ok(())
}

fn group_regexes(group: &ConsumerGroupState) -> Result<Vec<regex::Regex>, ControlError> {
    group
        .members
        .values()
        .filter_map(|member| member.subscribed_topic_regex.as_deref())
        .map(|pattern| {
            regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
                ControlError::InvalidRequest(format!("invalid subscribed topic regex: {error}"))
            })
        })
        .collect()
}

fn cached_regex_topic_ids(
    group: &ConsumerGroupState,
    topics: &[TopicInfo],
    regexes: &[regex::Regex],
) -> BTreeSet<Uuid> {
    let topic_names = topics
        .iter()
        .map(|topic| (topic.id, topic.name.as_str()))
        .collect::<HashMap<_, _>>();
    group
        .members
        .values()
        .flat_map(|member| &member.target_assignment)
        .filter(|assignment| {
            topic_names
                .get(&assignment.topic_id)
                .is_none_or(|name| regexes.iter().any(|regex| regex.is_match(name)))
        })
        .map(|assignment| assignment.topic_id)
        .collect()
}

fn regex_resolution_changed(
    group: &ConsumerGroupState,
    topics: &[TopicInfo],
    regexes: &[regex::Regex],
) -> bool {
    let resolved = topics
        .iter()
        .filter(|topic| regexes.iter().any(|regex| regex.is_match(&topic.name)))
        .map(|topic| topic.id)
        .collect::<BTreeSet<_>>();
    resolved != cached_regex_topic_ids(group, topics, regexes)
}

fn refresh_target_assignment(
    group: &mut ConsumerGroupState,
    topics: &[TopicInfo],
    metadata_changed: bool,
    now: DateTime<Utc>,
) -> Result<(), ControlError> {
    if metadata_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
    }
    if group.group_epoch > group.assignment_epoch
        && !assignment_interval::can_compute(
            group.assignment_timestamp,
            group.assignment_interval_ms,
            now,
        )
    {
        return Ok(());
    }

    let regexes = group_regexes(group)?;
    let cached_regex_topics = (!group.regex_refresh_pending && !regexes.is_empty())
        .then(|| cached_regex_topic_ids(group, topics, &regexes));
    let target = assign_with_regex_topics(group, topics, cached_regex_topics.as_ref())?;
    let target_changed = group.members.iter().any(|(member_id, member)| {
        assignment_set(&member.target_assignment)
            != assignment_set(target.get(member_id).map(Vec::as_slice).unwrap_or_default())
    });
    if !metadata_changed && group.group_epoch == group.assignment_epoch && target_changed {
        group.group_epoch = group.group_epoch.saturating_add(1);
        if !assignment_interval::can_compute(
            group.assignment_timestamp,
            group.assignment_interval_ms,
            now,
        ) {
            return Ok(());
        }
    }
    if group.group_epoch > group.assignment_epoch {
        group.assignment_epoch = group.group_epoch;
        group.assignment_timestamp = Some(now);
        for (member_id, member) in &mut group.members {
            member.target_assignment = target.get(member_id).cloned().unwrap_or_default();
        }
    }
    if group.regex_refresh_pending {
        group.regex_refresh_timestamp = Some(now);
        group.regex_refresh_pending = false;
    }
    Ok(())
}

fn defer_target_assignment(
    group: &mut ConsumerGroupState,
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
    group: &mut ConsumerGroupState,
    topics: &[TopicInfo],
    task: &crate::GroupAssignmentTask,
    now: DateTime<Utc>,
) -> Result<crate::GroupAssignmentCompletion, ControlError> {
    if !matches_assignment_task(group, task) {
        return Ok(crate::GroupAssignmentCompletion::Stale);
    }
    let previous_epoch = group.assignment_epoch;
    refresh_target_assignment(group, topics, false, now)?;
    if group.assignment_epoch == previous_epoch {
        group.assignment_timestamp = Some(now);
        Ok(crate::GroupAssignmentCompletion::Unchanged)
    } else {
        Ok(crate::GroupAssignmentCompletion::Published)
    }
}

fn assignment_task(group: &ConsumerGroupState) -> crate::GroupAssignmentTask {
    crate::GroupAssignmentTask {
        protocol: crate::AssignmentProtocol::Consumer,
        group_id: group.group_id.clone(),
        group_epoch: group.group_epoch,
        assignment_epoch: group.assignment_epoch,
        assignment_timestamp: group.assignment_timestamp,
    }
}

fn matches_assignment_task(group: &ConsumerGroupState, task: &crate::GroupAssignmentTask) -> bool {
    task.protocol == crate::AssignmentProtocol::Consumer
        && task.group_id == group.group_id
        && task.group_epoch == group.group_epoch
        && task.assignment_epoch == group.assignment_epoch
        && task.assignment_timestamp == group.assignment_timestamp
}

fn reconcile_member(
    group: &mut ConsumerGroupState,
    member_id: &str,
    heartbeat_interval_ms: i32,
) -> ConsumerGroupHeartbeatResult {
    let owned_by_others = group
        .members
        .iter()
        .filter(|(other_id, _)| other_id.as_str() != member_id)
        .flat_map(|(_, member)| assignment_set(&member.owned_assignment))
        .collect::<HashSet<_>>();
    let member = group
        .members
        .get_mut(member_id)
        .expect("heartbeat member exists");
    let owned = assignment_set(&member.owned_assignment);
    let target = assignment_set(&member.target_assignment);
    let previous_current = assignment_set(&member.current_assignment);
    let previous_assignment_epochs = member.assignment_epochs.clone();

    let (next, next_epoch) = if !owned.is_subset(&target) {
        (
            owned.intersection(&target).copied().collect(),
            member.member_epoch,
        )
    } else {
        (
            target
                .iter()
                .filter(|partition| !owned_by_others.contains(partition))
                .copied()
                .collect(),
            group.assignment_epoch,
        )
    };
    member.current_assignment = assignment_from_set(&next, &member.target_assignment);
    let pending_revocation = owned
        .difference(&target)
        .filter(|partition| previous_assignment_epochs.contains_key(*partition))
        .copied()
        .collect::<BTreeSet<_>>();
    member.assignment_epochs = next
        .union(&pending_revocation)
        .map(|partition| {
            (
                *partition,
                previous_assignment_epochs
                    .get(partition)
                    .copied()
                    .unwrap_or(next_epoch.max(0)),
            )
        })
        .collect();
    if member.member_epoch == next_epoch && previous_current != next {
        member.previous_member_epoch = member.member_epoch;
    }
    member_epoch::update(
        &mut member.member_epoch,
        &mut member.previous_member_epoch,
        next_epoch,
    );
    let converged = owned == next && next == target;
    let assignment_changed = previous_current != next;
    ConsumerGroupHeartbeatResult {
        member_id: member_id.to_owned(),
        member_epoch: member.member_epoch,
        heartbeat_interval_ms,
        assignment: (!converged || assignment_changed).then(|| member.current_assignment.clone()),
    }
}

fn normalize_owned(
    owned: &[ConsumerOwnedTopicPartitions],
    topics: &[TopicInfo],
) -> Vec<ConsumerTopicAssignment> {
    let known = topics
        .iter()
        .map(|topic| (topic.id, topic))
        .collect::<HashMap<_, _>>();
    let mut by_topic = BTreeMap::<Uuid, BTreeSet<i32>>::new();
    for assignment in owned {
        let Some(topic) = known.get(&assignment.topic_id) else {
            continue;
        };
        by_topic.entry(assignment.topic_id).or_default().extend(
            assignment
                .partitions
                .iter()
                .copied()
                .filter(|partition| (0..topic.partitions).contains(partition)),
        );
    }
    by_topic
        .into_iter()
        .map(|(topic_id, partitions)| ConsumerTopicAssignment {
            topic_id,
            topic_name: known[&topic_id].name.clone(),
            partitions: partitions.into_iter().collect(),
        })
        .collect()
}

fn assignment_set(assignment: &[ConsumerTopicAssignment]) -> BTreeSet<(Uuid, i32)> {
    assignment
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(move |partition| (topic.topic_id, *partition))
        })
        .collect()
}

fn owned_set(assignment: &[ConsumerOwnedTopicPartitions]) -> BTreeSet<(Uuid, i32)> {
    assignment
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(move |partition| (topic.topic_id, *partition))
        })
        .collect()
}

fn assignment_from_set(
    assignment: &BTreeSet<(Uuid, i32)>,
    topic_source: &[ConsumerTopicAssignment],
) -> Vec<ConsumerTopicAssignment> {
    let names = topic_source
        .iter()
        .map(|topic| (topic.topic_id, topic.topic_name.as_str()))
        .collect::<HashMap<_, _>>();
    let mut by_topic = BTreeMap::<Uuid, Vec<i32>>::new();
    for (topic_id, partition) in assignment {
        by_topic.entry(*topic_id).or_default().push(*partition);
    }
    by_topic
        .into_iter()
        .map(|(topic_id, partitions)| ConsumerTopicAssignment {
            topic_id,
            topic_name: names.get(&topic_id).copied().unwrap_or_default().to_owned(),
            partitions,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(group: &str, member: &str, epoch: i32, topic: &str) -> ConsumerGroupHeartbeat {
        ConsumerGroupHeartbeat {
            group_id: group.to_owned(),
            member_id: member.to_owned(),
            member_epoch: epoch,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: Some(vec![topic.to_owned()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: Some(Vec::new()),
            client_id: "test-client".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        }
    }

    fn topic(name: &str, partitions: i32) -> TopicInfo {
        TopicInfo {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            partitions,
        }
    }

    fn owned(assignment: &[ConsumerTopicAssignment]) -> Vec<ConsumerOwnedTopicPartitions> {
        assignment
            .iter()
            .map(|topic| ConsumerOwnedTopicPartitions {
                topic_id: topic.topic_id,
                partitions: topic.partitions.clone(),
            })
            .collect()
    }

    #[test]
    fn member_joins_and_acknowledges_assignment() {
        let topic = topic("orders", 2);
        let now = Utc::now();
        let (state, joined) = heartbeat(
            None,
            request("group", "member-a", 0, "orders"),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        assert_eq!(joined.member_epoch, 2);
        assert_eq!(joined.assignment.as_ref().unwrap()[0].partitions, [0, 1]);

        let mut next = request("group", "member-a", joined.member_epoch, "orders");
        next.subscribed_topic_names = None;
        next.owned_partitions = Some(owned(joined.assignment.as_ref().unwrap()));
        let (state, acknowledged) = heartbeat(
            Some(state),
            next,
            std::slice::from_ref(&topic),
            now + Duration::seconds(1),
        )
        .unwrap();
        assert!(acknowledged.assignment.is_none());
        assert_eq!(describe(&state).state, "Stable");
    }

    #[test]
    fn partition_moves_only_after_old_owner_revokes_it() {
        let topic = topic("orders", 2);
        let now = Utc::now();
        let (state, first) = heartbeat(
            None,
            request("group", "member-a", 0, "orders"),
            std::slice::from_ref(&topic),
            now,
        )
        .unwrap();
        let mut acknowledge = request("group", "member-a", first.member_epoch, "orders");
        acknowledge.subscribed_topic_names = None;
        acknowledge.owned_partitions = Some(owned(first.assignment.as_ref().unwrap()));
        let (state, _) = heartbeat(
            Some(state),
            acknowledge,
            std::slice::from_ref(&topic),
            now + Duration::seconds(1),
        )
        .unwrap();

        let (state, second) = heartbeat(
            Some(state),
            request("group", "member-b", 0, "orders"),
            std::slice::from_ref(&topic),
            now + Duration::seconds(2),
        )
        .unwrap();
        assert!(second.assignment.as_ref().unwrap().is_empty());

        let mut revoke = request("group", "member-a", first.member_epoch, "orders");
        revoke.subscribed_topic_names = None;
        revoke.owned_partitions = None;
        let (state, revoke_response) = heartbeat(
            Some(state),
            revoke,
            std::slice::from_ref(&topic),
            now + Duration::seconds(3),
        )
        .unwrap();
        assert_eq!(revoke_response.member_epoch, first.member_epoch);
        assert_eq!(
            revoke_response.assignment.as_ref().unwrap()[0].partitions,
            [0]
        );

        let mut revoked = request("group", "member-a", first.member_epoch, "orders");
        revoked.subscribed_topic_names = None;
        revoked.owned_partitions = Some(owned(revoke_response.assignment.as_ref().unwrap()));
        let (state, advanced) = heartbeat(
            Some(state),
            revoked,
            std::slice::from_ref(&topic),
            now + Duration::seconds(4),
        )
        .unwrap();
        assert_eq!(advanced.member_epoch, 3);
        let partitions = [
            PartitionKey::new("orders", 0),
            PartitionKey::new("orders", 1),
        ];
        assert_eq!(
            validate_offset_commit(&state, "member-a", first.member_epoch, &partitions).unwrap(),
            [true, false]
        );
        assert_eq!(
            validate_offset_commit(&state, "member-a", advanced.member_epoch, &partitions[1..],)
                .unwrap(),
            [true]
        );
        assert!(matches!(
            validate_transaction_offset_commit(
                &state,
                "member-a",
                None,
                first.member_epoch,
                &partitions,
            ),
            Err(ControlError::FencedMemberEpoch { .. })
        ));

        let mut acquire = request("group", "member-b", second.member_epoch, "orders");
        acquire.subscribed_topic_names = None;
        acquire.owned_partitions = Some(Vec::new());
        let (_, acquired) = heartbeat(
            Some(state),
            acquire,
            std::slice::from_ref(&topic),
            now + Duration::seconds(5),
        )
        .unwrap();
        assert_eq!(acquired.assignment.as_ref().unwrap()[0].partitions, [1]);
    }

    #[test]
    fn static_rejoin_retains_assignment_from_epoch_zero() {
        let topic = topic("orders", 2);
        let now = Utc::now();
        let mut join = request("group", "member-a", 0, "orders");
        join.instance_id = Some("instance-a".to_owned());
        join.max_size = 1;
        let (state, joined) = heartbeat(None, join, std::slice::from_ref(&topic), now).unwrap();

        let mut acknowledge = request("group", "member-a", joined.member_epoch, "orders");
        acknowledge.instance_id = Some("instance-a".to_owned());
        acknowledge.subscribed_topic_names = None;
        acknowledge.owned_partitions = Some(owned(joined.assignment.as_ref().unwrap()));
        let (state, _) = heartbeat(
            Some(state),
            acknowledge,
            std::slice::from_ref(&topic),
            now + Duration::seconds(1),
        )
        .unwrap();

        let mut leave = request("group", "member-a", -2, "orders");
        leave.instance_id = Some("instance-a".to_owned());
        leave.subscribed_topic_names = None;
        leave.owned_partitions = None;
        let (state, left) = heartbeat(
            Some(state),
            leave,
            std::slice::from_ref(&topic),
            now + Duration::seconds(2),
        )
        .unwrap();
        assert_eq!(left.member_epoch, -2);
        assert!(
            state.members["member-a"]
                .assignment_epochs
                .values()
                .all(|epoch| *epoch == 0)
        );

        let mut rejoin = request("group", "member-b", 0, "orders");
        rejoin.instance_id = Some("instance-a".to_owned());
        rejoin.max_size = 1;
        let (state, rejoined) = heartbeat(
            Some(state),
            rejoin,
            std::slice::from_ref(&topic),
            now + Duration::seconds(3),
        )
        .unwrap();
        assert_eq!(rejoined.member_epoch, joined.member_epoch);
        assert_eq!(
            validate_offset_commit(
                &state,
                "member-b",
                0,
                &[
                    PartitionKey::new("orders", 0),
                    PartitionKey::new("orders", 1)
                ],
            )
            .unwrap(),
            [true, true]
        );
    }

    #[test]
    fn assignment_updates_are_batched_until_the_interval_elapses() {
        let topic = topic("orders", 2);
        let topics = std::slice::from_ref(&topic);
        let now = Utc::now();
        let mut join = request("group", "member-a", 0, "orders");
        join.assignment_interval_ms = 1_000;
        let (state, first) = heartbeat(None, join, topics, now).unwrap();
        assert_eq!(state.group_epoch, 2);
        assert_eq!(state.assignment_epoch, 2);
        assert_eq!(state.assignment_timestamp, Some(now));

        let mut acknowledge = request("group", "member-a", first.member_epoch, "orders");
        acknowledge.subscribed_topic_names = None;
        acknowledge.owned_partitions = first.assignment.as_deref().map(owned);
        acknowledge.assignment_interval_ms = 1_000;
        let (state, _) = heartbeat(
            Some(state),
            acknowledge,
            topics,
            now + Duration::milliseconds(50),
        )
        .unwrap();

        let mut second_join = request("group", "member-b", 0, "orders");
        second_join.assignment_interval_ms = 1_000;
        let (state, second) = heartbeat(
            Some(state),
            second_join,
            topics,
            now + Duration::milliseconds(100),
        )
        .unwrap();
        assert_eq!(state.group_epoch, 3);
        assert_eq!(state.assignment_epoch, 2);
        assert_eq!(second.member_epoch, 2);
        assert_eq!(describe(&state).state, "Assigning");

        let mut poll = request("group", "member-b", second.member_epoch, "orders");
        poll.subscribed_topic_names = None;
        poll.assignment_interval_ms = 1_000;
        let (state, delayed) = heartbeat(
            Some(state),
            poll.clone(),
            topics,
            now + Duration::milliseconds(999),
        )
        .unwrap();
        assert_eq!(state.assignment_epoch, 2);
        assert_eq!(delayed.member_epoch, 2);

        let (state, assigned) = heartbeat(
            Some(state),
            poll,
            topics,
            now + Duration::milliseconds(1_000),
        )
        .unwrap();
        assert_eq!(state.group_epoch, 3);
        assert_eq!(state.assignment_epoch, 3);
        assert_eq!(
            state.assignment_timestamp,
            Some(now + Duration::milliseconds(1_000))
        );
        assert_eq!(assigned.member_epoch, 3);
    }

    #[test]
    fn regex_topics_refresh_after_metadata_minimum_and_configured_maximum() {
        let first_topic = topic("orders-1", 1);
        let second_topic = topic("orders-2", 1);
        let now = Utc::now();

        let mut join = request("regex-group", "member-a", 0, "unused");
        join.subscribed_topic_names = Some(Vec::new());
        join.subscribed_topic_regex = Some("orders-.*".to_owned());
        join.regex_refresh_interval_ms = 20_000;
        let (state, joined) =
            heartbeat(None, join, std::slice::from_ref(&first_topic), now).unwrap();
        assert_eq!(state.regex_refresh_timestamp, Some(now));
        assert_eq!(
            state.members["member-a"].target_assignment[0].topic_id,
            first_topic.id
        );

        let mut second_join = request("regex-group", "member-b", 0, "unused");
        second_join.subscribed_topic_names = Some(Vec::new());
        second_join.subscribed_topic_regex = Some("orders-.*".to_owned());
        second_join.regex_refresh_interval_ms = 20_000;
        let topics = [first_topic.clone(), second_topic.clone()];
        let (state, _) = heartbeat(
            Some(state),
            second_join,
            &topics,
            now + Duration::seconds(5),
        )
        .unwrap();
        let target_topic_ids = |state: &ConsumerGroupState| {
            state
                .members
                .values()
                .flat_map(|member| &member.target_assignment)
                .map(|assignment| assignment.topic_id)
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(target_topic_ids(&state), BTreeSet::from([first_topic.id]));

        let mut early = request(
            "regex-group",
            "member-a",
            state.members["member-a"].member_epoch,
            "unused",
        );
        early.subscribed_topic_names = None;
        early.subscribed_topic_regex = None;
        early.regex_refresh_interval_ms = 20_000;
        early.owned_partitions = Some(owned(&state.members["member-a"].current_assignment));
        let (state, _) = heartbeat(
            Some(state),
            early,
            &topics,
            now + Duration::milliseconds(9_999),
        )
        .unwrap();
        assert_eq!(target_topic_ids(&state), BTreeSet::from([first_topic.id]));
        assert_eq!(state.regex_refresh_timestamp, Some(now));

        let mut refresh = request(
            "regex-group",
            "member-a",
            state.members["member-a"].member_epoch,
            "unused",
        );
        refresh.subscribed_topic_names = None;
        refresh.subscribed_topic_regex = None;
        refresh.regex_refresh_interval_ms = 20_000;
        refresh.owned_partitions = Some(owned(&state.members["member-a"].current_assignment));
        let (state, _) =
            heartbeat(Some(state), refresh, &topics, now + Duration::seconds(10)).unwrap();
        assert_eq!(
            target_topic_ids(&state),
            BTreeSet::from([first_topic.id, second_topic.id])
        );
        assert_eq!(
            state.regex_refresh_timestamp,
            Some(now + Duration::seconds(10))
        );

        let mut before_max = request(
            "regex-group",
            "member-a",
            state.members["member-a"].member_epoch,
            "unused",
        );
        before_max.subscribed_topic_names = None;
        before_max.subscribed_topic_regex = None;
        before_max.regex_refresh_interval_ms = 20_000;
        before_max.owned_partitions = Some(owned(&state.members["member-a"].current_assignment));
        let (state, _) = heartbeat(
            Some(state),
            before_max,
            &topics,
            now + Duration::milliseconds(29_999),
        )
        .unwrap();
        assert_eq!(
            state.regex_refresh_timestamp,
            Some(now + Duration::seconds(10))
        );

        let mut at_max = request(
            "regex-group",
            "member-a",
            state.members["member-a"].member_epoch,
            "unused",
        );
        at_max.subscribed_topic_names = None;
        at_max.subscribed_topic_regex = None;
        at_max.regex_refresh_interval_ms = 20_000;
        at_max.owned_partitions = Some(owned(&state.members["member-a"].current_assignment));
        let (state, _) =
            heartbeat(Some(state), at_max, &topics, now + Duration::seconds(30)).unwrap();
        assert_eq!(
            state.regex_refresh_timestamp,
            Some(now + Duration::seconds(30))
        );
        assert!(!state.regex_refresh_pending);
        assert_eq!(joined.assignment.unwrap()[0].topic_id, first_topic.id);
    }

    #[test]
    fn stale_epoch_is_fenced() {
        let topic = topic("orders", 1);
        let (state, joined) = heartbeat(
            None,
            request("group", "member-a", 0, "orders"),
            std::slice::from_ref(&topic),
            Utc::now(),
        )
        .unwrap();
        let error = heartbeat(
            Some(state),
            request("group", "member-a", joined.member_epoch + 1, "orders"),
            &[topic],
            Utc::now(),
        )
        .unwrap_err();
        assert!(matches!(error, ControlError::FencedMemberEpoch { .. }));
    }
}
