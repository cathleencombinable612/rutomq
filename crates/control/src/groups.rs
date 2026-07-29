use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocols: Vec<GroupProtocol>,
    pub protocol_name: String,
    pub metadata: Vec<u8>,
    pub subscribed_topics: Vec<String>,
    pub client_id: String,
    pub client_host: String,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub last_heartbeat: DateTime<Utc>,
    pub joined_rebalance_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResult {
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<GroupMember>,
    pub skip_assignment: bool,
    pub pending_rebalance: Option<Uuid>,
    pub retry_after_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignment {
    pub member_id: String,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberIdentity {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveGroupMemberError {
    UnknownMemberId,
    FencedInstanceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberResult {
    pub identity: GroupMemberIdentity,
    pub error: Option<LeaveGroupMemberError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSummary {
    pub group_id: String,
    pub protocol_type: String,
    pub state: String,
    pub group_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicGroupDescription {
    pub group_id: String,
    pub state: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<ClassicGroupMemberDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicGroupMemberDescription {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

pub(crate) fn classic_group_state(
    member_count: usize,
    assignment_count: usize,
    preparing_rebalance: bool,
) -> &'static str {
    if preparing_rebalance {
        "PreparingRebalance"
    } else if member_count == 0 {
        "Empty"
    } else if member_count == assignment_count {
        "Stable"
    } else {
        "CompletingRebalance"
    }
}

pub(crate) fn protocols(protocols: &[(String, Vec<u8>)]) -> Vec<GroupProtocol> {
    protocols
        .iter()
        .map(|(name, metadata)| GroupProtocol {
            name: name.clone(),
            metadata: metadata.clone(),
        })
        .collect()
}

pub(crate) fn select_protocol(protocol_sets: &[&[GroupProtocol]]) -> Option<String> {
    let first = protocol_sets.first()?;
    let mut candidates = first
        .iter()
        .map(|protocol| protocol.name.clone())
        .collect::<BTreeSet<_>>();
    candidates.retain(|name| {
        protocol_sets
            .iter()
            .all(|protocols| protocols.iter().any(|protocol| &protocol.name == name))
    });
    if candidates.is_empty() {
        return None;
    }

    let mut votes = BTreeMap::<String, usize>::new();
    for protocols in protocol_sets {
        let vote = protocols
            .iter()
            .find(|protocol| candidates.contains(&protocol.name))?;
        *votes.entry(vote.name.clone()).or_default() += 1;
    }
    votes
        .into_iter()
        .max_by(|(left_name, left_votes), (right_name, right_votes)| {
            left_votes
                .cmp(right_votes)
                .then_with(|| right_name.cmp(left_name))
        })
        .map(|(name, _)| name)
}

pub(crate) fn select_member_protocol(member: &mut GroupMember, protocol_name: &str) {
    if let Some(protocol) = member
        .protocols
        .iter()
        .find(|protocol| protocol.name == protocol_name)
    {
        member.protocol_name = protocol_name.to_owned();
        member.metadata.clone_from(&protocol.metadata);
    }
}
