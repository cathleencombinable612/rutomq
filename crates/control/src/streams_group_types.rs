use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const STREAMS_STATUS_STALE_TOPOLOGY: i8 = 0;
pub const STREAMS_STATUS_MISSING_SOURCE_TOPICS: i8 = 1;
pub const STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS: i8 = 2;
pub const STREAMS_STATUS_MISSING_INTERNAL_TOPICS: i8 = 3;
pub const STREAMS_STATUS_SHUTDOWN_APPLICATION: i8 = 4;
pub const STREAMS_STATUS_ASSIGNMENT_DELAYED: i8 = 5;

#[derive(Debug, Clone)]
pub struct StreamsGroupHeartbeat {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub endpoint_information_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub topology: Option<StreamsTopology>,
    pub owned_assignment: Option<StreamsTaskAssignment>,
    pub process_id: Option<String>,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Option<Vec<StreamsKeyValue>>,
    pub task_offsets: Option<Vec<StreamsTaskOffset>>,
    pub task_end_offsets: Option<Vec<StreamsTaskOffset>>,
    pub shutdown_application: bool,
    pub client_id: String,
    pub client_host: String,
    pub heartbeat_interval_ms: i32,
    pub session_timeout_ms: i32,
    pub max_size: i32,
    pub assignment_interval_ms: i32,
    pub num_standby_replicas: i32,
    pub initial_rebalance_delay_ms: i32,
    pub acceptable_recovery_lag: i32,
    pub task_offset_interval_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatResult {
    pub member_id: String,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub acceptable_recovery_lag: i32,
    pub task_offset_interval_ms: i32,
    pub statuses: Vec<StreamsGroupStatus>,
    pub assignment: Option<StreamsTaskAssignment>,
    pub endpoint_information_epoch: i32,
    pub partitions_by_user_endpoint: Option<Vec<StreamsEndpointPartitions>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsGroupStatus {
    pub code: i8,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsKeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsTaskOffset {
    pub subtopology_id: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamsTaskId {
    pub subtopology_id: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsTaskAssignment {
    pub active_tasks: Vec<StreamsTaskId>,
    pub standby_tasks: Vec<StreamsTaskId>,
    pub warmup_tasks: Vec<StreamsTaskId>,
}

impl StreamsTaskAssignment {
    pub(crate) fn normalized(mut self) -> Self {
        normalize_tasks(&mut self.active_tasks);
        normalize_tasks(&mut self.standby_tasks);
        normalize_tasks(&mut self.warmup_tasks);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsEndpointPartitions {
    pub endpoint: StreamsEndpoint,
    pub active_partitions: Vec<StreamsTopicPartitions>,
    pub standby_partitions: Vec<StreamsTopicPartitions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsTopicPartitions {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsTopology {
    pub epoch: i32,
    pub subtopologies: Vec<StreamsSubtopology>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub state_changelog_topics: Vec<StreamsInternalTopic>,
    pub repartition_sink_topics: Vec<String>,
    pub repartition_source_topics: Vec<StreamsInternalTopic>,
    pub copartition_groups: Vec<StreamsCopartitionGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsInternalTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<StreamsKeyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsInternalTopicRequirement {
    pub topic: StreamsInternalTopic,
    pub partitions: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamsCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupDescription {
    pub group_id: String,
    pub state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology: StreamsTopology,
    pub topology_ready: bool,
    pub members: Vec<StreamsGroupMemberDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMemberDescription {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub topology_epoch: i32,
    pub process_id: String,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Vec<StreamsKeyValue>,
    pub task_offsets: Vec<StreamsTaskOffset>,
    pub task_end_offsets: Vec<StreamsTaskOffset>,
    pub assignment: StreamsTaskAssignment,
    pub target_assignment: StreamsTaskAssignment,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamsGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignment_timestamp: Option<DateTime<Utc>>,
    pub assignment_interval_ms: i32,
    pub endpoint_information_epoch: i32,
    pub topology: StreamsTopology,
    pub statuses: Vec<StreamsGroupStatus>,
    pub shutdown_requested: bool,
    pub num_standby_replicas: i32,
    pub initial_rebalance_deadline: Option<DateTime<Utc>>,
    pub members: HashMap<String, StreamsMemberState>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamsMemberState {
    pub member_id: String,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub topology_epoch: i32,
    pub process_id: String,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Vec<StreamsKeyValue>,
    pub task_offsets: Vec<StreamsTaskOffset>,
    pub task_end_offsets: Vec<StreamsTaskOffset>,
    pub client_id: String,
    pub client_host: String,
    pub current_assignment: StreamsTaskAssignment,
    pub target_assignment: StreamsTaskAssignment,
    pub owned_assignment: StreamsTaskAssignment,
    pub last_heartbeat: DateTime<Utc>,
}

fn normalize_tasks(tasks: &mut Vec<StreamsTaskId>) {
    tasks.sort();
    tasks.dedup();
}
