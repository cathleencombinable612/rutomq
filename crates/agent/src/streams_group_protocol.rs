use super::topic_name;
use crate::kafka_error::{
    FENCED_MEMBER_EPOCH, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED, INVALID_REQUEST, NO_ERROR,
    UNKNOWN_MEMBER_ID, UNKNOWN_SERVER_ERROR, UNRELEASED_INSTANCE_ID,
};
use kafka_protocol::messages::streams_group_describe_response::{
    Assignment as DescribeAssignment, DescribedGroup, Endpoint as DescribeEndpoint,
    KeyValue as DescribeKeyValue, Member as DescribeMember, Subtopology as DescribeSubtopology,
    TaskIds as DescribeTaskIds, TaskOffset as DescribeTaskOffset, TopicInfo as DescribeTopicInfo,
    Topology as DescribeTopology,
};
use kafka_protocol::messages::streams_group_heartbeat_response::{
    Endpoint as HeartbeatEndpoint, EndpointToPartitions, Status as HeartbeatStatus,
    TaskIds as HeartbeatTaskIds, TopicPartition,
};
use kafka_protocol::messages::{
    GroupId, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    ControlError, StreamsCopartitionGroup, StreamsEndpoint, StreamsEndpointPartitions,
    StreamsGroupDescription, StreamsGroupStatus, StreamsInternalTopic, StreamsKeyValue,
    StreamsSubtopology, StreamsTaskAssignment, StreamsTaskId, StreamsTaskOffset, StreamsTopology,
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn topology_from_request(
    topology: &kafka_protocol::messages::streams_group_heartbeat_request::Topology,
) -> StreamsTopology {
    StreamsTopology {
        epoch: topology.epoch,
        subtopologies: topology
            .subtopologies
            .iter()
            .map(|subtopology| StreamsSubtopology {
                subtopology_id: subtopology.subtopology_id.as_str().to_owned(),
                source_topics: subtopology
                    .source_topics
                    .iter()
                    .map(|topic| topic.as_str().to_owned())
                    .collect(),
                source_topic_regex: subtopology
                    .source_topic_regex
                    .iter()
                    .map(|pattern| pattern.as_str().to_owned())
                    .collect(),
                state_changelog_topics: subtopology
                    .state_changelog_topics
                    .iter()
                    .map(internal_topic_from_request)
                    .collect(),
                repartition_sink_topics: subtopology
                    .repartition_sink_topics
                    .iter()
                    .map(|topic| topic.as_str().to_owned())
                    .collect(),
                repartition_source_topics: subtopology
                    .repartition_source_topics
                    .iter()
                    .map(internal_topic_from_request)
                    .collect(),
                copartition_groups: subtopology
                    .copartition_groups
                    .iter()
                    .map(|group| StreamsCopartitionGroup {
                        source_topics: group.source_topics.clone(),
                        source_topic_regex: group.source_topic_regex.clone(),
                        repartition_source_topics: group.repartition_source_topics.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn internal_topic_from_request(
    topic: &kafka_protocol::messages::streams_group_heartbeat_request::TopicInfo,
) -> StreamsInternalTopic {
    StreamsInternalTopic {
        name: topic.name.as_str().to_owned(),
        partitions: topic.partitions,
        replication_factor: topic.replication_factor,
        topic_configs: topic
            .topic_configs
            .iter()
            .map(|config| StreamsKeyValue {
                key: config.key.as_str().to_owned(),
                value: config.value.as_str().to_owned(),
            })
            .collect(),
    }
}

pub(super) fn owned_assignment(
    request: &StreamsGroupHeartbeatRequest,
) -> Result<Option<StreamsTaskAssignment>, &'static str> {
    match (
        request.active_tasks.as_ref(),
        request.standby_tasks.as_ref(),
        request.warmup_tasks.as_ref(),
    ) {
        (None, None, None) => Ok(None),
        (Some(active), Some(standby), Some(warmup)) => Ok(Some(StreamsTaskAssignment {
            active_tasks: tasks_from_request(active),
            standby_tasks: tasks_from_request(standby),
            warmup_tasks: tasks_from_request(warmup),
        })),
        _ => Err("streams task collections must be all null or all non-null"),
    }
}

fn tasks_from_request(
    groups: &[kafka_protocol::messages::streams_group_heartbeat_request::TaskIds],
) -> Vec<StreamsTaskId> {
    groups
        .iter()
        .flat_map(|group| {
            group.partitions.iter().map(|partition| StreamsTaskId {
                subtopology_id: group.subtopology_id.as_str().to_owned(),
                partition: *partition,
            })
        })
        .collect()
}

pub(super) fn task_offsets_from_request(
    offsets: &[kafka_protocol::messages::streams_group_heartbeat_request::TaskOffset],
) -> Vec<StreamsTaskOffset> {
    offsets
        .iter()
        .map(|offset| StreamsTaskOffset {
            subtopology_id: offset.subtopology_id.as_str().to_owned(),
            partition: offset.partition,
            offset: offset.offset,
        })
        .collect()
}

pub(super) fn heartbeat_tasks(tasks: &[StreamsTaskId]) -> Vec<HeartbeatTaskIds> {
    grouped_tasks(tasks)
        .into_iter()
        .map(|(subtopology_id, partitions)| {
            HeartbeatTaskIds::default()
                .with_subtopology_id(string(subtopology_id))
                .with_partitions(partitions)
        })
        .collect()
}

fn describe_tasks(tasks: &[StreamsTaskId]) -> Vec<DescribeTaskIds> {
    grouped_tasks(tasks)
        .into_iter()
        .map(|(subtopology_id, partitions)| {
            DescribeTaskIds::default()
                .with_subtopology_id(string(subtopology_id))
                .with_partitions(partitions)
        })
        .collect()
}

fn grouped_tasks(tasks: &[StreamsTaskId]) -> BTreeMap<String, Vec<i32>> {
    let mut grouped = BTreeMap::<String, Vec<i32>>::new();
    for task in tasks {
        grouped
            .entry(task.subtopology_id.clone())
            .or_default()
            .push(task.partition);
    }
    for partitions in grouped.values_mut() {
        partitions.sort_unstable();
        partitions.dedup();
    }
    grouped
}

pub(super) fn heartbeat_status(status: &StreamsGroupStatus) -> HeartbeatStatus {
    HeartbeatStatus::default()
        .with_status_code(status.code)
        .with_status_detail(string(&status.detail))
}

pub(super) fn endpoint_partitions(
    endpoints: &[StreamsEndpointPartitions],
) -> Vec<EndpointToPartitions> {
    endpoints
        .iter()
        .map(|endpoint| {
            EndpointToPartitions::default()
                .with_user_endpoint(
                    HeartbeatEndpoint::default()
                        .with_host(string(&endpoint.endpoint.host))
                        .with_port(endpoint.endpoint.port),
                )
                .with_active_partitions(topic_partitions(&endpoint.active_partitions))
                .with_standby_partitions(topic_partitions(&endpoint.standby_partitions))
        })
        .collect()
}

fn topic_partitions(topics: &[rutomq_control::StreamsTopicPartitions]) -> Vec<TopicPartition> {
    topics
        .iter()
        .map(|topic| {
            TopicPartition::default()
                .with_topic(topic_name(&topic.topic))
                .with_partitions(topic.partitions.clone())
        })
        .collect()
}

pub(super) fn described_group(
    description: &StreamsGroupDescription,
    authorized_operations: i32,
) -> DescribedGroup {
    DescribedGroup::default()
        .with_error_code(NO_ERROR)
        .with_group_id(group_id(&description.group_id))
        .with_group_state(string(&description.state))
        .with_group_epoch(description.group_epoch)
        .with_assignment_epoch(description.assignment_epoch)
        .with_topology(Some(describe_topology(description)))
        .with_members(
            description
                .members
                .iter()
                .map(|member| {
                    DescribeMember::default()
                        .with_member_id(string(&member.member_id))
                        .with_member_epoch(member.member_epoch)
                        .with_instance_id(member.instance_id.as_deref().map(string))
                        .with_rack_id(member.rack_id.as_deref().map(string))
                        .with_client_id(string(&member.client_id))
                        .with_client_host(string(&member.client_host))
                        .with_topology_epoch(member.topology_epoch)
                        .with_process_id(string(&member.process_id))
                        .with_user_endpoint(member.user_endpoint.as_ref().map(describe_endpoint))
                        .with_client_tags(
                            member
                                .client_tags
                                .iter()
                                .map(|tag| {
                                    DescribeKeyValue::default()
                                        .with_key(string(&tag.key))
                                        .with_value(string(&tag.value))
                                })
                                .collect(),
                        )
                        .with_task_offsets(describe_task_offsets(&member.task_offsets))
                        .with_task_end_offsets(describe_task_offsets(&member.task_end_offsets))
                        .with_assignment(describe_assignment(&member.assignment))
                        .with_target_assignment(describe_assignment(&member.target_assignment))
                        .with_is_classic(false)
                })
                .collect(),
        )
        .with_authorized_operations(authorized_operations)
}

fn describe_topology(description: &StreamsGroupDescription) -> DescribeTopology {
    DescribeTopology::default()
        .with_epoch(description.topology.epoch)
        .with_subtopologies(description.topology_ready.then(|| {
            description
                .topology
                .subtopologies
                .iter()
                .map(|subtopology| {
                    DescribeSubtopology::default()
                        .with_subtopology_id(string(&subtopology.subtopology_id))
                        .with_source_topics(
                            subtopology
                                .source_topics
                                .iter()
                                .map(|topic| topic_name(topic))
                                .collect(),
                        )
                        .with_repartition_sink_topics(
                            subtopology
                                .repartition_sink_topics
                                .iter()
                                .map(|topic| topic_name(topic))
                                .collect(),
                        )
                        .with_state_changelog_topics(
                            subtopology
                                .state_changelog_topics
                                .iter()
                                .map(describe_internal_topic)
                                .collect(),
                        )
                        .with_repartition_source_topics(
                            subtopology
                                .repartition_source_topics
                                .iter()
                                .map(describe_internal_topic)
                                .collect(),
                        )
                })
                .collect()
        }))
}

fn describe_internal_topic(topic: &StreamsInternalTopic) -> DescribeTopicInfo {
    DescribeTopicInfo::default()
        .with_name(topic_name(&topic.name))
        .with_partitions(topic.partitions)
        .with_replication_factor(topic.replication_factor)
        .with_topic_configs(
            topic
                .topic_configs
                .iter()
                .map(|config| {
                    DescribeKeyValue::default()
                        .with_key(string(&config.key))
                        .with_value(string(&config.value))
                })
                .collect(),
        )
}

fn describe_endpoint(endpoint: &StreamsEndpoint) -> DescribeEndpoint {
    DescribeEndpoint::default()
        .with_host(string(&endpoint.host))
        .with_port(endpoint.port)
}

fn describe_task_offsets(offsets: &[StreamsTaskOffset]) -> Vec<DescribeTaskOffset> {
    offsets
        .iter()
        .map(|offset| {
            DescribeTaskOffset::default()
                .with_subtopology_id(string(&offset.subtopology_id))
                .with_partition(offset.partition)
                .with_offset(offset.offset)
        })
        .collect()
}

fn describe_assignment(assignment: &StreamsTaskAssignment) -> DescribeAssignment {
    DescribeAssignment::default()
        .with_active_tasks(describe_tasks(&assignment.active_tasks))
        .with_standby_tasks(describe_tasks(&assignment.standby_tasks))
        .with_warmup_tasks(describe_tasks(&assignment.warmup_tasks))
}

pub(super) fn heartbeat_error(
    request: &StreamsGroupHeartbeatRequest,
    error_code: i16,
    message: &str,
    heartbeat_interval_ms: i32,
) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
        .with_member_id(request.member_id.clone())
        .with_member_epoch(request.member_epoch)
        .with_heartbeat_interval_ms(heartbeat_interval_ms)
}

pub(super) fn described_group_error(
    group_id: GroupId,
    error_code: i16,
    message: &str,
) -> DescribedGroup {
    DescribedGroup::default()
        .with_group_id(group_id)
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

pub(super) fn streams_group_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::GroupNotFound(_) | ControlError::GroupProtocolMismatch(_) => {
            GROUP_ID_NOT_FOUND
        }
        ControlError::GroupMemberNotFound { .. } => UNKNOWN_MEMBER_ID,
        ControlError::FencedMemberEpoch { .. } => FENCED_MEMBER_EPOCH,
        ControlError::UnreleasedInstanceId { .. } => UNRELEASED_INSTANCE_ID,
        ControlError::GroupMaxSizeReached(_) => GROUP_MAX_SIZE_REACHED,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

pub(super) fn optional_string(value: Option<&StrBytes>) -> Option<String> {
    value.map(|value| value.as_str().to_owned())
}

pub(super) fn topology_topic_names(topology: &StreamsTopology) -> BTreeSet<String> {
    let mut topics = BTreeSet::new();
    for subtopology in &topology.subtopologies {
        topics.extend(subtopology.source_topics.iter().cloned());
        topics.extend(subtopology.repartition_sink_topics.iter().cloned());
        topics.extend(
            subtopology
                .state_changelog_topics
                .iter()
                .chain(subtopology.repartition_source_topics.iter())
                .map(|topic| topic.name.clone()),
        );
    }
    topics
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

pub(super) fn string(value: impl Into<String>) -> StrBytes {
    StrBytes::from_string(value.into())
}
