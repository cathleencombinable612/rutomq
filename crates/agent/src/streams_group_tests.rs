use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED, INVALID_REQUEST, STALE_MEMBER_EPOCH,
    STREAMS_INVALID_TOPOLOGY,
};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::streams_group_heartbeat_request::{
    KeyValue as RequestKeyValue, Subtopology, TaskIds, TopicInfo as RequestTopicInfo, Topology,
};
use kafka_protocol::messages::{
    ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, GroupId, OffsetCommitResponse,
    StreamsGroupDescribeRequest, StreamsGroupDescribeResponse, StreamsGroupHeartbeatRequest,
    StreamsGroupHeartbeatResponse,
};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;
use std::collections::BTreeMap;

fn limited_streams_broker() -> Broker {
    let config = AgentConfig {
        streams_group_assignment_interval_ms: 0,
        streams_group_initial_rebalance_delay_ms: 0,
        streams_assignor_offload_enable: false,
        streams_group_max_size: 1,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

#[tokio::test]
async fn streams_group_heartbeat_uses_bounded_group_timeout_overrides() {
    let broker = broker();
    broker
        .metadata
        .alter_group_config(
            "bounded-streams-workers",
            BTreeMap::from([
                (
                    "streams.heartbeat.interval.ms".to_owned(),
                    Some("15000".to_owned()),
                ),
                (
                    "streams.session.timeout.ms".to_owned(),
                    Some("60000".to_owned()),
                ),
            ]),
            false,
        )
        .await
        .unwrap();
    broker
        .metadata
        .create_topic("bounded-streams-input", 1)
        .await
        .unwrap();
    let request = joining_request(
        "bounded-streams-workers",
        "member-a",
        "bounded-streams-input",
    );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            877,
            &request,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.heartbeat_interval_ms, 15_000);

    let runtime = broker
        .group_runtime_config("bounded-streams-workers")
        .await
        .unwrap();
    assert_eq!(runtime.streams_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.streams_session_timeout_ms, 60_000);
}

#[tokio::test]
async fn streams_group_max_size_rejects_only_new_members_at_capacity() {
    let broker = limited_streams_broker();
    broker
        .metadata
        .create_topic("limited-streams-input", 1)
        .await
        .unwrap();
    let first = joining_request(
        "limited-streams-workers",
        "member-a",
        "limited-streams-input",
    );
    let response = broker
        .handle_request(request_frame(ApiKey::StreamsGroupHeartbeat, 0, 874, &first))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);

    let second = joining_request(
        "limited-streams-workers",
        "member-b",
        "limited-streams-input",
    );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            875,
            &second,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, GROUP_MAX_SIZE_REACHED);

    let response = broker
        .handle_request(request_frame(ApiKey::StreamsGroupHeartbeat, 0, 876, &first))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);

    let groups = broker
        .metadata
        .describe_streams_groups(&["limited-streams-workers".to_owned()])
        .await
        .unwrap();
    assert_eq!(groups["limited-streams-workers"].members.len(), 1);
    assert_eq!(
        groups["limited-streams-workers"].members[0].member_id,
        "member-a"
    );
}

#[tokio::test]
async fn streams_topology_rejects_internal_and_invalid_topic_names_before_mutation() {
    let broker = broker();
    let mut internal = joining_request("internal-topology", "member-a", "__consumer_offsets");
    let subtopology = &mut internal.topology.as_mut().unwrap().subtopologies[0];
    subtopology.repartition_sink_topics = vec![topic_name("__transaction_state")];
    subtopology.repartition_source_topics =
        vec![RequestTopicInfo::default().with_name(topic_name("__share_group_state"))];
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            878,
            &internal,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, STREAMS_INVALID_TOPOLOGY);
    assert_eq!(
        response.error_message.as_ref().unwrap().as_str(),
        "Use of Kafka internal topics __consumer_offsets,__transaction_state,__share_group_state in a Kafka Streams topology is prohibited."
    );
    assert!(response.member_id.as_str().is_empty());
    assert_eq!(response.heartbeat_interval_ms, 0);

    let mut invalid = joining_request("invalid-topology", "member-a", "a ");
    let subtopology = &mut invalid.topology.as_mut().unwrap().subtopologies[0];
    subtopology.repartition_sink_topics = vec![topic_name("b?")];
    subtopology.state_changelog_topics =
        vec![RequestTopicInfo::default().with_name(topic_name("d/"))];
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            879,
            &invalid,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, STREAMS_INVALID_TOPOLOGY);
    assert_eq!(
        response.error_message.as_ref().unwrap().as_str(),
        "Topic names a ,b?,d/ are not valid topic names."
    );
    assert!(response.member_id.as_str().is_empty());
    assert_eq!(response.heartbeat_interval_ms, 0);
    assert!(
        broker
            .metadata
            .describe_streams_groups(&[
                "internal-topology".to_owned(),
                "invalid-topology".to_owned(),
            ])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn streams_group_heartbeat_describe_and_offset_commit_round_trip() {
    let broker = broker();
    broker
        .metadata
        .create_topic("streams-input", 2)
        .await
        .unwrap();
    let join = joining_request("streams-workers", "member-a", "streams-input");
    let response = broker
        .handle_request(request_frame(ApiKey::StreamsGroupHeartbeat, 0, 880, &join))
        .await
        .unwrap();
    let joined: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(joined.error_code, NO_ERROR);
    assert_eq!(joined.member_epoch, 2);
    assert!(joined.status.as_ref().unwrap().is_empty());
    assert_eq!(joined.active_tasks.as_ref().unwrap()[0].partitions, [0, 1]);
    assert!(joined.standby_tasks.as_ref().unwrap().is_empty());
    assert!(joined.warmup_tasks.as_ref().unwrap().is_empty());

    let acknowledge = StreamsGroupHeartbeatRequest::default()
        .with_group_id(group_id("streams-workers"))
        .with_member_id(string("member-a"))
        .with_member_epoch(joined.member_epoch)
        .with_endpoint_information_epoch(joined.endpoint_information_epoch)
        .with_active_tasks(Some(vec![
            TaskIds::default()
                .with_subtopology_id(string("0"))
                .with_partitions(vec![0, 1]),
        ]))
        .with_standby_tasks(Some(Vec::new()))
        .with_warmup_tasks(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            881,
            &acknowledge,
        ))
        .await
        .unwrap();
    let acknowledged: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(acknowledged.error_code, NO_ERROR);
    assert!(acknowledged.active_tasks.is_none());
    assert!(acknowledged.standby_tasks.is_none());
    assert!(acknowledged.warmup_tasks.is_none());

    let describe = StreamsGroupDescribeRequest::default()
        .with_group_ids(vec![group_id("streams-workers")])
        .with_include_authorized_operations(true);
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupDescribe,
            0,
            882,
            &describe,
        ))
        .await
        .unwrap();
    let described: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_eq!(described.groups[0].error_code, NO_ERROR);
    assert_eq!(described.groups[0].group_state.as_str(), "Stable");
    assert_eq!(
        described.groups[0]
            .topology
            .as_ref()
            .unwrap()
            .subtopologies
            .as_ref()
            .unwrap()[0]
            .source_topics[0]
            .as_str(),
        "streams-input"
    );
    assert_eq!(
        described.groups[0].members[0].assignment.active_tasks[0].partitions,
        [0, 1]
    );

    let commit = offset_commit(acknowledged.member_epoch);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 9, 883, &commit))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    let commit = offset_commit(acknowledged.member_epoch + 1);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 9, 884, &commit))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        STALE_MEMBER_EPOCH
    );
}

#[tokio::test]
async fn streams_group_reports_not_ready_and_malformed_task_collections() {
    let broker = broker();
    let join = joining_request("not-ready", "member-a", "missing-source");
    let response = broker
        .handle_request(request_frame(ApiKey::StreamsGroupHeartbeat, 0, 884, &join))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.status.as_ref().unwrap()[0].status_code, 1);
    assert!(response.active_tasks.as_ref().unwrap().is_empty());

    broker
        .metadata
        .create_topic("malformed-input", 1)
        .await
        .unwrap();
    let malformed =
        joining_request("malformed", "member-a", "malformed-input").with_standby_tasks(None);
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            885,
            &malformed,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, INVALID_REQUEST);
}

#[tokio::test]
async fn streams_and_consumer_protocol_collisions_return_group_id_not_found() {
    let broker = broker();
    broker
        .metadata
        .create_topic("collision-input", 1)
        .await
        .unwrap();
    let consumer = consumer_join("consumer-owned", "collision-input");
    let response = broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            886,
            &consumer,
        ))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let streams = joining_request("consumer-owned", "streams-member", "collision-input");
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            887,
            &streams,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, GROUP_ID_NOT_FOUND);

    let streams = joining_request("streams-owned", "streams-member", "collision-input");
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            888,
            &streams,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);

    let consumer = consumer_join("streams-owned", "collision-input");
    let response = broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            889,
            &consumer,
        ))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, GROUP_ID_NOT_FOUND);
}

#[tokio::test]
async fn streams_group_creates_required_changelog_topic_before_assignment() {
    let broker = broker();
    broker
        .metadata
        .create_topic("stateful-input", 2)
        .await
        .unwrap();
    let mut join = joining_request("stateful-app", "member-a", "stateful-input");
    join.topology.as_mut().unwrap().subtopologies[0]
        .state_changelog_topics
        .push(
            RequestTopicInfo::default()
                .with_name(topic_name("stateful-app-store-changelog"))
                .with_partitions(0)
                .with_replication_factor(0)
                .with_topic_configs(vec![
                    RequestKeyValue::default()
                        .with_key(string("cleanup.policy"))
                        .with_value(string("compact")),
                    RequestKeyValue::default()
                        .with_key(string("segment.bytes"))
                        .with_value(string("52428800")),
                ]),
        );
    let response = broker
        .handle_request(request_frame(ApiKey::StreamsGroupHeartbeat, 0, 890, &join))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(response.status.as_ref().unwrap().is_empty());
    assert_eq!(
        response.active_tasks.as_ref().unwrap()[0].partitions,
        [0, 1]
    );

    let changelog = broker
        .metadata
        .topic("stateful-app-store-changelog")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changelog.partitions, 2);
    let config = broker
        .metadata
        .topic_config("stateful-app-store-changelog")
        .await
        .unwrap();
    assert_eq!(config.cleanup_policy, "compact");
    assert!(config.is_dynamic("cleanup.policy"));
    let description = broker
        .metadata
        .describe_streams_groups(&["stateful-app".to_owned()])
        .await
        .unwrap()
        .remove("stateful-app")
        .unwrap();
    assert_eq!(
        description.topology.subtopologies[0].state_changelog_topics[0].partitions,
        2
    );
}

#[tokio::test]
async fn streams_internal_topic_creation_rejects_invalid_virtual_topology_requests() {
    let broker = broker();
    broker
        .metadata
        .create_topic("internal-contract-input", 1)
        .await
        .unwrap();

    let mut replicated = joining_request(
        "internal-contract-replication",
        "member-a",
        "internal-contract-input",
    );
    replicated.topology.as_mut().unwrap().subtopologies[0]
        .state_changelog_topics
        .push(
            RequestTopicInfo::default()
                .with_name(topic_name("internal-contract-rf2-changelog"))
                .with_partitions(0)
                .with_replication_factor(2),
        );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            891,
            &replicated,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.status.as_ref().unwrap()[0].status_code, 3);
    assert!(
        response.status.as_ref().unwrap()[0]
            .status_detail
            .as_str()
            .contains("replication factor exceeds the one-broker virtual topology")
    );
    assert!(response.active_tasks.as_ref().unwrap().is_empty());
    assert!(
        broker
            .metadata
            .topic("internal-contract-rf2-changelog")
            .await
            .unwrap()
            .is_none()
    );

    let mut unsupported = joining_request(
        "internal-contract-config",
        "member-a",
        "internal-contract-input",
    );
    unsupported.topology.as_mut().unwrap().subtopologies[0]
        .state_changelog_topics
        .push(
            RequestTopicInfo::default()
                .with_name(topic_name("internal-contract-config-changelog"))
                .with_partitions(0)
                .with_replication_factor(0)
                .with_topic_configs(vec![
                    RequestKeyValue::default()
                        .with_key(string("remote.storage.enable"))
                        .with_value(string("true")),
                ]),
        );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            892,
            &unsupported,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.status.as_ref().unwrap()[0].status_code, 3);
    assert!(
        response.status.as_ref().unwrap()[0]
            .status_detail
            .as_str()
            .contains("topic configuration remote.storage.enable is unsupported")
    );
    assert!(
        broker
            .metadata
            .topic("internal-contract-config-changelog")
            .await
            .unwrap()
            .is_none()
    );

    let mut invalid_segment = joining_request(
        "internal-contract-segment",
        "member-a",
        "internal-contract-input",
    );
    invalid_segment.topology.as_mut().unwrap().subtopologies[0]
        .state_changelog_topics
        .push(
            RequestTopicInfo::default()
                .with_name(topic_name("internal-contract-segment-changelog"))
                .with_partitions(0)
                .with_replication_factor(0)
                .with_topic_configs(vec![
                    RequestKeyValue::default()
                        .with_key(string("segment.bytes"))
                        .with_value(string("1048575")),
                ]),
        );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            893,
            &invalid_segment,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.status.as_ref().unwrap()[0].status_code, 3);
    assert!(
        response.status.as_ref().unwrap()[0]
            .status_detail
            .as_str()
            .contains("configuration segment.bytes must be at least 1048576")
    );
    assert!(
        broker
            .metadata
            .topic("internal-contract-segment-changelog")
            .await
            .unwrap()
            .is_none()
    );

    broker
        .metadata
        .create_topic("internal-contract-existing-changelog", 1)
        .await
        .unwrap();
    let mut existing = joining_request(
        "internal-contract-existing",
        "member-a",
        "internal-contract-input",
    );
    existing.topology.as_mut().unwrap().subtopologies[0]
        .state_changelog_topics
        .push(
            RequestTopicInfo::default()
                .with_name(topic_name("internal-contract-existing-changelog"))
                .with_partitions(0)
                .with_replication_factor(2)
                .with_topic_configs(vec![
                    RequestKeyValue::default()
                        .with_key(string("segment.bytes"))
                        .with_value(string("1048576")),
                ]),
        );
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            894,
            &existing,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(response.status.as_ref().unwrap().is_empty());
    assert_eq!(response.active_tasks.as_ref().unwrap()[0].partitions, [0]);
}

fn joining_request(group: &str, member: &str, topic: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest::default()
        .with_group_id(group_id(group))
        .with_member_id(string(member))
        .with_member_epoch(0)
        .with_endpoint_information_epoch(-1)
        .with_rebalance_timeout_ms(300_000)
        .with_topology(Some(Topology::default().with_epoch(0).with_subtopologies(
            vec![
                    Subtopology::default()
                        .with_subtopology_id(string("0"))
                        .with_source_topics(vec![topic_name(topic)]),
                ],
        )))
        .with_active_tasks(Some(Vec::<TaskIds>::new()))
        .with_standby_tasks(Some(Vec::new()))
        .with_warmup_tasks(Some(Vec::new()))
        .with_process_id(Some(string(&format!("process-{member}"))))
        .with_client_tags(Some(Vec::new()))
}

fn offset_commit(epoch: i32) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(group_id("streams-workers"))
        .with_member_id(string("member-a"))
        .with_generation_id_or_member_epoch(epoch)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic_name("streams-input"))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(1),
                ]),
        ])
}

fn consumer_join(group: &str, topic: &str) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id(group))
        .with_member_id(string("consumer-member"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name(topic)]))
        .with_topic_partitions(Some(Vec::new()))
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
