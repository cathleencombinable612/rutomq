use super::*;
use crate::kafka_error::{
    GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED, STALE_MEMBER_EPOCH, UNSUPPORTED_ASSIGNOR,
    UNSUPPORTED_VERSION,
};
use bytes::Buf;
use kafka_protocol::messages::consumer_group_heartbeat_request::TopicPartitions;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatResponse, GroupId, OffsetCommitResponse,
    RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;
use std::collections::BTreeMap;

fn test_broker() -> Broker {
    test_broker_with_max_size(i32::MAX)
}

fn test_broker_with_max_size(consumer_group_max_size: i32) -> Broker {
    let config = AgentConfig {
        group_assignment_interval_ms: 0,
        consumer_assignor_offload_enable: false,
        consumer_group_max_size,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn test_broker_with_assignors(assignors: &[&str]) -> Broker {
    let config = AgentConfig {
        group_assignment_interval_ms: 0,
        consumer_assignor_offload_enable: false,
        consumer_group_assignors: assignors.iter().map(|name| (*name).to_owned()).collect(),
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn offload_broker() -> (Broker, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::new().unwrap());
    let config = AgentConfig {
        group_assignment_interval_ms: 0,
        ..AgentConfig::default()
    };
    (
        Broker::new(
            Arc::new(MemoryMetadataStore::new()),
            Arc::new(OpenDalObjectStore::memory().unwrap()),
            config,
            metrics.clone(),
        ),
        metrics,
    )
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn request_frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(42)
        .with_client_id(Some(StrBytes::from_string("consumer-test".to_owned())))
        .encode(&mut payload, api_key.request_header_version(version))
        .unwrap();
    body.encode(&mut payload, version).unwrap();
    payload.freeze()
}

fn decode_response<T: Decodable>(api_key: ApiKey, version: i16, mut frame: Bytes) -> T {
    let frame_size = frame.get_i32() as usize;
    assert_eq!(frame_size, frame.remaining());
    ResponseHeader::decode(&mut frame, api_key.response_header_version(version)).unwrap();
    T::decode(&mut frame, version).unwrap()
}

#[tokio::test]
async fn consumer_group_heartbeat_and_describe_round_trip() {
    let broker = test_broker();
    let topic = broker.metadata.create_topic("orders", 2).await.unwrap();
    let join = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &join))
        .await
        .unwrap();
    let joined: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(joined.error_code, NO_ERROR);
    assert_eq!(joined.member_epoch, 2);
    let assignment = joined.assignment.as_ref().unwrap();
    assert_eq!(assignment.topic_partitions[0].topic_id, topic.id);
    assert_eq!(assignment.topic_partitions[0].partitions, [0, 1]);

    let acknowledge = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(joined.member_epoch)
        .with_rebalance_timeout_ms(-1)
        .with_topic_partitions(Some(vec![
            TopicPartitions::default()
                .with_topic_id(topic.id)
                .with_partitions(vec![0, 1]),
        ]));
    let response = broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            &acknowledge,
        ))
        .await
        .unwrap();
    let acknowledged: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(acknowledged.error_code, NO_ERROR);
    assert!(acknowledged.assignment.is_none());

    let describe = ConsumerGroupDescribeRequest::default()
        .with_group_ids(vec![group_id("workers")])
        .with_include_authorized_operations(true);
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupDescribe, 1, &describe))
        .await
        .unwrap();
    let described: ConsumerGroupDescribeResponse =
        decode_response(ApiKey::ConsumerGroupDescribe, 1, response);
    assert_eq!(described.groups[0].error_code, NO_ERROR);
    assert_eq!(described.groups[0].group_state.as_str(), "Stable");
    assert_eq!(described.groups[0].assignor_name.as_str(), "uniform");
    assert_eq!(described.groups[0].members[0].member_type, 1);
    assert_eq!(
        described.groups[0].members[0].assignment.topic_partitions[0]
            .topic_name
            .as_str(),
        "orders"
    );
}

#[tokio::test]
async fn consumer_group_heartbeat_uses_bounded_group_timeout_overrides() {
    let broker = test_broker();
    broker
        .metadata
        .alter_group_config(
            "bounded-workers",
            BTreeMap::from([
                (
                    "consumer.heartbeat.interval.ms".to_owned(),
                    Some("15000".to_owned()),
                ),
                (
                    "consumer.session.timeout.ms".to_owned(),
                    Some("60000".to_owned()),
                ),
            ]),
            false,
        )
        .await
        .unwrap();
    broker
        .metadata
        .create_topic("bounded-orders", 1)
        .await
        .unwrap();
    let join = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("bounded-workers"))
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("bounded-orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &join))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.heartbeat_interval_ms, 15_000);

    let runtime = broker
        .group_runtime_config("bounded-workers")
        .await
        .unwrap();
    assert_eq!(runtime.consumer_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.consumer_session_timeout_ms, 60_000);
}

#[tokio::test]
async fn consumer_group_max_size_rejects_only_new_members_at_capacity() {
    let broker = test_broker_with_max_size(1);
    broker
        .metadata
        .create_topic("limited-orders", 1)
        .await
        .unwrap();
    let first = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("limited-workers"))
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("limited-orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &first))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let second = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("limited-workers"))
        .with_member_id(StrBytes::from_static_str("member-b"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("limited-orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &second))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, GROUP_MAX_SIZE_REACHED);

    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &first))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let groups = broker
        .metadata
        .describe_consumer_groups(&["limited-workers".to_owned()])
        .await
        .unwrap();
    assert_eq!(groups["limited-workers"].members.len(), 1);
    assert_eq!(groups["limited-workers"].members[0].member_id, "member-a");
}

#[tokio::test]
async fn configured_consumer_assignor_order_sets_default_and_rejects_disabled_choice() {
    let broker = test_broker_with_assignors(&["range"]);
    broker
        .metadata
        .create_topic("assignor-orders", 2)
        .await
        .unwrap();
    let first = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("assignor-workers"))
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("assignor-orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &first))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let disabled = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("assignor-workers"))
        .with_member_id(StrBytes::from_static_str("member-b"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("assignor-orders")]))
        .with_server_assignor(Some(StrBytes::from_static_str("uniform")))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &disabled))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_ASSIGNOR);

    let groups = broker
        .metadata
        .describe_consumer_groups(&["assignor-workers".to_owned()])
        .await
        .unwrap();
    assert_eq!(groups["assignor-workers"].assignor_name, "range");
    assert_eq!(groups["assignor-workers"].members.len(), 1);
    assert_eq!(groups["assignor-workers"].members[0].member_id, "member-a");
}

#[tokio::test]
async fn consumer_assignment_is_offloaded_after_an_epoch_one_empty_join() {
    let (broker, metrics) = offload_broker();
    broker
        .metadata
        .create_topic("offload-orders", 2)
        .await
        .unwrap();
    let join = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("offload-workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("offload-orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &join))
        .await
        .unwrap();
    let joined: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(joined.error_code, NO_ERROR);
    assert_eq!(joined.member_epoch, 1);
    assert!(
        joined
            .assignment
            .as_ref()
            .is_some_and(|assignment| assignment.topic_partitions.is_empty())
    );

    timeout(Duration::from_secs(1), async {
        loop {
            let descriptions = broker
                .metadata
                .describe_consumer_groups(&["offload-workers".to_owned()])
                .await
                .unwrap();
            if descriptions["offload-workers"].assignment_epoch == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let follow_up = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("offload-workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(1)
        .with_rebalance_timeout_ms(-1)
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &follow_up))
        .await
        .unwrap();
    let assigned: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(assigned.member_epoch, 2);
    assert_eq!(
        assigned.assignment.unwrap().topic_partitions[0].partitions,
        [0, 1]
    );
    timeout(Duration::from_secs(1), async {
        while metrics
            .group_assignment_background_completions
            .with_label_values(&["consumer", "published"])
            .get()
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumer_group_reports_protocol_specific_errors() {
    let broker = test_broker();
    broker.metadata.create_topic("orders", 1).await.unwrap();
    let missing = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("missing"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(1);
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &missing))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, GROUP_ID_NOT_FOUND);

    let unsupported = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("orders")]))
        .with_server_assignor(Some(StrBytes::from_string("custom".to_owned())))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            &unsupported,
        ))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_ASSIGNOR);
}

#[tokio::test]
async fn consumer_member_epoch_fences_offset_commits() {
    let broker = test_broker();
    let topic = broker.metadata.create_topic("orders", 2).await.unwrap();
    let join = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("orders")]))
        .with_topic_partitions(Some(Vec::new()));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &join))
        .await
        .unwrap();
    let joined: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);

    let acknowledge = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(joined.member_epoch)
        .with_rebalance_timeout_ms(-1)
        .with_topic_partitions(Some(vec![
            TopicPartitions::default()
                .with_topic_id(topic.id)
                .with_partitions(vec![0, 1]),
        ]));
    broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            &acknowledge,
        ))
        .await
        .unwrap();

    let second = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-b".to_owned()))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("orders")]))
        .with_topic_partitions(Some(Vec::new()));
    broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &second))
        .await
        .unwrap();

    let revoke = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(joined.member_epoch)
        .with_rebalance_timeout_ms(-1);
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &revoke))
        .await
        .unwrap();
    let revoking: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(
        revoking.assignment.as_ref().unwrap().topic_partitions[0].partitions,
        [0]
    );

    let revoked = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(joined.member_epoch)
        .with_rebalance_timeout_ms(-1)
        .with_topic_partitions(Some(vec![
            TopicPartitions::default()
                .with_topic_id(topic.id)
                .with_partitions(vec![0]),
        ]));
    let response = broker
        .handle_request(request_frame(ApiKey::ConsumerGroupHeartbeat, 1, &revoked))
        .await
        .unwrap();
    let advanced: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(advanced.member_epoch, joined.member_epoch + 1);

    let stale_partition_commit = OffsetCommitRequest::default()
        .with_group_id(group_id("workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_generation_id_or_member_epoch(joined.member_epoch)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic_name("orders"))
                .with_partitions(
                    [(0, 10), (1, 11)]
                        .into_iter()
                        .map(|(partition, offset)| {
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(partition)
                                .with_committed_offset(offset)
                        })
                        .collect(),
                ),
        ]);
    let legacy_version_commit = stale_partition_commit
        .clone()
        .with_generation_id_or_member_epoch(advanced.member_epoch);
    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetCommit,
            8,
            &legacy_version_commit,
        ))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 8, response);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.error_code == UNSUPPORTED_VERSION)
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetCommit,
            9,
            &stale_partition_commit,
        ))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.topics[0].partitions[1].error_code,
        STALE_MEMBER_EPOCH
    );
    let committed = broker
        .metadata
        .fetch_offsets(
            "workers",
            &[
                PartitionKey::new("orders", 0),
                PartitionKey::new("orders", 1),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[&PartitionKey::new("orders", 0)].offset, 10);

    let future_epoch_commit =
        stale_partition_commit.with_generation_id_or_member_epoch(advanced.member_epoch + 1);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 9, &future_epoch_commit))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.error_code == STALE_MEMBER_EPOCH)
    );
}
