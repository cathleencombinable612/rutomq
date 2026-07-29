use super::acl_tests::{handle_as, topic_rule};
use super::*;
use crate::kafka_error::{
    FENCED_LEADER_EPOCH, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_LEADER_EPOCH, UNKNOWN_SERVER_ERROR,
};
use bytes::Buf;
use kafka_protocol::messages::describe_producers_request::TopicRequest;
use kafka_protocol::messages::offset_for_leader_epoch_request::{
    OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use kafka_protocol::messages::{
    DescribeProducersRequest, DescribeProducersResponse, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, BatchDraft,
    MemoryMetadataStore, MetadataStore, ObjectRef, PostgresMetadataStore, ProducerBatch,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

fn test_broker() -> Broker {
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    )
}

fn request_frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(91)
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
async fn offset_for_virtual_leader_epoch_fences_and_returns_log_end() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 1).await.unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "objects/leader-epoch".to_owned(),
                size: 8,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 8,
                record_count: 2,
                timestamp_ms: 10,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let request = OffsetForLeaderEpochRequest::default().with_topics(vec![
        OffsetForLeaderTopic::default()
            .with_topic(topic_name("events"))
            .with_partitions(vec![
                epoch_partition(0, -1, 0),
                epoch_partition(0, 1, 0),
                epoch_partition(0, -2, 0),
                epoch_partition(0, 0, -1),
                epoch_partition(1, 0, 0),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetForLeaderEpoch, 4, &request))
        .await
        .unwrap();
    let response: OffsetForLeaderEpochResponse =
        decode_response(ApiKey::OffsetForLeaderEpoch, 4, response);
    let partitions = &response.topics[0].partitions;
    assert_eq!(
        (
            partitions[0].error_code,
            partitions[0].leader_epoch,
            partitions[0].end_offset,
        ),
        (NO_ERROR, 0, 2)
    );
    assert_eq!(partitions[1].error_code, UNKNOWN_LEADER_EPOCH);
    assert_eq!(partitions[2].error_code, FENCED_LEADER_EPOCH);
    assert_eq!(
        (
            partitions[3].error_code,
            partitions[3].leader_epoch,
            partitions[3].end_offset,
        ),
        (NO_ERROR, -1, -1)
    );
    assert_eq!(partitions[4].error_code, UNKNOWN_TOPIC_OR_PARTITION);
}

#[tokio::test]
async fn offset_for_leader_epoch_orders_authorized_before_denied_in_memory() {
    assert_offset_authorization_order(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn offset_for_leader_epoch_orders_authorized_before_denied_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_offset_authorization_order(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn offset_for_leader_epoch_authorizer_failure_is_request_wide() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    for topic in ["epoch-backend-a", "epoch-backend-b"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let broker = secured_broker(metadata);
    let request = OffsetForLeaderEpochRequest::default().with_topics(vec![
        offset_topic("epoch-backend-a"),
        offset_topic("epoch-backend-b"),
    ]);
    let response = handle_as(
        &broker,
        "epoch-reader",
        ApiKey::OffsetForLeaderEpoch,
        4,
        9201,
        &request,
    )
    .await;
    let response: OffsetForLeaderEpochResponse =
        decode_response(ApiKey::OffsetForLeaderEpoch, 4, response);

    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect::<Vec<_>>(),
        ["epoch-backend-a", "epoch-backend-b"]
    );
    assert!(
        response
            .topics
            .iter()
            .all(|topic| { topic.partitions[0].error_code == UNKNOWN_SERVER_ERROR })
    );
}

#[tokio::test]
async fn offset_for_leader_epoch_cluster_action_bypasses_topic_acl() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("cluster-authorized-epoch", 1)
        .await
        .unwrap();
    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Cluster,
            resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:epoch-reader".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::ClusterAction,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    let broker = secured_broker(metadata);
    let request = OffsetForLeaderEpochRequest::default()
        .with_topics(vec![offset_topic("cluster-authorized-epoch")]);
    let response = handle_as(
        &broker,
        "epoch-reader",
        ApiKey::OffsetForLeaderEpoch,
        4,
        9202,
        &request,
    )
    .await;
    let response: OffsetForLeaderEpochResponse =
        decode_response(ApiKey::OffsetForLeaderEpoch, 4, response);

    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions[0].end_offset, 0);
}

async fn assert_offset_authorization_order(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("visible-epoch-{suffix}");
    let hidden = format!("hidden-epoch-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:epoch-reader",
            &visible,
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);
    let request = OffsetForLeaderEpochRequest::default().with_topics(vec![
        offset_topic(&hidden),
        offset_topic(&visible),
        offset_topic(&hidden),
    ]);
    let response = handle_as(
        &broker,
        "epoch-reader",
        ApiKey::OffsetForLeaderEpoch,
        4,
        9200,
        &request,
    )
    .await;
    let response: OffsetForLeaderEpochResponse =
        decode_response(ApiKey::OffsetForLeaderEpoch, 4, response);

    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect::<Vec<_>>(),
        [visible.as_str(), hidden.as_str(), hidden.as_str()]
    );
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert!(
        response.topics[1..]
            .iter()
            .all(|topic| topic.partitions[0].error_code == TOPIC_AUTHORIZATION_FAILED)
    );
}

fn secured_broker(metadata: Arc<dyn MetadataStore>) -> Broker {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn offset_topic(name: &str) -> OffsetForLeaderTopic {
    OffsetForLeaderTopic::default()
        .with_topic(topic_name(name))
        .with_partitions(vec![epoch_partition(0, 0, 0)])
}

#[tokio::test]
async fn describe_producers_returns_persisted_sequence_state() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 1).await.unwrap();
    let producer = broker
        .metadata
        .init_producer(None, 60_000, None)
        .await
        .unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "objects/producer-state".to_owned(),
                size: 8,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 8,
                record_count: 2,
                timestamp_ms: 1_234,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 1,
                }),
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let request = DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(topic_name("events"))
            .with_partition_indexes(vec![0, 1]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, &request))
        .await
        .unwrap();
    let response: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, response);
    let producer_state = &response.topics[0].partitions[0].active_producers[0];
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(i64::from(producer_state.producer_id), producer.producer_id);
    assert_eq!(producer_state.producer_epoch, 0);
    assert_eq!(producer_state.last_sequence, 1);
    assert_eq!(producer_state.last_timestamp, 1_234);
    assert_eq!(producer_state.coordinator_epoch, -1);
    assert_eq!(producer_state.current_txn_start_offset, -1);
    assert_eq!(
        response.topics[0].partitions[1].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );

    assert_eq!(
        broker
            .metadata
            .expire_producer_sequences(2_234, 1_000, 10)
            .await
            .unwrap(),
        1
    );
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, &request))
        .await
        .unwrap();
    let response: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, response);
    assert!(response.topics[0].partitions[0].active_producers.is_empty());
}

fn epoch_partition(
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> OffsetForLeaderPartition {
    OffsetForLeaderPartition::default()
        .with_partition(partition)
        .with_current_leader_epoch(current_leader_epoch)
        .with_leader_epoch(leader_epoch)
}
