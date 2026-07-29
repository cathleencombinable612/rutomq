use super::acl_tests::{handle_as, topic_rule};
use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    FENCED_LEADER_EPOCH, INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_LEADER_EPOCH, UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION,
};
use crate::records::encode_records;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{BrokerId, ListOffsetsRequest, ListOffsetsResponse};
use kafka_protocol::records::{Record, TimestampType};
use rutomq_control::{
    AclOperation, AclPermission, AclResourceType, BatchDraft, MemoryMetadataStore, MetadataStore,
    ObjectRef, PartitionKey, PostgresMetadataStore, ProducerBatch,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

fn record(timestamp: i64, value: &'static [u8]) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp,
        key: None,
        value: Some(Bytes::from_static(value)),
        headers: Vec::new(),
    }
}

async fn append_records(broker: &Broker) {
    broker.metadata.create_topic("timestamps", 1).await.unwrap();
    let first = encode_records(&[record(100, b"a"), record(300, b"b")]).unwrap();
    let second = encode_records(&[record(200, b"c")]).unwrap();
    let mut object = BytesMut::new();
    object.extend_from_slice(&first);
    let split = object.len() as u64;
    object.extend_from_slice(&second);
    let size = object.len() as u64;
    broker
        .objects
        .put_immutable("list-offsets/base", object.freeze())
        .await
        .unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "list-offsets/base".to_owned(),
                size,
            },
            vec![
                BatchDraft {
                    partition: PartitionKey::new("timestamps", 0),
                    byte_start: 0,
                    byte_end: split,
                    record_count: 2,
                    timestamp_ms: 9_999,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
                BatchDraft {
                    partition: PartitionKey::new("timestamps", 0),
                    byte_start: split,
                    byte_end: size,
                    record_count: 1,
                    timestamp_ms: 9_999,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
            ],
        )
        .await
        .unwrap();
}

async fn append_pending_transaction(broker: &Broker) {
    let producer = broker
        .metadata
        .init_producer(Some("timestamps-tx"), 60_000, None)
        .await
        .unwrap();
    broker
        .metadata
        .add_partitions_to_transaction(
            "timestamps-tx",
            producer,
            &[PartitionKey::new("timestamps", 0)],
            false,
        )
        .await
        .unwrap();
    let mut pending = record(400, b"pending");
    pending.transactional = true;
    pending.producer_id = producer.producer_id;
    pending.producer_epoch = producer.producer_epoch;
    pending.sequence = 0;
    let encoded = encode_records(&[pending]).unwrap();
    let size = encoded.len() as u64;
    broker
        .objects
        .put_immutable("list-offsets/pending", encoded)
        .await
        .unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "list-offsets/pending".to_owned(),
                size,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("timestamps", 0),
                byte_start: 0,
                byte_end: size,
                record_count: 1,
                timestamp_ms: 9_999,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some("timestamps-tx".to_owned()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
}

async fn lookup(
    broker: &Broker,
    timestamp: i64,
    isolation_level: i8,
    leader_epoch: i32,
    replica_id: i32,
) -> kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse {
    lookup_at_version(
        broker,
        timestamp,
        isolation_level,
        leader_epoch,
        replica_id,
        11,
    )
    .await
}

async fn lookup_at_version(
    broker: &Broker,
    timestamp: i64,
    isolation_level: i8,
    leader_epoch: i32,
    replica_id: i32,
    version: i16,
) -> kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(replica_id))
        .with_isolation_level(isolation_level)
        .with_timeout_ms(1_000)
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic_name("timestamps"))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_current_leader_epoch(leader_epoch)
                        .with_timestamp(timestamp),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ListOffsets, version, 120, &request))
        .await
        .unwrap();
    let response: ListOffsetsResponse = decode_response(ApiKey::ListOffsets, version, response);
    response
        .topics
        .into_iter()
        .next()
        .unwrap()
        .partitions
        .remove(0)
}

#[tokio::test]
async fn list_offsets_v11_uses_record_timestamps_and_special_offsets() {
    let broker = broker();
    append_records(&broker).await;

    let timestamp = lookup(&broker, 150, 0, 0, -1).await;
    assert_eq!((timestamp.offset, timestamp.timestamp), (1, 300));
    let maximum = lookup(&broker, -3, 0, 0, -1).await;
    assert_eq!((maximum.offset, maximum.timestamp), (1, 300));
    let earliest = lookup(&broker, -2, 0, 0, -1).await;
    assert_eq!((earliest.offset, earliest.timestamp), (0, -1));
    let earliest_local = lookup(&broker, -4, 0, 0, -1).await;
    assert_eq!((earliest_local.offset, earliest_local.timestamp), (0, -1));
    let latest_tiered = lookup(&broker, -5, 0, 0, -1).await;
    assert_eq!((latest_tiered.offset, latest_tiered.timestamp), (-1, -1));
    let earliest_pending_upload = lookup(&broker, -6, 0, 0, -1).await;
    assert_eq!(
        (
            earliest_pending_upload.offset,
            earliest_pending_upload.timestamp
        ),
        (-1, -1)
    );
    let latest = lookup(&broker, -1, 0, 0, -1).await;
    assert_eq!((latest.offset, latest.timestamp), (3, -1));
    let missing = lookup(&broker, 301, 0, 0, -1).await;
    assert_eq!((missing.offset, missing.timestamp), (-1, -1));
    assert_eq!(timestamp.leader_epoch, 0);
}

#[tokio::test]
async fn list_offsets_honors_isolation_epoch_and_virtual_replica_boundary() {
    let broker = broker();
    append_records(&broker).await;
    append_pending_transaction(&broker).await;

    assert_eq!(lookup(&broker, -1, 0, 0, -1).await.offset, 4);
    assert_eq!(lookup(&broker, -1, 1, 0, -1).await.offset, 3);
    assert_eq!(lookup(&broker, 350, 0, 0, -1).await.offset, 3);
    assert_eq!(lookup(&broker, 350, 1, 0, -1).await.offset, -1);
    assert_eq!(
        lookup(&broker, -1, 0, -2, -1).await.error_code,
        FENCED_LEADER_EPOCH
    );
    assert_eq!(
        lookup(&broker, -1, 0, 1, -1).await.error_code,
        UNKNOWN_LEADER_EPOCH
    );
    assert_eq!(
        lookup(&broker, -1, 0, 0, 0).await.error_code,
        INVALID_REQUEST
    );
    assert_eq!(
        lookup(&broker, -7, 0, 0, -1).await.error_code,
        UNSUPPORTED_VERSION
    );
}

#[tokio::test]
async fn negative_selectors_require_their_introducing_versions() {
    let broker = broker();
    append_records(&broker).await;
    for (timestamp, unsupported_version, supported_version) in
        [(-3, 6, 7), (-4, 7, 8), (-5, 8, 9), (-6, 10, 11)]
    {
        let unsupported =
            lookup_at_version(&broker, timestamp, 0, 0, -1, unsupported_version).await;
        assert_eq!(unsupported.error_code, UNSUPPORTED_VERSION);
        assert_eq!(unsupported.leader_epoch, -1);

        let supported = lookup_at_version(&broker, timestamp, 0, 0, -1, supported_version).await;
        assert_eq!(supported.error_code, NO_ERROR);
    }
    let unknown = lookup_at_version(&broker, -7, 0, 0, -1, 11).await;
    assert_eq!(unknown.error_code, UNSUPPORTED_VERSION);
    assert_eq!(unknown.leader_epoch, -1);
}

#[tokio::test]
async fn list_offsets_collapses_duplicate_authorized_partitions() {
    let broker = broker();
    broker.metadata.create_topic("duplicates", 2).await.unwrap();
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_isolation_level(0)
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic_name("duplicates"))
                .with_partitions(vec![list_offsets_partition(0), list_offsets_partition(1)]),
            ListOffsetsTopic::default()
                .with_name(topic_name("duplicates"))
                .with_partitions(vec![list_offsets_partition(0)]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ListOffsets, 11, 122, &request))
        .await
        .unwrap();
    let response: ListOffsetsResponse = decode_response(ApiKey::ListOffsets, 11, response);

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 2);
    assert_eq!(response.topics[0].partitions[0].partition_index, 0);
    assert_eq!(response.topics[0].partitions[0].error_code, INVALID_REQUEST);
    assert_eq!(response.topics[0].partitions[0].leader_epoch, -1);
    assert_eq!(response.topics[0].partitions[1].partition_index, 1);
    assert_eq!(response.topics[0].partitions[1].error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions[1].leader_epoch, 0);
}

#[tokio::test]
async fn list_offsets_authorizer_failure_is_request_wide() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let broker = secured_broker(metadata);
    let request = list_offsets_request(&["backend-a", "backend-b"]);
    let response = handle_as(
        &broker,
        "offset-reader",
        ApiKey::ListOffsets,
        11,
        123,
        &request,
    )
    .await;
    let response: ListOffsetsResponse = decode_response(ApiKey::ListOffsets, 11, response);

    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>(),
        ["backend-a", "backend-b"]
    );
    assert!(response.topics.iter().all(|topic| {
        let partition = &topic.partitions[0];
        partition.error_code == UNKNOWN_SERVER_ERROR
            && partition.offset == -1
            && partition.timestamp == -1
            && partition.leader_epoch == -1
    }));
}

#[tokio::test]
async fn list_offsets_orders_authorized_before_denied_in_memory() {
    assert_list_offsets_authorization_order(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn list_offsets_orders_authorized_before_denied_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_list_offsets_authorization_order(Arc::new(store), &Uuid::new_v4().simple().to_string())
        .await;
}

async fn assert_list_offsets_authorization_order(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("visible-list-offsets-{suffix}");
    let hidden = format!("hidden-list-offsets-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:offset-reader",
            &visible,
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);
    let request = list_offsets_request(&[&hidden, &visible, &hidden]);
    let response = handle_as(
        &broker,
        "offset-reader",
        ApiKey::ListOffsets,
        11,
        124,
        &request,
    )
    .await;
    let response: ListOffsetsResponse = decode_response(ApiKey::ListOffsets, 11, response);

    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>(),
        [visible.as_str(), hidden.as_str(), hidden.as_str()]
    );
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions[0].offset, 0);
    assert_eq!(response.topics[0].partitions[0].leader_epoch, 0);
    assert!(response.topics[1..].iter().all(|topic| {
        topic.partitions[0].error_code == TOPIC_AUTHORIZATION_FAILED
            && topic.partitions[0].leader_epoch == -1
    }));
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

fn list_offsets_request(names: &[&str]) -> ListOffsetsRequest {
    ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_isolation_level(0)
        .with_timeout_ms(1_000)
        .with_topics(
            names
                .iter()
                .map(|name| {
                    ListOffsetsTopic::default()
                        .with_name(topic_name(name))
                        .with_partitions(vec![list_offsets_partition(0)])
                })
                .collect(),
        )
}

fn list_offsets_partition(partition_index: i32) -> ListOffsetsPartition {
    ListOffsetsPartition::default()
        .with_partition_index(partition_index)
        .with_current_leader_epoch(0)
        .with_timestamp(-1)
}
