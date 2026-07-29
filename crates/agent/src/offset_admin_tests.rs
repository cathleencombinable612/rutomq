use super::acl_tests::{handle_as, topic_rule};
use super::*;
use crate::kafka_error::{GROUP_ID_NOT_FOUND, GROUP_SUBSCRIBED_TO_TOPIC, OFFSET_OUT_OF_RANGE};
use bytes::Buf;
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::offset_delete_request::{
    OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};
use kafka_protocol::messages::{
    BrokerId, DeleteRecordsRequest, DeleteRecordsResponse, FetchRequest, FetchResponse, GroupId,
    OffsetDeleteRequest, OffsetDeleteResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, BatchDraft,
    FetchIsolation, GroupMemberIdentity, MemoryMetadataStore, MetadataStore, ObjectRef,
    OffsetCommit, PartitionKey, PostgresMetadataStore,
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
        .with_correlation_id(81)
        .with_client_id(Some(StrBytes::from_string("offset-admin-test".to_owned())))
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
async fn delete_records_advances_the_log_start_offset() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 1).await.unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "objects/delete-records".to_owned(),
                size: 20,
            },
            vec![batch(0, 10), batch(10, 20)],
        )
        .await
        .unwrap();

    let request = delete_records_request(2);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteRecords, 2, &request))
        .await
        .unwrap();
    let response: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions[0].low_watermark, 2);
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("events", 0), -2)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        broker
            .metadata
            .fetch(
                &PartitionKey::new("events", 0),
                2,
                1024,
                FetchIsolation::ReadUncommitted,
            )
            .await
            .unwrap()
            .spans
            .len(),
        1
    );
    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("events"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(1)
                        .with_partition_max_bytes(1024),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 5, &fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 5, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(partition.error_code, OFFSET_OUT_OF_RANGE);
    assert_eq!(partition.log_start_offset, 2);
    assert_eq!(partition.high_watermark, 4);

    let response = broker
        .handle_request(request_frame(
            ApiKey::DeleteRecords,
            2,
            &delete_records_request(5),
        ))
        .await
        .unwrap();
    let response: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        OFFSET_OUT_OF_RANGE
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::DeleteRecords,
            2,
            &delete_records_request(-1),
        ))
        .await
        .unwrap();
    let response: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, response);
    assert_eq!(response.topics[0].partitions[0].low_watermark, 4);
}

#[tokio::test]
async fn offset_delete_blocks_active_subscriptions_and_removes_empty_group_offsets() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 2).await.unwrap();
    let offsets = vec![
        OffsetCommit {
            partition: PartitionKey::new("events", 0),
            offset: 7,
            leader_epoch: -1,
            metadata: None,
            retention_time_ms: None,
        },
        OffsetCommit {
            partition: PartitionKey::new("events", 1),
            offset: 8,
            leader_epoch: -1,
            metadata: None,
            retention_time_ms: None,
        },
    ];
    broker
        .metadata
        .commit_offsets("workers", offsets)
        .await
        .unwrap();
    let joined = broker
        .metadata
        .join_group(
            "workers",
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "offset-admin-test",
                "127.0.0.1",
                &["events".to_owned()],
                45_000,
            ),
            3,
        )
        .await
        .unwrap();

    let request = offset_delete_request("workers");
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetDelete, 0, &request))
        .await
        .unwrap();
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.error_code == GROUP_SUBSCRIBED_TO_TOPIC)
    );

    broker
        .metadata
        .leave_group(
            "workers",
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetDelete, 0, &request))
        .await
        .unwrap();
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.error_code == NO_ERROR)
    );
    assert!(
        broker
            .metadata
            .fetch_offsets(
                "workers",
                &[
                    PartitionKey::new("events", 0),
                    PartitionKey::new("events", 1),
                ],
            )
            .await
            .unwrap()
            .is_empty()
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetDelete,
            0,
            &offset_delete_request("missing"),
        ))
        .await
        .unwrap();
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert_eq!(response.error_code, GROUP_ID_NOT_FOUND);
}

#[tokio::test]
async fn destructive_offset_authorizer_failures_are_request_wide_and_non_mutating() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    for topic in ["delete-backend-a", "delete-backend-b"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    metadata
        .commit_object(
            ObjectRef {
                key: "objects/delete-backend".to_owned(),
                size: 20,
            },
            vec![
                batch_for("delete-backend-a", 0, 10),
                batch_for("delete-backend-b", 10, 20),
            ],
        )
        .await
        .unwrap();
    metadata
        .commit_offsets(
            "delete-backend-group",
            vec![offset("delete-backend-a", 4), offset("delete-backend-b", 5)],
        )
        .await
        .unwrap();
    metadata
        .create_acl(allow_rule(
            "User:delete-user",
            AclResourceType::Group,
            "delete-backend-group",
            AclOperation::Delete,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let delete_records = delete_records_for(&[("delete-backend-a", 1), ("delete-backend-b", 1)]);
    let response = handle_as(
        &broker,
        "delete-user",
        ApiKey::DeleteRecords,
        2,
        8201,
        &delete_records,
    )
    .await;
    let response: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, response);
    assert!(response.topics.iter().all(|topic| {
        topic.partitions[0].error_code == UNKNOWN_SERVER_ERROR
            && topic.partitions[0].low_watermark == -1
    }));
    for topic in ["delete-backend-a", "delete-backend-b"] {
        assert_eq!(
            metadata
                .list_offset(&PartitionKey::new(topic, 0), -2)
                .await
                .unwrap(),
            0
        );
    }

    let offset_delete = offset_delete_for(
        "delete-backend-group",
        &["delete-backend-a", "delete-backend-b"],
    );
    let response = handle_as(
        &broker,
        "delete-user",
        ApiKey::OffsetDelete,
        0,
        8202,
        &offset_delete,
    )
    .await;
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.topics.is_empty());
    assert_eq!(
        metadata
            .fetch_offsets(
                "delete-backend-group",
                &[
                    PartitionKey::new("delete-backend-a", 0),
                    PartitionKey::new("delete-backend-b", 0),
                ],
            )
            .await
            .unwrap()
            .len(),
        2
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Group));
    let response = handle_as(
        &broker,
        "delete-user",
        ApiKey::OffsetDelete,
        0,
        8203,
        &offset_delete,
    )
    .await;
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.topics.is_empty());
}

#[tokio::test]
async fn destructive_offset_authorization_preserves_mixed_results_in_memory() {
    assert_destructive_offset_authorization(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn destructive_offset_authorization_preserves_mixed_results_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_destructive_offset_authorization(Arc::new(store), &Uuid::new_v4().simple().to_string())
        .await;
}

async fn assert_destructive_offset_authorization(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("visible-delete-{suffix}");
    let hidden = format!("hidden-delete-{suffix}");
    let group = format!("delete-group-{suffix}");
    let user = format!("delete-user-{suffix}");
    for topic in [&visible, &hidden] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    let object = ObjectRef {
        key: format!("objects/delete-authorization-{suffix}"),
        size: 20,
    };
    metadata.stage_object(object.clone()).await.unwrap();
    metadata
        .commit_object(
            object,
            vec![batch_for(&visible, 0, 10), batch_for(&hidden, 10, 20)],
        )
        .await
        .unwrap();
    metadata
        .commit_offsets(&group, vec![offset(&visible, 4), offset(&hidden, 5)])
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            &format!("User:{user}"),
            &visible,
            AclOperation::Delete,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            &format!("User:{user}"),
            &visible,
            AclOperation::Read,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    metadata
        .create_acl(allow_rule(
            &format!("User:{user}"),
            AclResourceType::Group,
            &group,
            AclOperation::Delete,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());

    let response = handle_as(
        &broker,
        &user,
        ApiKey::DeleteRecords,
        2,
        8204,
        &delete_records_for(&[(&visible, 1), (&hidden, 1)]),
    )
    .await;
    let response: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.topics[1].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(
        metadata
            .list_offset(&PartitionKey::new(&visible, 0), -2)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        metadata
            .list_offset(&PartitionKey::new(&hidden, 0), -2)
            .await
            .unwrap(),
        0
    );

    let response = handle_as(
        &broker,
        &user,
        ApiKey::OffsetDelete,
        0,
        8205,
        &offset_delete_for(&group, &[&visible, &hidden]),
    )
    .await;
    let response: OffsetDeleteResponse = decode_response(ApiKey::OffsetDelete, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.topics[1].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    let remaining = metadata
        .fetch_offsets(
            &group,
            &[
                PartitionKey::new(&visible, 0),
                PartitionKey::new(&hidden, 0),
            ],
        )
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(!remaining.contains_key(&PartitionKey::new(&visible, 0)));
    assert_eq!(
        remaining
            .get(&PartitionKey::new(&hidden, 0))
            .unwrap()
            .offset,
        5
    );
}

fn batch(byte_start: u64, byte_end: u64) -> BatchDraft {
    batch_for("events", byte_start, byte_end)
}

fn batch_for(topic: &str, byte_start: u64, byte_end: u64) -> BatchDraft {
    BatchDraft {
        partition: PartitionKey::new(topic, 0),
        byte_start,
        byte_end,
        record_count: 2,
        timestamp_ms: 1,
        checksum: None,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}

fn offset(topic: &str, offset: i64) -> OffsetCommit {
    OffsetCommit {
        partition: PartitionKey::new(topic, 0),
        offset,
        leader_epoch: -1,
        metadata: None,
        retention_time_ms: None,
    }
}

fn delete_records_request(offset: i64) -> DeleteRecordsRequest {
    delete_records_for(&[("events", offset)])
}

fn delete_records_for(topics: &[(&str, i64)]) -> DeleteRecordsRequest {
    DeleteRecordsRequest::default().with_topics(
        topics
            .iter()
            .map(|(topic, offset)| {
                DeleteRecordsTopic::default()
                    .with_name(topic_name(topic))
                    .with_partitions(vec![
                        DeleteRecordsPartition::default()
                            .with_partition_index(0)
                            .with_offset(*offset),
                    ])
            })
            .collect(),
    )
}

fn offset_delete_request(group: &str) -> OffsetDeleteRequest {
    OffsetDeleteRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group.to_owned())))
        .with_topics(vec![
            OffsetDeleteRequestTopic::default()
                .with_name(topic_name("events"))
                .with_partitions(vec![
                    OffsetDeleteRequestPartition::default().with_partition_index(0),
                    OffsetDeleteRequestPartition::default().with_partition_index(1),
                ]),
        ])
}

fn offset_delete_for(group: &str, topics: &[&str]) -> OffsetDeleteRequest {
    OffsetDeleteRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group.to_owned())))
        .with_topics(
            topics
                .iter()
                .map(|topic| {
                    OffsetDeleteRequestTopic::default()
                        .with_name(topic_name(topic))
                        .with_partitions(vec![
                            OffsetDeleteRequestPartition::default().with_partition_index(0),
                        ])
                })
                .collect(),
        )
}

fn allow_rule(
    principal: &str,
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
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
