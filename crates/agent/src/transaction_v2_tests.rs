use super::acl_tests::handle_as;
use super::tests::{broker, decode_response, producer_records, request_frame};
use super::topic_name;
use super::*;
use crate::kafka_error::{
    ILLEGAL_GENERATION, INVALID_PRODUCER_EPOCH, INVALID_REQUEST, INVALID_TRANSACTION_TIMEOUT,
    INVALID_TXN_STATE, NO_ERROR, OFFSET_METADATA_TOO_LARGE, PRODUCER_FENCED,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse,
    InitProducerIdRequest, InitProducerIdResponse, ProduceRequest, ProduceResponse,
    TransactionalId, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclPatternType, AclPermission, AclRule, ConsumerGroupHeartbeat, ConsumerOwnedTopicPartitions,
    FetchIsolation, MemoryMetadataStore, PostgresMetadataStore, ProducerSession,
};
use rutomq_storage::OpenDalObjectStore;
use std::collections::BTreeMap;

#[tokio::test]
async fn init_producer_id_enforces_transaction_timeout_boundaries() {
    let broker = broker();
    let idempotent = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(i32::MAX);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 1, &idempotent))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(response.producer_id.0 >= 0);

    let transactional = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id("invalid-timeout")))
        .with_transaction_timeout_ms(0);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 2, &transactional))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, INVALID_TRANSACTION_TIMEOUT);

    let at_limit = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id("timeout-at-limit")))
        .with_transaction_timeout_ms(900_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 3, &at_limit))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, NO_ERROR);

    let above_limit = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id("timeout-above-limit")))
        .with_transaction_timeout_ms(900_001);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 4, &above_limit))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, INVALID_TRANSACTION_TIMEOUT);
    assert!(
        broker
            .metadata
            .describe_transactions(&["timeout-above-limit".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn transaction_v2_implicitly_registers_produce_partition_and_offset_group() {
    let broker = broker();
    broker
        .metadata
        .alter_broker_config(
            BTreeMap::from([(
                "transaction.partition.verification.enable".to_owned(),
                Some("false".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    broker
        .metadata
        .create_topic("transaction-v2", 1)
        .await
        .unwrap();
    let transactional_id = transactional_id("transaction-v2-id");
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 1, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(producer.error_code, NO_ERROR);

    let produce = transactional_produce(&transactional_id, &producer);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 12, 2, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 12, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    let active = broker
        .metadata
        .describe_transactions(&["transaction-v2-id".to_owned()])
        .await
        .unwrap();
    assert_eq!(
        active["transaction-v2-id"].partitions,
        [PartitionKey::new("transaction-v2", 0)]
    );

    let offset_commit = TxnOffsetCommitRequest::default()
        .with_transactional_id(transactional_id.clone())
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_static_str("transaction-v2-group"),
        ))
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_generation_id(-1)
        .with_topics(vec![
            TxnOffsetCommitRequestTopic::default()
                .with_name(topic_name("transaction-v2"))
                .with_partitions(vec![
                    TxnOffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(1)
                        .with_committed_leader_epoch(0),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::TxnOffsetCommit, 5, 3, &offset_commit))
        .await
        .unwrap();
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    let end = EndTxnRequest::default()
        .with_transactional_id(transactional_id)
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(true);
    let response = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 4, &end))
        .await
        .unwrap();
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.producer_epoch, producer.producer_epoch + 1);
    let partition = PartitionKey::new("transaction-v2", 0);
    let committed = broker
        .metadata
        .fetch_offsets("transaction-v2-group", std::slice::from_ref(&partition))
        .await
        .unwrap();
    assert_eq!(committed[&partition].offset, 1);
    assert_eq!(committed[&partition].leader_epoch, 0);
}

#[tokio::test]
async fn transaction_v1_produce_still_requires_add_partitions() {
    let broker = broker();
    broker
        .metadata
        .create_topic("transaction-v1", 1)
        .await
        .unwrap();
    let transactional_id = transactional_id("transaction-v1-id");
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 1, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);

    let produce = transactional_produce(&transactional_id, &producer);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 11, 2, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 11, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TXN_STATE
    );
}

#[tokio::test]
async fn transaction_v1_dynamic_verification_switch_only_relaxes_partition_membership() {
    let broker = broker();
    broker
        .metadata
        .create_topic("transaction-v1", 2)
        .await
        .unwrap();
    let transactional_id = transactional_id("transaction-v1-verification-id");
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 1, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    broker
        .metadata
        .add_partitions_to_transaction(
            transactional_id.as_str(),
            producer_session(&producer),
            &[PartitionKey::new("transaction-v1", 1)],
            false,
        )
        .await
        .unwrap();

    let produce = transactional_produce(&transactional_id, &producer);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 11, 2, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 11, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TXN_STATE
    );

    broker
        .metadata
        .alter_broker_config(
            BTreeMap::from([(
                "transaction.partition.verification.enable".to_owned(),
                Some("false".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 11, 3, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 11, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );

    let end = EndTxnRequest::default()
        .with_transactional_id(transactional_id)
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(true);
    let response = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 4, &end))
        .await
        .unwrap();
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .fetch(
                &PartitionKey::new("transaction-v1", 0),
                0,
                1024,
                FetchIsolation::ReadCommitted,
            )
            .await
            .unwrap()
            .spans
            .len(),
        1
    );
}

#[tokio::test]
async fn end_txn_v5_empty_abort_bumps_epoch_and_errors_hide_identity() {
    let broker = broker();
    let transactional_id = transactional_id("empty-transaction-v2-id");
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 1, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);

    let commit = EndTxnRequest::default()
        .with_transactional_id(transactional_id.clone())
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(true);
    let response = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 2, &commit))
        .await
        .unwrap();
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, INVALID_TXN_STATE);
    assert_eq!(response.producer_id.0, -1);
    assert_eq!(response.producer_epoch, -1);

    let abort = EndTxnRequest::default()
        .with_transactional_id(transactional_id)
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(false);
    let response = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 3, &abort))
        .await
        .unwrap();
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.producer_epoch, producer.producer_epoch + 1);

    let retry = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 4, &abort))
        .await
        .unwrap();
    let retry: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, retry);
    assert_eq!(retry.producer_id, response.producer_id);
    assert_eq!(retry.producer_epoch, response.producer_epoch);

    let next_abort = EndTxnRequest::default()
        .with_transactional_id(abort.transactional_id)
        .with_producer_id(response.producer_id)
        .with_producer_epoch(response.producer_epoch)
        .with_committed(false);
    let next = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 5, &next_abort))
        .await
        .unwrap();
    let next: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, next);
    assert_eq!(next.error_code, NO_ERROR);
    assert_eq!(next.producer_epoch, response.producer_epoch + 1);
}

#[tokio::test]
async fn transaction_identity_and_legacy_fencing_match_kafka_in_memory() {
    assert_transaction_identity_and_legacy_fencing(Arc::new(MemoryMetadataStore::new()), "memory")
        .await;
}

#[tokio::test]
async fn transaction_identity_and_legacy_fencing_match_kafka_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_transaction_identity_and_legacy_fencing(
        Arc::new(store),
        &Uuid::new_v4().simple().to_string(),
    )
    .await;
}

async fn assert_transaction_identity_and_legacy_fencing(
    metadata: Arc<dyn MetadataStore>,
    suffix: &str,
) {
    let broker = Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );
    let transactional_id_value = format!("legacy-fencing-{suffix}");
    let transactional_id = transactional_id(&transactional_id_value);
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let first = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 40, &init))
        .await
        .unwrap();
    let first: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, first);
    assert_eq!(first.error_code, NO_ERROR);
    let current = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 41, &init))
        .await
        .unwrap();
    let current: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, current);
    assert_eq!(current.error_code, NO_ERROR);
    assert_eq!(current.producer_id, first.producer_id);
    assert!(current.producer_epoch > first.producer_epoch);

    let malformed = init
        .clone()
        .with_producer_id(first.producer_id)
        .with_producer_epoch(-1);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 3, 42, &malformed))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 3, response);
    assert_eq!(response.error_code, INVALID_REQUEST);

    let stale_init = init
        .with_producer_id(first.producer_id)
        .with_producer_epoch(first.producer_epoch);
    for (version, expected) in [(3, INVALID_PRODUCER_EPOCH), (4, PRODUCER_FENCED)] {
        let response = broker
            .handle_request(request_frame(
                ApiKey::InitProducerId,
                version,
                43 + i32::from(version),
                &stale_init,
            ))
            .await
            .unwrap();
        let response: InitProducerIdResponse =
            decode_response(ApiKey::InitProducerId, version, response);
        assert_eq!(response.error_code, expected);
    }

    let add_offsets = AddOffsetsToTxnRequest::default()
        .with_transactional_id(transactional_id.clone())
        .with_producer_id(first.producer_id)
        .with_producer_epoch(first.producer_epoch)
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_static_str("legacy-fencing-group"),
        ));
    let end = EndTxnRequest::default()
        .with_transactional_id(transactional_id)
        .with_producer_id(first.producer_id)
        .with_producer_epoch(first.producer_epoch)
        .with_committed(false);
    for (version, expected) in [(1, INVALID_PRODUCER_EPOCH), (2, PRODUCER_FENCED)] {
        let response = broker
            .handle_request(request_frame(
                ApiKey::AddOffsetsToTxn,
                version,
                50 + i32::from(version),
                &add_offsets,
            ))
            .await
            .unwrap();
        let response: AddOffsetsToTxnResponse =
            decode_response(ApiKey::AddOffsetsToTxn, version, response);
        assert_eq!(response.error_code, expected);

        let response = broker
            .handle_request(request_frame(
                ApiKey::EndTxn,
                version,
                60 + i32::from(version),
                &end,
            ))
            .await
            .unwrap();
        let response: EndTxnResponse = decode_response(ApiKey::EndTxn, version, response);
        assert_eq!(response.error_code, expected);
    }
}

#[tokio::test]
async fn txn_offset_commit_authorizer_failure_is_request_wide_and_stages_nothing() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let transactional_id = "txn-offset-backend-id";
    let group = "txn-offset-backend-group";
    let user = "txn-offset-backend-user";
    for topic in ["txn-offset-backend-a", "txn-offset-backend-b"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    create_allow_rules(
        metadata.as_ref(),
        user,
        transactional_id,
        group,
        &["txn-offset-backend-a", "txn-offset-backend-b"],
    )
    .await;
    let broker = secured_broker(metadata.clone());
    let producer = init_producer_as(&broker, user, transactional_id, 50).await;
    let session = producer_session(&producer);
    metadata
        .add_partitions_to_transaction(
            transactional_id,
            session,
            &[PartitionKey::new("txn-offset-backend-a", 0)],
            false,
        )
        .await
        .unwrap();

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let request = txn_offset_request(
        transactional_id,
        group,
        session,
        &[
            ("txn-offset-backend-a", &[(0, 12)]),
            ("txn-offset-backend-b", &[(0, 13)]),
        ],
    );
    let response = handle_as(&broker, user, ApiKey::TxnOffsetCommit, 5, 51, &request).await;
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert!(response.topics.iter().all(|topic| {
        topic
            .partitions
            .iter()
            .all(|partition| partition.error_code == UNKNOWN_SERVER_ERROR)
    }));

    metadata.set_authorization_failure_for(None);
    metadata
        .end_transaction(transactional_id, session, true)
        .await
        .unwrap();
    let committed = metadata
        .fetch_offsets(
            group,
            &[
                PartitionKey::new("txn-offset-backend-a", 0),
                PartitionKey::new("txn-offset-backend-b", 0),
            ],
        )
        .await
        .unwrap();
    assert!(committed.is_empty());
}

#[tokio::test]
async fn txn_offset_commit_enforces_kafka_metadata_string_length_before_staging() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("txn-offset-metadata-limit", 2)
        .await
        .unwrap();
    let config = AgentConfig {
        offset_metadata_max_bytes: 4,
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let transaction_id = "txn-offset-metadata-id";
    let group = "txn-offset-metadata-group";
    let producer = metadata
        .init_producer(Some(transaction_id), 60_000, None)
        .await
        .unwrap();
    let mut request = txn_offset_request(
        transaction_id,
        group,
        producer,
        &[("txn-offset-metadata-limit", &[(0, 21), (1, 22), (9, 23)])],
    );
    request.topics[0].partitions[0].committed_metadata =
        Some(StrBytes::from_string("😀😀".to_owned()));
    request.topics[0].partitions[1].committed_metadata =
        Some(StrBytes::from_string("😀😀x".to_owned()));
    request.topics[0].partitions[2].committed_metadata =
        Some(StrBytes::from_string("also-too-large".to_owned()));

    let response = broker
        .handle_request(request_frame(ApiKey::TxnOffsetCommit, 5, 52, &request))
        .await
        .unwrap();
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.topics[0].partitions[1].error_code,
        OFFSET_METADATA_TOO_LARGE
    );
    assert_eq!(
        response.topics[0].partitions[2].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );

    metadata
        .end_transaction(transaction_id, producer, true)
        .await
        .unwrap();
    let accepted = PartitionKey::new("txn-offset-metadata-limit", 0);
    let rejected = PartitionKey::new("txn-offset-metadata-limit", 1);
    let committed = metadata
        .fetch_offsets(group, &[accepted.clone(), rejected.clone()])
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[&accepted].offset, 21);
    assert_eq!(committed[&accepted].metadata.as_deref(), Some("😀😀"));
    assert!(!committed.contains_key(&rejected));
}

#[tokio::test]
async fn txn_offset_commit_validates_mixed_resources_in_memory() {
    assert_txn_offset_validation(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn txn_offset_commit_validates_mixed_resources_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_txn_offset_validation(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn txn_offset_commit_uses_partition_assignment_epochs_in_memory() {
    assert_txn_offset_assignment_epochs(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn txn_offset_commit_uses_partition_assignment_epochs_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_txn_offset_assignment_epochs(Arc::new(store), &Uuid::new_v4().simple().to_string())
        .await;
}

async fn assert_txn_offset_assignment_epochs(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let topic_name_value = format!("txn-assignment-topic-{suffix}");
    let group_id = format!("txn-assignment-group-{suffix}");
    let topic = metadata.create_topic(&topic_name_value, 2).await.unwrap();
    let first = metadata
        .consumer_group_heartbeat(consumer_heartbeat(
            &group_id,
            "member-a",
            0,
            Some(&topic_name_value),
            Some(Vec::new()),
        ))
        .await
        .unwrap();
    metadata
        .consumer_group_heartbeat(consumer_heartbeat(
            &group_id,
            "member-a",
            first.member_epoch,
            None,
            Some(vec![ConsumerOwnedTopicPartitions {
                topic_id: topic.id,
                partitions: vec![0, 1],
            }]),
        ))
        .await
        .unwrap();
    metadata
        .consumer_group_heartbeat(consumer_heartbeat(
            &group_id,
            "member-b",
            0,
            Some(&topic_name_value),
            Some(Vec::new()),
        ))
        .await
        .unwrap();
    let revoking = metadata
        .consumer_group_heartbeat(consumer_heartbeat(
            &group_id,
            "member-a",
            first.member_epoch,
            None,
            None,
        ))
        .await
        .unwrap();
    let retained = revoking.assignment.unwrap()[0].partitions.clone();
    assert_eq!(retained, [0]);
    let advanced = metadata
        .consumer_group_heartbeat(consumer_heartbeat(
            &group_id,
            "member-a",
            first.member_epoch,
            None,
            Some(vec![ConsumerOwnedTopicPartitions {
                topic_id: topic.id,
                partitions: retained,
            }]),
        ))
        .await
        .unwrap();
    assert_eq!(advanced.member_epoch, first.member_epoch + 1);

    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );
    let valid_transactional_id = format!("txn-assignment-valid-{suffix}");
    let valid_producer = metadata
        .init_producer(Some(&valid_transactional_id), 60_000, None)
        .await
        .unwrap();
    let valid = member_txn_offset_request(
        &valid_transactional_id,
        &group_id,
        valid_producer,
        first.member_epoch,
        &topic_name_value,
        &[(0, 20)],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::TxnOffsetCommit, 5, 70, &valid))
        .await
        .unwrap();
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    metadata
        .end_transaction(&valid_transactional_id, valid_producer, true)
        .await
        .unwrap();

    let invalid_transactional_id = format!("txn-assignment-invalid-{suffix}");
    let invalid_producer = metadata
        .init_producer(Some(&invalid_transactional_id), 60_000, None)
        .await
        .unwrap();
    metadata
        .add_offsets_to_transaction(&invalid_transactional_id, invalid_producer, &group_id)
        .await
        .unwrap();
    let invalid = member_txn_offset_request(
        &invalid_transactional_id,
        &group_id,
        invalid_producer,
        first.member_epoch,
        &topic_name_value,
        &[(0, 30), (1, 31)],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::TxnOffsetCommit, 5, 71, &invalid))
        .await
        .unwrap();
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.error_code == ILLEGAL_GENERATION)
    );
    metadata
        .end_transaction(&invalid_transactional_id, invalid_producer, true)
        .await
        .unwrap();

    let retained_partition = PartitionKey::new(&topic_name_value, 0);
    let moved_partition = PartitionKey::new(&topic_name_value, 1);
    let committed = metadata
        .fetch_offsets(
            &group_id,
            &[retained_partition.clone(), moved_partition.clone()],
        )
        .await
        .unwrap();
    assert_eq!(committed[&retained_partition].offset, 20);
    assert!(!committed.contains_key(&moved_partition));
}

async fn assert_txn_offset_validation(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("txn-offset-visible-{suffix}");
    let hidden = format!("txn-offset-hidden-{suffix}");
    let missing = format!("txn-offset-missing-{suffix}");
    let transaction_id = format!("txn-offset-id-{suffix}");
    let group = format!("txn-offset-group-{suffix}");
    let user = format!("txn-offset-user-{suffix}");
    metadata.create_topic(&visible, 2).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    create_allow_rules(
        metadata.as_ref(),
        &user,
        &transaction_id,
        &group,
        &[&visible, &missing],
    )
    .await;
    metadata
        .create_acl(acl_rule(
            &format!("User:{user}"),
            AclResourceType::Topic,
            &hidden,
            AclOperation::Read,
            AclPermission::Deny,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());
    let producer = init_producer_as(&broker, &user, &transaction_id, 60).await;
    let session = producer_session(&producer);
    let request = txn_offset_request(
        &transaction_id,
        &group,
        session,
        &[
            (&hidden, &[(0, 20)]),
            (&missing, &[(0, 21)]),
            (&visible, &[(9, 22), (0, 23)]),
        ],
    );
    let response = handle_as(&broker, &user, ApiKey::TxnOffsetCommit, 5, 61, &request).await;
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 5, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(
        response.topics[1].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        response.topics[2].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(response.topics[2].partitions[1].error_code, NO_ERROR);

    let end = EndTxnRequest::default()
        .with_transactional_id(transactional_id(&transaction_id))
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(true);
    let response = handle_as(&broker, &user, ApiKey::EndTxn, 5, 62, &end).await;
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, NO_ERROR);

    let visible_partition = PartitionKey::new(&visible, 0);
    let committed = metadata
        .fetch_offsets(
            &group,
            &[visible_partition.clone(), PartitionKey::new(&hidden, 0)],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[&visible_partition].offset, 23);
}

fn consumer_heartbeat(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
    owned_partitions: Option<Vec<ConsumerOwnedTopicPartitions>>,
) -> ConsumerGroupHeartbeat {
    ConsumerGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
        subscribed_topic_names: topic.map(|topic| vec![topic.to_owned()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions,
        client_id: "transaction-assignment-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        assignment_interval_ms: 0,
        max_size: i32::MAX,
    }
}

fn member_txn_offset_request(
    transactional_id_value: &str,
    group_id: &str,
    producer: ProducerSession,
    member_epoch: i32,
    topic: &str,
    partitions: &[(i32, i64)],
) -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest::default()
        .with_transactional_id(transactional_id(transactional_id_value))
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group_id.to_owned()),
        ))
        .with_producer_id(producer.producer_id.into())
        .with_producer_epoch(producer.producer_epoch)
        .with_generation_id(member_epoch)
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_topics(vec![
            TxnOffsetCommitRequestTopic::default()
                .with_name(topic_name(topic))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|(partition, offset)| {
                            TxnOffsetCommitRequestPartition::default()
                                .with_partition_index(*partition)
                                .with_committed_offset(*offset)
                        })
                        .collect(),
                ),
        ])
}

async fn init_producer_as(
    broker: &Broker,
    user: &str,
    transaction_id: &str,
    correlation_id: i32,
) -> InitProducerIdResponse {
    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id(transaction_id)))
        .with_transaction_timeout_ms(60_000);
    let response = handle_as(
        broker,
        user,
        ApiKey::InitProducerId,
        6,
        correlation_id,
        &request,
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, NO_ERROR);
    response
}

fn txn_offset_request(
    transactional_id_value: &str,
    group: &str,
    producer: ProducerSession,
    topics: &[(&str, &[(i32, i64)])],
) -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest::default()
        .with_transactional_id(transactional_id(transactional_id_value))
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group.to_owned()),
        ))
        .with_producer_id(producer.producer_id.into())
        .with_producer_epoch(producer.producer_epoch)
        .with_generation_id(-1)
        .with_topics(
            topics
                .iter()
                .map(|(topic, partitions)| {
                    TxnOffsetCommitRequestTopic::default()
                        .with_name(topic_name(topic))
                        .with_partitions(
                            partitions
                                .iter()
                                .map(|(partition, offset)| {
                                    TxnOffsetCommitRequestPartition::default()
                                        .with_partition_index(*partition)
                                        .with_committed_offset(*offset)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}

async fn create_allow_rules(
    metadata: &dyn MetadataStore,
    user: &str,
    transactional_id: &str,
    group: &str,
    topics: &[&str],
) {
    let principal = format!("User:{user}");
    for rule in [
        acl_rule(
            &principal,
            AclResourceType::TransactionalId,
            transactional_id,
            AclOperation::Write,
            AclPermission::Allow,
        ),
        acl_rule(
            &principal,
            AclResourceType::Group,
            group,
            AclOperation::Read,
            AclPermission::Allow,
        ),
    ] {
        metadata.create_acl(rule).await.unwrap();
    }
    for topic in topics {
        metadata
            .create_acl(acl_rule(
                &principal,
                AclResourceType::Topic,
                topic,
                AclOperation::Read,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
}

fn acl_rule(
    principal: &str,
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
    permission: AclPermission,
) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission,
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

fn producer_session(response: &InitProducerIdResponse) -> ProducerSession {
    ProducerSession {
        producer_id: response.producer_id.0,
        producer_epoch: response.producer_epoch,
    }
}

fn transactional_produce(
    transactional_id: &TransactionalId,
    producer: &InitProducerIdResponse,
) -> ProduceRequest {
    ProduceRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name(if transactional_id.as_str().contains("v2") {
                    "transaction-v2"
                } else {
                    "transaction-v1"
                }))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(producer_records(
                            producer.producer_id.0,
                            producer.producer_epoch,
                            0,
                            true,
                            b"transactional",
                        ))),
                ]),
        ])
}

fn transactional_id(value: &str) -> TransactionalId {
    TransactionalId::from(StrBytes::from_string(value.to_owned()))
}
