use super::acl_tests::{handle_as, topic_rule};
use super::tests::{decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_PRODUCER_EPOCH, NO_ERROR, OPERATION_NOT_ATTEMPTED,
    PRODUCER_FENCED, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
};
use kafka_protocol::messages::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
    MetadataStore, PostgresMetadataStore, ProducerSession,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn client_add_partitions_is_atomic_in_memory() {
    assert_client_add_partitions_atomic(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn client_add_partitions_is_atomic_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_client_add_partitions_atomic(Arc::new(store), &Uuid::new_v4().simple().to_string())
        .await;
}

async fn assert_client_add_partitions_atomic(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("add-partitions-visible-{suffix}");
    let hidden = format!("add-partitions-hidden-{suffix}");
    let missing = format!("add-partitions-missing-{suffix}");
    let transactional_id = format!("add-partitions-tx-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    let producer = metadata
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    for rule in [
        transactional_rule("User:txn-client", &transactional_id, AclOperation::Write),
        topic_rule(
            "User:txn-client",
            &visible,
            AclOperation::Write,
            AclPermission::Allow,
        ),
        topic_rule(
            "User:txn-client",
            &missing,
            AclOperation::Write,
            AclPermission::Allow,
        ),
    ] {
        metadata.create_acl(rule).await.unwrap();
    }
    let broker = secured_broker(metadata.clone());
    let request = client_request(
        &transactional_id,
        producer,
        vec![
            add_topic(&visible, &[0, 1]),
            add_topic(&hidden, &[0]),
            add_topic(&missing, &[0]),
        ],
    );
    let response = handle_as(
        &broker,
        "txn-client",
        ApiKey::AddPartitionsToTxn,
        3,
        11_100,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 3, response);

    assert_eq!(
        partition_codes(&response),
        [
            OPERATION_NOT_ATTEMPTED,
            UNKNOWN_TOPIC_OR_PARTITION,
            TOPIC_AUTHORIZATION_FAILED,
            UNKNOWN_TOPIC_OR_PARTITION,
        ]
    );
    assert_transaction_partitions(metadata.as_ref(), &transactional_id, &[]).await;

    let request = client_request(&transactional_id, producer, vec![add_topic(&visible, &[0])]);
    let response = handle_as(
        &broker,
        "txn-client",
        ApiKey::AddPartitionsToTxn,
        3,
        11_101,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 3, response);
    assert_eq!(partition_codes(&response), [NO_ERROR]);
    assert_transaction_partitions(
        metadata.as_ref(),
        &transactional_id,
        &[(visible.as_str(), 0)],
    )
    .await;
}

#[tokio::test]
async fn client_add_partitions_authorizer_failure_is_request_wide_and_non_mutating() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("add-partitions-backend", 1)
        .await
        .unwrap();
    let transactional_id = "add-partitions-backend-tx";
    let producer = metadata
        .init_producer(Some(transactional_id), 60_000, None)
        .await
        .unwrap();
    metadata
        .create_acl(transactional_rule(
            "User:txn-client",
            transactional_id,
            AclOperation::Write,
        ))
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            "User:txn-client",
            "add-partitions-backend",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let broker = secured_broker(metadata.clone());
    let request = client_request(
        transactional_id,
        producer,
        vec![add_topic("add-partitions-backend", &[0])],
    );
    let response = handle_as(
        &broker,
        "txn-client",
        ApiKey::AddPartitionsToTxn,
        3,
        11_102,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 3, response);

    assert_eq!(partition_codes(&response), [UNKNOWN_SERVER_ERROR]);
    assert_transaction_partitions(metadata.as_ref(), transactional_id, &[]).await;
}

#[tokio::test]
async fn broker_add_partitions_requires_only_cluster_action_and_is_atomic() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("broker-add-partitions", 1)
        .await
        .unwrap();
    let transactional_id = "broker-add-partitions-tx";
    let producer = metadata
        .init_producer(Some(transactional_id), 60_000, None)
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());
    let request = broker_request(
        transactional_id,
        producer,
        false,
        vec![add_topic("broker-add-partitions", &[0])],
    );

    let response = handle_as(
        &broker,
        "broker-node",
        ApiKey::AddPartitionsToTxn,
        4,
        11_103,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 4, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);
    assert!(response.results_by_transaction.is_empty());

    metadata
        .create_acl(cluster_action_rule("User:broker-node"))
        .await
        .unwrap();
    let invalid = broker_request(
        transactional_id,
        producer,
        false,
        vec![add_topic("broker-add-partitions", &[0, 1])],
    );
    let response = handle_as(
        &broker,
        "broker-node",
        ApiKey::AddPartitionsToTxn,
        4,
        11_104,
        &invalid,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        transaction_partition_codes(&response),
        [OPERATION_NOT_ATTEMPTED, UNKNOWN_TOPIC_OR_PARTITION]
    );
    assert_transaction_partitions(metadata.as_ref(), transactional_id, &[]).await;

    let response = handle_as(
        &broker,
        "broker-node",
        ApiKey::AddPartitionsToTxn,
        4,
        11_105,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 4, response);
    assert_eq!(transaction_partition_codes(&response), [NO_ERROR]);
    assert_transaction_partitions(
        metadata.as_ref(),
        transactional_id,
        &[("broker-add-partitions", 0)],
    )
    .await;

    metadata.set_authorization_failure_for(Some(AclResourceType::Cluster));
    let response = handle_as(
        &broker,
        "broker-node",
        ApiKey::AddPartitionsToTxn,
        4,
        11_106,
        &request,
    )
    .await;
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 4, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.results_by_transaction.is_empty());
}

#[tokio::test]
async fn old_add_partitions_versions_downconvert_producer_fencing() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("add-partitions-fenced", 1)
        .await
        .unwrap();
    let transactional_id = "add-partitions-fenced-tx";
    let old = metadata
        .init_producer(Some(transactional_id), 60_000, None)
        .await
        .unwrap();
    let current = metadata
        .init_producer(Some(transactional_id), 60_000, None)
        .await
        .unwrap();
    assert!(current.producer_epoch > old.producer_epoch);
    let broker = Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );
    let request = client_request(
        transactional_id,
        old,
        vec![add_topic("add-partitions-fenced", &[0])],
    );

    for (version, expected) in [(1, INVALID_PRODUCER_EPOCH), (2, PRODUCER_FENCED)] {
        let response = broker
            .handle_request(request_frame(
                ApiKey::AddPartitionsToTxn,
                version,
                11_107 + i32::from(version),
                &request,
            ))
            .await
            .unwrap();
        let response: AddPartitionsToTxnResponse =
            decode_response(ApiKey::AddPartitionsToTxn, version, response);
        assert_eq!(partition_codes(&response), [expected]);
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

fn client_request(
    transactional_id: &str,
    producer: ProducerSession,
    topics: Vec<AddPartitionsToTxnTopic>,
) -> AddPartitionsToTxnRequest {
    AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(kafka_transactional_id(transactional_id.to_owned()))
        .with_v3_and_below_producer_id(producer.producer_id.into())
        .with_v3_and_below_producer_epoch(producer.producer_epoch)
        .with_v3_and_below_topics(topics)
}

fn broker_request(
    transactional_id: &str,
    producer: ProducerSession,
    verify_only: bool,
    topics: Vec<AddPartitionsToTxnTopic>,
) -> AddPartitionsToTxnRequest {
    AddPartitionsToTxnRequest::default().with_transactions(vec![
        AddPartitionsToTxnTransaction::default()
            .with_transactional_id(kafka_transactional_id(transactional_id.to_owned()))
            .with_producer_id(producer.producer_id.into())
            .with_producer_epoch(producer.producer_epoch)
            .with_verify_only(verify_only)
            .with_topics(topics),
    ])
}

fn add_topic(name: &str, partitions: &[i32]) -> AddPartitionsToTxnTopic {
    AddPartitionsToTxnTopic::default()
        .with_name(topic_name(name))
        .with_partitions(partitions.to_vec())
}

fn partition_codes(response: &AddPartitionsToTxnResponse) -> Vec<i16> {
    response
        .results_by_topic_v3_and_below
        .iter()
        .flat_map(|topic| {
            topic
                .results_by_partition
                .iter()
                .map(|partition| partition.partition_error_code)
        })
        .collect()
}

fn transaction_partition_codes(response: &AddPartitionsToTxnResponse) -> Vec<i16> {
    response
        .results_by_transaction
        .iter()
        .flat_map(|transaction| &transaction.topic_results)
        .flat_map(|topic| {
            topic
                .results_by_partition
                .iter()
                .map(|partition| partition.partition_error_code)
        })
        .collect()
}

async fn assert_transaction_partitions(
    metadata: &dyn MetadataStore,
    transactional_id: &str,
    expected: &[(&str, i32)],
) {
    let descriptions = metadata
        .describe_transactions(&[transactional_id.to_owned()])
        .await
        .unwrap();
    let mut actual = descriptions
        .get(transactional_id)
        .map(|description| {
            description
                .partitions
                .iter()
                .map(|partition| (partition.topic.as_str(), partition.partition))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

fn kafka_transactional_id(value: String) -> kafka_protocol::messages::TransactionalId {
    kafka_protocol::messages::TransactionalId::from(StrBytes::from_string(value))
}

fn transactional_rule(principal: &str, transactional_id: &str, operation: AclOperation) -> AclRule {
    AclRule {
        resource_type: AclResourceType::TransactionalId,
        resource_name: transactional_id.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}

fn cluster_action_rule(principal: &str) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Cluster,
        resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::ClusterAction,
        permission: AclPermission::Allow,
    }
}
