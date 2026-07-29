use super::acl_tests::{acl_broker, handle_as, topic_rule};
use super::tests::{broker, decode_response, producer_records, request_frame, sample_records};
use super::*;
use crate::kafka_error::{
    INVALID_RECORD, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceResponse, TransactionalId};
use rutomq_control::{
    AclOperation, AclPermission, AclResourceType, MemoryMetadataStore, MetadataStore, PartitionKey,
    PostgresMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn produce_rejects_missing_topics_and_invalid_partitions_without_mutation() {
    let broker = broker();
    let existing = broker
        .metadata
        .create_topic("produce-existing", 1)
        .await
        .unwrap();
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            produce_topic(&existing.name, &[0, 4]),
            produce_topic("produce-missing", &[0]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 12, 6001, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 12, response);

    let existing_response = topic_response(&response, &existing.name);
    assert_eq!(partition_error(existing_response, 0), NO_ERROR);
    assert_eq!(
        partition_error(existing_response, 4),
        UNKNOWN_TOPIC_OR_PARTITION
    );
    let missing_response = topic_response(&response, "produce-missing");
    assert_eq!(
        partition_error(missing_response, 0),
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert!(
        broker
            .metadata
            .topic("produce-missing")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        existing_response
            .partition_responses
            .iter()
            .find(|partition| partition.index == 0)
            .unwrap()
            .base_offset,
        0
    );
}

#[tokio::test]
async fn produce_checks_write_before_revealing_a_missing_topic() {
    let (broker, metadata) = acl_broker();
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![produce_topic("private-missing-produce", &[0])]);

    let response = handle_as(&broker, "alice", ApiKey::Produce, 12, 6002, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);
    assert_eq!(
        partition_error(&response.responses[0], 0),
        TOPIC_AUTHORIZATION_FAILED
    );

    metadata
        .create_acl(topic_rule(
            "User:alice",
            "private-missing-produce",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = handle_as(&broker, "alice", ApiKey::Produce, 12, 6003, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);
    assert_eq!(
        partition_error(&response.responses[0], 0),
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert!(
        metadata
            .topic("private-missing-produce")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn produce_authorizer_failure_is_request_wide_and_non_mutating() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    for topic in ["produce-backend-a", "produce-backend-b"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let broker = secured_broker(metadata.clone());
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            produce_topic("produce-backend-a", &[0]),
            produce_topic("produce-backend-b", &[0]),
        ]);
    let response = handle_as(&broker, "producer", ApiKey::Produce, 12, 6004, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);

    assert_eq!(
        response
            .responses
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>(),
        ["produce-backend-a", "produce-backend-b"]
    );
    assert!(response.responses.iter().all(|topic| {
        topic.partition_responses[0].error_code == UNKNOWN_SERVER_ERROR
            && topic.partition_responses[0].base_offset == -1
    }));
    for topic in ["produce-backend-a", "produce-backend-b"] {
        assert_eq!(
            metadata
                .list_offset(&PartitionKey::new(topic, 0), -1)
                .await
                .unwrap(),
            0
        );
    }

    metadata.set_authorization_failure_for(Some(AclResourceType::TransactionalId));
    let transactional = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId::from(StrBytes::from_string(
            "produce-backend-transaction".to_owned(),
        ))))
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("produce-backend-a"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(producer_records(1, 0, 0, true, b"transactional"))),
                ]),
        ]);
    let response = handle_as(
        &broker,
        "producer",
        ApiKey::Produce,
        12,
        6009,
        &transactional,
    )
    .await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        UNKNOWN_SERVER_ERROR
    );
    assert_eq!(
        metadata
            .list_offset(&PartitionKey::new("produce-backend-a", 0), -1)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn produce_orders_authorized_before_denied_in_memory() {
    assert_produce_authorization_order(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn produce_orders_authorized_before_denied_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_produce_authorization_order(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn non_transactional_payload_does_not_require_transactional_id_acl() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("non-transactional-payload", 1)
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            "User:producer",
            "non-transactional-payload",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());
    let request = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId::from(StrBytes::from_string(
            "unprivileged-transaction".to_owned(),
        ))))
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![produce_topic("non-transactional-payload", &[0])]);
    let response = handle_as(&broker, "producer", ApiKey::Produce, 12, 6005, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_RECORD
    );
    assert_eq!(
        metadata
            .list_offset(&PartitionKey::new("non-transactional-payload", 0), -1)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn idempotent_produce_does_not_repeat_cluster_authorization() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("topic-writer", 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:producer",
            "topic-writer",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let producer = metadata.init_producer(None, 60_000, None).await.unwrap();
    let broker = secured_broker(metadata);
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("topic-writer"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(producer_records(
                            producer.producer_id,
                            producer.producer_epoch,
                            0,
                            false,
                            b"idempotent",
                        ))),
                ]),
        ]);
    let response = handle_as(&broker, "producer", ApiKey::Produce, 12, 6006, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 0);
}

#[tokio::test]
async fn acks_zero_closes_the_request_on_produce_error() {
    let broker = broker();
    let request = ProduceRequest::default()
        .with_acks(0)
        .with_topic_data(vec![produce_topic("acks-zero-missing", &[0])]);
    let error = broker
        .handle_request(request_frame(ApiKey::Produce, 12, 6007, &request))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("acks=0 Produce failed"));
}

async fn assert_produce_authorization_order(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let principal = format!("producer-{suffix}");
    let visible = format!("visible-produce-{suffix}");
    let hidden = format!("hidden-produce-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            &format!("User:{principal}"),
            &visible,
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            produce_topic(&hidden, &[0]),
            produce_topic(&visible, &[0]),
        ]);
    let response = handle_as(&broker, &principal, ApiKey::Produce, 12, 6008, &request).await;
    let response: ProduceResponse =
        super::acl_tests::decode_response(ApiKey::Produce, 12, response);

    assert_eq!(
        response
            .responses
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>(),
        [visible.as_str(), hidden.as_str()]
    );
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    assert_eq!(
        response.responses[1].partition_responses[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert!(metadata.topic(&hidden).await.unwrap().is_none());
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

fn produce_topic(name: &str, partitions: &[i32]) -> TopicProduceData {
    TopicProduceData::default()
        .with_name(topic_name(name))
        .with_partition_data(
            partitions
                .iter()
                .map(|index| {
                    PartitionProduceData::default()
                        .with_index(*index)
                        .with_records(Some(sample_records()))
                })
                .collect(),
        )
}

fn topic_response<'a>(
    response: &'a ProduceResponse,
    name: &str,
) -> &'a kafka_protocol::messages::produce_response::TopicProduceResponse {
    response
        .responses
        .iter()
        .find(|topic| topic.name.as_str() == name)
        .unwrap()
}

fn partition_error(
    topic: &kafka_protocol::messages::produce_response::TopicProduceResponse,
    index: i32,
) -> i16 {
    topic
        .partition_responses
        .iter()
        .find(|partition| partition.index == index)
        .unwrap()
        .error_code
}
