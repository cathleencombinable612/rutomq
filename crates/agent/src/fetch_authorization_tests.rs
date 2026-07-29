use super::acl_tests::{handle_as, topic_rule};
use super::tests::{decode_response, request_frame, sample_records};
use super::*;
use crate::kafka_error::{
    NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{FetchRequest, FetchResponse, ProduceRequest};
use rutomq_control::{
    AclOperation, AclPermission, AclResourceType, MemoryMetadataStore, MetadataStore,
    PostgresMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn fetch_orders_authorized_before_denied_and_hides_missing_topics_in_memory() {
    assert_fetch_authorization_order(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn fetch_orders_authorized_before_denied_and_hides_missing_topics_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_fetch_authorization_order(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

async fn assert_fetch_authorization_order(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("visible-fetch-{suffix}");
    let hidden = format!("hidden-fetch-{suffix}");
    let missing = format!("missing-fetch-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:fetch-reader",
            &visible,
            AclOperation::Read,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata, Arc::new(Metrics::new().unwrap()));
    let request = named_fetch(&[&hidden, &visible, &missing]);
    let response = handle_as(&broker, "fetch-reader", ApiKey::Fetch, 12, 10_900, &request).await;
    let response: FetchResponse = decode_response(ApiKey::Fetch, 12, response);

    assert_eq!(
        response
            .responses
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect::<Vec<_>>(),
        [visible.as_str(), hidden.as_str(), missing.as_str()]
    );
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(response.responses[0].partitions[0].high_watermark, 0);
    for topic in &response.responses[1..] {
        assert_eq!(topic.partitions[0].error_code, TOPIC_AUTHORIZATION_FAILED);
        assert_eq!(topic.partitions[0].high_watermark, -1);
    }
}

#[tokio::test]
async fn fetch_authorizer_failure_uses_versioned_request_wide_errors_before_object_reads() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let first = metadata.create_topic("fetch-backend-a", 1).await.unwrap();
    let second = metadata.create_topic("fetch-backend-b", 1).await.unwrap();
    let metrics = Arc::new(Metrics::new().unwrap());
    let broker = secured_broker(metadata.clone(), metrics.clone());
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("fetch-backend-a"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records())),
                ]),
        ]);
    let response = handle_as(&broker, "admin", ApiKey::Produce, 12, 10_901, &produce).await;
    let response: kafka_protocol::messages::ProduceResponse =
        decode_response(ApiKey::Produce, 12, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let legacy = named_fetch(&["fetch-backend-a", "fetch-backend-b"]).with_session_id(41);
    let response = handle_as(&broker, "fetch-reader", ApiKey::Fetch, 12, 10_902, &legacy).await;
    let response: FetchResponse = decode_response(ApiKey::Fetch, 12, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert_eq!(response.session_id, 41);
    assert_eq!(response.responses.len(), 2);
    assert!(response.responses.iter().all(|topic| {
        topic.partitions[0].error_code == UNKNOWN_SERVER_ERROR
            && topic.partitions[0].high_watermark == -1
    }));

    let by_id = id_fetch(&[first.id, second.id]).with_session_id(42);
    let response = handle_as(&broker, "fetch-reader", ApiKey::Fetch, 13, 10_903, &by_id).await;
    let response: FetchResponse = decode_response(ApiKey::Fetch, 13, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert_eq!(response.session_id, 42);
    assert!(response.responses.is_empty());
    assert_eq!(
        metrics
            .object_store_requests
            .with_label_values(&["get"])
            .get(),
        0
    );
}

#[tokio::test]
async fn fetch_collapses_duplicate_topic_partitions() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let topic = metadata.create_topic("fetch-duplicates", 1).await.unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );

    let legacy = FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("fetch-duplicates"))
                .with_partitions(vec![fetch_partition().with_fetch_offset(1)]),
            FetchTopic::default()
                .with_topic(topic_name("fetch-duplicates"))
                .with_partitions(vec![fetch_partition()]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 10_904, &legacy))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partitions.len(), 1);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);

    let by_id = id_fetch(&[topic.id, topic.id]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 18, 10_905, &by_id))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 18, response);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partitions.len(), 1);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);
}

#[tokio::test]
async fn fetch_preserves_noncontiguous_topic_partition_order() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("fetch-order-a", 2).await.unwrap();
    metadata.create_topic("fetch-order-b", 1).await.unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );
    let request = FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_topics(vec![
            named_topic_partition("fetch-order-a", 0),
            named_topic_partition("fetch-order-b", 0),
            named_topic_partition("fetch-order-a", 1),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 12, 10_906, &request))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 12, response);

    assert_eq!(
        response
            .responses
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect::<Vec<_>>(),
        ["fetch-order-a", "fetch-order-b", "fetch-order-a"]
    );
    assert_eq!(response.responses[0].partitions[0].partition_index, 0);
    assert_eq!(response.responses[1].partitions[0].partition_index, 0);
    assert_eq!(response.responses[2].partitions[0].partition_index, 1);
}

#[tokio::test]
async fn authorized_missing_topic_uses_unknown_topic_error() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_acl(topic_rule(
            "User:fetch-reader",
            "allowed-but-missing",
            AclOperation::Read,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata, Arc::new(Metrics::new().unwrap()));
    let request = named_fetch(&["allowed-but-missing"]);
    let response = handle_as(&broker, "fetch-reader", ApiKey::Fetch, 12, 10_907, &request).await;
    let response: FetchResponse = decode_response(ApiKey::Fetch, 12, response);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(response.responses[0].partitions[0].high_watermark, -1);
}

fn secured_broker(metadata: Arc<dyn MetadataStore>, metrics: Arc<Metrics>) -> Broker {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    config.security.super_users.insert("User:admin".to_owned());
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        metrics,
    )
}

fn named_fetch(names: &[&str]) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(
            names
                .iter()
                .map(|name| {
                    FetchTopic::default()
                        .with_topic(topic_name(name))
                        .with_partitions(vec![fetch_partition()])
                })
                .collect(),
        )
}

fn id_fetch(ids: &[Uuid]) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(
            ids.iter()
                .map(|id| {
                    FetchTopic::default()
                        .with_topic_id(*id)
                        .with_partitions(vec![fetch_partition()])
                })
                .collect(),
        )
}

fn fetch_partition() -> FetchPartition {
    FetchPartition::default()
        .with_partition(0)
        .with_current_leader_epoch(0)
        .with_fetch_offset(0)
        .with_partition_max_bytes(1024 * 1024)
}

fn named_topic_partition(name: &str, partition: i32) -> FetchTopic {
    FetchTopic::default()
        .with_topic(topic_name(name))
        .with_partitions(vec![fetch_partition().with_partition(partition)])
}
