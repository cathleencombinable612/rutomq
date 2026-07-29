use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, NO_ERROR, UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::{ListConfigResourcesRequest, ListConfigResourcesResponse};
use rutomq_control::{MemoryMetadataStore, OffsetCommit, PartitionKey, PostgresMetadataStore};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn lists_versioned_config_resource_types() {
    let broker = broker();
    broker.metadata.create_topic("orders", 1).await.unwrap();
    broker
        .metadata
        .alter_group_config(
            "streams-app",
            std::collections::BTreeMap::from([(
                "streams.num.standby.replicas".to_owned(),
                Some("1".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();

    let legacy = ListConfigResourcesRequest::default();
    let response = broker
        .handle_request(request_frame(ApiKey::ListConfigResources, 0, 140, &legacy))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(response.config_resources.is_empty());

    let request = ListConfigResourcesRequest::default().with_resource_types(vec![2, 4, 8, 32]);
    let response = broker
        .handle_request(request_frame(ApiKey::ListConfigResources, 1, 141, &request))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response
            .config_resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str()))
            .collect::<Vec<_>>(),
        vec![(32, "streams-app"), (8, "0"), (4, "0"), (2, "orders")]
    );
}

#[tokio::test]
async fn rejects_unknown_type_and_missing_cluster_acl() {
    let broker = broker();
    let request = ListConfigResourcesRequest::default().with_resource_types(vec![1]);
    let response = broker
        .handle_request(request_frame(ApiKey::ListConfigResources, 1, 142, &request))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);

    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let request = ListConfigResourcesRequest::default();
    let response = broker
        .handle_request(request_frame(ApiKey::ListConfigResources, 1, 143, &request))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 1, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);
}

#[tokio::test]
async fn group_resources_only_include_groups_with_dynamic_configs() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let (configured, offset_only) = seed_group_resources(metadata.as_ref(), "memory").await;
    let broker = broker_with_metadata(metadata);

    let resources = list_group_resources(&broker, 144).await;
    assert!(resources.contains(&configured));
    assert!(!resources.contains(&offset_only));
}

#[tokio::test]
async fn postgres_group_resources_are_exact_after_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let setup = PostgresMetadataStore::connect(&database_url).await.unwrap();
    setup.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let (configured, offset_only) = seed_group_resources(&setup, &suffix).await;

    let fresh: Arc<dyn MetadataStore> =
        Arc::new(PostgresMetadataStore::connect(&database_url).await.unwrap());
    let resources = list_group_resources(&broker_with_metadata(fresh), 145).await;
    assert!(resources.contains(&configured));
    assert!(!resources.contains(&offset_only));
}

#[tokio::test]
async fn authorization_backend_failure_is_not_reported_as_acl_denial() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    metadata.set_authorization_failure(true);

    let response = broker
        .handle_request(request_frame(
            ApiKey::ListConfigResources,
            1,
            146,
            &ListConfigResourcesRequest::default(),
        ))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 1, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.config_resources.is_empty());
}

async fn seed_group_resources(metadata: &dyn MetadataStore, suffix: &str) -> (String, String) {
    let topic = format!("config-resource-topic-{suffix}");
    let configured = format!("configured-group-{suffix}");
    let offset_only = format!("offset-only-group-{suffix}");
    metadata.create_topic(&topic, 1).await.unwrap();
    metadata
        .commit_offsets(
            &offset_only,
            vec![OffsetCommit {
                partition: PartitionKey::new(&topic, 0),
                offset: 0,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    metadata
        .alter_group_config(
            &configured,
            std::collections::BTreeMap::from([(
                "streams.num.standby.replicas".to_owned(),
                Some("1".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    (configured, offset_only)
}

async fn list_group_resources(broker: &Broker, correlation_id: i32) -> Vec<String> {
    let request = ListConfigResourcesRequest::default().with_resource_types(vec![32]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ListConfigResources,
            1,
            correlation_id,
            &request,
        ))
        .await
        .unwrap();
    let response: ListConfigResourcesResponse =
        decode_response(ApiKey::ListConfigResources, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    response
        .config_resources
        .into_iter()
        .map(|resource| resource.resource_name.as_str().to_owned())
        .collect()
}

fn broker_with_metadata(metadata: Arc<dyn MetadataStore>) -> Broker {
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    )
}
