use super::acl_tests::{acl_broker, decode_response, handle_as, topic_rule};
use super::*;
use crate::kafka_error::{
    INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use rutomq_control::{
    AclOperation, AclPermission, MemoryMetadataStore, MetadataStore, PostgresMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

const USERNAME: &str = "config-reader";

fn resource(resource_type: i8, name: &str) -> DescribeConfigsResource {
    DescribeConfigsResource::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from_string(name.to_owned()))
        .with_configuration_keys(None)
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

#[tokio::test]
async fn describe_configs_authorization_backend_failure_is_request_wide() {
    let (broker, metadata) = acl_broker();
    metadata
        .create_topic("describe-configs-backend-failure", 1)
        .await
        .unwrap();
    metadata.set_authorization_failure(true);

    let request = DescribeConfigsRequest::default().with_resources(vec![
        resource(2, "describe-configs-backend-failure"),
        resource(4, "0"),
    ]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeConfigs,
        4,
        8501,
        &request,
    )
    .await;
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);

    assert_eq!(response.results.len(), 2);
    assert!(
        response.results.iter().all(|result| {
            result.error_code == UNKNOWN_SERVER_ERROR && result.configs.is_empty()
        })
    );
    assert_eq!(
        response
            .results
            .iter()
            .map(|result| result.resource_name.as_str())
            .collect::<Vec<_>>(),
        ["describe-configs-backend-failure", "0"]
    );
}

#[tokio::test]
async fn describe_configs_classification_obeys_request_order() {
    let (broker, metadata) = acl_broker();
    metadata.set_authorization_failure(true);

    let invalid_first = DescribeConfigsRequest::default()
        .with_resources(vec![resource(99, "invalid"), resource(2, "topic")]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeConfigs,
        4,
        8502,
        &invalid_first,
    )
    .await;
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert!(
        response
            .results
            .iter()
            .all(|result| result.error_code == INVALID_REQUEST && result.configs.is_empty())
    );

    let authorization_first = DescribeConfigsRequest::default()
        .with_resources(vec![resource(2, "topic"), resource(99, "invalid")]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeConfigs,
        4,
        8503,
        &authorization_first,
    )
    .await;
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert!(
        response
            .results
            .iter()
            .all(|result| result.error_code == UNKNOWN_SERVER_ERROR && result.configs.is_empty())
    );
}

#[tokio::test]
async fn describe_configs_orders_authorized_resources_before_denied_resources_in_memory() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    assert_authorized_before_denied(metadata, "memory").await;
}

#[tokio::test]
async fn describe_configs_orders_authorized_resources_before_denied_resources_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let metadata: Arc<dyn MetadataStore> = Arc::new(store);
    assert_authorized_before_denied(metadata, &Uuid::new_v4().simple().to_string()).await;
}

async fn assert_authorized_before_denied(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("visible-config-{suffix}");
    let hidden = format!("hidden-config-{suffix}");
    metadata.create_topic(&visible, 1).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:config-reader",
            &visible,
            AclOperation::DescribeConfigs,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);

    let request = DescribeConfigsRequest::default()
        .with_resources(vec![resource(2, &hidden), resource(2, &visible)]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeConfigs,
        4,
        8504,
        &request,
    )
    .await;
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].resource_name.as_str(), visible.as_str());
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert!(!response.results[0].configs.is_empty());
    assert_eq!(response.results[1].resource_name.as_str(), hidden.as_str());
    assert_eq!(response.results[1].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert!(response.results[1].configs.is_empty());
}
