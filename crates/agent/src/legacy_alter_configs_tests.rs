use super::tests::{decode_response, request_frame};
use super::*;
use crate::kafka_error::NO_ERROR;
use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use kafka_protocol::messages::{AlterConfigsResponse, IncrementalAlterConfigsResponse};
use rutomq_control::{MemoryMetadataStore, MetadataStore, PostgresMetadataStore};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn legacy_alter_configs_replaces_group_and_client_metrics_in_memory() {
    assert_legacy_non_topic_replacement(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn legacy_alter_configs_replaces_group_and_client_metrics_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_legacy_non_topic_replacement(Arc::new(store), &Uuid::new_v4().simple().to_string())
        .await;
}

async fn assert_legacy_non_topic_replacement(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );
    let group = format!("legacy-group-{suffix}");
    let client_metrics = format!("legacy-client-metrics-{suffix}");
    let incremental = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(32)
            .with_resource_name(StrBytes::from_string(group.clone()))
            .with_configs(vec![
                incremental_config("consumer.heartbeat.interval.ms", "5000"),
                incremental_config("consumer.session.timeout.ms", "45000"),
                incremental_config("streams.num.standby.replicas", "1"),
            ]),
        IncrementalResource::default()
            .with_resource_type(16)
            .with_resource_name(StrBytes::from_string(client_metrics.clone()))
            .with_configs(vec![
                incremental_config("metrics", "*"),
                incremental_config("interval.ms", "100"),
                incremental_config("match", "client_id=legacy-.*"),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            6501,
            &incremental,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_success(
        response
            .responses
            .iter()
            .map(|resource| resource.error_code),
    );

    let replacement = AlterConfigsRequest::default()
        .with_validate_only(true)
        .with_resources(vec![
            legacy_resource(
                32,
                &group,
                vec![legacy_config("consumer.session.timeout.ms", "60000")],
            ),
            legacy_resource(
                16,
                &client_metrics,
                vec![legacy_config("metrics", "producer.")],
            ),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 6502, &replacement))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_success(
        response
            .responses
            .iter()
            .map(|resource| resource.error_code),
    );
    assert_eq!(metadata.group_config(&group).await.unwrap().len(), 3);
    assert_eq!(
        metadata
            .client_metric_subscription(&client_metrics)
            .await
            .unwrap()
            .unwrap()
            .configs
            .len(),
        3
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterConfigs,
            2,
            6503,
            &replacement.with_validate_only(false),
        ))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_success(
        response
            .responses
            .iter()
            .map(|resource| resource.error_code),
    );
    assert_eq!(
        metadata.group_config(&group).await.unwrap(),
        std::collections::BTreeMap::from([(
            "consumer.session.timeout.ms".to_owned(),
            "60000".to_owned(),
        )])
    );
    assert_eq!(
        metadata
            .client_metric_subscription(&client_metrics)
            .await
            .unwrap()
            .unwrap()
            .configs,
        std::collections::BTreeMap::from([("metrics".to_owned(), "producer.".to_owned(),)])
    );

    let clear = AlterConfigsRequest::default().with_resources(vec![
        legacy_resource(32, &group, Vec::new()),
        legacy_resource(16, &client_metrics, Vec::new()),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 6504, &clear))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_success(
        response
            .responses
            .iter()
            .map(|resource| resource.error_code),
    );
    assert!(metadata.group_config(&group).await.unwrap().is_empty());
    assert!(
        metadata
            .client_metric_subscription(&client_metrics)
            .await
            .unwrap()
            .is_none()
    );
}

fn incremental_config(name: &str, value: &str) -> IncrementalConfig {
    IncrementalConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(Some(StrBytes::from_string(value.to_owned())))
        .with_config_operation(0)
}

fn legacy_config(name: &str, value: &str) -> AlterableConfig {
    AlterableConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(Some(StrBytes::from_string(value.to_owned())))
}

fn legacy_resource(
    resource_type: i8,
    name: &str,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    AlterConfigsResource::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from_string(name.to_owned()))
        .with_configs(configs)
}

fn assert_success(error_codes: impl Iterator<Item = i16>) {
    assert!(error_codes.into_iter().all(|code| code == NO_ERROR));
}
