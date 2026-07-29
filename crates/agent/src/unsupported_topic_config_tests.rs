use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{INVALID_CONFIG, NO_ERROR};
use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsResource, AlterableConfig as LegacyConfig,
};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use kafka_protocol::messages::{AlterConfigsResponse, IncrementalAlterConfigsResponse};

#[tokio::test]
async fn create_topics_rejects_physical_log_configs_without_blocking_other_topics() {
    let broker = broker();
    let request = CreateTopicsRequest::default().with_topics(vec![
        create_topic("physical-segment", "segment.bytes", "1048576"),
        create_topic("object-backed", "retention.ms", "600000"),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 570, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);
    assert!(
        response.topics[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("stateless object-storage"))
    );
    assert_eq!(response.topics[1].error_code, NO_ERROR);
    assert!(
        broker
            .metadata
            .topic("physical-segment")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        broker
            .metadata
            .topic("object-backed")
            .await
            .unwrap()
            .is_some()
    );

    let validate_only = CreateTopicsRequest::default()
        .with_validate_only(true)
        .with_topics(vec![create_topic(
            "physical-index",
            "segment.index.bytes",
            "1048576",
        )]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 571, &validate_only))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);
    assert!(
        broker
            .metadata
            .topic("physical-index")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn alter_configs_rejects_physical_log_configs_atomically() {
    let broker = broker();
    create_configured_topic(&broker, "legacy-physical", 600_000).await;
    let resource = AlterConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(topic_name("legacy-physical").into())
        .with_configs(vec![
            legacy_config("retention.ms", Some("1")),
            legacy_config("index.interval.bytes", Some("4096")),
        ]);
    for (correlation, validate_only) in [(572, false), (573, true)] {
        let request = AlterConfigsRequest::default()
            .with_resources(vec![resource.clone()])
            .with_validate_only(validate_only);
        let response = broker
            .handle_request(request_frame(
                ApiKey::AlterConfigs,
                2,
                correlation,
                &request,
            ))
            .await
            .unwrap();
        let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
        assert_eq!(response.responses[0].error_code, INVALID_CONFIG);
        assert_eq!(retention_ms(&broker, "legacy-physical").await, 600_000);
    }
}

#[tokio::test]
async fn incremental_alter_configs_is_atomic_per_topic_for_physical_settings() {
    let broker = broker();
    create_configured_topic(&broker, "remote-physical", 600_000).await;
    create_configured_topic(&broker, "independent-valid", 600_000).await;
    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![
        incremental_resource(
            "remote-physical",
            vec![
                incremental_config("retention.ms", Some("1"), 0),
                incremental_config("remote.storage.enable", Some("true"), 0),
            ],
        ),
        incremental_resource(
            "independent-valid",
            vec![incremental_config("retention.ms", Some("700000"), 0)],
        ),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            574,
            &request,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_CONFIG);
    assert_eq!(response.responses[1].error_code, NO_ERROR);
    assert_eq!(retention_ms(&broker, "remote-physical").await, 600_000);
    assert_eq!(retention_ms(&broker, "independent-valid").await, 700_000);

    let delete =
        IncrementalAlterConfigsRequest::default().with_resources(vec![incremental_resource(
            "remote-physical",
            vec![incremental_config("local.retention.ms", None, 1)],
        )]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            575,
            &delete,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_CONFIG);
    assert_eq!(retention_ms(&broker, "remote-physical").await, 600_000);
}

fn create_topic(name: &str, config_name: &str, value: &str) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(topic_name(name))
        .with_num_partitions(1)
        .with_replication_factor(1)
        .with_configs(vec![
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_string(config_name.to_owned()))
                .with_value(Some(StrBytes::from_string(value.to_owned()))),
        ])
}

fn legacy_config(name: &str, value: Option<&str>) -> LegacyConfig {
    LegacyConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(value.map(|value| StrBytes::from_string(value.to_owned())))
}

fn incremental_resource(name: &str, configs: Vec<IncrementalConfig>) -> IncrementalResource {
    IncrementalResource::default()
        .with_resource_type(2)
        .with_resource_name(topic_name(name).into())
        .with_configs(configs)
}

fn incremental_config(name: &str, value: Option<&str>, operation: i8) -> IncrementalConfig {
    IncrementalConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(value.map(|value| StrBytes::from_string(value.to_owned())))
        .with_config_operation(operation)
}

async fn create_configured_topic(broker: &Broker, name: &str, retention_ms: i64) {
    broker.metadata.create_topic(name, 1).await.unwrap();
    broker
        .metadata
        .set_topic_config(
            name,
            TopicConfig {
                retention_ms,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
}

async fn retention_ms(broker: &Broker, name: &str) -> i64 {
    broker
        .metadata
        .topic_config(name)
        .await
        .unwrap()
        .retention_ms
}
