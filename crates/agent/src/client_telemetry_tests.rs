use super::*;
use crate::kafka_error::{
    INVALID_REQUEST, NO_ERROR, TELEMETRY_TOO_LARGE, THROTTLING_QUOTA_EXCEEDED,
    UNKNOWN_SUBSCRIPTION_ID, UNSUPPORTED_COMPRESSION_TYPE,
};
use bytes::Buf;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::{
    DescribeConfigsRequest, DescribeConfigsResponse, GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse, ListConfigResourcesRequest, ListConfigResourcesResponse,
    PushTelemetryRequest, PushTelemetryResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn telemetry_broker(max_bytes: usize) -> (Broker, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::new().unwrap());
    let config = AgentConfig {
        telemetry_max_bytes: max_bytes,
        ..AgentConfig::default()
    };
    (
        Broker::new(
            Arc::new(MemoryMetadataStore::new()),
            Arc::new(OpenDalObjectStore::memory().unwrap()),
            config,
            metrics.clone(),
        ),
        metrics,
    )
}

fn frame<T: Encodable>(api_key: ApiKey, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(0)
        .with_correlation_id(71)
        .with_client_id(Some(StrBytes::from_static_str("telemetry-client")))
        .encode(&mut payload, api_key.request_header_version(0))
        .unwrap();
    body.encode(&mut payload, 0).unwrap();
    payload.freeze()
}

fn response<T: Decodable>(api_key: ApiKey, mut frame: Bytes) -> T {
    let frame_size = frame.get_i32() as usize;
    assert_eq!(frame_size, frame.remaining());
    ResponseHeader::decode(&mut frame, api_key.response_header_version(0)).unwrap();
    T::decode(&mut frame, 0).unwrap()
}

async fn subscribe(broker: &Broker) -> GetTelemetrySubscriptionsResponse {
    let frame = broker
        .handle_request(frame(
            ApiKey::GetTelemetrySubscriptions,
            &GetTelemetrySubscriptionsRequest::default(),
        ))
        .await
        .unwrap();
    response(ApiKey::GetTelemetrySubscriptions, frame)
}

fn push(
    subscription: &GetTelemetrySubscriptionsResponse,
    subscription_id: i32,
    compression_type: i8,
    metrics: Bytes,
    terminating: bool,
) -> PushTelemetryRequest {
    PushTelemetryRequest::default()
        .with_client_instance_id(subscription.client_instance_id)
        .with_subscription_id(subscription_id)
        .with_compression_type(compression_type)
        .with_metrics(metrics)
        .with_terminating(terminating)
}

fn config(name: &str, value: Option<&str>, operation: i8) -> AlterableConfig {
    AlterableConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(value.map(|value| StrBytes::from_string(value.to_owned())))
        .with_config_operation(operation)
}

fn client_metric_alter(
    name: &str,
    configs: Vec<AlterableConfig>,
    validate_only: bool,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest::default()
        .with_validate_only(validate_only)
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(16)
                .with_resource_name(StrBytes::from_string(name.to_owned()))
                .with_configs(configs),
        ])
}

#[tokio::test]
async fn client_metric_config_resources_round_trip_and_validate() {
    let (broker, _) = telemetry_broker(1024);
    let configs = || {
        vec![
            config("metrics", Some("*"), 0),
            config("interval.ms", Some("100"), 0),
            config("match", Some("client_id=telemetry-.*"), 0),
        ]
    };
    let validate = client_metric_alter("java-clients", configs(), true);
    let result = broker
        .handle_request(super::tests::request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            72,
            &validate,
        ))
        .await
        .unwrap();
    let result: IncrementalAlterConfigsResponse =
        super::tests::decode_response(ApiKey::IncrementalAlterConfigs, 1, result);
    assert_eq!(result.responses[0].error_code, NO_ERROR);
    assert!(
        broker
            .metadata
            .client_metric_subscriptions()
            .await
            .unwrap()
            .is_empty()
    );

    let alter = client_metric_alter("java-clients", configs(), false);
    let result = broker
        .handle_request(super::tests::request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            73,
            &alter,
        ))
        .await
        .unwrap();
    let result: IncrementalAlterConfigsResponse =
        super::tests::decode_response(ApiKey::IncrementalAlterConfigs, 1, result);
    assert_eq!(result.responses[0].error_code, NO_ERROR);

    let list = broker
        .handle_request(super::tests::request_frame(
            ApiKey::ListConfigResources,
            0,
            74,
            &ListConfigResourcesRequest::default(),
        ))
        .await
        .unwrap();
    let list: ListConfigResourcesResponse =
        super::tests::decode_response(ApiKey::ListConfigResources, 0, list);
    assert_eq!(list.config_resources.len(), 1);
    assert_eq!(
        list.config_resources[0].resource_name.as_str(),
        "java-clients"
    );

    let describe = DescribeConfigsRequest::default()
        .with_include_synonyms(true)
        .with_include_documentation(true)
        .with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(16)
                .with_resource_name(StrBytes::from_static_str("java-clients"))
                .with_configuration_keys(None),
        ]);
    let result = broker
        .handle_request(super::tests::request_frame(
            ApiKey::DescribeConfigs,
            4,
            75,
            &describe,
        ))
        .await
        .unwrap();
    let result: DescribeConfigsResponse =
        super::tests::decode_response(ApiKey::DescribeConfigs, 4, result);
    assert_eq!(result.results[0].error_code, NO_ERROR);
    assert_eq!(result.results[0].configs.len(), 3);
    assert!(result.results[0].configs.iter().all(|entry| {
        entry.config_source == 7
            && entry.documentation.is_some()
            && entry.synonyms.len() == 1
            && entry.synonyms[0].name == entry.name
            && entry.synonyms[0].value == entry.value
            && entry.synonyms[0].source == 7
    }));

    let without_synonyms = describe.with_include_synonyms(false);
    let result = broker
        .handle_request(super::tests::request_frame(
            ApiKey::DescribeConfigs,
            4,
            751,
            &without_synonyms,
        ))
        .await
        .unwrap();
    let result: DescribeConfigsResponse =
        super::tests::decode_response(ApiKey::DescribeConfigs, 4, result);
    assert!(
        result.results[0]
            .configs
            .iter()
            .all(|entry| entry.synonyms.is_empty())
    );

    let invalid = client_metric_alter(
        "java-clients",
        vec![config("interval.ms", Some("99"), 0)],
        false,
    );
    let result = broker
        .handle_request(super::tests::request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            76,
            &invalid,
        ))
        .await
        .unwrap();
    let result: IncrementalAlterConfigsResponse =
        super::tests::decode_response(ApiKey::IncrementalAlterConfigs, 1, result);
    assert_ne!(result.responses[0].error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .client_metric_subscription("java-clients")
            .await
            .unwrap()
            .unwrap()
            .push_interval_ms(),
        100
    );
}

#[tokio::test]
async fn telemetry_get_and_push_enforce_kafka_errors_and_metrics() {
    let (broker, metrics) = telemetry_broker(4);
    let alter = client_metric_alter(
        "java-clients",
        vec![
            config("metrics", Some("*"), 0),
            config("interval.ms", Some("100"), 0),
            config("match", Some("client_id=telemetry-client"), 0),
        ],
        false,
    );
    broker
        .handle_request(super::tests::request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            77,
            &alter,
        ))
        .await
        .unwrap();

    let subscription = subscribe(&broker).await;
    assert_eq!(subscription.error_code, NO_ERROR);
    assert!(!subscription.client_instance_id.is_nil());
    assert_eq!(subscription.push_interval_ms, 100);
    assert_eq!(subscription.telemetry_max_bytes, 4);
    assert_eq!(
        subscription
            .requested_metrics
            .iter()
            .map(StrBytes::as_str)
            .collect::<Vec<_>>(),
        vec!["*"]
    );
    assert_eq!(subscription.accepted_compression_types, vec![4, 3, 1, 2]);

    let valid = push(
        &subscription,
        subscription.subscription_id,
        0,
        Bytes::from_static(b"otel"),
        false,
    );
    let result = broker
        .handle_request(frame(ApiKey::PushTelemetry, &valid))
        .await
        .unwrap();
    let result: PushTelemetryResponse = response(ApiKey::PushTelemetry, result);
    assert_eq!(result.error_code, NO_ERROR);
    assert_eq!(metrics.client_telemetry_pushes.get(), 1);
    assert_eq!(metrics.client_telemetry_bytes.get(), 4);

    let throttled = broker
        .handle_request(frame(
            ApiKey::GetTelemetrySubscriptions,
            &GetTelemetrySubscriptionsRequest::default()
                .with_client_instance_id(subscription.client_instance_id),
        ))
        .await
        .unwrap();
    let throttled: GetTelemetrySubscriptionsResponse =
        response(ApiKey::GetTelemetrySubscriptions, throttled);
    assert_eq!(throttled.error_code, THROTTLING_QUOTA_EXCEEDED);

    let unknown = subscribe(&broker).await;
    let result = broker
        .handle_request(frame(
            ApiKey::PushTelemetry,
            &push(
                &unknown,
                unknown.subscription_id + 1,
                0,
                Bytes::new(),
                false,
            ),
        ))
        .await
        .unwrap();
    assert_eq!(
        response::<PushTelemetryResponse>(ApiKey::PushTelemetry, result).error_code,
        UNKNOWN_SUBSCRIPTION_ID
    );

    let unsupported = subscribe(&broker).await;
    let result = broker
        .handle_request(frame(
            ApiKey::PushTelemetry,
            &push(
                &unsupported,
                unsupported.subscription_id,
                9,
                Bytes::new(),
                false,
            ),
        ))
        .await
        .unwrap();
    assert_eq!(
        response::<PushTelemetryResponse>(ApiKey::PushTelemetry, result).error_code,
        UNSUPPORTED_COMPRESSION_TYPE
    );

    let oversized = subscribe(&broker).await;
    let result = broker
        .handle_request(frame(
            ApiKey::PushTelemetry,
            &push(
                &oversized,
                oversized.subscription_id,
                0,
                Bytes::from_static(b"large"),
                false,
            ),
        ))
        .await
        .unwrap();
    assert_eq!(
        response::<PushTelemetryResponse>(ApiKey::PushTelemetry, result).error_code,
        TELEMETRY_TOO_LARGE
    );

    let terminating = subscribe(&broker).await;
    let final_push = push(
        &terminating,
        terminating.subscription_id,
        0,
        Bytes::new(),
        true,
    );
    let result = broker
        .handle_request(frame(ApiKey::PushTelemetry, &final_push))
        .await
        .unwrap();
    assert_eq!(
        response::<PushTelemetryResponse>(ApiKey::PushTelemetry, result).error_code,
        NO_ERROR
    );
    let result = broker
        .handle_request(frame(ApiKey::PushTelemetry, &final_push))
        .await
        .unwrap();
    assert_eq!(
        response::<PushTelemetryResponse>(ApiKey::PushTelemetry, result).error_code,
        INVALID_REQUEST
    );
    assert!(metrics.client_telemetry_errors.get() >= 5);
}
