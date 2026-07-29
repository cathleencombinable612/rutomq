use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR, UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::alter_client_quotas_request::{
    EntityData as AlterEntity, EntryData as AlterEntry, OpData,
};
use kafka_protocol::messages::describe_client_quotas_request::ComponentData;
use kafka_protocol::messages::{AlterClientQuotasResponse, DescribeClientQuotasResponse};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn entity(entity_type: &str, name: Option<&str>) -> AlterEntity {
    AlterEntity::default()
        .with_entity_type(StrBytes::from_string(entity_type.to_owned()))
        .with_entity_name(name.map(|name| StrBytes::from_string(name.to_owned())))
}

fn op(key: &str, value: f64, remove: bool) -> OpData {
    OpData::default()
        .with_key(StrBytes::from_string(key.to_owned()))
        .with_value(value)
        .with_remove(remove)
}

fn component(entity_type: &str, match_type: i8, value: Option<&str>) -> ComponentData {
    ComponentData::default()
        .with_entity_type(StrBytes::from_string(entity_type.to_owned()))
        .with_match_type(match_type)
        .with_match(value.map(|value| StrBytes::from_string(value.to_owned())))
}

#[tokio::test]
async fn client_quota_crud_supports_flexible_and_legacy_versions() {
    for version in [0, 1] {
        let broker = broker();
        let alter = AlterClientQuotasRequest::default().with_entries(vec![
            AlterEntry::default()
                .with_entity(vec![
                    entity("user", Some("alice")),
                    entity("client-id", None),
                ])
                .with_ops(vec![
                    op("producer_byte_rate", 1_024.0, false),
                    op("request_percentage", 25.5, false),
                ]),
            AlterEntry::default()
                .with_entity(vec![entity("ip", Some("127.0.0.1"))])
                .with_ops(vec![op("connection_creation_rate", 10.0, false)]),
        ]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::AlterClientQuotas,
                version,
                200,
                &alter,
            ))
            .await
            .unwrap();
        let response: AlterClientQuotasResponse =
            decode_response(ApiKey::AlterClientQuotas, version, response);
        assert!(
            response
                .entries
                .iter()
                .all(|entry| entry.error_code == NO_ERROR)
        );

        let describe = DescribeClientQuotasRequest::default()
            .with_components(vec![component("user", 0, Some("alice"))])
            .with_strict(false);
        let response = broker
            .handle_request(request_frame(
                ApiKey::DescribeClientQuotas,
                version,
                201,
                &describe,
            ))
            .await
            .unwrap();
        let response: DescribeClientQuotasResponse =
            decode_response(ApiKey::DescribeClientQuotas, version, response);
        assert_eq!(response.error_code, NO_ERROR);
        let entries = response.entries.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entity.len(), 2);
        assert_eq!(entries[0].values.len(), 2);

        let strict = DescribeClientQuotasRequest::default()
            .with_components(vec![component("user", 0, Some("alice"))])
            .with_strict(true);
        let response = broker
            .handle_request(request_frame(
                ApiKey::DescribeClientQuotas,
                version,
                202,
                &strict,
            ))
            .await
            .unwrap();
        let response: DescribeClientQuotasResponse =
            decode_response(ApiKey::DescribeClientQuotas, version, response);
        assert!(response.entries.unwrap().is_empty());

        let remove = AlterClientQuotasRequest::default().with_entries(vec![
            AlterEntry::default()
                .with_entity(vec![
                    entity("user", Some("alice")),
                    entity("client-id", None),
                ])
                .with_ops(vec![
                    op("producer_byte_rate", 0.0, true),
                    op("request_percentage", 0.0, true),
                ]),
        ]);
        broker
            .handle_request(request_frame(
                ApiKey::AlterClientQuotas,
                version,
                203,
                &remove,
            ))
            .await
            .unwrap();
        assert_eq!(broker.metadata.client_quotas().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn client_quota_validation_is_per_entity_and_validate_only_does_not_mutate() {
    let broker = broker();
    let request = AlterClientQuotasRequest::default()
        .with_validate_only(true)
        .with_entries(vec![
            AlterEntry::default()
                .with_entity(vec![entity("user", Some("alice"))])
                .with_ops(vec![op("producer_byte_rate", 100.0, false)]),
            AlterEntry::default()
                .with_entity(vec![entity("ip", Some("not-an-ip"))])
                .with_ops(vec![op("connection_creation_rate", 1.0, false)]),
            AlterEntry::default()
                .with_entity(vec![entity("client-id", Some("client"))])
                .with_ops(vec![op("producer_byte_rate", 1.5, false)]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterClientQuotas, 1, 210, &request))
        .await
        .unwrap();
    let response: AlterClientQuotasResponse =
        decode_response(ApiKey::AlterClientQuotas, 1, response);
    assert_eq!(response.entries[0].error_code, NO_ERROR);
    assert_eq!(response.entries[1].error_code, INVALID_REQUEST);
    assert_eq!(response.entries[2].error_code, INVALID_REQUEST);
    assert!(broker.metadata.client_quotas().await.unwrap().is_empty());
}

#[tokio::test]
async fn client_quota_filters_reject_invalid_combinations_and_unknown_types() {
    let broker = broker();
    let invalid = DescribeClientQuotasRequest::default()
        .with_components(vec![component("ip", 2, None), component("user", 2, None)]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeClientQuotas,
            1,
            220,
            &invalid,
        ))
        .await
        .unwrap();
    let response: DescribeClientQuotasResponse =
        decode_response(ApiKey::DescribeClientQuotas, 1, response);
    assert_eq!(response.error_code, INVALID_REQUEST);

    let unknown =
        DescribeClientQuotasRequest::default().with_components(vec![component("tenant", 2, None)]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeClientQuotas,
            1,
            221,
            &unknown,
        ))
        .await
        .unwrap();
    let response: DescribeClientQuotasResponse =
        decode_response(ApiKey::DescribeClientQuotas, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);
}

#[tokio::test]
async fn client_quota_apis_require_cluster_config_acls() {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let describe = DescribeClientQuotasRequest::default();
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeClientQuotas,
            1,
            230,
            &describe,
        ))
        .await
        .unwrap();
    let response: DescribeClientQuotasResponse =
        decode_response(ApiKey::DescribeClientQuotas, 1, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);

    let alter = AlterClientQuotasRequest::default().with_entries(vec![
        AlterEntry::default()
            .with_entity(vec![entity("user", Some("alice"))])
            .with_ops(vec![op("producer_byte_rate", 1_024.0, false)]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterClientQuotas, 1, 231, &alter))
        .await
        .unwrap();
    let response: AlterClientQuotasResponse =
        decode_response(ApiKey::AlterClientQuotas, 1, response);
    assert_eq!(response.entries[0].error_code, CLUSTER_AUTHORIZATION_FAILED);
    assert!(broker.metadata.client_quotas().await.unwrap().is_empty());
}
