use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_UPDATE_VERSION, NO_ERROR, UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey};
use kafka_protocol::messages::update_features_request::FeatureUpdateKey;
use kafka_protocol::messages::{
    ApiVersionsResponse, ConsumerGroupHeartbeatResponse, GroupId, ShareGroupHeartbeatRequest,
    ShareGroupHeartbeatResponse, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse,
    UpdateFeaturesResponse,
};
use rutomq_control::{
    GROUP_VERSION_FEATURE, KAFKA_4_0_IV0, KAFKA_4_2_IV0, KAFKA_4_2_IV1, KAFKA_4_3_IV0,
    METADATA_VERSION_FEATURE, MemoryMetadataStore, SHARE_VERSION_FEATURE, STREAMS_VERSION_FEATURE,
    TRANSACTION_VERSION_FEATURE,
};
use rutomq_storage::OpenDalObjectStore;

fn feature(name: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    FeatureUpdateKey::default()
        .with_feature(StrBytes::from_string(name.to_owned()))
        .with_max_version_level(level)
        .with_upgrade_type(upgrade_type)
}

fn find_supported<'a>(
    response: &'a ApiVersionsResponse,
    name: &str,
) -> Option<&'a SupportedFeatureKey> {
    response
        .supported_features
        .iter()
        .find(|feature| feature.name.as_str() == name)
}

fn find_finalized<'a>(
    response: &'a ApiVersionsResponse,
    name: &str,
) -> Option<&'a FinalizedFeatureKey> {
    response
        .finalized_features
        .iter()
        .find(|feature| feature.name.as_str() == name)
}

#[tokio::test]
async fn api_versions_exposes_versioned_supported_and_finalized_features() {
    let broker = broker();
    for version in [3, 4] {
        let response = broker
            .handle_request(request_frame(
                ApiKey::ApiVersions,
                version,
                300,
                &ApiVersionsRequest::default(),
            ))
            .await
            .unwrap();
        let response: ApiVersionsResponse = decode_response(ApiKey::ApiVersions, version, response);
        let metadata = find_supported(&response, METADATA_VERSION_FEATURE).unwrap();
        assert_eq!(metadata.min_version, KAFKA_4_0_IV0);
        assert_eq!(metadata.max_version, KAFKA_4_3_IV0);
        for feature in [
            TRANSACTION_VERSION_FEATURE,
            GROUP_VERSION_FEATURE,
            SHARE_VERSION_FEATURE,
            STREAMS_VERSION_FEATURE,
        ] {
            assert_eq!(find_supported(&response, feature).is_some(), version >= 4);
        }
        assert!(find_supported(&response, "kraft.version").is_none());
        assert!(find_supported(&response, "eligible.leader.replicas.version").is_none());
        assert_eq!(
            find_finalized(&response, METADATA_VERSION_FEATURE)
                .unwrap()
                .max_version_level,
            KAFKA_4_2_IV1
        );
        assert_eq!(
            find_finalized(&response, TRANSACTION_VERSION_FEATURE)
                .unwrap()
                .max_version_level,
            2
        );
        assert_eq!(
            find_finalized(&response, GROUP_VERSION_FEATURE)
                .unwrap()
                .max_version_level,
            1
        );
        assert_eq!(
            find_finalized(&response, STREAMS_VERSION_FEATURE)
                .unwrap()
                .max_version_level,
            1
        );
        assert_eq!(
            find_finalized(&response, SHARE_VERSION_FEATURE)
                .unwrap()
                .max_version_level,
            1
        );
        assert_eq!(response.finalized_features_epoch, 0);
        assert!(response.api_keys.iter().any(|api| {
            api.api_key == ApiKey::UpdateFeatures as i16
                && api.min_version == 0
                && api.max_version == 2
        }));
    }
}

#[tokio::test]
async fn kafka_43_metadata_version_upgrade_is_explicit_and_one_way() {
    let broker = broker();
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(METADATA_VERSION_FEATURE),
        KAFKA_4_2_IV1
    );

    let upgrade = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(METADATA_VERSION_FEATURE, KAFKA_4_3_IV0, 1)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 2, 313, &upgrade))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    let upgraded = broker.metadata.features().await.unwrap();
    assert_eq!(upgraded.level(METADATA_VERSION_FEATURE), KAFKA_4_3_IV0);
    assert_eq!(upgraded.epoch, 1);

    for (correlation_id, upgrade_type) in [(314, 2), (315, 3)] {
        let downgrade = UpdateFeaturesRequest::default()
            .with_feature_updates(vec![feature(
                METADATA_VERSION_FEATURE,
                KAFKA_4_2_IV1,
                upgrade_type,
            )])
            .with_validate_only(false);
        let response = broker
            .handle_request(request_frame(
                ApiKey::UpdateFeatures,
                2,
                correlation_id,
                &downgrade,
            ))
            .await
            .unwrap();
        let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 2, response);
        assert_eq!(response.error_code, INVALID_UPDATE_VERSION);
        let unchanged = broker.metadata.features().await.unwrap();
        assert_eq!(unchanged.level(METADATA_VERSION_FEATURE), KAFKA_4_3_IV0);
        assert_eq!(unchanged.epoch, upgraded.epoch);
    }
}

#[tokio::test]
async fn streams_version_gates_streams_protocol_and_can_be_restored() {
    let broker = broker();
    let downgrade = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(STREAMS_VERSION_FEATURE, 0, 2)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 1, 310, &downgrade))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let heartbeat = StreamsGroupHeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("streams-workers")))
        .with_member_id(StrBytes::from_static_str("member"))
        .with_member_epoch(0);
    let response = broker
        .handle_request(request_frame(
            ApiKey::StreamsGroupHeartbeat,
            0,
            311,
            &heartbeat,
        ))
        .await
        .unwrap();
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);

    let restore = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(STREAMS_VERSION_FEATURE, 1, 1)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 1, 312, &restore))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(STREAMS_VERSION_FEATURE),
        1
    );
}

#[tokio::test]
async fn share_version_gates_share_protocol_and_can_be_restored() {
    let broker = broker();
    let downgrade = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(SHARE_VERSION_FEATURE, 0, 2)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 1, 307, &downgrade))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let heartbeat = ShareGroupHeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("share-workers")))
        .with_member_id(StrBytes::from_static_str("member"))
        .with_member_epoch(0);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareGroupHeartbeat,
            1,
            308,
            &heartbeat,
        ))
        .await
        .unwrap();
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);

    let restore = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(SHARE_VERSION_FEATURE, 1, 1)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 1, 309, &restore))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(SHARE_VERSION_FEATURE),
        1
    );
}

#[tokio::test]
async fn update_features_is_atomic_versioned_and_gates_consumer_protocol() {
    let broker = broker();
    let validate = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(METADATA_VERSION_FEATURE, KAFKA_4_2_IV0, 2)])
        .with_validate_only(true);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 2, 301, &validate))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(METADATA_VERSION_FEATURE),
        KAFKA_4_2_IV1
    );

    let atomic_failure = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![
            feature(METADATA_VERSION_FEATURE, KAFKA_4_2_IV0, 2),
            feature("unknown.feature", 1, 1),
        ])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::UpdateFeatures,
            2,
            302,
            &atomic_failure,
        ))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 2, response);
    assert_eq!(response.error_code, INVALID_UPDATE_VERSION);
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(METADATA_VERSION_FEATURE),
        KAFKA_4_2_IV1
    );

    let downgrade_group = UpdateFeaturesRequest::default().with_feature_updates(vec![
        FeatureUpdateKey::default()
            .with_feature(StrBytes::from_static_str(GROUP_VERSION_FEATURE))
            .with_max_version_level(0)
            .with_allow_downgrade(true),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::UpdateFeatures,
            0,
            303,
            &downgrade_group,
        ))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        broker
            .metadata
            .features()
            .await
            .unwrap()
            .level(GROUP_VERSION_FEATURE),
        0
    );

    let heartbeat = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("workers")))
        .with_member_id(StrBytes::from_static_str("member"))
        .with_member_epoch(0);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            304,
            &heartbeat,
        ))
        .await
        .unwrap();
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);

    let restore_group = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(GROUP_VERSION_FEATURE, 1, 1)])
        .with_validate_only(false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::UpdateFeatures,
            1,
            305,
            &restore_group,
        ))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.results.len(), 1);
}

#[tokio::test]
async fn update_features_requires_cluster_alter_acl() {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let request = UpdateFeaturesRequest::default()
        .with_feature_updates(vec![feature(METADATA_VERSION_FEATURE, KAFKA_4_2_IV0, 2)])
        .with_validate_only(true);
    let response = broker
        .handle_request(request_frame(ApiKey::UpdateFeatures, 2, 306, &request))
        .await
        .unwrap();
    let response: UpdateFeaturesResponse = decode_response(ApiKey::UpdateFeatures, 2, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);
}
