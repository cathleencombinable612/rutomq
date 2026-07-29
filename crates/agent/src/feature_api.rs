use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, FEATURE_UPDATE_FAILED, INVALID_REQUEST, INVALID_UPDATE_VERSION,
    NO_ERROR,
};
use kafka_protocol::messages::api_versions_response::{
    ApiVersion, FinalizedFeatureKey, SupportedFeatureKey,
};
use kafka_protocol::messages::update_features_response::UpdatableFeatureResult;
use kafka_protocol::messages::{
    ApiVersionsResponse, UpdateFeaturesRequest, UpdateFeaturesResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, FeatureLevelUpdate, FeatureUpgradeType,
    SUPPORTED_FEATURES,
};
use rutomq_protocol::advertised_api_versions;

impl Broker {
    pub(super) async fn handle_api_versions(&self, version: i16) -> ApiVersionsResponse {
        let mut response = ApiVersionsResponse::default().with_api_keys(
            advertised_api_versions()
                .into_iter()
                .map(|(api_key, min_version, max_version)| {
                    ApiVersion::default()
                        .with_api_key(api_key)
                        .with_min_version(min_version)
                        .with_max_version(max_version)
                })
                .collect(),
        );
        if version < 3 {
            return response;
        }

        response.supported_features = SUPPORTED_FEATURES
            .iter()
            .filter(|feature| version >= 4 || feature.min_version > 0)
            .map(|feature| {
                SupportedFeatureKey::default()
                    .with_name(StrBytes::from_static_str(feature.name))
                    .with_min_version(feature.min_version)
                    .with_max_version(feature.max_version)
            })
            .collect();
        if let Ok(metadata) = self.metadata.features().await {
            response.finalized_features_epoch = metadata.epoch;
            response.finalized_features = metadata
                .finalized
                .into_iter()
                .filter(|(_, level)| *level != 0)
                .map(|(name, level)| {
                    FinalizedFeatureKey::default()
                        .with_name(StrBytes::from_string(name))
                        .with_min_version_level(level)
                        .with_max_version_level(level)
                })
                .collect();
        }
        response
    }

    pub(super) async fn handle_update_features(
        &self,
        request: UpdateFeaturesRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> UpdateFeaturesResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
            .unwrap_or(false)
        {
            return update_error(CLUSTER_AUTHORIZATION_FAILED, "cluster authorization failed");
        }

        let names = request
            .feature_updates
            .iter()
            .map(|update| update.feature.clone())
            .collect::<Vec<_>>();
        let updates = match request
            .feature_updates
            .into_iter()
            .map(|update| {
                let upgrade_type = if version == 0 {
                    if update.allow_downgrade {
                        FeatureUpgradeType::SafeDowngrade
                    } else {
                        FeatureUpgradeType::Upgrade
                    }
                } else {
                    match update.upgrade_type {
                        1 => FeatureUpgradeType::Upgrade,
                        2 => FeatureUpgradeType::SafeDowngrade,
                        3 => FeatureUpgradeType::UnsafeDowngrade,
                        code => {
                            return Err(ControlError::InvalidUpdateVersion(format!(
                                "unsupported feature upgrade type {code}"
                            )));
                        }
                    }
                };
                Ok(FeatureLevelUpdate {
                    name: update.feature.as_str().to_owned(),
                    max_version_level: update.max_version_level,
                    upgrade_type,
                })
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(updates) => updates,
            Err(error) => return control_error(error),
        };

        match self
            .metadata
            .update_features(updates, version >= 1 && request.validate_only)
            .await
        {
            Ok(_) => {
                let results = if version <= 1 {
                    names
                        .into_iter()
                        .map(|name| {
                            UpdatableFeatureResult::default()
                                .with_feature(name)
                                .with_error_code(NO_ERROR)
                                .with_error_message(None)
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                UpdateFeaturesResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_error_message(None)
                    .with_results(results)
            }
            Err(error) => control_error(error),
        }
    }
}

fn control_error(error: ControlError) -> UpdateFeaturesResponse {
    let code = match error {
        ControlError::InvalidUpdateVersion(_) => INVALID_UPDATE_VERSION,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        _ => FEATURE_UPDATE_FAILED,
    };
    update_error(
        code,
        &format!("the update failed for all features because validation failed: {error}"),
    )
}

fn update_error(error_code: i16, message: &str) -> UpdateFeaturesResponse {
    UpdateFeaturesResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(StrBytes::from_string(message.to_owned())))
        .with_results(Vec::new())
}
