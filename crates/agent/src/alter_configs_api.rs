use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::broker_config::{BROKER_LOGGER_RESOURCE, BROKER_RESOURCE};
use super::client_metric_config::CLIENT_METRICS_RESOURCE;
use super::config_api::TOPIC_RESOURCE;
use super::group_config::GROUP_RESOURCE;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, GROUP_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, control_error_code,
};
use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsResource as LegacyResource, AlterableConfig as LegacyConfig,
};
use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse as LegacyResponse;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, ControlError};
use std::collections::HashSet;

#[derive(Clone)]
struct ConfigError {
    code: i16,
    message: String,
}

impl ConfigError {
    fn from_control(error: ControlError) -> Self {
        Self {
            code: control_error_code(&error),
            message: error.to_string(),
        }
    }

    fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

struct ResourceIdentity {
    resource_type: i8,
    resource_name: String,
}

impl Broker {
    pub(super) async fn handle_alter_configs(
        &self,
        request: AlterConfigsRequest,
        context: &AuthorizationContext,
    ) -> AlterConfigsResponse {
        let preprocessed = preprocess_legacy(&request.resources);
        let identities = request
            .resources
            .iter()
            .map(|resource| ResourceIdentity {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.as_str().to_owned(),
            })
            .collect::<Vec<_>>();
        let authorization = match self
            .authorize_config_resources(context, &identities, &preprocessed)
            .await
        {
            Ok(results) => results,
            Err(error) => {
                return legacy_backend_error(request.resources, preprocessed, &error.to_string());
            }
        };

        let mut responses = Vec::with_capacity(request.resources.len());
        for ((resource, preprocessed), authorization) in request
            .resources
            .into_iter()
            .zip(preprocessed)
            .zip(authorization)
        {
            let error = match preprocessed.or(authorization) {
                Some(error) => Some(error),
                None => self
                    .apply_legacy_resource(&resource, request.validate_only)
                    .await
                    .err()
                    .map(ConfigError::from_control),
            };
            responses.push(legacy_result(resource, error));
        }
        AlterConfigsResponse::default().with_responses(responses)
    }

    pub(super) async fn handle_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsRequest,
        context: &AuthorizationContext,
    ) -> IncrementalAlterConfigsResponse {
        let preprocessed = preprocess_incremental(&request.resources);
        let identities = request
            .resources
            .iter()
            .map(|resource| ResourceIdentity {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.as_str().to_owned(),
            })
            .collect::<Vec<_>>();
        let authorization = match self
            .authorize_config_resources(context, &identities, &preprocessed)
            .await
        {
            Ok(results) => results,
            Err(error) => {
                return incremental_backend_error(
                    request.resources,
                    preprocessed,
                    &error.to_string(),
                );
            }
        };

        let mut responses = Vec::with_capacity(request.resources.len());
        for ((resource, preprocessed), authorization) in request
            .resources
            .into_iter()
            .zip(preprocessed)
            .zip(authorization)
        {
            let error = match preprocessed.or(authorization) {
                Some(error) => Some(error),
                None => self
                    .apply_incremental_resource(&resource, request.validate_only)
                    .await
                    .err()
                    .map(ConfigError::from_control),
            };
            responses.push(incremental_result(resource, error));
        }
        IncrementalAlterConfigsResponse::default().with_responses(responses)
    }

    async fn authorize_config_resources(
        &self,
        context: &AuthorizationContext,
        resources: &[ResourceIdentity],
        preprocessed: &[Option<ConfigError>],
    ) -> anyhow::Result<Vec<Option<ConfigError>>> {
        let mut results = vec![None; resources.len()];
        for (index, resource) in resources.iter().enumerate() {
            if preprocessed[index].is_some() {
                continue;
            }
            let (resource_type, resource_name, denied_error) =
                acl_target(resource.resource_type, &resource.resource_name)?;
            if !self
                .authorized(
                    context,
                    resource_type,
                    resource_name,
                    AclOperation::AlterConfigs,
                )
                .await?
            {
                results[index] = Some(ConfigError::new(
                    denied_error,
                    "configuration authorization failed",
                ));
            }
        }
        Ok(results)
    }

    async fn apply_incremental_resource(
        &self,
        resource: &AlterConfigsResource,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        match resource.resource_type {
            TOPIC_RESOURCE => {
                self.alter_topic_config(
                    resource.resource_type,
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            CLIENT_METRICS_RESOURCE => {
                self.alter_client_metric_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            GROUP_RESOURCE => {
                self.alter_group_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            BROKER_RESOURCE => {
                self.alter_broker_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            resource_type => Err(ControlError::InvalidRequest(format!(
                "resource type {resource_type} is not supported"
            ))),
        }
    }

    async fn apply_legacy_resource(
        &self,
        resource: &LegacyResource,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        match resource.resource_type {
            TOPIC_RESOURCE => {
                self.replace_topic_config(
                    resource.resource_type,
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            CLIENT_METRICS_RESOURCE => {
                self.replace_client_metric_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            GROUP_RESOURCE => {
                self.replace_group_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            BROKER_RESOURCE => {
                self.replace_broker_config(
                    resource.resource_name.as_str(),
                    &resource.configs,
                    validate_only,
                )
                .await
            }
            resource_type => Err(ControlError::InvalidRequest(format!(
                "resource type {resource_type} is not supported"
            ))),
        }
    }
}

fn preprocess_legacy(resources: &[LegacyResource]) -> Vec<Option<ConfigError>> {
    let duplicate_resources = duplicate_resource_keys(
        resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str())),
    );
    resources
        .iter()
        .map(|resource| {
            let key = (
                resource.resource_type,
                resource.resource_name.as_str().to_owned(),
            );
            if duplicate_resources.contains(&key) {
                return Some(invalid("each resource must appear at most once"));
            }
            preprocess_legacy_configs(&resource.configs)
                .or_else(|| (!legacy_resource_type(resource.resource_type)).then(unknown_resource))
        })
        .collect()
}

fn preprocess_incremental(resources: &[AlterConfigsResource]) -> Vec<Option<ConfigError>> {
    let duplicate_resources = duplicate_resource_keys(
        resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str())),
    );
    resources
        .iter()
        .map(|resource| {
            let key = (
                resource.resource_type,
                resource.resource_name.as_str().to_owned(),
            );
            if duplicate_resources.contains(&key) {
                return Some(invalid("each resource must appear at most once"));
            }
            preprocess_incremental_configs(&resource.configs).or_else(|| {
                (!incremental_resource_type(resource.resource_type)).then(unknown_resource)
            })
        })
        .collect()
}

fn preprocess_legacy_configs(configs: &[LegacyConfig]) -> Option<ConfigError> {
    if duplicate_config_names(configs.iter().map(|config| config.name.as_str())) {
        return Some(invalid("configuration keys must be unique"));
    }
    configs
        .iter()
        .any(|config| config.value.is_none())
        .then(|| invalid("null configuration values are not supported"))
}

fn preprocess_incremental_configs(configs: &[AlterableConfig]) -> Option<ConfigError> {
    if duplicate_config_names(configs.iter().map(|config| config.name.as_str())) {
        return Some(invalid("configuration keys must be unique"));
    }
    configs
        .iter()
        .any(|config| config.config_operation != 1 && config.value.is_none())
        .then(|| invalid("null configuration values are only valid for DELETE"))
}

fn legacy_resource_type(resource_type: i8) -> bool {
    matches!(
        resource_type,
        TOPIC_RESOURCE | BROKER_RESOURCE | CLIENT_METRICS_RESOURCE | GROUP_RESOURCE
    )
}

fn incremental_resource_type(resource_type: i8) -> bool {
    legacy_resource_type(resource_type) || resource_type == BROKER_LOGGER_RESOURCE
}

fn acl_target(
    resource_type: i8,
    resource_name: &str,
) -> Result<(AclResourceType, &str, i16), ControlError> {
    match resource_type {
        TOPIC_RESOURCE => Ok((
            AclResourceType::Topic,
            resource_name,
            TOPIC_AUTHORIZATION_FAILED,
        )),
        GROUP_RESOURCE => Ok((
            AclResourceType::Group,
            resource_name,
            GROUP_AUTHORIZATION_FAILED,
        )),
        BROKER_RESOURCE | BROKER_LOGGER_RESOURCE | CLIENT_METRICS_RESOURCE => Ok((
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
            CLUSTER_AUTHORIZATION_FAILED,
        )),
        _ => Err(ControlError::InvalidRequest(format!(
            "unexpected configuration resource type {resource_type}"
        ))),
    }
}

fn legacy_backend_error(
    resources: Vec<LegacyResource>,
    preprocessed: Vec<Option<ConfigError>>,
    message: &str,
) -> AlterConfigsResponse {
    let backend = ConfigError::new(UNKNOWN_SERVER_ERROR, message);
    AlterConfigsResponse::default().with_responses(
        resources
            .into_iter()
            .zip(preprocessed)
            .map(|(resource, preprocessed)| {
                legacy_result(
                    resource,
                    Some(preprocessed.unwrap_or_else(|| backend.clone())),
                )
            })
            .collect(),
    )
}

fn incremental_backend_error(
    resources: Vec<AlterConfigsResource>,
    preprocessed: Vec<Option<ConfigError>>,
    message: &str,
) -> IncrementalAlterConfigsResponse {
    let backend = ConfigError::new(UNKNOWN_SERVER_ERROR, message);
    IncrementalAlterConfigsResponse::default().with_responses(
        resources
            .into_iter()
            .zip(preprocessed)
            .map(|(resource, preprocessed)| {
                incremental_result(
                    resource,
                    Some(preprocessed.unwrap_or_else(|| backend.clone())),
                )
            })
            .collect(),
    )
}

fn legacy_result(resource: LegacyResource, error: Option<ConfigError>) -> LegacyResponse {
    LegacyResponse::default()
        .with_error_code(error.as_ref().map_or(NO_ERROR, |error| error.code))
        .with_error_message(error.map(|error| StrBytes::from_string(error.message)))
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name)
}

fn incremental_result(
    resource: AlterConfigsResource,
    error: Option<ConfigError>,
) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse::default()
        .with_error_code(error.as_ref().map_or(NO_ERROR, |error| error.code))
        .with_error_message(error.map(|error| StrBytes::from_string(error.message)))
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name)
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::new(INVALID_REQUEST, message)
}

fn unknown_resource() -> ConfigError {
    invalid("unknown configuration resource type")
}

fn duplicate_resource_keys<'a>(
    resources: impl Iterator<Item = (i8, &'a str)>,
) -> HashSet<(i8, String)> {
    let mut seen = HashSet::new();
    let mut duplicates = HashSet::new();
    for (resource_type, resource_name) in resources {
        let key = (resource_type, resource_name.to_owned());
        if !seen.insert(key.clone()) {
            duplicates.insert(key);
        }
    }
    duplicates
}

fn duplicate_config_names<'a>(mut names: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    names.any(|name| !seen.insert(name))
}
