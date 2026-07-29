use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::broker_config::{
    BROKER_LOGGER_RESOURCE, BROKER_RESOURCE, describe_broker, describe_broker_logger,
};
use super::client_metric_config::{CLIENT_METRICS_RESOURCE, describe_client_metric_config};
use super::config_api::describe_topic_config;
use super::group_config::GROUP_RESOURCE;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, GROUP_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, control_error_code,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, ControlError};

const TOPIC_RESOURCE: i8 = 2;

impl Broker {
    pub(super) async fn handle_describe_configs(
        &self,
        request: DescribeConfigsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> DescribeConfigsResponse {
        let mut decisions = Vec::with_capacity(request.resources.len());
        for resource in &request.resources {
            let (resource_type, resource_name, denied_error) =
                match acl_target(resource.resource_type, resource.resource_name.as_str()) {
                    Ok(target) => target,
                    Err(error) => {
                        return request_error(
                            &request.resources,
                            INVALID_REQUEST,
                            &error.to_string(),
                        );
                    }
                };
            match self
                .authorized(
                    context,
                    resource_type,
                    resource_name,
                    AclOperation::DescribeConfigs,
                )
                .await
            {
                Ok(authorized) => decisions.push((authorized, denied_error)),
                Err(error) => {
                    return request_error(
                        &request.resources,
                        UNKNOWN_SERVER_ERROR,
                        &error.to_string(),
                    );
                }
            }
        }

        let include_synonyms = request.include_synonyms;
        let include_documentation = request.include_documentation;
        let mut authorized = Vec::with_capacity(request.resources.len());
        let mut denied = Vec::new();
        for (resource, (is_authorized, denied_error)) in
            request.resources.into_iter().zip(decisions)
        {
            if is_authorized {
                authorized.push(
                    self.describe_config_resource(
                        resource,
                        version,
                        include_synonyms,
                        include_documentation,
                    )
                    .await,
                );
            } else {
                denied.push(resource_error(
                    &resource,
                    denied_error,
                    "configuration authorization failed",
                ));
            }
        }
        authorized.extend(denied);
        DescribeConfigsResponse::default().with_results(authorized)
    }

    async fn describe_config_resource(
        &self,
        resource: DescribeConfigsResource,
        version: i16,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> DescribeConfigsResult {
        let described = match resource.resource_type {
            TOPIC_RESOURCE => self
                .metadata
                .topic_config(resource.resource_name.as_str())
                .await
                .map(|config| {
                    describe_topic_config(
                        &config,
                        resource.configuration_keys.as_deref(),
                        version,
                        include_synonyms,
                        include_documentation,
                    )
                }),
            CLIENT_METRICS_RESOURCE => self
                .metadata
                .client_metric_subscription(resource.resource_name.as_str())
                .await
                .and_then(|subscription| {
                    subscription
                        .ok_or_else(|| {
                            ControlError::ClientMetricSubscriptionNotFound(
                                resource.resource_name.as_str().to_owned(),
                            )
                        })
                        .map(|subscription| {
                            describe_client_metric_config(
                                &subscription,
                                resource.configuration_keys.as_deref(),
                                version,
                                include_synonyms,
                                include_documentation,
                            )
                        })
                }),
            GROUP_RESOURCE => {
                self.describe_group_config(
                    resource.resource_name.as_str(),
                    resource.configuration_keys.as_deref(),
                    version,
                    include_synonyms,
                    include_documentation,
                )
                .await
            }
            BROKER_RESOURCE => self.metadata.broker_config().await.and_then(|dynamic| {
                describe_broker(
                    &self.config,
                    &dynamic,
                    resource.resource_name.as_str(),
                    resource.configuration_keys.as_deref(),
                    version,
                    include_synonyms,
                    include_documentation,
                )
            }),
            BROKER_LOGGER_RESOURCE => describe_broker_logger(
                &self.config,
                resource.resource_name.as_str(),
                resource.configuration_keys.as_deref(),
                version,
                include_synonyms,
                include_documentation,
            ),
            _ => unreachable!("DescribeConfigs resource types were validated"),
        };
        match described {
            Ok(configs) => DescribeConfigsResult::default()
                .with_error_code(NO_ERROR)
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name)
                .with_configs(configs),
            Err(error) => resource_error(&resource, control_error_code(&error), &error.to_string()),
        }
    }
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
            "unexpected DescribeConfigs resource type {resource_type}"
        ))),
    }
}

fn request_error(
    resources: &[DescribeConfigsResource],
    error_code: i16,
    message: &str,
) -> DescribeConfigsResponse {
    DescribeConfigsResponse::default().with_results(
        resources
            .iter()
            .map(|resource| resource_error(resource, error_code, message))
            .collect(),
    )
}

fn resource_error(
    resource: &DescribeConfigsResource,
    error_code: i16,
    message: &str,
) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .with_error_code(error_code)
        .with_error_message(Some(StrBytes::from_string(message.to_owned())))
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
}
