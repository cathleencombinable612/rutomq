use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::broker_config::{BROKER_LOGGER_RESOURCE, BROKER_RESOURCE};
use super::group_config::GROUP_RESOURCE;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, NO_ERROR, UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION,
    control_error_code,
};
use kafka_protocol::messages::list_config_resources_response::ConfigResource;
use kafka_protocol::messages::{ListConfigResourcesRequest, ListConfigResourcesResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType};

const TOPIC: i8 = 2;
const CLIENT_METRICS: i8 = 16;
const V1_RESOURCE_TYPES: [i8; 5] = [
    GROUP_RESOURCE,
    CLIENT_METRICS,
    BROKER_LOGGER_RESOURCE,
    BROKER_RESOURCE,
    TOPIC,
];

impl Broker {
    pub(super) async fn handle_list_config_resources(
        &self,
        request: ListConfigResourcesRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> ListConfigResourcesResponse {
        match self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::DescribeConfigs,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return ListConfigResourcesResponse::default()
                    .with_error_code(CLUSTER_AUTHORIZATION_FAILED);
            }
            Err(_) => {
                return ListConfigResourcesResponse::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR);
            }
        }

        let requested = if version == 0 {
            vec![CLIENT_METRICS]
        } else if request.resource_types.is_empty() {
            V1_RESOURCE_TYPES.to_vec()
        } else {
            request.resource_types
        };
        if requested
            .iter()
            .any(|resource_type| !V1_RESOURCE_TYPES.contains(resource_type))
        {
            return ListConfigResourcesResponse::default().with_error_code(UNSUPPORTED_VERSION);
        }

        let mut resources = Vec::new();
        if requested.contains(&GROUP_RESOURCE) {
            let group_ids = match self.metadata.group_config_ids().await {
                Ok(ids) => ids,
                Err(error) => {
                    return ListConfigResourcesResponse::default()
                        .with_error_code(control_error_code(&error));
                }
            };
            resources.extend(
                group_ids
                    .into_iter()
                    .map(|group_id| resource(GROUP_RESOURCE, &group_id)),
            );
        }
        if requested.contains(&BROKER_LOGGER_RESOURCE) {
            resources.push(resource(BROKER_LOGGER_RESOURCE, "0"));
        }
        if requested.contains(&BROKER_RESOURCE) {
            resources.push(resource(BROKER_RESOURCE, "0"));
        }
        if requested.contains(&CLIENT_METRICS) {
            match self.metadata.client_metric_subscriptions().await {
                Ok(subscriptions) => resources.extend(
                    subscriptions
                        .into_iter()
                        .map(|subscription| resource(CLIENT_METRICS, &subscription.name)),
                ),
                Err(error) => {
                    return ListConfigResourcesResponse::default()
                        .with_error_code(control_error_code(&error));
                }
            }
        }
        if requested.contains(&TOPIC) {
            match self.metadata.topics(None).await {
                Ok(topics) => {
                    resources.extend(topics.into_iter().map(|topic| resource(TOPIC, &topic.name)))
                }
                Err(error) => {
                    return ListConfigResourcesResponse::default()
                        .with_error_code(control_error_code(&error));
                }
            }
        }
        ListConfigResourcesResponse::default()
            .with_error_code(NO_ERROR)
            .with_config_resources(resources)
    }
}

fn resource(resource_type: i8, name: &str) -> ConfigResource {
    ConfigResource::default()
        .with_resource_name(StrBytes::from_string(name.to_owned()))
        .with_resource_type(resource_type)
}
