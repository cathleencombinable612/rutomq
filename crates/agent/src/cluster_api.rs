use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    INVALID_REQUEST, MISMATCHED_ENDPOINT_TYPE, NO_ERROR, UNKNOWN_SERVER_ERROR,
    UNSUPPORTED_ENDPOINT_TYPE,
};
use anyhow::Result;
use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
use kafka_protocol::messages::{BrokerId, DescribeClusterRequest, DescribeClusterResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType};

const BROKER_ENDPOINT_TYPE: i8 = 1;
const CONTROLLER_ENDPOINT_TYPE: i8 = 2;

impl Broker {
    pub(super) async fn handle_describe_cluster(
        &self,
        request: DescribeClusterRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> DescribeClusterResponse {
        if request.endpoint_type != BROKER_ENDPOINT_TYPE {
            let (error_code, message) = if version == 0 {
                (
                    INVALID_REQUEST,
                    format!("unsupported endpoint type {}", request.endpoint_type),
                )
            } else if request.endpoint_type == CONTROLLER_ENDPOINT_TYPE {
                (
                    MISMATCHED_ENDPOINT_TYPE,
                    "the broker endpoint cannot describe controller endpoints".to_owned(),
                )
            } else {
                (
                    UNSUPPORTED_ENDPOINT_TYPE,
                    format!("unsupported endpoint type {}", request.endpoint_type),
                )
            };
            return cluster_error(error_code, &message);
        }

        let authorized_operations = if request.include_cluster_authorized_operations {
            match self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(false) => 0,
                Ok(true) => match self.checked_cluster_authorized_operations(context).await {
                    Ok(operations) => operations,
                    Err(error) => return cluster_error(UNKNOWN_SERVER_ERROR, &error.to_string()),
                },
                Err(error) => return cluster_error(UNKNOWN_SERVER_ERROR, &error.to_string()),
            }
        } else {
            i32::MIN
        };
        DescribeClusterResponse::default()
            .with_error_code(NO_ERROR)
            .with_endpoint_type(request.endpoint_type)
            .with_cluster_id(string(&self.config.cluster_id))
            .with_controller_id(BrokerId::from(0))
            .with_brokers(vec![
                DescribeClusterBroker::default()
                    .with_broker_id(BrokerId::from(0))
                    .with_host(string(&self.config.advertise_host))
                    .with_port(self.config.advertise_port),
            ])
            .with_cluster_authorized_operations(authorized_operations)
    }

    pub(super) async fn cluster_authorized_operations(
        &self,
        context: &AuthorizationContext,
    ) -> i32 {
        self.checked_cluster_authorized_operations(context)
            .await
            .unwrap_or_default()
    }

    async fn checked_cluster_authorized_operations(
        &self,
        context: &AuthorizationContext,
    ) -> Result<i32> {
        let mut bitfield = 0;
        for operation in [
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
            AclOperation::IdempotentWrite,
        ] {
            if self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    operation,
                )
                .await?
            {
                bitfield |= 1_i32 << operation as i8;
            }
        }
        Ok(bitfield)
    }
}

fn cluster_error(error_code: i16, message: &str) -> DescribeClusterResponse {
    DescribeClusterResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
