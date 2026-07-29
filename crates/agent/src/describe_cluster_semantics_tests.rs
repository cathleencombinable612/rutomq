use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::*;
use crate::kafka_error::{
    INVALID_REQUEST, MISMATCHED_ENDPOINT_TYPE, NO_ERROR, UNKNOWN_SERVER_ERROR,
    UNSUPPORTED_ENDPOINT_TYPE,
};
use kafka_protocol::messages::{DescribeClusterRequest, DescribeClusterResponse};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MetadataStore,
};

const USERNAME: &str = "cluster-reader";

#[tokio::test]
async fn describe_cluster_topology_does_not_require_cluster_describe() {
    let (broker, _) = acl_broker();
    for version in 0..=2 {
        let request = DescribeClusterRequest::default()
            .with_endpoint_type(1)
            .with_include_cluster_authorized_operations(true);
        let response = handle_as(
            &broker,
            USERNAME,
            ApiKey::DescribeCluster,
            version,
            8500 + i32::from(version),
            &request,
        )
        .await;
        let response: DescribeClusterResponse =
            decode_response(ApiKey::DescribeCluster, version, response);
        assert_eq!(response.error_code, NO_ERROR);
        assert_eq!(response.cluster_id.as_str(), "rutomq-cluster");
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.cluster_authorized_operations, 0);
    }

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeCluster,
        2,
        8503,
        &DescribeClusterRequest::default(),
    )
    .await;
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.cluster_authorized_operations, i32::MIN);
}

#[tokio::test]
async fn describe_cluster_reports_exact_authorized_operations() {
    let (broker, metadata) = acl_broker();
    metadata
        .create_acl(cluster_rule(AclOperation::Alter))
        .await
        .unwrap();

    let request =
        DescribeClusterRequest::default().with_include_cluster_authorized_operations(true);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeCluster,
        2,
        8510,
        &request,
    )
    .await;
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.cluster_authorized_operations,
        (1_i32 << AclOperation::Alter as i8) | (1_i32 << AclOperation::Describe as i8)
    );
}

#[tokio::test]
async fn describe_cluster_authorizer_failure_is_only_observed_when_operations_are_requested() {
    let (broker, metadata) = acl_broker();
    metadata.set_authorization_failure(true);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeCluster,
        2,
        8520,
        &DescribeClusterRequest::default(),
    )
    .await;
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.cluster_authorized_operations, i32::MIN);

    let request =
        DescribeClusterRequest::default().with_include_cluster_authorized_operations(true);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeCluster,
        2,
        8521,
        &request,
    )
    .await;
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert_eq!(response.endpoint_type, 1);
    assert!(response.brokers.is_empty());
}

#[tokio::test]
async fn describe_cluster_endpoint_errors_are_versioned() {
    let (broker, _) = acl_broker();
    let context =
        AuthorizationContext::authenticated(USERNAME, std::net::Ipv4Addr::LOCALHOST.into());
    for endpoint_type in [2, 3] {
        let response = broker
            .handle_describe_cluster(
                DescribeClusterRequest::default().with_endpoint_type(endpoint_type),
                0,
                &context,
            )
            .await;
        assert_eq!(response.error_code, INVALID_REQUEST);
        assert_eq!(response.endpoint_type, 1);
    }

    for version in 1..=2 {
        for (endpoint_type, expected_error) in [
            (2, MISMATCHED_ENDPOINT_TYPE),
            (3, UNSUPPORTED_ENDPOINT_TYPE),
        ] {
            let request = DescribeClusterRequest::default().with_endpoint_type(endpoint_type);
            let response = handle_as(
                &broker,
                USERNAME,
                ApiKey::DescribeCluster,
                version,
                8530 + i32::from(version) * 10 + i32::from(endpoint_type),
                &request,
            )
            .await;
            let response: DescribeClusterResponse =
                decode_response(ApiKey::DescribeCluster, version, response);
            assert_eq!(response.error_code, expected_error);
            assert_eq!(response.endpoint_type, 1);
            assert!(response.brokers.is_empty());
        }
    }
}

fn cluster_rule(operation: AclOperation) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Cluster,
        resource_name: CLUSTER_RESOURCE_NAME.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: format!("User:{USERNAME}"),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}
