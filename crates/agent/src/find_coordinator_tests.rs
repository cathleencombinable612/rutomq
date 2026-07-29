use super::acl_tests::{acl_broker, decode_response as decode_acl_response, handle_as};
use super::tests::{broker, decode_response, request_frame};
use super::*;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use rutomq_control::{AclPatternType, AclPermission, AclRule, MemoryMetadataStore};

async fn find_coordinator(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    request: &FindCoordinatorRequest,
) -> FindCoordinatorResponse {
    let frame = broker
        .handle_request(request_frame(
            ApiKey::FindCoordinator,
            version,
            correlation_id,
            request,
        ))
        .await
        .unwrap();
    decode_response(ApiKey::FindCoordinator, version, frame)
}

fn assert_no_node(node_id: BrokerId, host: &StrBytes, port: i32) {
    assert_eq!(*node_id, -1);
    assert_eq!(host.as_str(), "");
    assert_eq!(port, -1);
}

fn assert_coordinator_no_node(coordinator: &Coordinator, error_code: i16) {
    assert_eq!(coordinator.error_code, error_code);
    assert_no_node(coordinator.node_id, &coordinator.host, coordinator.port);
}

fn cluster_action_rule(principal: &str) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Cluster,
        resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::ClusterAction,
        permission: AclPermission::Allow,
    }
}

async fn find_coordinator_as(
    broker: &Broker,
    username: &str,
    version: i16,
    correlation_id: i32,
    request: &FindCoordinatorRequest,
) -> FindCoordinatorResponse {
    let frame = handle_as(
        broker,
        username,
        ApiKey::FindCoordinator,
        version,
        correlation_id,
        request,
    )
    .await;
    decode_acl_response(ApiKey::FindCoordinator, version, frame)
}

#[tokio::test]
async fn batched_find_coordinator_preserves_empty_and_duplicate_keys() {
    let broker = broker();
    let empty = FindCoordinatorRequest::default()
        .with_key_type(0)
        .with_coordinator_keys(Vec::new());
    let response = find_coordinator(&broker, 4, 1, &empty).await;
    assert!(response.coordinators.is_empty());

    let duplicate = FindCoordinatorRequest::default()
        .with_key_type(0)
        .with_coordinator_keys(vec![
            StrBytes::from_static_str("workers"),
            StrBytes::from_static_str("workers"),
        ]);
    let response = find_coordinator(&broker, 4, 2, &duplicate).await;
    assert_eq!(response.coordinators.len(), 2);
    assert!(
        response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == NO_ERROR
                && coordinator.key.as_str() == "workers"
                && *coordinator.node_id == 0)
    );
}

#[tokio::test]
async fn find_coordinator_rejects_unknown_and_old_share_types_without_endpoint() {
    let broker = broker();
    let unknown = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_static_str("unknown"))
        .with_key_type(9);
    let response = find_coordinator(&broker, 3, 10, &unknown).await;
    assert_eq!(response.error_code, INVALID_REQUEST);
    assert_no_node(response.node_id, &response.host, response.port);

    let old_share = FindCoordinatorRequest::default()
        .with_key_type(2)
        .with_coordinator_keys(vec![StrBytes::from_static_str(
            "share-group:AAAAAAAAAAAAAAAAAAAAAA:0",
        )]);
    let response = find_coordinator(&broker, 5, 11, &old_share).await;
    assert_eq!(response.coordinators.len(), 1);
    assert_coordinator_no_node(&response.coordinators[0], INVALID_REQUEST);
}

#[tokio::test]
async fn find_coordinator_denials_and_backend_failures_use_no_node() {
    let (broker, metadata) = acl_broker();
    let group = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_static_str("workers"))
        .with_key_type(0);
    let response = find_coordinator_as(&broker, "alice", 3, 20, &group).await;
    assert_eq!(response.error_code, GROUP_AUTHORIZATION_FAILED);
    assert_no_node(response.node_id, &response.host, response.port);

    let transactional = FindCoordinatorRequest::default()
        .with_key_type(1)
        .with_coordinator_keys(vec![StrBytes::from_static_str("tx-1")]);
    let response = find_coordinator_as(&broker, "alice", 4, 21, &transactional).await;
    assert_coordinator_no_node(
        &response.coordinators[0],
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Group));
    let response = find_coordinator_as(&broker, "alice", 3, 22, &group).await;
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert_no_node(response.node_id, &response.host, response.port);
}

#[tokio::test]
async fn find_coordinator_v6_share_requires_cluster_action_and_valid_key() {
    let (broker, metadata): (Broker, Arc<MemoryMetadataStore>) = acl_broker();
    let valid = FindCoordinatorRequest::default()
        .with_key_type(2)
        .with_coordinator_keys(vec![StrBytes::from_static_str(
            "share-group:AAAAAAAAAAAAAAAAAAAAAA:0",
        )]);
    let response = find_coordinator_as(&broker, "alice", 6, 30, &valid).await;
    assert_coordinator_no_node(&response.coordinators[0], CLUSTER_AUTHORIZATION_FAILED);

    metadata
        .create_acl(cluster_action_rule("User:alice"))
        .await
        .unwrap();
    let response = find_coordinator_as(&broker, "alice", 6, 31, &valid).await;
    let coordinator = &response.coordinators[0];
    assert_eq!(coordinator.error_code, NO_ERROR);
    assert_eq!(*coordinator.node_id, 0);
    assert!(coordinator.port > 0);
    assert!(!coordinator.host.is_empty());

    let malformed = FindCoordinatorRequest::default()
        .with_key_type(2)
        .with_coordinator_keys(vec![StrBytes::from_static_str(
            "share-group:not-a-topic-id:0",
        )]);
    let response = find_coordinator_as(&broker, "alice", 6, 32, &malformed).await;
    assert_coordinator_no_node(&response.coordinators[0], INVALID_REQUEST);

    metadata.set_authorization_failure_for(Some(AclResourceType::Cluster));
    let response = find_coordinator_as(&broker, "alice", 6, 33, &valid).await;
    assert_coordinator_no_node(&response.coordinators[0], UNKNOWN_SERVER_ERROR);
}
