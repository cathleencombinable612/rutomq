use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::*;
use kafka_protocol::messages::create_acls_request::AclCreation;
use kafka_protocol::messages::{CreateAclsRequest, CreateAclsResponse};
use rutomq_control::{
    AclFilter, AclOperation, AclPatternFilter, AclPatternType, AclPermission, AclResourceType,
};

fn creation(resource_name: &str) -> AclCreation {
    AclCreation::default()
        .with_resource_type(AclResourceType::Topic.code())
        .with_resource_name(StrBytes::from_string(resource_name.to_owned()))
        .with_resource_pattern_type(AclPatternType::Literal.code())
        .with_principal(StrBytes::from_string("User:alice".to_owned()))
        .with_host(StrBytes::from_string("*".to_owned()))
        .with_operation(AclOperation::Read.code())
        .with_permission_type(AclPermission::Allow.code())
}

fn all_acls() -> AclFilter {
    AclFilter {
        resource_type: None,
        resource_name: None,
        pattern_type: AclPatternFilter::Any,
        principal: None,
        host: None,
        operation: None,
        permission: None,
    }
}

#[tokio::test]
async fn non_specific_acl_enums_reject_the_complete_request_before_mutation() {
    let (broker, metadata) = acl_broker();
    let mut malformed = Vec::new();

    for resource_type in [0, 1] {
        malformed.push(creation("invalid-resource").with_resource_type(resource_type));
    }
    for pattern_type in [0, 1, 2] {
        malformed.push(creation("invalid-pattern").with_resource_pattern_type(pattern_type));
    }
    for operation in [0, 1] {
        malformed.push(creation("invalid-operation").with_operation(operation));
    }
    for permission in [0, 1] {
        malformed.push(creation("invalid-permission").with_permission_type(permission));
    }

    for (index, invalid) in malformed.into_iter().enumerate() {
        let request = CreateAclsRequest::default()
            .with_creations(vec![creation(&format!("valid-{index}")), invalid]);
        let response = handle_as(
            &broker,
            "admin",
            ApiKey::CreateAcls,
            3,
            index as i32,
            &request,
        )
        .await;
        let response: CreateAclsResponse = decode_response(ApiKey::CreateAcls, 3, response);

        assert_eq!(response.results.len(), 2);
        assert!(
            response
                .results
                .iter()
                .all(|result| result.error_code == INVALID_REQUEST)
        );
        assert_eq!(
            response.results[0].error_message,
            response.results[1].error_message
        );
        assert!(
            metadata
                .describe_acls(&all_acls())
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn semantic_acl_errors_remain_independent_per_creation() {
    let (broker, metadata) = acl_broker();
    let invalid_cluster =
        creation("another-cluster").with_resource_type(AclResourceType::Cluster.code());
    let request = CreateAclsRequest::default().with_creations(vec![
        creation("valid-topic"),
        creation(""),
        invalid_cluster,
    ]);

    let response = handle_as(&broker, "admin", ApiKey::CreateAcls, 3, 20, &request).await;
    let response: CreateAclsResponse = decode_response(ApiKey::CreateAcls, 3, response);

    assert_eq!(
        response
            .results
            .iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
        vec![NO_ERROR, INVALID_REQUEST, INVALID_REQUEST]
    );
    let rules = metadata.describe_acls(&all_acls()).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].resource_name, "valid-topic");
}
