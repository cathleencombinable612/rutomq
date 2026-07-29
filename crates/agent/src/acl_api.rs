use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR, SECURITY_DISABLED, control_error_code,
};
use kafka_protocol::messages::create_acls_request::AclCreation;
use kafka_protocol::messages::create_acls_response::AclCreationResult;
use kafka_protocol::messages::delete_acls_response::{
    DeleteAclsFilterResult, DeleteAclsMatchingAcl,
};
use kafka_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};
use kafka_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclFilter, AclOperation, AclPatternFilter, AclPatternType, AclPermission, AclResourceType,
    AclRule, ControlError,
};
use std::collections::BTreeMap;

impl Broker {
    pub(super) async fn handle_describe_acls(
        &self,
        request: DescribeAclsRequest,
        context: &AuthorizationContext,
    ) -> DescribeAclsResponse {
        if !self.acl_enabled() {
            return acl_describe_error(SECURITY_DISABLED, "ACL authorization is disabled");
        }
        match self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Describe,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return acl_describe_error(
                    CLUSTER_AUTHORIZATION_FAILED,
                    "principal is not authorized to describe ACLs",
                );
            }
            Err(error) => return acl_describe_error(-1, &error.to_string()),
        }
        let filter = match describe_filter(request) {
            Ok(filter) => filter,
            Err(error) => return acl_describe_error(INVALID_REQUEST, &error.to_string()),
        };
        match self.metadata.describe_acls(&filter).await {
            Ok(rules) => DescribeAclsResponse::default()
                .with_error_code(NO_ERROR)
                .with_error_message(None)
                .with_resources(describe_resources(rules)),
            Err(error) => acl_describe_error(control_error_code(&error), &error.to_string()),
        }
    }

    pub(super) async fn handle_create_acls(
        &self,
        request: CreateAclsRequest,
        context: &AuthorizationContext,
    ) -> CreateAclsResponse {
        let authorization_error = if !self.acl_enabled() {
            Some((SECURITY_DISABLED, "ACL authorization is disabled"))
        } else {
            match self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    AclOperation::Alter,
                )
                .await
            {
                Ok(true) => None,
                Ok(false) => Some((
                    CLUSTER_AUTHORIZATION_FAILED,
                    "principal is not authorized to create ACLs",
                )),
                Err(_) => Some((-1, "ACL authorization lookup failed")),
            }
        };
        if let Some((code, message)) = authorization_error {
            return CreateAclsResponse::default().with_results(
                request
                    .creations
                    .iter()
                    .map(|_| creation_error(code, message))
                    .collect(),
            );
        }

        if let Some(message) = create_request_error(&request.creations) {
            return CreateAclsResponse::default().with_results(
                request
                    .creations
                    .iter()
                    .map(|_| creation_error(INVALID_REQUEST, message))
                    .collect(),
            );
        }

        let mut results = Vec::with_capacity(request.creations.len());
        for creation in request.creations {
            let result = match creation_rule(creation) {
                Ok(rule) => self.metadata.create_acl(rule).await,
                Err(error) => Err(error),
            };
            results.push(match result {
                Ok(()) => AclCreationResult::default()
                    .with_error_code(NO_ERROR)
                    .with_error_message(None),
                Err(error) => creation_error(control_error_code(&error), &error.to_string()),
            });
        }
        CreateAclsResponse::default().with_results(results)
    }

    pub(super) async fn handle_delete_acls(
        &self,
        request: DeleteAclsRequest,
        context: &AuthorizationContext,
    ) -> DeleteAclsResponse {
        let authorization_error = if !self.acl_enabled() {
            Some((SECURITY_DISABLED, "ACL authorization is disabled"))
        } else {
            match self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    AclOperation::Alter,
                )
                .await
            {
                Ok(true) => None,
                Ok(false) => Some((
                    CLUSTER_AUTHORIZATION_FAILED,
                    "principal is not authorized to delete ACLs",
                )),
                Err(_) => Some((-1, "ACL authorization lookup failed")),
            }
        };
        if let Some((code, message)) = authorization_error {
            return DeleteAclsResponse::default().with_filter_results(
                request
                    .filters
                    .iter()
                    .map(|_| delete_filter_error(code, message))
                    .collect(),
            );
        }

        let mut filter_results = Vec::with_capacity(request.filters.len());
        for request_filter in request.filters {
            let filter = delete_filter(request_filter);
            let deleted = match filter {
                Ok(filter) => self
                    .metadata
                    .delete_acls(std::slice::from_ref(&filter))
                    .await
                    .map(|mut results| results.pop().unwrap_or_default()),
                Err(error) => Err(error),
            };
            filter_results.push(match deleted {
                Ok(rules) => DeleteAclsFilterResult::default()
                    .with_error_code(NO_ERROR)
                    .with_error_message(None)
                    .with_matching_acls(rules.into_iter().map(delete_matching_acl).collect()),
                Err(error) => delete_filter_error(control_error_code(&error), &error.to_string()),
            });
        }
        DeleteAclsResponse::default().with_filter_results(filter_results)
    }
}

fn create_request_error(creations: &[AclCreation]) -> Option<&'static str> {
    creations.iter().find_map(|creation| {
        let has_unknown = creation.resource_type == 0
            || creation.resource_pattern_type == 0
            || creation.operation == 0
            || creation.permission_type == 0;
        let is_non_specific = creation.resource_type == 1
            || matches!(creation.resource_pattern_type, 1 | 2)
            || creation.operation == 1
            || creation.permission_type == 1;
        (has_unknown || is_non_specific)
            .then_some("creatable ACLs contain unknown or non-specific elements")
    })
}

fn creation_rule(creation: AclCreation) -> Result<AclRule, ControlError> {
    let rule = AclRule {
        resource_type: AclResourceType::try_from(creation.resource_type)?,
        resource_name: creation.resource_name.as_str().to_owned(),
        pattern_type: AclPatternType::try_from(creation.resource_pattern_type)?,
        principal: creation.principal.as_str().to_owned(),
        host: creation.host.as_str().to_owned(),
        operation: AclOperation::try_from(creation.operation)?,
        permission: AclPermission::try_from(creation.permission_type)?,
    };
    if rule.resource_type == AclResourceType::Cluster && rule.resource_name != CLUSTER_RESOURCE_NAME
    {
        return Err(ControlError::InvalidRequest(format!(
            "the only valid name for the CLUSTER resource is {CLUSTER_RESOURCE_NAME}"
        )));
    }
    rule.validate()?;
    Ok(rule)
}

fn describe_filter(request: DescribeAclsRequest) -> Result<AclFilter, ControlError> {
    acl_filter(
        request.resource_type_filter,
        request
            .resource_name_filter
            .map(|value| value.as_str().to_owned()),
        request.pattern_type_filter,
        request
            .principal_filter
            .map(|value| value.as_str().to_owned()),
        request.host_filter.map(|value| value.as_str().to_owned()),
        request.operation,
        request.permission_type,
    )
}

fn delete_filter(
    request: kafka_protocol::messages::delete_acls_request::DeleteAclsFilter,
) -> Result<AclFilter, ControlError> {
    acl_filter(
        request.resource_type_filter,
        request
            .resource_name_filter
            .map(|value| value.as_str().to_owned()),
        request.pattern_type_filter,
        request
            .principal_filter
            .map(|value| value.as_str().to_owned()),
        request.host_filter.map(|value| value.as_str().to_owned()),
        request.operation,
        request.permission_type,
    )
}

fn acl_filter(
    resource_type: i8,
    resource_name: Option<String>,
    pattern_type: i8,
    principal: Option<String>,
    host: Option<String>,
    operation: i8,
    permission: i8,
) -> Result<AclFilter, ControlError> {
    Ok(AclFilter {
        resource_type: (resource_type != 1)
            .then(|| AclResourceType::try_from(resource_type))
            .transpose()?,
        resource_name,
        pattern_type: AclPatternFilter::try_from(pattern_type)?,
        principal,
        host,
        operation: (operation != 1)
            .then(|| AclOperation::try_from(operation))
            .transpose()?,
        permission: (permission != 1)
            .then(|| AclPermission::try_from(permission))
            .transpose()?,
    })
}

fn describe_resources(rules: Vec<AclRule>) -> Vec<DescribeAclsResource> {
    let mut resources = BTreeMap::<(i8, String, i8), Vec<AclDescription>>::new();
    for rule in rules {
        resources
            .entry((
                rule.resource_type.code(),
                rule.resource_name,
                rule.pattern_type.code(),
            ))
            .or_default()
            .push(
                AclDescription::default()
                    .with_principal(text(rule.principal))
                    .with_host(text(rule.host))
                    .with_operation(rule.operation.code())
                    .with_permission_type(rule.permission.code()),
            );
    }
    resources
        .into_iter()
        .map(|((resource_type, resource_name, pattern_type), acls)| {
            DescribeAclsResource::default()
                .with_resource_type(resource_type)
                .with_resource_name(text(resource_name))
                .with_pattern_type(pattern_type)
                .with_acls(acls)
        })
        .collect()
}

fn delete_matching_acl(rule: AclRule) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl::default()
        .with_error_code(NO_ERROR)
        .with_error_message(None)
        .with_resource_type(rule.resource_type.code())
        .with_resource_name(text(rule.resource_name))
        .with_pattern_type(rule.pattern_type.code())
        .with_principal(text(rule.principal))
        .with_host(text(rule.host))
        .with_operation(rule.operation.code())
        .with_permission_type(rule.permission.code())
}

fn acl_describe_error(code: i16, message: &str) -> DescribeAclsResponse {
    DescribeAclsResponse::default()
        .with_error_code(code)
        .with_error_message(Some(text(message)))
}

fn creation_error(code: i16, message: &str) -> AclCreationResult {
    AclCreationResult::default()
        .with_error_code(code)
        .with_error_message(Some(text(message)))
}

fn delete_filter_error(code: i16, message: &str) -> DeleteAclsFilterResult {
    DeleteAclsFilterResult::default()
        .with_error_code(code)
        .with_error_message(Some(text(message)))
}

fn text(value: impl Into<String>) -> StrBytes {
    StrBytes::from_string(value.into())
}
