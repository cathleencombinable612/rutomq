use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, NO_ERROR, NON_EMPTY_GROUP,
    UNKNOWN_SERVER_ERROR, control_error_code,
};
use bytes::Bytes;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, ListGroupsRequest, ListGroupsResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, ClassicGroupDescription, ControlError, GroupSummary,
};
use std::collections::HashSet;

impl Broker {
    pub(super) async fn handle_list_groups(
        &self,
        request: ListGroupsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> ListGroupsResponse {
        let cluster_describe = match self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Describe,
            )
            .await
        {
            Ok(allowed) => allowed,
            Err(_) => {
                return ListGroupsResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };
        let groups = match self.metadata.list_groups().await {
            Ok(groups) => groups,
            Err(_) => {
                return ListGroupsResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };
        let mut listed = Vec::new();
        for group in groups
            .into_iter()
            .filter(|group| matches_filters(group, &request))
        {
            if cluster_describe {
                listed.push(listed_group(group, version));
                continue;
            }
            match self
                .authorized(
                    context,
                    AclResourceType::Group,
                    &group.group_id,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(true) => listed.push(listed_group(group, version)),
                Ok(false) => {}
                Err(_) => {
                    return ListGroupsResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
                }
            }
        }
        ListGroupsResponse::default()
            .with_error_code(NO_ERROR)
            .with_groups(listed)
    }

    pub(super) async fn handle_describe_groups(
        &self,
        request: DescribeGroupsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> DescribeGroupsResponse {
        let include_authorized_operations = request.include_authorized_operations;
        let mut authorized_ids = Vec::new();
        let mut authorized_names = Vec::new();
        let mut groups = Vec::with_capacity(request.groups.len());
        for group_id in request.groups {
            let name = group_id.as_str().to_owned();
            match self
                .authorized(
                    context,
                    AclResourceType::Group,
                    &name,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(true) => {
                    authorized_names.push(name);
                    authorized_ids.push(group_id);
                }
                Ok(false) => groups.push(described_group_error(
                    group_id,
                    GROUP_AUTHORIZATION_FAILED,
                    "group authorization failed",
                    version,
                )),
                Err(error) => groups.push(described_group_error(
                    group_id,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    version,
                )),
            }
        }
        let descriptions = self
            .metadata
            .describe_classic_groups(&authorized_names)
            .await;
        for group_id in authorized_ids {
            let name = group_id.as_str();
            match &descriptions {
                Ok(descriptions) => match descriptions.get(name) {
                    Some(description) => {
                        let operations = if include_authorized_operations {
                            self.group_authorized_operations(context, name).await
                        } else {
                            i32::MIN
                        };
                        groups.push(described_group(description, operations, version));
                    }
                    None => groups.push(described_group_error(
                        group_id,
                        GROUP_ID_NOT_FOUND,
                        "group was not found",
                        version,
                    )),
                },
                Err(error) => groups.push(described_group_error(
                    group_id,
                    control_error_code(error),
                    &error.to_string(),
                    version,
                )),
            }
        }
        DescribeGroupsResponse::default().with_groups(groups)
    }

    pub(super) async fn handle_delete_groups(
        &self,
        request: DeleteGroupsRequest,
        context: &AuthorizationContext,
    ) -> DeleteGroupsResponse {
        let mut results = Vec::with_capacity(request.groups_names.len());
        let mut seen = HashSet::new();
        for group_id in request.groups_names {
            let name = group_id.as_str();
            if !seen.insert(name.to_owned()) {
                continue;
            }
            let error_code = match self
                .authorized(context, AclResourceType::Group, name, AclOperation::Delete)
                .await
            {
                Ok(false) => GROUP_AUTHORIZATION_FAILED,
                Err(_) => UNKNOWN_SERVER_ERROR,
                Ok(true) => match self.metadata.delete_group(name).await {
                    Ok(()) => NO_ERROR,
                    Err(ControlError::NonEmptyGroup(_)) => NON_EMPTY_GROUP,
                    Err(ControlError::GroupNotFound(_)) => GROUP_ID_NOT_FOUND,
                    Err(error) => control_error_code(&error),
                },
            };
            results.push(
                DeletableGroupResult::default()
                    .with_group_id(group_id)
                    .with_error_code(error_code),
            );
        }
        DeleteGroupsResponse::default().with_results(results)
    }
}

fn matches_filters(group: &GroupSummary, request: &ListGroupsRequest) -> bool {
    (request.states_filter.is_empty()
        || request
            .states_filter
            .iter()
            .any(|state| state.as_str().eq_ignore_ascii_case(&group.state)))
        && (request.types_filter.is_empty()
            || request
                .types_filter
                .iter()
                .any(|kind| kind.as_str().eq_ignore_ascii_case(&group.group_type)))
}

fn listed_group(group: GroupSummary, version: i16) -> ListedGroup {
    let response = ListedGroup::default()
        .with_group_id(group_id(&group.group_id))
        .with_protocol_type(string(group.protocol_type));
    let response = if version >= 4 {
        response.with_group_state(string(group.state))
    } else {
        response
    };
    if version >= 5 {
        response.with_group_type(string(group.group_type))
    } else {
        response
    }
}

fn described_group(
    description: &ClassicGroupDescription,
    authorized_operations: i32,
    version: i16,
) -> DescribedGroup {
    let response = DescribedGroup::default()
        .with_error_code(NO_ERROR)
        .with_group_id(group_id(&description.group_id))
        .with_group_state(string(&description.state))
        .with_protocol_type(string(&description.protocol_type))
        .with_protocol_data(string(&description.protocol_data))
        .with_members(
            description
                .members
                .iter()
                .map(|member| {
                    let response = DescribedGroupMember::default()
                        .with_member_id(string(&member.member_id))
                        .with_client_id(string(&member.client_id))
                        .with_client_host(string(&member.client_host))
                        .with_member_metadata(Bytes::copy_from_slice(&member.member_metadata))
                        .with_member_assignment(Bytes::copy_from_slice(&member.member_assignment));
                    if version >= 4 {
                        response
                            .with_group_instance_id(member.group_instance_id.as_deref().map(string))
                    } else {
                        response
                    }
                })
                .collect(),
        );
    if version >= 3 {
        response.with_authorized_operations(authorized_operations)
    } else {
        response
    }
}

fn described_group_error(
    group_id: GroupId,
    error_code: i16,
    message: &str,
    version: i16,
) -> DescribedGroup {
    let response = DescribedGroup::default()
        .with_group_id(group_id)
        .with_error_code(error_code);
    if version >= 6 {
        response.with_error_message(Some(string(message)))
    } else {
        response
    }
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

fn string(value: impl Into<String>) -> StrBytes {
    StrBytes::from_string(value.into())
}
