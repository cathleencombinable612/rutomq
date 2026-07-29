use super::authorization::{AuthorizationContext, authorization_failure};
use super::{Broker, topic_name};
use crate::kafka_error::{
    FENCED_MEMBER_EPOCH, GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED,
    INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_MEMBER_ID, UNKNOWN_SERVER_ERROR,
};
use kafka_protocol::messages::share_group_describe_response::{
    Assignment as DescribeAssignment, DescribedGroup, Member as DescribedMember,
    TopicPartitions as DescribeTopicPartitions,
};
use kafka_protocol::messages::share_group_heartbeat_response::{
    Assignment as HeartbeatAssignment, TopicPartitions as HeartbeatTopicPartitions,
};
use kafka_protocol::messages::{
    GroupId, ShareGroupDescribeRequest, ShareGroupDescribeResponse, ShareGroupHeartbeatRequest,
    ShareGroupHeartbeatResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, GroupHeartbeatOutcome, ShareGroupDescription,
    ShareGroupHeartbeat, ShareTopicAssignment,
};

impl Broker {
    pub(super) async fn handle_share_group_heartbeat(
        &self,
        request: ShareGroupHeartbeatRequest,
        context: &AuthorizationContext,
        client_id: String,
    ) -> ShareGroupHeartbeatResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return heartbeat_error(
                &request,
                code,
                &message,
                self.config.share_group_heartbeat_interval_ms,
            );
        }
        let group_id = request.group_id.as_str().to_owned();
        if let Some((error_code, backend_message)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                &group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            let message = backend_message
                .as_deref()
                .unwrap_or("share group authorization failed");
            return heartbeat_error(
                &request,
                error_code,
                message,
                self.config.share_group_heartbeat_interval_ms,
            );
        }
        let group_config = match self.group_runtime_config(&group_id).await {
            Ok(config) => config,
            Err(error) => {
                return heartbeat_error(
                    &request,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    self.config.share_group_heartbeat_interval_ms,
                );
            }
        };
        let heartbeat = ShareGroupHeartbeat {
            group_id,
            member_id: request.member_id.as_str().to_owned(),
            member_epoch: request.member_epoch,
            rack_id: request
                .rack_id
                .as_ref()
                .map(|rack| rack.as_str().to_owned()),
            subscribed_topic_names: request.subscribed_topic_names.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| topic.as_str().to_owned())
                    .collect()
            }),
            client_id,
            client_host: context.host.clone(),
            heartbeat_interval_ms: group_config.share_heartbeat_interval_ms,
            session_timeout_ms: group_config.share_session_timeout_ms,
            assignment_interval_ms: group_config.share_assignment_interval_ms,
            max_size: self.config.share_group_max_size,
        };
        let outcome = if group_config.share_assignor_offload_enable {
            self.metadata
                .share_group_heartbeat_deferred(heartbeat)
                .await
        } else {
            self.metadata
                .share_group_heartbeat(heartbeat)
                .await
                .map(|result| GroupHeartbeatOutcome {
                    result,
                    assignment_task: None,
                })
        };
        match outcome {
            Ok(outcome) => {
                if let Some(task) = outcome.assignment_task {
                    self.assignment_executor.submit(task);
                }
                let result = outcome.result;
                ShareGroupHeartbeatResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_member_id(Some(string(result.member_id)))
                    .with_member_epoch(result.member_epoch)
                    .with_heartbeat_interval_ms(result.heartbeat_interval_ms)
                    .with_assignment(result.assignment.map(heartbeat_assignment))
            }
            Err(error) => heartbeat_error(
                &request,
                share_group_error_code(&error),
                &error.to_string(),
                group_config.share_heartbeat_interval_ms,
            ),
        }
    }

    pub(super) async fn handle_share_group_describe(
        &self,
        request: ShareGroupDescribeRequest,
        context: &AuthorizationContext,
    ) -> ShareGroupDescribeResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return ShareGroupDescribeResponse::default().with_groups(
                request
                    .group_ids
                    .into_iter()
                    .map(|group_id| described_group_error(group_id, code, &message))
                    .collect(),
            );
        }
        let include_authorized_operations = request.include_authorized_operations;
        let mut authorized_ids = Vec::new();
        let mut authorized_names = Vec::new();
        let mut groups = Vec::with_capacity(request.group_ids.len());
        for group_id in request.group_ids {
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
                    "share group authorization failed",
                )),
                Err(error) => groups.push(described_group_error(
                    group_id,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                )),
            }
        }
        let descriptions = self.metadata.describe_share_groups(&authorized_names).await;
        for group_id in authorized_ids {
            let name = group_id.as_str();
            match &descriptions {
                Ok(descriptions) => match descriptions.get(name) {
                    Some(description) => {
                        let topic_names = description
                            .members
                            .iter()
                            .flat_map(|member| member.assignment.iter())
                            .map(|assignment| assignment.topic_name.as_str())
                            .collect::<Vec<_>>();
                        let topic_access =
                            self.topic_names_describable(context, &topic_names).await;
                        match topic_access {
                            Ok(false) => groups.push(described_group_error(
                                group_id,
                                TOPIC_AUTHORIZATION_FAILED,
                                "The group has described topic(s) that the client is not authorized to describe.",
                            )),
                            Err(error) => groups.push(described_group_error(
                                group_id,
                                UNKNOWN_SERVER_ERROR,
                                &error.to_string(),
                            )),
                            Ok(true) => {
                                let operations = if include_authorized_operations {
                                    self.group_authorized_operations(context, name).await
                                } else {
                                    i32::MIN
                                };
                                groups.push(described_group(description, operations));
                            }
                        }
                    }
                    None => groups.push(described_group_error(
                        group_id,
                        GROUP_ID_NOT_FOUND,
                        "share group was not found",
                    )),
                },
                Err(error) => groups.push(described_group_error(
                    group_id,
                    share_group_error_code(error),
                    &error.to_string(),
                )),
            }
        }
        ShareGroupDescribeResponse::default().with_groups(groups)
    }
}

fn heartbeat_error(
    request: &ShareGroupHeartbeatRequest,
    error_code: i16,
    message: &str,
    heartbeat_interval_ms: i32,
) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
        .with_member_id(Some(request.member_id.clone()))
        .with_member_epoch(request.member_epoch)
        .with_heartbeat_interval_ms(heartbeat_interval_ms)
}

fn share_group_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::GroupNotFound(_) | ControlError::GroupProtocolMismatch(_) => {
            GROUP_ID_NOT_FOUND
        }
        ControlError::GroupMemberNotFound { .. } => UNKNOWN_MEMBER_ID,
        ControlError::FencedMemberEpoch { .. } => FENCED_MEMBER_EPOCH,
        ControlError::GroupMaxSizeReached(_) => GROUP_MAX_SIZE_REACHED,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn heartbeat_assignment(assignments: Vec<ShareTopicAssignment>) -> HeartbeatAssignment {
    HeartbeatAssignment::default().with_topic_partitions(
        assignments
            .into_iter()
            .map(|assignment| {
                HeartbeatTopicPartitions::default()
                    .with_topic_id(assignment.topic_id)
                    .with_partitions(assignment.partitions)
            })
            .collect(),
    )
}

fn described_group(
    description: &ShareGroupDescription,
    authorized_operations: i32,
) -> DescribedGroup {
    DescribedGroup::default()
        .with_error_code(NO_ERROR)
        .with_group_id(group_id(&description.group_id))
        .with_group_state(string(&description.state))
        .with_group_epoch(description.group_epoch)
        .with_assignment_epoch(description.assignment_epoch)
        .with_assignor_name(string(&description.assignor_name))
        .with_members(
            description
                .members
                .iter()
                .map(|member| {
                    DescribedMember::default()
                        .with_member_id(string(&member.member_id))
                        .with_rack_id(member.rack_id.as_deref().map(string))
                        .with_member_epoch(member.member_epoch)
                        .with_client_id(string(&member.client_id))
                        .with_client_host(string(&member.client_host))
                        .with_subscribed_topic_names(
                            member
                                .subscribed_topic_names
                                .iter()
                                .map(|topic| topic_name(topic))
                                .collect(),
                        )
                        .with_assignment(describe_assignment(&member.assignment))
                })
                .collect(),
        )
        .with_authorized_operations(authorized_operations)
}

fn describe_assignment(assignments: &[ShareTopicAssignment]) -> DescribeAssignment {
    DescribeAssignment::default().with_topic_partitions(
        assignments
            .iter()
            .map(|assignment| {
                DescribeTopicPartitions::default()
                    .with_topic_id(assignment.topic_id)
                    .with_topic_name(topic_name(&assignment.topic_name))
                    .with_partitions(assignment.partitions.clone())
            })
            .collect(),
    )
}

fn described_group_error(group_id: GroupId, error_code: i16, message: &str) -> DescribedGroup {
    DescribedGroup::default()
        .with_group_id(group_id)
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

fn string(value: impl Into<String>) -> StrBytes {
    StrBytes::from_string(value.into())
}
