use super::authorization::{AuthorizationContext, authorization_failure};
use super::{Broker, topic_name};
use crate::kafka_error::{
    FENCED_INSTANCE_ID, FENCED_MEMBER_EPOCH, GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND,
    GROUP_MAX_SIZE_REACHED, INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_MEMBER_ID, UNKNOWN_SERVER_ERROR, UNRELEASED_INSTANCE_ID, UNSUPPORTED_ASSIGNOR,
    UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::consumer_group_describe_response::{
    Assignment as DescribeAssignment, DescribedGroup, Member as DescribedMember,
    TopicPartitions as DescribedTopicPartitions,
};
use kafka_protocol::messages::consumer_group_heartbeat_response::{
    Assignment as HeartbeatAssignment, TopicPartitions as HeartbeatTopicPartitions,
};
use kafka_protocol::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatResponse, GroupId,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, ConsumerGroupDescription, ConsumerGroupHeartbeat,
    ConsumerOwnedTopicPartitions, ConsumerTopicAssignment, ControlError, GROUP_VERSION_FEATURE,
    GroupHeartbeatOutcome,
};

impl Broker {
    pub(super) async fn handle_consumer_group_heartbeat(
        &self,
        request: ConsumerGroupHeartbeatRequest,
        context: &AuthorizationContext,
        client_id: String,
    ) -> ConsumerGroupHeartbeatResponse {
        match self.metadata.features().await {
            Ok(features) if features.level(GROUP_VERSION_FEATURE) < 1 => {
                return heartbeat_error(
                    &request,
                    UNSUPPORTED_VERSION,
                    "consumer group protocol is disabled by group.version",
                    self.config.group_heartbeat_interval_ms,
                );
            }
            Err(error) => {
                return heartbeat_error(
                    &request,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    self.config.group_heartbeat_interval_ms,
                );
            }
            _ => {}
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
                .unwrap_or("consumer group authorization failed");
            return heartbeat_error(
                &request,
                error_code,
                message,
                self.config.group_heartbeat_interval_ms,
            );
        }
        let group_config = match self.group_runtime_config(&group_id).await {
            Ok(config) => config,
            Err(error) => {
                return heartbeat_error(
                    &request,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    self.config.group_heartbeat_interval_ms,
                );
            }
        };
        let heartbeat = ConsumerGroupHeartbeat {
            group_id,
            member_id: request.member_id.as_str().to_owned(),
            member_epoch: request.member_epoch,
            instance_id: request
                .instance_id
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            rack_id: request
                .rack_id
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            subscribed_topic_names: request.subscribed_topic_names.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| topic.as_str().to_owned())
                    .collect()
            }),
            subscribed_topic_regex: request
                .subscribed_topic_regex
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            server_assignor: request
                .server_assignor
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            configured_assignors: self.config.consumer_group_assignors.clone(),
            owned_partitions: request.topic_partitions.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| ConsumerOwnedTopicPartitions {
                        topic_id: topic.topic_id,
                        partitions: topic.partitions.clone(),
                    })
                    .collect()
            }),
            client_id,
            client_host: context.host.clone(),
            heartbeat_interval_ms: group_config.consumer_heartbeat_interval_ms,
            session_timeout_ms: group_config.consumer_session_timeout_ms,
            regex_refresh_interval_ms: self.config.consumer_group_regex_refresh_interval_ms,
            assignment_interval_ms: group_config.consumer_assignment_interval_ms,
            max_size: self.config.consumer_group_max_size,
        };
        let outcome = if group_config.consumer_assignor_offload_enable {
            self.metadata
                .consumer_group_heartbeat_deferred(heartbeat)
                .await
        } else {
            self.metadata
                .consumer_group_heartbeat(heartbeat)
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
                ConsumerGroupHeartbeatResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_member_id(Some(string(result.member_id)))
                    .with_member_epoch(result.member_epoch)
                    .with_heartbeat_interval_ms(result.heartbeat_interval_ms)
                    .with_assignment(result.assignment.map(heartbeat_assignment))
            }
            Err(error) => {
                let code = consumer_group_error_code(&error);
                heartbeat_error(
                    &request,
                    code,
                    &error.to_string(),
                    group_config.consumer_heartbeat_interval_ms,
                )
            }
        }
    }

    pub(super) async fn handle_consumer_group_describe(
        &self,
        request: ConsumerGroupDescribeRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> ConsumerGroupDescribeResponse {
        match self.metadata.features().await {
            Ok(features) if features.level(GROUP_VERSION_FEATURE) < 1 => {
                return ConsumerGroupDescribeResponse::default().with_groups(
                    request
                        .group_ids
                        .into_iter()
                        .map(|group_id| {
                            described_group_error(
                                group_id,
                                UNSUPPORTED_VERSION,
                                "consumer group protocol is disabled by group.version",
                            )
                        })
                        .collect(),
                );
            }
            Err(error) => {
                return ConsumerGroupDescribeResponse::default().with_groups(
                    request
                        .group_ids
                        .into_iter()
                        .map(|group_id| {
                            described_group_error(
                                group_id,
                                UNKNOWN_SERVER_ERROR,
                                &error.to_string(),
                            )
                        })
                        .collect(),
                );
            }
            _ => {}
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
                    "consumer group authorization failed",
                )),
                Err(error) => groups.push(described_group_error(
                    group_id,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                )),
            }
        }
        let descriptions = self
            .metadata
            .describe_consumer_groups(&authorized_names)
            .await;
        for group_id in authorized_ids {
            let name = group_id.as_str();
            match &descriptions {
                Ok(descriptions) => match descriptions.get(name) {
                    Some(description) => {
                        let topic_names = description
                            .members
                            .iter()
                            .flat_map(|member| {
                                member.assignment.iter().chain(&member.target_assignment)
                            })
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
                                groups.push(described_group(description, operations, version));
                            }
                        }
                    }
                    None => groups.push(described_group_error(
                        group_id,
                        GROUP_ID_NOT_FOUND,
                        "consumer group was not found",
                    )),
                },
                Err(error) => groups.push(described_group_error(
                    group_id,
                    consumer_group_error_code(error),
                    &error.to_string(),
                )),
            }
        }
        ConsumerGroupDescribeResponse::default().with_groups(groups)
    }

    pub(super) async fn group_authorized_operations(
        &self,
        context: &AuthorizationContext,
        group_id: &str,
    ) -> i32 {
        let mut bitfield = 0;
        for operation in [
            AclOperation::Read,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
        ] {
            if self
                .authorized(context, AclResourceType::Group, group_id, operation)
                .await
                .unwrap_or(false)
            {
                bitfield |= 1_i32 << operation as i8;
            }
        }
        bitfield
    }
}

fn heartbeat_error(
    request: &ConsumerGroupHeartbeatRequest,
    error_code: i16,
    message: &str,
    heartbeat_interval_ms: i32,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
        .with_member_id(Some(request.member_id.clone()))
        .with_member_epoch(request.member_epoch)
        .with_heartbeat_interval_ms(heartbeat_interval_ms)
}

fn consumer_group_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::GroupNotFound(_) | ControlError::GroupProtocolMismatch(_) => {
            GROUP_ID_NOT_FOUND
        }
        ControlError::GroupMemberNotFound { .. } => UNKNOWN_MEMBER_ID,
        ControlError::FencedInstanceId { .. } => FENCED_INSTANCE_ID,
        ControlError::FencedMemberEpoch { .. } => FENCED_MEMBER_EPOCH,
        ControlError::UnsupportedConsumerAssignor(_) => UNSUPPORTED_ASSIGNOR,
        ControlError::UnreleasedInstanceId { .. } => UNRELEASED_INSTANCE_ID,
        ControlError::GroupMaxSizeReached(_) => GROUP_MAX_SIZE_REACHED,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn heartbeat_assignment(assignments: Vec<ConsumerTopicAssignment>) -> HeartbeatAssignment {
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
    description: &ConsumerGroupDescription,
    authorized_operations: i32,
    version: i16,
) -> DescribedGroup {
    DescribedGroup::default()
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
                    let response = DescribedMember::default()
                        .with_member_id(string(&member.member_id))
                        .with_instance_id(member.instance_id.as_deref().map(string))
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
                        .with_subscribed_topic_regex(
                            member.subscribed_topic_regex.as_deref().map(string),
                        )
                        .with_assignment(describe_assignment(&member.assignment))
                        .with_target_assignment(describe_assignment(&member.target_assignment));
                    if version >= 1 {
                        response.with_member_type(1)
                    } else {
                        response
                    }
                })
                .collect(),
        )
        .with_authorized_operations(authorized_operations)
}

fn describe_assignment(assignments: &[ConsumerTopicAssignment]) -> DescribeAssignment {
    DescribeAssignment::default().with_topic_partitions(
        assignments
            .iter()
            .map(|assignment| {
                DescribedTopicPartitions::default()
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
