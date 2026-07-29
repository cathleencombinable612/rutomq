use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use super::streams_group_protocol::{
    described_group, described_group_error, endpoint_partitions, heartbeat_error, heartbeat_status,
    heartbeat_tasks, optional_string, owned_assignment, streams_group_error_code, string,
    task_offsets_from_request, topology_from_request, topology_topic_names,
};
use super::streams_internal_topics::StreamsInternalTopicPreparation;
use super::streams_topology_validation;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, INVALID_REQUEST, NO_ERROR,
    STREAMS_INVALID_TOPOLOGY, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
    UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::{
    StreamsGroupDescribeRequest, StreamsGroupDescribeResponse, StreamsGroupHeartbeatRequest,
    StreamsGroupHeartbeatResponse,
};
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, GroupHeartbeatOutcome, STREAMS_VERSION_FEATURE,
    StreamsEndpoint, StreamsGroupHeartbeat, StreamsKeyValue, StreamsTopology, TopicInfo,
    streams_topology_topic_names,
};

impl Broker {
    pub(super) async fn handle_streams_group_heartbeat(
        &self,
        request: StreamsGroupHeartbeatRequest,
        context: &AuthorizationContext,
        client_id: String,
    ) -> StreamsGroupHeartbeatResponse {
        if let Some(response) = self.streams_feature_error(&request).await {
            return response;
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
                .unwrap_or("streams group authorization failed");
            return heartbeat_error(
                &request,
                error_code,
                message,
                self.config.streams_group_heartbeat_interval_ms,
            );
        }
        if let Some(topology) = request.topology.as_ref()
            && let Err(message) = streams_topology_validation::validate(topology)
        {
            return StreamsGroupHeartbeatResponse::default()
                .with_error_code(STREAMS_INVALID_TOPOLOGY)
                .with_error_message(Some(string(message)));
        }
        let group_config = match self.group_runtime_config(&group_id).await {
            Ok(config) => config,
            Err(error) => {
                return heartbeat_error(
                    &request,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    self.config.streams_group_heartbeat_interval_ms,
                );
            }
        };
        let topology = match self
            .streams_topology_context(&group_id, request.topology.as_ref())
            .await
        {
            Ok(topology) => topology,
            Err(error) => {
                return heartbeat_error(
                    &request,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    group_config.streams_heartbeat_interval_ms,
                );
            }
        };
        let preparation = if let Some((topology, topics)) = topology.as_ref() {
            match self
                .streams_topics_authorized(context, topology, topics)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return heartbeat_error(
                        &request,
                        TOPIC_AUTHORIZATION_FAILED,
                        "streams topology topic authorization failed",
                        group_config.streams_heartbeat_interval_ms,
                    );
                }
                Err(error) => {
                    return heartbeat_error(
                        &request,
                        UNKNOWN_SERVER_ERROR,
                        &error.to_string(),
                        group_config.streams_heartbeat_interval_ms,
                    );
                }
            }
            self.prepare_streams_internal_topics(context, topology, topics)
                .await
        } else {
            StreamsInternalTopicPreparation::default()
        };

        let owned_assignment = match owned_assignment(&request) {
            Ok(assignment) => assignment,
            Err(message) => {
                return heartbeat_error(
                    &request,
                    INVALID_REQUEST,
                    message,
                    group_config.streams_heartbeat_interval_ms,
                );
            }
        };
        let heartbeat = StreamsGroupHeartbeat {
            group_id,
            member_id: request.member_id.as_str().to_owned(),
            member_epoch: request.member_epoch,
            endpoint_information_epoch: request.endpoint_information_epoch,
            instance_id: optional_string(request.instance_id.as_ref()),
            rack_id: optional_string(request.rack_id.as_ref()),
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            topology: request.topology.as_ref().map(topology_from_request),
            owned_assignment,
            process_id: optional_string(request.process_id.as_ref()),
            user_endpoint: request
                .user_endpoint
                .as_ref()
                .map(|endpoint| StreamsEndpoint {
                    host: endpoint.host.as_str().to_owned(),
                    port: endpoint.port,
                }),
            client_tags: request.client_tags.as_ref().map(|tags| {
                tags.iter()
                    .map(|tag| StreamsKeyValue {
                        key: tag.key.as_str().to_owned(),
                        value: tag.value.as_str().to_owned(),
                    })
                    .collect()
            }),
            task_offsets: request
                .task_offsets
                .as_ref()
                .map(|offsets| task_offsets_from_request(offsets)),
            task_end_offsets: request
                .task_end_offsets
                .as_ref()
                .map(|offsets| task_offsets_from_request(offsets)),
            shutdown_application: request.shutdown_application,
            client_id,
            client_host: context.host.clone(),
            heartbeat_interval_ms: group_config.streams_heartbeat_interval_ms,
            session_timeout_ms: group_config.streams_session_timeout_ms,
            max_size: self.config.streams_group_max_size,
            assignment_interval_ms: group_config.streams_assignment_interval_ms,
            num_standby_replicas: group_config.streams_num_standby_replicas,
            initial_rebalance_delay_ms: group_config.streams_initial_rebalance_delay_ms,
            acceptable_recovery_lag: self.config.streams_acceptable_recovery_lag,
            task_offset_interval_ms: self.config.streams_task_offset_interval_ms,
        };
        let outcome = if group_config.streams_assignor_offload_enable {
            self.metadata
                .streams_group_heartbeat_deferred(heartbeat)
                .await
        } else {
            self.metadata
                .streams_group_heartbeat(heartbeat)
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
                let mut result = outcome.result;
                preparation.decorate(&mut result.statuses);
                let mut response = StreamsGroupHeartbeatResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_member_id(string(result.member_id))
                    .with_member_epoch(result.member_epoch)
                    .with_heartbeat_interval_ms(result.heartbeat_interval_ms)
                    .with_acceptable_recovery_lag(result.acceptable_recovery_lag)
                    .with_task_offset_interval_ms(result.task_offset_interval_ms)
                    .with_status(Some(result.statuses.iter().map(heartbeat_status).collect()))
                    .with_endpoint_information_epoch(result.endpoint_information_epoch)
                    .with_partitions_by_user_endpoint(
                        result
                            .partitions_by_user_endpoint
                            .as_deref()
                            .map(endpoint_partitions),
                    );
                if let Some(assignment) = result.assignment {
                    response = response
                        .with_active_tasks(Some(heartbeat_tasks(&assignment.active_tasks)))
                        .with_standby_tasks(Some(heartbeat_tasks(&assignment.standby_tasks)))
                        .with_warmup_tasks(Some(heartbeat_tasks(&assignment.warmup_tasks)));
                }
                response
            }
            Err(error) => heartbeat_error(
                &request,
                streams_group_error_code(&error),
                &error.to_string(),
                group_config.streams_heartbeat_interval_ms,
            ),
        }
    }

    pub(super) async fn handle_streams_group_describe(
        &self,
        request: StreamsGroupDescribeRequest,
        context: &AuthorizationContext,
    ) -> StreamsGroupDescribeResponse {
        let feature_error = match self.metadata.features().await {
            Ok(features) if features.level(STREAMS_VERSION_FEATURE) < 1 => Some((
                UNSUPPORTED_VERSION,
                "streams group protocol is disabled by streams.version".to_owned(),
            )),
            Err(error) => Some((UNKNOWN_SERVER_ERROR, error.to_string())),
            _ => None,
        };
        if let Some((error_code, message)) = feature_error {
            return StreamsGroupDescribeResponse::default().with_groups(
                request
                    .group_ids
                    .into_iter()
                    .map(|group_id| described_group_error(group_id, error_code, &message))
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
                    "streams group authorization failed",
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
            .describe_streams_groups(&authorized_names)
            .await;
        for group_id in authorized_ids {
            let name = group_id.as_str();
            match &descriptions {
                Ok(descriptions) => match descriptions.get(name) {
                    Some(description) => {
                        let topic_names = topology_topic_names(&description.topology);
                        let topic_names =
                            topic_names.iter().map(String::as_str).collect::<Vec<_>>();
                        let topic_access =
                            self.topic_names_describable(context, &topic_names).await;
                        match topic_access {
                            Ok(false) => groups.push(described_group_error(
                                group_id,
                                TOPIC_AUTHORIZATION_FAILED,
                                "The described group uses topics that the client is not authorized to describe.",
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
                        "streams group was not found",
                    )),
                },
                Err(error) => groups.push(described_group_error(
                    group_id,
                    streams_group_error_code(error),
                    &error.to_string(),
                )),
            }
        }
        StreamsGroupDescribeResponse::default().with_groups(groups)
    }

    async fn streams_feature_error(
        &self,
        request: &StreamsGroupHeartbeatRequest,
    ) -> Option<StreamsGroupHeartbeatResponse> {
        match self.metadata.features().await {
            Ok(features) if features.level(STREAMS_VERSION_FEATURE) < 1 => Some(heartbeat_error(
                request,
                UNSUPPORTED_VERSION,
                "streams group protocol is disabled by streams.version",
                self.config.streams_group_heartbeat_interval_ms,
            )),
            Err(error) => Some(heartbeat_error(
                request,
                UNKNOWN_SERVER_ERROR,
                &error.to_string(),
                self.config.streams_group_heartbeat_interval_ms,
            )),
            _ => None,
        }
    }

    async fn streams_topology_context(
        &self,
        group_id: &str,
        request_topology: Option<
            &kafka_protocol::messages::streams_group_heartbeat_request::Topology,
        >,
    ) -> Result<Option<(StreamsTopology, Vec<TopicInfo>)>, ControlError> {
        let topology = if let Some(topology) = request_topology {
            topology_from_request(topology)
        } else {
            let descriptions = self
                .metadata
                .describe_streams_groups(&[group_id.to_owned()])
                .await?;
            let Some(description) = descriptions.get(group_id) else {
                return Ok(None);
            };
            description.topology.clone()
        };
        let topics = self.metadata.topics(None).await?;
        Ok(Some((topology, topics)))
    }

    async fn streams_topics_authorized(
        &self,
        context: &AuthorizationContext,
        topology: &StreamsTopology,
        topics: &[TopicInfo],
    ) -> anyhow::Result<bool> {
        let topic_names = streams_topology_topic_names(topology, topics)?;
        let topic_names = topic_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.topic_names_describable(context, &topic_names).await
    }
}
