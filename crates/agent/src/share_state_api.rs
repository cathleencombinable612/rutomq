use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, NO_ERROR, UNKNOWN_SERVER_ERROR, control_error_code,
};
use kafka_protocol::messages::delete_share_group_state_response::{
    DeleteStateResult, PartitionResult as DeletePartitionResult,
};
use kafka_protocol::messages::initialize_share_group_state_response::{
    InitializeStateResult, PartitionResult as InitializePartitionResult,
};
use kafka_protocol::messages::read_share_group_state_response::{
    PartitionResult as ReadPartitionResult, ReadStateResult, StateBatch as ReadStateBatch,
};
use kafka_protocol::messages::read_share_group_state_summary_response::{
    PartitionResult as SummaryPartitionResult, ReadStateSummaryResult,
};
use kafka_protocol::messages::write_share_group_state_response::{
    PartitionResult as WritePartitionResult, WriteStateResult,
};
use kafka_protocol::messages::{
    DeleteShareGroupStateRequest, DeleteShareGroupStateResponse, InitializeShareGroupStateRequest,
    InitializeShareGroupStateResponse, ReadShareGroupStateRequest, ReadShareGroupStateResponse,
    ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse,
    WriteShareGroupStateRequest, WriteShareGroupStateResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, ShareStateBatch, ShareStateInitialization,
    ShareStateKey, ShareStateRead, ShareStateWrite,
};

impl Broker {
    pub(super) async fn handle_initialize_share_group_state(
        &self,
        request: InitializeShareGroupStateRequest,
        context: &AuthorizationContext,
    ) -> InitializeShareGroupStateResponse {
        if let Some(error) = self.share_state_authorization_error(context).await {
            return initialize_global_error(&request, error);
        }
        if invalid_initialize_envelope(&request) {
            return InitializeShareGroupStateResponse::default();
        }

        let group_id = request.group_id.as_str().to_owned();
        let mut results = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let result = self
                    .metadata
                    .initialize_share_group_state(ShareStateInitialization {
                        key: state_key(&group_id, topic.topic_id, partition.partition),
                        state_epoch: partition.state_epoch,
                        start_offset: partition.start_offset,
                    })
                    .await;
                partitions.push(initialize_result(partition.partition, result));
            }
            results.push(
                InitializeStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        InitializeShareGroupStateResponse::default().with_results(results)
    }

    pub(super) async fn handle_read_share_group_state(
        &self,
        request: ReadShareGroupStateRequest,
        context: &AuthorizationContext,
    ) -> ReadShareGroupStateResponse {
        if let Some(error) = self.share_state_authorization_error(context).await {
            return read_global_error(&request, error);
        }
        if invalid_read_envelope(&request) {
            return ReadShareGroupStateResponse::default();
        }

        let group_id = request.group_id.as_str().to_owned();
        let mut results = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let result = self
                    .metadata
                    .read_share_group_state(ShareStateRead {
                        key: state_key(&group_id, topic.topic_id, partition.partition),
                        leader_epoch: partition.leader_epoch,
                    })
                    .await;
                partitions.push(match result {
                    Ok(snapshot) => ReadPartitionResult::default()
                        .with_partition(partition.partition)
                        .with_error_code(NO_ERROR)
                        .with_error_message(None)
                        .with_state_epoch(snapshot.state_epoch)
                        .with_start_offset(snapshot.start_offset)
                        .with_state_batches(
                            snapshot
                                .state_batches
                                .into_iter()
                                .map(|batch| {
                                    ReadStateBatch::default()
                                        .with_first_offset(batch.first_offset)
                                        .with_last_offset(batch.last_offset)
                                        .with_delivery_state(batch.delivery_state)
                                        .with_delivery_count(batch.delivery_count)
                                })
                                .collect(),
                        ),
                    Err(error) => read_partition_error(partition.partition, &error),
                });
            }
            results.push(
                ReadStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        ReadShareGroupStateResponse::default().with_results(results)
    }

    pub(super) async fn handle_write_share_group_state(
        &self,
        request: WriteShareGroupStateRequest,
        context: &AuthorizationContext,
    ) -> WriteShareGroupStateResponse {
        if let Some(error) = self.share_state_authorization_error(context).await {
            return write_global_error(&request, error);
        }
        if invalid_write_envelope(&request) {
            return WriteShareGroupStateResponse::default();
        }

        let group_id = request.group_id.as_str().to_owned();
        let mut results = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let result = self
                    .metadata
                    .write_share_group_state(ShareStateWrite {
                        key: state_key(&group_id, topic.topic_id, partition.partition),
                        state_epoch: partition.state_epoch,
                        leader_epoch: partition.leader_epoch,
                        start_offset: partition.start_offset,
                        delivery_complete_count: partition.delivery_complete_count,
                        state_batches: partition
                            .state_batches
                            .into_iter()
                            .map(|batch| ShareStateBatch {
                                first_offset: batch.first_offset,
                                last_offset: batch.last_offset,
                                delivery_state: batch.delivery_state,
                                delivery_count: batch.delivery_count,
                            })
                            .collect(),
                    })
                    .await;
                partitions.push(write_result(partition.partition, result));
            }
            results.push(
                WriteStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        WriteShareGroupStateResponse::default().with_results(results)
    }

    pub(super) async fn handle_delete_share_group_state(
        &self,
        request: DeleteShareGroupStateRequest,
        context: &AuthorizationContext,
    ) -> DeleteShareGroupStateResponse {
        if let Some(error) = self.share_state_authorization_error(context).await {
            return delete_global_error(&request, error);
        }
        if invalid_delete_envelope(&request) {
            return DeleteShareGroupStateResponse::default();
        }

        let group_id = request.group_id.as_str().to_owned();
        let mut results = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let result = self
                    .metadata
                    .delete_share_group_state(&state_key(
                        &group_id,
                        topic.topic_id,
                        partition.partition,
                    ))
                    .await;
                partitions.push(delete_result(partition.partition, result));
            }
            results.push(
                DeleteStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        DeleteShareGroupStateResponse::default().with_results(results)
    }

    pub(super) async fn handle_read_share_group_state_summary(
        &self,
        request: ReadShareGroupStateSummaryRequest,
        context: &AuthorizationContext,
    ) -> ReadShareGroupStateSummaryResponse {
        if let Some(error) = self.share_state_authorization_error(context).await {
            return summary_global_error(&request, error);
        }
        if invalid_summary_envelope(&request) {
            return ReadShareGroupStateSummaryResponse::default();
        }

        let group_id = request.group_id.as_str().to_owned();
        let mut results = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let result = self
                    .metadata
                    .summarize_share_group_state(&state_key(
                        &group_id,
                        topic.topic_id,
                        partition.partition,
                    ))
                    .await;
                partitions.push(match result {
                    Ok(Some(summary)) => SummaryPartitionResult::default()
                        .with_partition(partition.partition)
                        .with_error_code(NO_ERROR)
                        .with_error_message(None)
                        .with_state_epoch(summary.state_epoch)
                        .with_leader_epoch(summary.leader_epoch)
                        .with_start_offset(summary.start_offset)
                        .with_delivery_complete_count(summary.delivery_complete_count),
                    Ok(None) => SummaryPartitionResult::default()
                        .with_partition(partition.partition)
                        .with_error_code(NO_ERROR)
                        .with_error_message(None)
                        .with_state_epoch(0)
                        .with_leader_epoch(0)
                        .with_start_offset(-1)
                        .with_delivery_complete_count(-1),
                    Err(error) => summary_partition_error(partition.partition, &error),
                });
            }
            results.push(
                ReadStateSummaryResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        ReadShareGroupStateSummaryResponse::default().with_results(results)
    }

    async fn share_state_authorization_error(&self, context: &AuthorizationContext) -> Option<i16> {
        match self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::ClusterAction,
            )
            .await
        {
            Ok(true) => None,
            Ok(false) => Some(CLUSTER_AUTHORIZATION_FAILED),
            Err(_) => Some(UNKNOWN_SERVER_ERROR),
        }
    }
}

fn state_key(group_id: &str, topic_id: uuid::Uuid, partition: i32) -> ShareStateKey {
    ShareStateKey {
        group_id: group_id.to_owned(),
        topic_id,
        partition,
    }
}

fn initialize_result(
    partition: i32,
    result: Result<(), ControlError>,
) -> InitializePartitionResult {
    match result {
        Ok(()) => InitializePartitionResult::default()
            .with_partition(partition)
            .with_error_code(NO_ERROR)
            .with_error_message(None),
        Err(error) => InitializePartitionResult::default()
            .with_partition(partition)
            .with_error_code(control_error_code(&error))
            .with_error_message(control_error_message(&error)),
    }
}

fn write_result(partition: i32, result: Result<(), ControlError>) -> WritePartitionResult {
    match result {
        Ok(()) => WritePartitionResult::default()
            .with_partition(partition)
            .with_error_code(NO_ERROR)
            .with_error_message(None),
        Err(error) => WritePartitionResult::default()
            .with_partition(partition)
            .with_error_code(control_error_code(&error))
            .with_error_message(control_error_message(&error)),
    }
}

fn delete_result(partition: i32, result: Result<(), ControlError>) -> DeletePartitionResult {
    match result {
        Ok(()) => DeletePartitionResult::default()
            .with_partition(partition)
            .with_error_code(NO_ERROR)
            .with_error_message(None),
        Err(error) => DeletePartitionResult::default()
            .with_partition(partition)
            .with_error_code(control_error_code(&error))
            .with_error_message(control_error_message(&error)),
    }
}

fn read_partition_error(partition: i32, error: &ControlError) -> ReadPartitionResult {
    ReadPartitionResult::default()
        .with_partition(partition)
        .with_error_code(control_error_code(error))
        .with_error_message(control_error_message(error))
}

fn summary_partition_error(partition: i32, error: &ControlError) -> SummaryPartitionResult {
    SummaryPartitionResult::default()
        .with_partition(partition)
        .with_error_code(control_error_code(error))
        .with_error_message(control_error_message(error))
}

fn control_error_message(error: &ControlError) -> Option<StrBytes> {
    Some(StrBytes::from_string(error.to_string()))
}

fn authorization_message(error: i16) -> Option<StrBytes> {
    Some(StrBytes::from_static_str(
        if error == CLUSTER_AUTHORIZATION_FAILED {
            "cluster authorization failed"
        } else {
            "authorization backend failure"
        },
    ))
}

fn initialize_global_error(
    request: &InitializeShareGroupStateRequest,
    error: i16,
) -> InitializeShareGroupStateResponse {
    InitializeShareGroupStateResponse::default().with_results(
        request
            .topics
            .iter()
            .map(|topic| {
                InitializeStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                InitializePartitionResult::default()
                                    .with_partition(partition.partition)
                                    .with_error_code(error)
                                    .with_error_message(authorization_message(error))
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn read_global_error(
    request: &ReadShareGroupStateRequest,
    error: i16,
) -> ReadShareGroupStateResponse {
    ReadShareGroupStateResponse::default().with_results(
        request
            .topics
            .iter()
            .map(|topic| {
                ReadStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                ReadPartitionResult::default()
                                    .with_partition(partition.partition)
                                    .with_error_code(error)
                                    .with_error_message(authorization_message(error))
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn write_global_error(
    request: &WriteShareGroupStateRequest,
    error: i16,
) -> WriteShareGroupStateResponse {
    WriteShareGroupStateResponse::default().with_results(
        request
            .topics
            .iter()
            .map(|topic| {
                WriteStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                WritePartitionResult::default()
                                    .with_partition(partition.partition)
                                    .with_error_code(error)
                                    .with_error_message(authorization_message(error))
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn delete_global_error(
    request: &DeleteShareGroupStateRequest,
    error: i16,
) -> DeleteShareGroupStateResponse {
    DeleteShareGroupStateResponse::default().with_results(
        request
            .topics
            .iter()
            .map(|topic| {
                DeleteStateResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                DeletePartitionResult::default()
                                    .with_partition(partition.partition)
                                    .with_error_code(error)
                                    .with_error_message(authorization_message(error))
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn summary_global_error(
    request: &ReadShareGroupStateSummaryRequest,
    error: i16,
) -> ReadShareGroupStateSummaryResponse {
    ReadShareGroupStateSummaryResponse::default().with_results(
        request
            .topics
            .iter()
            .map(|topic| {
                ReadStateSummaryResult::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                SummaryPartitionResult::default()
                                    .with_partition(partition.partition)
                                    .with_error_code(error)
                                    .with_error_message(authorization_message(error))
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn invalid_initialize_envelope(request: &InitializeShareGroupStateRequest) -> bool {
    request.group_id.as_str().is_empty()
        || request.topics.is_empty()
        || request
            .topics
            .iter()
            .any(|topic| topic.partitions.is_empty())
}

fn invalid_read_envelope(request: &ReadShareGroupStateRequest) -> bool {
    request.group_id.as_str().is_empty()
        || request.topics.is_empty()
        || request
            .topics
            .iter()
            .any(|topic| topic.partitions.is_empty())
}

fn invalid_write_envelope(request: &WriteShareGroupStateRequest) -> bool {
    request.group_id.as_str().is_empty()
        || request.topics.is_empty()
        || request
            .topics
            .iter()
            .any(|topic| topic.partitions.is_empty())
}

fn invalid_delete_envelope(request: &DeleteShareGroupStateRequest) -> bool {
    request.group_id.as_str().is_empty()
        || request.topics.is_empty()
        || request
            .topics
            .iter()
            .any(|topic| topic.partitions.is_empty())
}

fn invalid_summary_envelope(request: &ReadShareGroupStateSummaryRequest) -> bool {
    request.group_id.as_str().is_empty()
        || request.topics.is_empty()
        || request
            .topics
            .iter()
            .any(|topic| topic.partitions.is_empty())
}
