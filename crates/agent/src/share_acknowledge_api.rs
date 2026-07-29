use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use super::partition_state_api::VIRTUAL_LEADER_EPOCH;
use super::share_api::{error_code, identity, string};
use super::share_protocol::{
    RENEW_DISABLED_MESSAGE, has_renew_acknowledgement, validate_acknowledgement_types,
};
use super::share_topic_authorization::ShareTopicAccess;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, INVALID_RECORD_STATE, INVALID_REQUEST, NO_ERROR,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID,
};
use kafka_protocol::messages::share_acknowledge_request::AcknowledgementBatch;
use kafka_protocol::messages::share_acknowledge_response::{
    LeaderIdAndEpoch, PartitionData, ShareAcknowledgeTopicResponse,
};
use kafka_protocol::messages::{ShareAcknowledgeRequest, ShareAcknowledgeResponse};
use rutomq_control::{
    AclOperation, AclResourceType, ShareAcknowledgeRecords,
    ShareAcknowledgementBatch as ControlAcknowledgementBatch, ShareFetchSessionUpdate,
};

impl Broker {
    pub(super) async fn handle_share_acknowledge(
        &self,
        request: ShareAcknowledgeRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> ShareAcknowledgeResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return top_error(code, &message, self.config.share_record_lock_duration_ms);
        }
        let identity = match identity(&request.group_id, &request.member_id) {
            Ok(identity) => identity,
            Err(message) => {
                return top_error(
                    INVALID_REQUEST,
                    message,
                    self.config.share_record_lock_duration_ms,
                );
            }
        };
        if let Some((error_code, backend_message)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                &identity.group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return top_error(
                error_code,
                backend_message
                    .as_deref()
                    .unwrap_or("share group authorization failed"),
                self.config.share_record_lock_duration_ms,
            );
        }
        let group_config = match self.group_runtime_config(&identity.group_id).await {
            Ok(config) => config,
            Err(error) => {
                return top_error(
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    self.config.share_record_lock_duration_ms,
                );
            }
        };
        if let Err(error) = self
            .metadata
            .update_share_fetch_session(ShareFetchSessionUpdate {
                group_id: identity.group_id.clone(),
                member_id: identity.member_id.clone(),
                session_epoch: request.share_session_epoch,
                added: Vec::new(),
                forgotten: Vec::new(),
            })
            .await
        {
            return top_error(
                error_code(&error),
                &error.to_string(),
                group_config.share_record_lock_duration_ms,
            );
        }

        let topic_accesses = match self
            .share_topic_accesses(context, request.topics.iter().map(|topic| topic.topic_id))
            .await
        {
            Ok(accesses) => accesses,
            Err(error) => {
                return top_error(
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    group_config.share_record_lock_duration_ms,
                );
            }
        };
        let is_renew_ack = request.is_renew_ack;
        let mut responses = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let access = topic_accesses.get(&topic.topic_id);
            match access {
                Some(ShareTopicAccess::Missing) => {
                    responses.push(topic_error(
                        topic.topic_id,
                        &topic.partitions,
                        UNKNOWN_TOPIC_ID,
                    ));
                    continue;
                }
                Some(ShareTopicAccess::MetadataError(_)) | None => {
                    responses.push(topic_error(
                        topic.topic_id,
                        &topic.partitions,
                        UNKNOWN_SERVER_ERROR,
                    ));
                    continue;
                }
                Some(ShareTopicAccess::Allowed(_) | ShareTopicAccess::Denied) => {}
            }
            let authorized = matches!(access, Some(ShareTopicAccess::Allowed(_)));
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let validation = validate_acknowledgement_types(
                    version,
                    is_renew_ack,
                    partition
                        .acknowledgement_batches
                        .iter()
                        .map(|batch| batch.acknowledge_types.as_slice()),
                );
                let renew_disabled = !group_config.share_renew_acknowledge_enable
                    && has_renew_acknowledgement(
                        partition
                            .acknowledgement_batches
                            .iter()
                            .map(|batch| batch.acknowledge_types.as_slice()),
                    );
                let (error, message) = if let Err(message) = validation {
                    (INVALID_REQUEST, Some(message.to_owned()))
                } else if renew_disabled {
                    (
                        INVALID_RECORD_STATE,
                        Some(RENEW_DISABLED_MESSAGE.to_owned()),
                    )
                } else if !authorized {
                    (
                        TOPIC_AUTHORIZATION_FAILED,
                        Some("topic authorization failed".to_owned()),
                    )
                } else {
                    let result = self
                        .metadata
                        .acknowledge_share_records(ShareAcknowledgeRecords {
                            group_id: identity.group_id.clone(),
                            member_id: identity.member_id.clone(),
                            topic_id: topic.topic_id,
                            partition: partition.partition_index,
                            batches: acknowledgement_batches(&partition.acknowledgement_batches),
                            lock_duration_ms: group_config.share_record_lock_duration_ms,
                            delivery_count_limit: group_config.share_delivery_count_limit,
                        })
                        .await;
                    match result {
                        Ok(()) => (NO_ERROR, None),
                        Err(error) => (error_code(&error), Some(error.to_string())),
                    }
                };
                partitions.push(partition_response(
                    partition.partition_index,
                    error,
                    message.as_deref(),
                ));
            }
            responses.push(
                ShareAcknowledgeTopicResponse::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        ShareAcknowledgeResponse::default()
            .with_error_code(NO_ERROR)
            .with_acquisition_lock_timeout_ms(group_config.share_record_lock_duration_ms)
            .with_responses(responses)
    }
}

fn acknowledgement_batches(batches: &[AcknowledgementBatch]) -> Vec<ControlAcknowledgementBatch> {
    batches
        .iter()
        .map(|batch| ControlAcknowledgementBatch {
            first_offset: batch.first_offset,
            last_offset: batch.last_offset,
            types: batch.acknowledge_types.clone(),
        })
        .collect()
}

fn topic_error(
    topic_id: uuid::Uuid,
    partitions: &[kafka_protocol::messages::share_acknowledge_request::AcknowledgePartition],
    error: i16,
) -> ShareAcknowledgeTopicResponse {
    ShareAcknowledgeTopicResponse::default()
        .with_topic_id(topic_id)
        .with_partitions(
            partitions
                .iter()
                .map(|partition| partition_response(partition.partition_index, error, None))
                .collect(),
        )
}

fn partition_response(partition: i32, error: i16, message: Option<&str>) -> PartitionData {
    let mut response = PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(error)
        .with_current_leader(
            LeaderIdAndEpoch::default()
                .with_leader_id(0)
                .with_leader_epoch(VIRTUAL_LEADER_EPOCH),
        );
    if error != NO_ERROR {
        response = response.with_error_message(Some(string(
            message.unwrap_or("share acknowledgement failed"),
        )));
    }
    response
}

fn top_error(error: i16, message: &str, lock_duration_ms: i32) -> ShareAcknowledgeResponse {
    ShareAcknowledgeResponse::default()
        .with_error_code(error)
        .with_error_message(Some(string(message)))
        .with_acquisition_lock_timeout_ms(lock_duration_ms)
}
