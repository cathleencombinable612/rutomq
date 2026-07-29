use super::Broker;
use super::authorization::AuthorizationContext;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, GROUP_SUBSCRIBED_TO_TOPIC, NO_ERROR,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
    control_error_code,
};
use kafka_protocol::messages::delete_records_request::DeleteRecordsTopic;
use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
};
use kafka_protocol::messages::offset_delete_response::{
    OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafka_protocol::messages::{
    DeleteRecordsRequest, DeleteRecordsResponse, OffsetDeleteRequest, OffsetDeleteResponse,
};
use rutomq_control::{AclOperation, AclResourceType, ControlError, PartitionKey};

impl Broker {
    pub(super) async fn handle_delete_records(
        &self,
        request: DeleteRecordsRequest,
        context: &AuthorizationContext,
    ) -> DeleteRecordsResponse {
        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Delete)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(_) => return delete_records_error(request.topics, UNKNOWN_SERVER_ERROR),
        };

        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let name = topic.name.as_str();
            let authorized = authorizations.get(name).copied().unwrap_or(false);
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let (low_watermark, error_code) = if !authorized {
                    (-1, TOPIC_AUTHORIZATION_FAILED)
                } else {
                    match self
                        .metadata
                        .delete_records(
                            &PartitionKey::new(name, partition.partition_index),
                            partition.offset,
                        )
                        .await
                    {
                        Ok(low_watermark) => (low_watermark, NO_ERROR),
                        Err(error) => (-1, control_error_code(&error)),
                    }
                };
                partitions.push(
                    DeleteRecordsPartitionResult::default()
                        .with_partition_index(partition.partition_index)
                        .with_low_watermark(low_watermark)
                        .with_error_code(error_code),
                );
            }
            topics.push(
                DeleteRecordsTopicResult::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }
        DeleteRecordsResponse::default().with_topics(topics)
    }

    pub(super) async fn handle_offset_delete(
        &self,
        request: OffsetDeleteRequest,
        context: &AuthorizationContext,
    ) -> OffsetDeleteResponse {
        let group_id = request.group_id.as_str();
        match self
            .authorized(
                context,
                AclResourceType::Group,
                group_id,
                AclOperation::Delete,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return OffsetDeleteResponse::default().with_error_code(GROUP_AUTHORIZATION_FAILED);
            }
            Err(_) => {
                return OffsetDeleteResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        }

        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Read)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(_) => {
                return OffsetDeleteResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };

        let mut response_topics = Vec::with_capacity(request.topics.len());
        let mut deletable = Vec::new();
        for topic in request.topics {
            let name = topic.name.as_str();
            let authorized = authorizations.get(name).copied().unwrap_or(false);
            let topic_info = if authorized {
                match self.metadata.topic(name).await {
                    Ok(topic) => topic,
                    Err(error) => {
                        return OffsetDeleteResponse::default()
                            .with_error_code(control_error_code(&error));
                    }
                }
            } else {
                None
            };
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let key = PartitionKey::new(name, partition.partition_index);
                let error_code = if !authorized {
                    TOPIC_AUTHORIZATION_FAILED
                } else if topic_info.as_ref().is_none_or(|topic| {
                    partition.partition_index < 0 || partition.partition_index >= topic.partitions
                }) {
                    UNKNOWN_TOPIC_OR_PARTITION
                } else {
                    deletable.push(key);
                    NO_ERROR
                };
                partitions.push(
                    OffsetDeleteResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(error_code),
                );
            }
            response_topics.push(
                OffsetDeleteResponseTopic::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }

        let blocked = match self.metadata.delete_offsets(group_id, &deletable).await {
            Ok(blocked) => blocked,
            Err(ControlError::GroupNotFound(_)) => {
                return OffsetDeleteResponse::default().with_error_code(GROUP_ID_NOT_FOUND);
            }
            Err(error) => {
                return OffsetDeleteResponse::default().with_error_code(control_error_code(&error));
            }
        };
        for topic in &mut response_topics {
            for partition in &mut topic.partitions {
                let key = PartitionKey::new(topic.name.as_str(), partition.partition_index);
                if partition.error_code == NO_ERROR && blocked.contains(&key) {
                    partition.error_code = GROUP_SUBSCRIBED_TO_TOPIC;
                }
            }
        }
        OffsetDeleteResponse::default()
            .with_error_code(NO_ERROR)
            .with_topics(response_topics)
    }
}

fn delete_records_error(topics: Vec<DeleteRecordsTopic>, error_code: i16) -> DeleteRecordsResponse {
    DeleteRecordsResponse::default().with_topics(
        topics
            .into_iter()
            .map(|topic| {
                DeleteRecordsTopicResult::default()
                    .with_name(topic.name)
                    .with_partitions(
                        topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                DeleteRecordsPartitionResult::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_low_watermark(-1)
                                    .with_error_code(error_code)
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}
