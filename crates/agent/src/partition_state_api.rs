use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    FENCED_LEADER_EPOCH, INVALID_TOPIC_EXCEPTION, NO_ERROR, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_LEADER_EPOCH, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use kafka_protocol::messages::describe_producers_response::{
    PartitionResponse, ProducerState, TopicResponse,
};
use kafka_protocol::messages::offset_for_leader_epoch_request::OffsetForLeaderTopic;
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderTopicResult,
};
use kafka_protocol::messages::{
    DescribeProducersRequest, DescribeProducersResponse, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, ActiveProducer, PartitionKey, TopicInfo};
use std::collections::HashMap;

pub(crate) const VIRTUAL_LEADER_EPOCH: i32 = 0;

impl Broker {
    pub(super) async fn handle_offset_for_leader_epoch(
        &self,
        request: OffsetForLeaderEpochRequest,
        context: &AuthorizationContext,
    ) -> OffsetForLeaderEpochResponse {
        let cluster_authorized = match self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::ClusterAction,
            )
            .await
        {
            Ok(authorized) => authorized,
            Err(_) => return offset_for_leader_error(request.topics, UNKNOWN_SERVER_ERROR),
        };

        let mut topic_authorization = HashMap::new();
        if !cluster_authorized {
            for topic in &request.topics {
                let name = topic.topic.as_str();
                if topic_authorization.contains_key(name) {
                    continue;
                }
                let authorized = match self
                    .authorized(
                        context,
                        AclResourceType::Topic,
                        name,
                        AclOperation::Describe,
                    )
                    .await
                {
                    Ok(authorized) => authorized,
                    Err(_) => {
                        return offset_for_leader_error(request.topics, UNKNOWN_SERVER_ERROR);
                    }
                };
                topic_authorization.insert(name.to_owned(), authorized);
            }
        }

        let mut authorized = Vec::with_capacity(request.topics.len());
        let mut denied = Vec::new();
        for topic in request.topics {
            if cluster_authorized
                || topic_authorization
                    .get(topic.topic.as_str())
                    .copied()
                    .unwrap_or(false)
            {
                authorized.push(topic);
            } else {
                denied.push(offset_for_leader_topic_error(
                    topic,
                    TOPIC_AUTHORIZATION_FAILED,
                ));
            }
        }

        let mut topics = Vec::with_capacity(authorized.len() + denied.len());
        for topic in authorized {
            topics.push(self.offset_for_leader_topic(topic).await);
        }
        topics.extend(denied);
        OffsetForLeaderEpochResponse::default().with_topics(topics)
    }

    async fn offset_for_leader_topic(
        &self,
        topic: OffsetForLeaderTopic,
    ) -> OffsetForLeaderTopicResult {
        let name = topic.topic.as_str().to_owned();
        let stored = self.metadata.topic(&name).await;
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let response = EpochEndOffset::default().with_partition(partition.partition);
            let info = match &stored {
                Ok(Some(info))
                    if partition.partition >= 0 && partition.partition < info.partitions =>
                {
                    info
                }
                Ok(_) => {
                    partitions.push(response.with_error_code(UNKNOWN_TOPIC_OR_PARTITION));
                    continue;
                }
                Err(_) => {
                    partitions.push(response.with_error_code(UNKNOWN_SERVER_ERROR));
                    continue;
                }
            };
            let error = current_epoch_error(partition.current_leader_epoch);
            let response = if error != NO_ERROR {
                response.with_error_code(error)
            } else if partition.leader_epoch < 0 {
                response
            } else {
                match self
                    .metadata
                    .list_offset(&PartitionKey::new(&info.name, partition.partition), -1)
                    .await
                {
                    Ok(end_offset) => response
                        .with_leader_epoch(VIRTUAL_LEADER_EPOCH)
                        .with_end_offset(end_offset),
                    Err(error) => response.with_error_code(control_error_code(&error)),
                }
            };
            partitions.push(response);
        }
        OffsetForLeaderTopicResult::default()
            .with_topic(topic.topic)
            .with_partitions(partitions)
    }

    pub(super) async fn handle_describe_producers(
        &self,
        request: DescribeProducersRequest,
        context: &AuthorizationContext,
    ) -> DescribeProducersResponse {
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let name = topic.name.as_str();
            let (topic_error, info) = self.describe_producers_topic(name, context).await;
            let mut partitions = Vec::with_capacity(topic.partition_indexes.len());
            for partition in topic.partition_indexes {
                partitions.push(
                    self.describe_producer_partition(name, partition, topic_error, info.as_ref())
                        .await,
                );
            }
            topics.push(
                TopicResponse::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }
        DescribeProducersResponse::default().with_topics(topics)
    }

    async fn describe_producers_topic(
        &self,
        topic: &str,
        context: &AuthorizationContext,
    ) -> (i16, Option<TopicInfo>) {
        if !valid_topic_name(topic) {
            return (INVALID_TOPIC_EXCEPTION, None);
        }
        match self
            .authorized(context, AclResourceType::Topic, topic, AclOperation::Read)
            .await
        {
            Ok(true) => match self.metadata.topic(topic).await {
                Ok(Some(info)) => (NO_ERROR, Some(info)),
                Ok(None) => (UNKNOWN_TOPIC_OR_PARTITION, None),
                Err(_) => (UNKNOWN_SERVER_ERROR, None),
            },
            Ok(false) => (TOPIC_AUTHORIZATION_FAILED, None),
            Err(_) => (UNKNOWN_SERVER_ERROR, None),
        }
    }

    async fn describe_producer_partition(
        &self,
        topic: &str,
        partition: i32,
        topic_error: i16,
        info: Option<&TopicInfo>,
    ) -> PartitionResponse {
        let mut response = PartitionResponse::default().with_partition_index(partition);
        let error = if topic_error != NO_ERROR {
            topic_error
        } else if info.is_none_or(|info| partition < 0 || partition >= info.partitions) {
            UNKNOWN_TOPIC_OR_PARTITION
        } else {
            match self
                .metadata
                .describe_producers(&PartitionKey::new(topic, partition))
                .await
            {
                Ok(producers) => {
                    return response.with_active_producers(
                        producers.into_iter().map(producer_state).collect(),
                    );
                }
                Err(error) => control_error_code(&error),
            }
        };
        response.error_code = error;
        response.error_message = Some(StrBytes::from_string(error_message(error).to_owned()));
        response
    }
}

fn offset_for_leader_error(
    topics: Vec<OffsetForLeaderTopic>,
    error_code: i16,
) -> OffsetForLeaderEpochResponse {
    OffsetForLeaderEpochResponse::default().with_topics(
        topics
            .into_iter()
            .map(|topic| offset_for_leader_topic_error(topic, error_code))
            .collect(),
    )
}

fn offset_for_leader_topic_error(
    topic: OffsetForLeaderTopic,
    error_code: i16,
) -> OffsetForLeaderTopicResult {
    OffsetForLeaderTopicResult::default()
        .with_topic(topic.topic)
        .with_partitions(
            topic
                .partitions
                .into_iter()
                .map(|partition| {
                    EpochEndOffset::default()
                        .with_partition(partition.partition)
                        .with_error_code(error_code)
                })
                .collect(),
        )
}

fn producer_state(producer: ActiveProducer) -> ProducerState {
    ProducerState::default()
        .with_producer_id(producer.producer_id.into())
        .with_producer_epoch(i32::from(producer.producer_epoch))
        .with_last_sequence(producer.last_sequence)
        .with_last_timestamp(producer.last_timestamp)
        .with_coordinator_epoch(-1)
        .with_current_txn_start_offset(producer.current_transaction_start_offset)
}

pub(super) fn current_epoch_error(epoch: i32) -> i16 {
    match epoch {
        -1 | VIRTUAL_LEADER_EPOCH => NO_ERROR,
        epoch if epoch < VIRTUAL_LEADER_EPOCH => FENCED_LEADER_EPOCH,
        _ => UNKNOWN_LEADER_EPOCH,
    }
}

fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= 249
        && topic != "."
        && topic != ".."
        && topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn error_message(error: i16) -> &'static str {
    match error {
        INVALID_TOPIC_EXCEPTION => "invalid topic name",
        TOPIC_AUTHORIZATION_FAILED => "topic authorization failed",
        UNKNOWN_TOPIC_OR_PARTITION => "unknown topic or partition",
        _ => "metadata operation failed",
    }
}
