use super::authorization::AuthorizationContext;
use super::partition_state_api::VIRTUAL_LEADER_EPOCH;
use super::share_api::string;
use super::{Broker, topic_name};
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, INVALID_REQUEST, NO_ERROR, NON_EMPTY_GROUP,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use anyhow::Result;
use kafka_protocol::messages::describe_share_group_offsets_request::{
    DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::describe_share_group_offsets_response::{
    DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
    DescribeShareGroupOffsetsResponseTopic,
};
use kafka_protocol::messages::{
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse, GroupId,
};
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, PartitionKey, SharePartitionOffset,
};
use std::collections::BTreeMap;
use uuid::Uuid;

impl Broker {
    pub(super) async fn handle_describe_share_group_offsets(
        &self,
        request: DescribeShareGroupOffsetsRequest,
        context: &AuthorizationContext,
    ) -> DescribeShareGroupOffsetsResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return DescribeShareGroupOffsetsResponse::default().with_groups(
                request
                    .groups
                    .into_iter()
                    .map(|group| group_error(group.group_id, code, &message))
                    .collect(),
            );
        }

        let group_ids = request
            .groups
            .iter()
            .map(|group| group.group_id.clone())
            .collect::<Vec<_>>();
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            match self.describe_share_offsets_group(group, context).await {
                Ok(group) => groups.push(group),
                Err(error) => {
                    return describe_request_error(&group_ids, &error.to_string());
                }
            }
        }
        DescribeShareGroupOffsetsResponse::default().with_groups(groups)
    }

    async fn describe_share_offsets_group(
        &self,
        request: DescribeShareGroupOffsetsRequestGroup,
        context: &AuthorizationContext,
    ) -> Result<DescribeShareGroupOffsetsResponseGroup> {
        let group_id = request.group_id;
        let name = group_id.as_str();
        match self
            .authorized(
                context,
                AclResourceType::Group,
                name,
                AclOperation::Describe,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Ok(group_error(
                    group_id,
                    GROUP_AUTHORIZATION_FAILED,
                    "share group authorization failed",
                ));
            }
            Err(error) => return Err(error),
        }

        match request.topics {
            Some(topics) if topics.is_empty() => {
                Ok(DescribeShareGroupOffsetsResponseGroup::default()
                    .with_group_id(group_id)
                    .with_error_code(NO_ERROR)
                    .with_topics(Vec::new()))
            }
            Some(topics) => {
                let topic_names = topics
                    .iter()
                    .map(|topic| topic.topic_name.as_str())
                    .collect::<Vec<_>>();
                let authorizations = self
                    .topic_authorizations(context, &topic_names, AclOperation::Describe)
                    .await?;
                if let Err(error) = self
                    .metadata
                    .describe_share_group_offsets(name, Some(&[]))
                    .await
                {
                    return Ok(control_group_error(group_id, error));
                }
                let mut responses = Vec::with_capacity(topics.len());
                let mut denied = Vec::new();
                for topic in topics {
                    if authorizations
                        .get(topic.topic_name.as_str())
                        .copied()
                        .unwrap_or(false)
                    {
                        responses.push(self.describe_requested_share_topic(name, topic).await);
                    } else {
                        denied.push(describe_topic_error(
                            topic.topic_name,
                            Uuid::nil(),
                            &topic.partitions,
                            TOPIC_AUTHORIZATION_FAILED,
                            "topic authorization failed",
                        ));
                    }
                }
                responses.extend(denied);
                Ok(DescribeShareGroupOffsetsResponseGroup::default()
                    .with_group_id(group_id)
                    .with_error_code(NO_ERROR)
                    .with_topics(responses))
            }
            None => match self.metadata.describe_share_group_offsets(name, None).await {
                Ok(offsets) => {
                    let topics = self.describe_all_share_topics(offsets, context).await?;
                    Ok(DescribeShareGroupOffsetsResponseGroup::default()
                        .with_group_id(group_id)
                        .with_error_code(NO_ERROR)
                        .with_topics(topics))
                }
                Err(error) => Ok(control_group_error(group_id, error)),
            },
        }
    }

    async fn describe_requested_share_topic(
        &self,
        group_id: &str,
        request: DescribeShareGroupOffsetsRequestTopic,
    ) -> DescribeShareGroupOffsetsResponseTopic {
        let name = request.topic_name.as_str();
        let topic = match self.metadata.topic(name).await {
            Ok(Some(topic)) => topic,
            Ok(None) => {
                return describe_topic_error(
                    request.topic_name,
                    Uuid::nil(),
                    &request.partitions,
                    UNKNOWN_TOPIC_OR_PARTITION,
                    "topic was not found",
                );
            }
            Err(error) => {
                return describe_topic_error(
                    request.topic_name,
                    Uuid::nil(),
                    &request.partitions,
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                );
            }
        };

        let mut partitions = Vec::with_capacity(request.partitions.len());
        for partition in request.partitions {
            if partition < 0 || partition >= topic.partitions {
                partitions.push(describe_partition_error(
                    partition,
                    UNKNOWN_TOPIC_OR_PARTITION,
                    "partition was not found",
                ));
                continue;
            }
            let key = PartitionKey::new(name, partition);
            match self
                .metadata
                .describe_share_group_offsets(group_id, Some(std::slice::from_ref(&key)))
                .await
            {
                Ok(offsets) => partitions.push(describe_partition(
                    offsets
                        .first()
                        .expect("one requested share partition is returned"),
                )),
                Err(error) => partitions.push(describe_partition_error(
                    partition,
                    share_offset_error_code(&error),
                    &error.to_string(),
                )),
            }
        }
        DescribeShareGroupOffsetsResponseTopic::default()
            .with_topic_name(request.topic_name)
            .with_topic_id(topic.id)
            .with_partitions(partitions)
    }

    async fn describe_all_share_topics(
        &self,
        offsets: Vec<SharePartitionOffset>,
        context: &AuthorizationContext,
    ) -> Result<Vec<DescribeShareGroupOffsetsResponseTopic>> {
        let topic_names = offsets
            .iter()
            .map(|offset| offset.partition.topic.as_str())
            .collect::<Vec<_>>();
        let authorizations = self
            .topic_authorizations(context, &topic_names, AclOperation::Describe)
            .await?;
        let mut topics =
            BTreeMap::<String, (Uuid, Vec<DescribeShareGroupOffsetsResponsePartition>)>::new();
        for offset in offsets {
            if !authorizations
                .get(&offset.partition.topic)
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
            let partition = describe_partition(&offset);
            topics
                .entry(offset.partition.topic)
                .or_insert_with(|| (offset.topic_id, Vec::new()))
                .1
                .push(partition);
        }
        Ok(topics
            .into_iter()
            .map(|(name, (id, partitions))| {
                DescribeShareGroupOffsetsResponseTopic::default()
                    .with_topic_name(topic_name(&name))
                    .with_topic_id(id)
                    .with_partitions(partitions)
            })
            .collect())
    }
}

fn describe_request_error(
    group_ids: &[GroupId],
    message: &str,
) -> DescribeShareGroupOffsetsResponse {
    DescribeShareGroupOffsetsResponse::default().with_groups(
        group_ids
            .iter()
            .cloned()
            .map(|group_id| group_error(group_id, UNKNOWN_SERVER_ERROR, message))
            .collect(),
    )
}

pub(super) fn share_offset_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::GroupNotFound(_) => GROUP_ID_NOT_FOUND,
        ControlError::NonEmptyGroup(_) => NON_EMPTY_GROUP,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        ControlError::TopicNotFound(_) | ControlError::PartitionNotFound { .. } => {
            UNKNOWN_TOPIC_OR_PARTITION
        }
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn control_group_error(
    group_id: GroupId,
    error: ControlError,
) -> DescribeShareGroupOffsetsResponseGroup {
    group_error(
        group_id,
        share_offset_error_code(&error),
        &error.to_string(),
    )
}

fn group_error(
    group_id: GroupId,
    error_code: i16,
    message: &str,
) -> DescribeShareGroupOffsetsResponseGroup {
    DescribeShareGroupOffsetsResponseGroup::default()
        .with_group_id(group_id)
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

fn describe_topic_error(
    name: kafka_protocol::messages::TopicName,
    topic_id: Uuid,
    partitions: &[i32],
    error_code: i16,
    message: &str,
) -> DescribeShareGroupOffsetsResponseTopic {
    DescribeShareGroupOffsetsResponseTopic::default()
        .with_topic_name(name)
        .with_topic_id(topic_id)
        .with_partitions(
            partitions
                .iter()
                .map(|partition| describe_partition_error(*partition, error_code, message))
                .collect(),
        )
}

fn describe_partition(offset: &SharePartitionOffset) -> DescribeShareGroupOffsetsResponsePartition {
    DescribeShareGroupOffsetsResponsePartition::default()
        .with_partition_index(offset.partition.partition)
        .with_start_offset(offset.start_offset)
        .with_leader_epoch(offset.leader_epoch)
        .with_lag(offset.lag())
        .with_error_code(NO_ERROR)
}

fn describe_partition_error(
    partition: i32,
    error_code: i16,
    message: &str,
) -> DescribeShareGroupOffsetsResponsePartition {
    DescribeShareGroupOffsetsResponsePartition::default()
        .with_partition_index(partition)
        .with_start_offset(-1)
        .with_leader_epoch(VIRTUAL_LEADER_EPOCH)
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}
