use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use super::share_api::string;
use super::share_offset_api::share_offset_error_code;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::alter_share_group_offsets_response::{
    AlterShareGroupOffsetsResponsePartition, AlterShareGroupOffsetsResponseTopic,
};
use kafka_protocol::messages::delete_share_group_offsets_response::DeleteShareGroupOffsetsResponseTopic;
use kafka_protocol::messages::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse, DeleteShareGroupOffsetsRequest,
    DeleteShareGroupOffsetsResponse,
};
use rutomq_control::{
    AclOperation, AclResourceType, PartitionKey, ShareOffsetDeleteResult, ShareOffsetUpdate,
    ShareOffsetUpdateResult,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

impl Broker {
    pub(super) async fn handle_alter_share_group_offsets(
        &self,
        request: AlterShareGroupOffsetsRequest,
        context: &AuthorizationContext,
    ) -> AlterShareGroupOffsetsResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return alter_top_error(code, &message);
        }
        let group_id = request.group_id.as_str();
        if let Some((error_code, backend_message)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return alter_top_error(
                error_code,
                backend_message
                    .as_deref()
                    .unwrap_or("share group authorization failed"),
            );
        }
        if let Some(message) = duplicate_alter_request(&request) {
            return alter_top_error(INVALID_REQUEST, &message);
        }

        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.topic_name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Read)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(error) => return alter_top_error(UNKNOWN_SERVER_ERROR, &error.to_string()),
        };
        let mut authorized = Vec::new();
        let mut denied = HashMap::new();
        for topic in &request.topics {
            let name = topic.topic_name.as_str();
            if authorizations.get(name).copied().unwrap_or(false) {
                authorized.extend(topic.partitions.iter().map(|partition| ShareOffsetUpdate {
                    partition: PartitionKey::new(name, partition.partition_index),
                    start_offset: partition.start_offset,
                }));
            } else {
                let topic_id = match self.metadata.topic(name).await {
                    Ok(Some(topic)) => topic.id,
                    Ok(None) => Uuid::nil(),
                    Err(error) => {
                        return alter_top_error(UNKNOWN_SERVER_ERROR, &error.to_string());
                    }
                };
                denied.insert(name.to_owned(), topic_id);
            }
        }

        let results = if authorized.is_empty() {
            Vec::new()
        } else {
            match self
                .metadata
                .alter_share_group_offsets(group_id, &authorized)
                .await
            {
                Ok(results) => results,
                Err(error) => {
                    return alter_top_error(share_offset_error_code(&error), &error.to_string());
                }
            }
        };
        let results = results
            .into_iter()
            .map(|result| (result.partition.clone(), result))
            .collect::<HashMap<_, _>>();
        AlterShareGroupOffsetsResponse::default()
            .with_error_code(NO_ERROR)
            .with_responses(
                request
                    .topics
                    .into_iter()
                    .map(|topic| alter_topic_response(topic, &denied, &results))
                    .collect(),
            )
    }

    pub(super) async fn handle_delete_share_group_offsets(
        &self,
        request: DeleteShareGroupOffsetsRequest,
        context: &AuthorizationContext,
    ) -> DeleteShareGroupOffsetsResponse {
        if let Some((code, message)) = self.share_feature_error().await {
            return delete_top_error(code, &message);
        }
        let group_id = request.group_id.as_str();
        if let Some((error_code, backend_message)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                group_id,
                AclOperation::Delete,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return delete_top_error(
                error_code,
                backend_message
                    .as_deref()
                    .unwrap_or("share group authorization failed"),
            );
        }
        if duplicate_delete_request(&request) {
            return delete_top_error(
                INVALID_REQUEST,
                "share offset deletion contains duplicate topics",
            );
        }

        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.topic_name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Read)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(error) => return delete_top_error(UNKNOWN_SERVER_ERROR, &error.to_string()),
        };
        let mut authorized = Vec::new();
        let mut denied = HashSet::new();
        for topic in &request.topics {
            let name = topic.topic_name.as_str();
            if authorizations.get(name).copied().unwrap_or(false) {
                authorized.push(name.to_owned());
            } else {
                denied.insert(name.to_owned());
            }
        }
        let results = match self
            .metadata
            .delete_share_group_offsets(group_id, &authorized)
            .await
        {
            Ok(results) => results
                .into_iter()
                .map(|result| (result.topic.clone(), result))
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                return delete_top_error(share_offset_error_code(&error), &error.to_string());
            }
        };
        let mut responses = request
            .topics
            .iter()
            .filter(|topic| denied.contains(topic.topic_name.as_str()))
            .map(|topic| delete_denied_topic_response(topic.topic_name.clone()))
            .collect::<Vec<_>>();
        responses.extend(
            request
                .topics
                .into_iter()
                .filter(|topic| !denied.contains(topic.topic_name.as_str()))
                .map(|topic| delete_topic_response(topic.topic_name, &results)),
        );
        DeleteShareGroupOffsetsResponse::default()
            .with_error_code(NO_ERROR)
            .with_responses(responses)
    }
}

fn duplicate_alter_request(request: &AlterShareGroupOffsetsRequest) -> Option<String> {
    let mut topics = HashSet::new();
    let mut partitions = HashSet::new();
    for topic in &request.topics {
        let name = topic.topic_name.as_str();
        if !topics.insert(name) {
            return Some(format!("share topic {name} appears more than once"));
        }
        for partition in &topic.partitions {
            if !partitions.insert((name, partition.partition_index)) {
                return Some(format!(
                    "share partition {name}-{} appears more than once",
                    partition.partition_index
                ));
            }
        }
    }
    None
}

fn duplicate_delete_request(request: &DeleteShareGroupOffsetsRequest) -> bool {
    let mut topics = HashSet::new();
    request
        .topics
        .iter()
        .any(|topic| !topics.insert(topic.topic_name.as_str()))
}

fn alter_topic_response(
    topic: kafka_protocol::messages::alter_share_group_offsets_request::
        AlterShareGroupOffsetsRequestTopic,
    denied: &HashMap<String, Uuid>,
    results: &HashMap<PartitionKey, ShareOffsetUpdateResult>,
) -> AlterShareGroupOffsetsResponseTopic {
    let name = topic.topic_name.as_str().to_owned();
    let denied_topic_id = denied.get(&name).copied();
    let topic_id = denied_topic_id.unwrap_or_else(|| {
        topic
            .partitions
            .iter()
            .find_map(|partition| {
                results
                    .get(&PartitionKey::new(&name, partition.partition_index))
                    .and_then(|result| result.topic_id)
            })
            .unwrap_or_else(Uuid::nil)
    });
    AlterShareGroupOffsetsResponseTopic::default()
        .with_topic_name(topic.topic_name)
        .with_topic_id(topic_id)
        .with_partitions(
            topic
                .partitions
                .into_iter()
                .map(|partition| {
                    if denied_topic_id.is_some() {
                        return alter_partition_error(
                            partition.partition_index,
                            TOPIC_AUTHORIZATION_FAILED,
                            "topic authorization failed",
                        );
                    }
                    match results.get(&PartitionKey::new(&name, partition.partition_index)) {
                        Some(result) if result.updated => {
                            AlterShareGroupOffsetsResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(NO_ERROR)
                        }
                        Some(_) => alter_partition_error(
                            partition.partition_index,
                            UNKNOWN_TOPIC_OR_PARTITION,
                            "topic or partition was not found",
                        ),
                        None => alter_partition_error(
                            partition.partition_index,
                            UNKNOWN_SERVER_ERROR,
                            "share offset update result is missing",
                        ),
                    }
                })
                .collect(),
        )
}

fn delete_topic_response(
    topic: kafka_protocol::messages::TopicName,
    results: &HashMap<String, ShareOffsetDeleteResult>,
) -> DeleteShareGroupOffsetsResponseTopic {
    let name = topic.as_str().to_owned();
    match results.get(&name) {
        Some(result) if result.deleted => DeleteShareGroupOffsetsResponseTopic::default()
            .with_topic_name(topic)
            .with_topic_id(result.topic_id.unwrap_or_else(Uuid::nil))
            .with_error_code(NO_ERROR),
        Some(result) => DeleteShareGroupOffsetsResponseTopic::default()
            .with_topic_name(topic)
            .with_topic_id(result.topic_id.unwrap_or_else(Uuid::nil))
            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
            .with_error_message(Some(string("share offsets were not found"))),
        None => DeleteShareGroupOffsetsResponseTopic::default()
            .with_topic_name(topic)
            .with_topic_id(Uuid::nil())
            .with_error_code(UNKNOWN_SERVER_ERROR)
            .with_error_message(Some(string("share offset deletion result is missing"))),
    }
}

fn delete_denied_topic_response(
    topic: kafka_protocol::messages::TopicName,
) -> DeleteShareGroupOffsetsResponseTopic {
    DeleteShareGroupOffsetsResponseTopic::default()
        .with_topic_name(topic)
        .with_topic_id(Uuid::nil())
        .with_error_code(TOPIC_AUTHORIZATION_FAILED)
        .with_error_message(Some(string("topic authorization failed")))
}

fn alter_partition_error(
    partition: i32,
    error_code: i16,
    message: &str,
) -> AlterShareGroupOffsetsResponsePartition {
    AlterShareGroupOffsetsResponsePartition::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

fn alter_top_error(error_code: i16, message: &str) -> AlterShareGroupOffsetsResponse {
    AlterShareGroupOffsetsResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}

fn delete_top_error(error_code: i16, message: &str) -> DeleteShareGroupOffsetsResponse {
    DeleteShareGroupOffsetsResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(string(message)))
}
