use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_ID,
    UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use anyhow::Result;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{DeleteTopicsRequest, DeleteTopicsResponse};
use rand::seq::SliceRandom;
use rutomq_control::{AclOperation, AclResourceType, TopicInfo};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

struct ResolvedTopic {
    topic: TopicInfo,
    by_id: bool,
}

impl Broker {
    pub(super) async fn handle_delete_topics(
        &self,
        request: DeleteTopicsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<DeleteTopicsResponse> {
        let mut responses = Vec::new();
        let (mut names, mut ids) = request_identities(request, version, &mut responses);
        reject_duplicates(&mut names, &mut ids, &mut responses);

        let cluster_delete = self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Delete,
            )
            .await?;
        let mut resolved = Vec::with_capacity(names.len() + ids.len());
        for id in ids {
            match self.metadata.topic_by_id(id).await? {
                Some(topic) => {
                    let (can_describe, can_delete) = self
                        .delete_topic_permissions(context, &topic.name, cluster_delete)
                        .await?;
                    if can_delete {
                        resolved.push(ResolvedTopic { topic, by_id: true });
                    } else {
                        responses.push(result(
                            can_describe.then_some(topic.name.as_str()),
                            id,
                            TOPIC_AUTHORIZATION_FAILED,
                            Some("topic authorization failed"),
                        ));
                    }
                }
                None => responses.push(result(
                    None,
                    id,
                    UNKNOWN_TOPIC_ID,
                    Some("topic ID was not found"),
                )),
            }
        }
        for name in names {
            let (can_describe, can_delete) = self
                .delete_topic_permissions(context, &name, cluster_delete)
                .await?;
            if !can_describe {
                responses.push(result(
                    Some(&name),
                    Uuid::nil(),
                    TOPIC_AUTHORIZATION_FAILED,
                    Some("topic authorization failed"),
                ));
                continue;
            }
            match self.metadata.topic(&name).await? {
                Some(topic) if can_delete => resolved.push(ResolvedTopic {
                    topic,
                    by_id: false,
                }),
                Some(_) => {
                    responses.push(result(
                        Some(&name),
                        Uuid::nil(),
                        TOPIC_AUTHORIZATION_FAILED,
                        Some("topic authorization failed"),
                    ));
                }
                None => responses.push(result(
                    Some(&name),
                    Uuid::nil(),
                    UNKNOWN_TOPIC_OR_PARTITION,
                    Some("topic was not found"),
                )),
            }
        }
        reject_resolved_duplicates(&mut resolved, &mut responses);

        for resolved in resolved {
            responses.push(
                match self.metadata.delete_topic_by_id(resolved.topic.id).await {
                    Ok(Some(deleted)) => result(Some(&deleted.name), deleted.id, NO_ERROR, None),
                    Ok(None) if resolved.by_id => result(
                        None,
                        resolved.topic.id,
                        UNKNOWN_TOPIC_ID,
                        Some("topic ID was not found"),
                    ),
                    Ok(None) => result(
                        Some(&resolved.topic.name),
                        Uuid::nil(),
                        UNKNOWN_TOPIC_OR_PARTITION,
                        Some("topic was not found"),
                    ),
                    Err(error) => result(
                        Some(&resolved.topic.name),
                        resolved.topic.id,
                        control_error_code(&error),
                        Some(&error.to_string()),
                    ),
                },
            );
        }
        responses.shuffle(&mut rand::rng());
        Ok(DeleteTopicsResponse::default().with_responses(responses))
    }

    async fn delete_topic_permissions(
        &self,
        context: &AuthorizationContext,
        topic: &str,
        cluster_delete: bool,
    ) -> Result<(bool, bool)> {
        if cluster_delete {
            return Ok((true, true));
        }
        let can_describe = self
            .authorized(
                context,
                AclResourceType::Topic,
                topic,
                AclOperation::Describe,
            )
            .await?;
        let can_delete = self
            .authorized(context, AclResourceType::Topic, topic, AclOperation::Delete)
            .await?;
        Ok((can_describe, can_delete))
    }
}

fn request_identities(
    request: DeleteTopicsRequest,
    version: i16,
    responses: &mut Vec<DeletableTopicResult>,
) -> (Vec<String>, Vec<Uuid>) {
    if version < 6 {
        return (
            request
                .topic_names
                .into_iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            Vec::new(),
        );
    }
    let mut names = Vec::new();
    let mut ids = Vec::new();
    for topic in request.topics {
        match (topic.name, topic.topic_id) {
            (Some(name), id) if id.is_nil() => names.push(name.as_str().to_owned()),
            (None, id) if !id.is_nil() => ids.push(id),
            (Some(name), id) => responses.push(result(
                Some(name.as_str()),
                id,
                INVALID_REQUEST,
                Some("topic name and ID may not both be specified"),
            )),
            (None, id) => responses.push(result(
                None,
                id,
                INVALID_REQUEST,
                Some("topic name or ID must be specified"),
            )),
        }
    }
    (names, ids)
}

fn reject_duplicates(
    names: &mut Vec<String>,
    ids: &mut Vec<Uuid>,
    responses: &mut Vec<DeletableTopicResult>,
) {
    let name_counts = names.iter().fold(HashMap::new(), |mut counts, name| {
        *counts.entry(name.clone()).or_insert(0) += 1;
        counts
    });
    let mut emitted_names = HashSet::new();
    for name in names.iter() {
        if name_counts[name] > 1 && emitted_names.insert(name.clone()) {
            responses.push(result(
                Some(name),
                Uuid::nil(),
                INVALID_REQUEST,
                Some("duplicate topic name"),
            ));
        }
    }
    names.retain(|name| name_counts[name] == 1);

    let id_counts = ids.iter().fold(HashMap::new(), |mut counts, id| {
        *counts.entry(*id).or_insert(0) += 1;
        counts
    });
    let mut emitted_ids = HashSet::new();
    for id in ids.iter().copied() {
        if id_counts[&id] > 1 && emitted_ids.insert(id) {
            responses.push(result(
                None,
                id,
                INVALID_REQUEST,
                Some("duplicate topic ID"),
            ));
        }
    }
    ids.retain(|id| id_counts[id] == 1);
}

fn reject_resolved_duplicates(
    resolved: &mut Vec<ResolvedTopic>,
    responses: &mut Vec<DeletableTopicResult>,
) {
    let counts = resolved.iter().fold(HashMap::new(), |mut counts, topic| {
        *counts.entry(topic.topic.id).or_insert(0) += 1;
        counts
    });
    let mut emitted = HashSet::new();
    for topic in resolved.iter() {
        if counts[&topic.topic.id] > 1 && emitted.insert(topic.topic.id) {
            responses.push(result(
                Some(&topic.topic.name),
                topic.topic.id,
                INVALID_REQUEST,
                Some("topic name and ID resolve to the same topic"),
            ));
        }
    }
    resolved.retain(|topic| counts[&topic.topic.id] == 1);
}

fn result(
    name: Option<&str>,
    topic_id: Uuid,
    error_code: i16,
    message: Option<&str>,
) -> DeletableTopicResult {
    DeletableTopicResult::default()
        .with_name(name.map(super::topic_name))
        .with_topic_id(topic_id)
        .with_error_code(error_code)
        .with_error_message(message.map(|message| message.to_owned().into()))
}
