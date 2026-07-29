use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::{Broker, topic_name};
use crate::kafka_error::{
    INVALID_REQUEST, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_ID,
    UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use anyhow::Result;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse};
use rutomq_control::{AclOperation, AclResourceType, ControlError, TopicInfo};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

impl Broker {
    pub(super) async fn handle_metadata(
        &self,
        request: MetadataRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<MetadataResponse> {
        if invalid_request(&request, version) {
            return Ok(request_error(&request));
        }

        let include_topic_operations = request.include_topic_authorized_operations;
        let mut response_topics = Vec::new();
        if is_all_topics(&request, version) {
            for topic in self.metadata.topics(None).await? {
                if self
                    .authorized(
                        context,
                        AclResourceType::Topic,
                        &topic.name,
                        AclOperation::Describe,
                    )
                    .await?
                {
                    response_topics.push(
                        self.metadata_topic(context, &topic, version, include_topic_operations)
                            .await,
                    );
                }
            }
        } else if uses_topic_ids(&request, version) {
            for topic_id in unique_topic_ids(request.topics.as_deref().unwrap_or_default()) {
                match self.metadata.topic_by_id(topic_id).await? {
                    None => response_topics.push(id_error(topic_id, UNKNOWN_TOPIC_ID)),
                    Some(topic)
                        if !self
                            .authorized(
                                context,
                                AclResourceType::Topic,
                                &topic.name,
                                AclOperation::Describe,
                            )
                            .await? =>
                    {
                        response_topics.push(id_error(topic_id, TOPIC_AUTHORIZATION_FAILED));
                    }
                    Some(topic) => {
                        response_topics.push(
                            self.metadata_topic(context, &topic, version, include_topic_operations)
                                .await,
                        );
                    }
                }
            }
        } else {
            let names = unique_topic_names(request.topics.as_deref().unwrap_or_default());
            let existing = self
                .metadata
                .topics(Some(&names))
                .await?
                .into_iter()
                .map(|topic| (topic.name.clone(), topic))
                .collect::<HashMap<_, _>>();
            for name in names {
                response_topics.push(
                    self.named_metadata(
                        context,
                        &name,
                        existing.get(&name),
                        version,
                        request.allow_auto_topic_creation,
                        include_topic_operations,
                    )
                    .await?,
                );
            }
        }

        let cluster_operations =
            if (8..=10).contains(&version) && request.include_cluster_authorized_operations {
                if self
                    .authorized(
                        context,
                        AclResourceType::Cluster,
                        CLUSTER_RESOURCE_NAME,
                        AclOperation::Describe,
                    )
                    .await?
                {
                    self.cluster_authorized_operations(context).await
                } else {
                    0
                }
            } else {
                i32::MIN
            };
        Ok(self.metadata_response(version, response_topics, cluster_operations))
    }

    async fn named_metadata(
        &self,
        context: &AuthorizationContext,
        name: &str,
        existing: Option<&TopicInfo>,
        version: i16,
        allow_auto_creation: bool,
        include_operations: bool,
    ) -> Result<MetadataResponseTopic> {
        if !self
            .authorized(
                context,
                AclResourceType::Topic,
                name,
                AclOperation::Describe,
            )
            .await?
        {
            return Ok(name_error(name, TOPIC_AUTHORIZATION_FAILED));
        }
        if let Some(topic) = existing {
            return Ok(self
                .metadata_topic(context, topic, version, include_operations)
                .await);
        }

        let allow_auto_creation = allow_auto_creation && self.config.auto_create_topics_enable;
        if allow_auto_creation {
            let can_create = self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    AclOperation::Create,
                )
                .await?
                || self
                    .authorized(context, AclResourceType::Topic, name, AclOperation::Create)
                    .await?;
            if !can_create {
                return Ok(name_error(name, TOPIC_AUTHORIZATION_FAILED));
            }
        }

        if let Err(error) = self.metadata.validate_topic_creation(name).await {
            if matches!(error, ControlError::TopicAlreadyExists(_))
                && let Some(topic) = self.metadata.topic(name).await?
            {
                return Ok(self
                    .metadata_topic(context, &topic, version, include_operations)
                    .await);
            }
            return Ok(name_error(name, control_error_code(&error)));
        }
        if !allow_auto_creation {
            return Ok(name_error(name, UNKNOWN_TOPIC_OR_PARTITION));
        }

        match self
            .metadata
            .create_topic(name, self.config.num_partitions)
            .await
        {
            Ok(topic) => Ok(self
                .metadata_topic(context, &topic, version, include_operations)
                .await),
            Err(ControlError::TopicAlreadyExists(_)) => match self.metadata.topic(name).await? {
                Some(topic) => Ok(self
                    .metadata_topic(context, &topic, version, include_operations)
                    .await),
                None => Ok(name_error(name, UNKNOWN_TOPIC_OR_PARTITION)),
            },
            Err(error) => Ok(name_error(name, control_error_code(&error))),
        }
    }

    async fn metadata_topic(
        &self,
        context: &AuthorizationContext,
        topic: &TopicInfo,
        version: i16,
        include_operations: bool,
    ) -> MetadataResponseTopic {
        let operations = if include_operations {
            self.topic_authorized_operations(context, &topic.name).await
        } else {
            i32::MIN
        };
        metadata_topic(topic, version).with_topic_authorized_operations(operations)
    }

    fn metadata_response(
        &self,
        version: i16,
        topics: Vec<MetadataResponseTopic>,
        cluster_operations: i32,
    ) -> MetadataResponse {
        let mut response = MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId::from(0))
                    .with_host(self.config.advertise_host.clone().into())
                    .with_port(self.config.advertise_port),
            ])
            .with_topics(topics)
            .with_cluster_authorized_operations(cluster_operations);
        if version >= 2 {
            response = response.with_cluster_id(Some(self.config.cluster_id.clone().into()));
        }
        if version >= 1 {
            response = response.with_controller_id(BrokerId::from(0));
        }
        response
    }
}

fn invalid_request(request: &MetadataRequest, version: i16) -> bool {
    let Some(topics) = request.topics.as_ref() else {
        return false;
    };
    let use_topic_ids = uses_topic_ids(request, version);
    topics.iter().any(|topic| {
        (version < 12 && (topic.name.is_none() || topic.topic_id != Uuid::nil()))
            || (version >= 12 && !use_topic_ids && topic.name.is_none())
    })
}

fn is_all_topics(request: &MetadataRequest, version: i16) -> bool {
    request.topics.is_none() || (version == 0 && request.topics.as_ref().is_some_and(Vec::is_empty))
}

fn uses_topic_ids(request: &MetadataRequest, version: i16) -> bool {
    version >= 12
        && request
            .topics
            .as_ref()
            .is_some_and(|topics| topics.iter().any(|topic| topic.topic_id != Uuid::nil()))
}

fn unique_topic_names(topics: &[MetadataRequestTopic]) -> Vec<String> {
    let mut seen = HashSet::new();
    topics
        .iter()
        .filter_map(|topic| topic.name.as_ref())
        .map(|name| name.as_str().to_owned())
        .filter(|name| seen.insert(name.clone()))
        .collect()
}

fn unique_topic_ids(topics: &[MetadataRequestTopic]) -> Vec<Uuid> {
    let mut seen = HashSet::new();
    topics
        .iter()
        .map(|topic| topic.topic_id)
        .filter(|topic_id| *topic_id != Uuid::nil())
        .filter(|topic_id| seen.insert(*topic_id))
        .collect()
}

fn request_error(request: &MetadataRequest) -> MetadataResponse {
    let topics = request
        .topics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|topic| {
            MetadataResponseTopic::default()
                .with_error_code(INVALID_REQUEST)
                .with_name(Some(topic.name.clone().unwrap_or_else(|| topic_name(""))))
                .with_topic_id(topic.topic_id)
        })
        .collect();
    MetadataResponse::default()
        .with_error_code(INVALID_REQUEST)
        .with_topics(topics)
}

fn name_error(name: &str, error_code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(error_code)
        .with_name(Some(topic_name(name)))
}

fn id_error(topic_id: Uuid, error_code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(error_code)
        .with_name(None)
        .with_topic_id(topic_id)
}

fn metadata_topic(topic: &TopicInfo, version: i16) -> MetadataResponseTopic {
    let replicas = vec![BrokerId::from(0)];
    let partitions = (0..topic.partitions)
        .map(|partition| {
            MetadataResponsePartition::default()
                .with_error_code(NO_ERROR)
                .with_partition_index(partition)
                .with_leader_id(BrokerId::from(0))
                .with_leader_epoch(0)
                .with_replica_nodes(replicas.clone())
                .with_isr_nodes(replicas.clone())
        })
        .collect();
    let response = MetadataResponseTopic::default()
        .with_error_code(NO_ERROR)
        .with_name(Some(topic_name(&topic.name)))
        .with_partitions(partitions);
    if version >= 10 {
        response.with_topic_id(topic.id)
    } else {
        response
    }
}
