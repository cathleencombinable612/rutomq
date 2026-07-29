use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::{Broker, config_api, create_topics_validation};
use rutomq_control::{
    AclOperation, AclResourceType, ControlError, STREAMS_STATUS_MISSING_INTERNAL_TOPICS,
    StreamsGroupStatus, StreamsTopology, TopicConfig, TopicInfo,
    streams_internal_topic_requirements,
};
use std::collections::BTreeSet;

const KAFKA_MIN_SEGMENT_BYTES: i32 = 1_048_576;
const STREAMS_SEGMENT_BYTES_CONFIG: &str = "segment.bytes";

#[derive(Default)]
pub(super) struct StreamsInternalTopicPreparation {
    denied: BTreeSet<String>,
    failed: BTreeSet<String>,
}

impl StreamsInternalTopicPreparation {
    pub(super) fn decorate(&self, statuses: &mut [StreamsGroupStatus]) {
        let Some(status) = statuses
            .iter_mut()
            .find(|status| status.code == STREAMS_STATUS_MISSING_INTERNAL_TOPICS)
        else {
            return;
        };
        if !self.denied.is_empty() {
            status.detail.push_str(&format!(
                "; creation not attempted because Create ACL is missing for {}",
                self.denied.iter().cloned().collect::<Vec<_>>().join(",")
            ));
        }
        if !self.failed.is_empty() {
            status.detail.push_str(&format!(
                "; creation attempts failed for {}",
                self.failed.iter().cloned().collect::<Vec<_>>().join(",")
            ));
        }
    }
}

impl Broker {
    pub(super) async fn prepare_streams_internal_topics(
        &self,
        context: &AuthorizationContext,
        topology: &StreamsTopology,
        topics: &[TopicInfo],
    ) -> StreamsInternalTopicPreparation {
        let mut preparation = StreamsInternalTopicPreparation::default();
        let requirements = match streams_internal_topic_requirements(topology, topics) {
            Ok(requirements) => requirements,
            Err(error) => {
                preparation.failed.insert(error.to_string());
                return preparation;
            }
        };
        let existing = topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<BTreeSet<_>>();
        for requirement in requirements {
            let name = requirement.topic.name.clone();
            if existing.contains(name.as_str()) {
                continue;
            }
            let can_create = self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                    AclOperation::Create,
                )
                .await
                .unwrap_or(false)
                || self
                    .authorized(context, AclResourceType::Topic, &name, AclOperation::Create)
                    .await
                    .unwrap_or(false);
            if !can_create {
                preparation.denied.insert(name);
                continue;
            }
            let config = match streams_internal_topic_config(
                requirement
                    .topic
                    .topic_configs
                    .iter()
                    .map(|entry| (entry.key.as_str(), entry.value.as_str())),
            ) {
                Ok(config) => config,
                Err(error) => {
                    preparation.failed.insert(format!("{name}: {error}"));
                    continue;
                }
            };
            let replication_factor = if requirement.topic.replication_factor == 0 {
                -1
            } else {
                requirement.topic.replication_factor
            };
            if let Err((_, message)) = create_topics_validation::resolve_replication_factor(
                replication_factor,
                self.config.default_replication_factor,
            ) {
                preparation.failed.insert(format!("{name}: {message}"));
                continue;
            }
            match self
                .metadata
                .create_topic_with_config(&name, requirement.partitions, config)
                .await
            {
                Ok(_) | Err(ControlError::TopicAlreadyExists(_)) => {}
                Err(error) => {
                    preparation.failed.insert(format!("{name}: {error}"));
                }
            }
        }
        preparation
    }
}

fn streams_internal_topic_config<'a>(
    changes: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<TopicConfig, ControlError> {
    let changes = changes.into_iter().collect::<Vec<_>>();
    let mut names = BTreeSet::new();
    for (name, _) in &changes {
        if !names.insert(*name) {
            return Err(ControlError::InvalidRequest(
                "configuration keys must be unique".to_owned(),
            ));
        }
    }

    let mut applicable = Vec::with_capacity(changes.len());
    for (name, value) in changes {
        if name == STREAMS_SEGMENT_BYTES_CONFIG {
            let segment_bytes = value.parse::<i32>().map_err(|_| {
                ControlError::InvalidConfiguration(
                    "configuration segment.bytes must be a 32-bit integer".to_owned(),
                )
            })?;
            if segment_bytes < KAFKA_MIN_SEGMENT_BYTES {
                return Err(ControlError::InvalidConfiguration(format!(
                    "configuration segment.bytes must be at least {KAFKA_MIN_SEGMENT_BYTES}"
                )));
            }

            // Kafka Streams emits this local-log hint for repartition topics.
            // Object packing has no local segments, so only its wire contract applies.
            continue;
        }
        applicable.push((name, value));
    }
    config_api::create_topic_config_entries(applicable)
}
