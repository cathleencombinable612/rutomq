use super::authorization::CLUSTER_RESOURCE_NAME;
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, KAFKA_STORAGE_ERROR, REPLICA_NOT_AVAILABLE,
};
use kafka_protocol::messages::alter_replica_log_dirs_response::{
    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
};
use kafka_protocol::messages::describe_log_dirs_response::{
    DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
};
use rutomq_control::PartitionKey;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const VIRTUAL_LOG_DIR: &str = "/rutomq/object-store";

impl Broker {
    pub(super) async fn handle_alter_replica_log_dirs(
        &self,
        request: AlterReplicaLogDirsRequest,
        context: &AuthorizationContext,
    ) -> AlterReplicaLogDirsResponse {
        let authorized = self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
            .unwrap_or(false);
        let mut requested = BTreeMap::new();
        for directory in request.dirs {
            for topic in directory.topics {
                for partition in topic.partitions {
                    requested.insert(
                        (topic.name.as_str().to_owned(), partition),
                        directory.path.as_str().to_owned(),
                    );
                }
            }
        }

        let mut topics = BTreeMap::<String, Vec<AlterReplicaLogDirPartitionResult>>::new();
        for ((topic, partition), path) in requested {
            let error_code = if !authorized {
                CLUSTER_AUTHORIZATION_FAILED
            } else if path != VIRTUAL_LOG_DIR {
                KAFKA_STORAGE_ERROR
            } else {
                match self.metadata.topic(&topic).await {
                    Ok(Some(info)) if partition >= 0 && partition < info.partitions => NO_ERROR,
                    Ok(_) => REPLICA_NOT_AVAILABLE,
                    Err(_) => UNKNOWN_SERVER_ERROR,
                }
            };
            topics.entry(topic).or_default().push(
                AlterReplicaLogDirPartitionResult::default()
                    .with_partition_index(partition)
                    .with_error_code(error_code),
            );
        }
        AlterReplicaLogDirsResponse::default().with_results(
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    AlterReplicaLogDirTopicResult::default()
                        .with_topic_name(topic_name(&topic))
                        .with_partitions(partitions)
                })
                .collect(),
        )
    }

    pub(super) async fn handle_describe_log_dirs(
        &self,
        request: DescribeLogDirsRequest,
        context: &AuthorizationContext,
    ) -> DescribeLogDirsResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Describe,
            )
            .await
            .unwrap_or(false)
        {
            return DescribeLogDirsResponse::default()
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED);
        }

        let requested = match self.requested_log_partitions(request).await {
            Ok(requested) => requested,
            Err(_) => {
                return DescribeLogDirsResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };
        let mut topics = Vec::new();
        for (topic, partitions) in requested {
            let info = match self.metadata.topic(&topic).await {
                Ok(Some(info)) => info,
                Ok(None) => continue,
                Err(_) => {
                    return DescribeLogDirsResponse::default()
                        .with_error_code(UNKNOWN_SERVER_ERROR);
                }
            };
            let mut described = Vec::new();
            for partition in partitions {
                if partition < 0 || partition >= info.partitions {
                    continue;
                }
                let size = match self
                    .metadata
                    .partition_size(&PartitionKey::new(&topic, partition))
                    .await
                {
                    Ok(size) => size,
                    Err(_) => {
                        return DescribeLogDirsResponse::default()
                            .with_error_code(UNKNOWN_SERVER_ERROR);
                    }
                };
                described.push(
                    DescribeLogDirsPartition::default()
                        .with_partition_index(partition)
                        .with_partition_size(size)
                        .with_offset_lag(0)
                        .with_is_future_key(false),
                );
            }
            if !described.is_empty() {
                topics.push(
                    DescribeLogDirsTopic::default()
                        .with_name(topic_name(&topic))
                        .with_partitions(described),
                );
            }
        }

        let result = DescribeLogDirsResult::default()
            .with_error_code(NO_ERROR)
            .with_log_dir(StrBytes::from_static_str(VIRTUAL_LOG_DIR))
            .with_topics(topics)
            .with_total_bytes(-1)
            .with_usable_bytes(-1)
            .with_is_cordoned(false);
        DescribeLogDirsResponse::default()
            .with_error_code(NO_ERROR)
            .with_results(vec![result])
    }

    async fn requested_log_partitions(
        &self,
        request: DescribeLogDirsRequest,
    ) -> Result<BTreeMap<String, BTreeSet<i32>>, rutomq_control::ControlError> {
        let mut requested = BTreeMap::<String, BTreeSet<i32>>::new();
        if let Some(topics) = request.topics {
            for topic in topics {
                requested
                    .entry(topic.topic.as_str().to_owned())
                    .or_default()
                    .extend(topic.partitions);
            }
        } else {
            for topic in self.metadata.topics(None).await? {
                requested
                    .entry(topic.name)
                    .or_default()
                    .extend(0..topic.partitions);
            }
        }
        Ok(requested)
    }
}
