use super::partition_state_api::{VIRTUAL_LEADER_EPOCH, current_epoch_error};
use super::{AuthorizationContext, Broker};
use crate::kafka_error::{
    INVALID_REQUEST, KAFKA_STORAGE_ERROR, NO_ERROR, REQUEST_TIMED_OUT, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION, control_error_code,
};
use crate::records::decode_stored_records;
use anyhow::Context;
use indexmap::IndexMap;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse, TopicName};
use rutomq_control::{
    AclOperation, ControlError, FetchIsolation, PartitionKey, PartitionWatermarks,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

const LATEST_TIMESTAMP: i64 = -1;
const EARLIEST_TIMESTAMP: i64 = -2;
const MAX_TIMESTAMP: i64 = -3;
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;
const LATEST_TIERED_TIMESTAMP: i64 = -5;
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OffsetMatch {
    pub(super) timestamp: i64,
    pub(super) offset: i64,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum LookupError {
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error("storage offset lookup failed: {0}")]
    Storage(anyhow::Error),
    #[error("offset lookup timed out")]
    TimedOut,
}

impl Broker {
    pub(super) async fn handle_list_offsets(
        &self,
        request: ListOffsetsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> ListOffsetsResponse {
        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Describe)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(_) => {
                return list_offsets_error(request.topics, UNKNOWN_SERVER_ERROR);
            }
        };
        let duplicates = duplicate_partitions(&request.topics);
        let isolation = match request.isolation_level {
            0 => Some(FetchIsolation::ReadUncommitted),
            1 => Some(FetchIsolation::ReadCommitted),
            _ => None,
        };
        let follower_request = request.replica_id.0 >= 0;
        let timeout_ms = request.timeout_ms.max(0) as u64;

        let mut authorized = IndexMap::<String, (TopicName, Vec<ListOffsetsPartition>)>::new();
        let mut denied = Vec::new();
        let mut included_partitions = HashSet::new();
        for topic in request.topics {
            let name = topic.name.as_str().to_owned();
            if !authorizations.get(&name).copied().unwrap_or(false) {
                denied.push(list_offsets_topic_error(topic, TOPIC_AUTHORIZATION_FAILED));
                continue;
            }
            let (_, partitions) = authorized
                .entry(name)
                .or_insert_with(|| (topic.name.clone(), Vec::new()));
            for partition in topic.partitions {
                if included_partitions
                    .insert((topic.name.as_str().to_owned(), partition.partition_index))
                {
                    partitions.push(partition);
                }
            }
        }

        let mut topics = Vec::with_capacity(authorized.len() + denied.len());
        for (name, (topic_name, requested_partitions)) in authorized {
            let mut partitions = Vec::with_capacity(requested_partitions.len());
            for partition in requested_partitions {
                let key = (name.clone(), partition.partition_index);
                let error = if duplicates.contains(&key) || follower_request || isolation.is_none()
                {
                    Some(INVALID_REQUEST)
                } else if unsupported_timestamp(partition.timestamp, version) {
                    Some(UNSUPPORTED_VERSION)
                } else {
                    let epoch_error = current_epoch_error(partition.current_leader_epoch);
                    (epoch_error != NO_ERROR).then_some(epoch_error)
                };
                if let Some(error) = error {
                    partitions.push(partition_error(&partition, error));
                    continue;
                }
                let key = PartitionKey::new(&name, partition.partition_index);
                let lookup = self.lookup_offset(
                    &key,
                    partition.timestamp,
                    isolation.expect("validated isolation level"),
                );
                let result = if version >= 10 {
                    match timeout(Duration::from_millis(timeout_ms), lookup).await {
                        Ok(result) => result,
                        Err(_) => Err(LookupError::TimedOut),
                    }
                } else {
                    lookup.await
                };
                partitions.push(match result {
                    Ok(found) => partition_response(&partition, version)
                        .with_error_code(NO_ERROR)
                        .with_timestamp(found.timestamp)
                        .with_offset(found.offset),
                    Err(error) => partition_error(&partition, lookup_error_code(&error)),
                });
            }
            topics.push(
                ListOffsetsTopicResponse::default()
                    .with_name(topic_name)
                    .with_partitions(partitions),
            );
        }
        topics.extend(denied);
        ListOffsetsResponse::default().with_topics(topics)
    }

    pub(super) async fn lookup_offset(
        &self,
        partition: &PartitionKey,
        target_timestamp: i64,
        isolation: FetchIsolation,
    ) -> std::result::Result<OffsetMatch, LookupError> {
        let watermarks = self.metadata.partition_watermarks(partition).await?;
        match target_timestamp {
            EARLIEST_TIMESTAMP | EARLIEST_LOCAL_TIMESTAMP => Ok(OffsetMatch {
                timestamp: -1,
                offset: watermarks.log_start_offset,
            }),
            LATEST_TIMESTAMP => Ok(OffsetMatch {
                timestamp: -1,
                offset: visible_end(watermarks, isolation),
            }),
            LATEST_TIERED_TIMESTAMP => Ok(OffsetMatch {
                timestamp: -1,
                offset: -1,
            }),
            EARLIEST_PENDING_UPLOAD_TIMESTAMP => Ok(OffsetMatch {
                timestamp: -1,
                offset: -1,
            }),
            MAX_TIMESTAMP => {
                self.scan_offset(partition, watermarks, isolation, None)
                    .await
            }
            timestamp => {
                self.scan_offset(partition, watermarks, isolation, Some(timestamp))
                    .await
            }
        }
    }

    async fn scan_offset(
        &self,
        partition: &PartitionKey,
        watermarks: PartitionWatermarks,
        isolation: FetchIsolation,
        target_timestamp: Option<i64>,
    ) -> std::result::Result<OffsetMatch, LookupError> {
        let visible_end = visible_end(watermarks, isolation);
        let fetched = self
            .metadata
            .fetch(
                partition,
                watermarks.log_start_offset,
                usize::MAX,
                isolation,
            )
            .await?;
        let mut maximum = None;
        for span in fetched.spans {
            let raw = self
                .objects
                .get_range(&span.object_key, span.byte_start..span.byte_end)
                .await
                .with_context(|| format!("read ListOffsets source {}", span.object_key))
                .map_err(LookupError::Storage)?;
            for record in decode_stored_records(&raw, span.base_offset, span.offsets_preserved)
                .map_err(LookupError::Storage)?
            {
                if record.offset < watermarks.log_start_offset || record.offset >= visible_end {
                    continue;
                }
                if let Some(target) = target_timestamp {
                    if record.timestamp >= target {
                        return Ok(OffsetMatch {
                            timestamp: record.timestamp,
                            offset: record.offset,
                        });
                    }
                } else if maximum
                    .as_ref()
                    .is_none_or(|current: &OffsetMatch| record.timestamp > current.timestamp)
                {
                    maximum = Some(OffsetMatch {
                        timestamp: record.timestamp,
                        offset: record.offset,
                    });
                }
            }
        }
        Ok(maximum.unwrap_or(OffsetMatch {
            timestamp: -1,
            offset: -1,
        }))
    }
}

fn visible_end(watermarks: PartitionWatermarks, isolation: FetchIsolation) -> i64 {
    match isolation {
        FetchIsolation::ReadUncommitted => watermarks.high_watermark,
        FetchIsolation::ReadCommitted => watermarks.last_stable_offset,
    }
}

fn unsupported_timestamp(timestamp: i64, version: i16) -> bool {
    match timestamp {
        timestamp if timestamp >= 0 => false,
        LATEST_TIMESTAMP | EARLIEST_TIMESTAMP => false,
        MAX_TIMESTAMP => version < 7,
        EARLIEST_LOCAL_TIMESTAMP => version < 8,
        LATEST_TIERED_TIMESTAMP => version < 9,
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => version < 11,
        _ => true,
    }
}

fn partition_response(
    partition: &ListOffsetsPartition,
    version: i16,
) -> ListOffsetsPartitionResponse {
    let response =
        ListOffsetsPartitionResponse::default().with_partition_index(partition.partition_index);
    if version >= 4 {
        response.with_leader_epoch(VIRTUAL_LEADER_EPOCH)
    } else {
        response
    }
}

fn partition_error(partition: &ListOffsetsPartition, error: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition.partition_index)
        .with_error_code(error)
}

fn duplicate_partitions(topics: &[ListOffsetsTopic]) -> HashSet<(String, i32)> {
    let mut seen = HashSet::new();
    let mut duplicates = HashSet::new();
    for topic in topics {
        for partition in &topic.partitions {
            let key = (topic.name.as_str().to_owned(), partition.partition_index);
            if !seen.insert(key.clone()) {
                duplicates.insert(key);
            }
        }
    }
    duplicates
}

fn list_offsets_error(topics: Vec<ListOffsetsTopic>, error_code: i16) -> ListOffsetsResponse {
    ListOffsetsResponse::default().with_topics(
        topics
            .into_iter()
            .map(|topic| list_offsets_topic_error(topic, error_code))
            .collect(),
    )
}

fn list_offsets_topic_error(topic: ListOffsetsTopic, error_code: i16) -> ListOffsetsTopicResponse {
    ListOffsetsTopicResponse::default()
        .with_name(topic.name)
        .with_partitions(
            topic
                .partitions
                .iter()
                .map(|partition| partition_error(partition, error_code))
                .collect(),
        )
}

pub(super) fn lookup_error_code(error: &LookupError) -> i16 {
    match error {
        LookupError::Control(error) => control_error_code(error),
        LookupError::Storage(error) => {
            warn!(%error, "failed to read ListOffsets records");
            KAFKA_STORAGE_ERROR
        }
        LookupError::TimedOut => REQUEST_TIMED_OUT,
    }
}
