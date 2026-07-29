use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use super::group_config::GroupRuntimeConfig;
use super::group_offset_reset::ShareOffsetReset;
use super::list_offsets_api::lookup_error_code;
use super::partition_state_api::VIRTUAL_LEADER_EPOCH;
use super::share_api::{ShareIdentity, error_code, identity, string};
use super::share_protocol::{
    RENEW_DISABLED_MESSAGE, ShareAcquireMode, acquisition_candidates, has_renew_acknowledgement,
    validate_acknowledgement_types, validate_renew_fetch,
};
use super::share_topic_authorization::{ShareTopicAccess, ShareTopicAccesses};
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, INVALID_RECORD_STATE, INVALID_REQUEST, KAFKA_STORAGE_ERROR,
    NO_ERROR, OFFSET_NOT_AVAILABLE, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
    UNKNOWN_TOPIC_ID,
};
use crate::records::{decode_stored_record_batches, encode_records};
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use kafka_protocol::messages::share_fetch_request::AcknowledgementBatch;
use kafka_protocol::messages::share_fetch_response::{
    AcquiredRecords, LeaderIdAndEpoch, PartitionData, ShareFetchableTopicResponse,
};
use kafka_protocol::messages::{ShareFetchRequest, ShareFetchResponse};
use rutomq_control::{
    AclOperation, AclResourceType, FetchIsolation, PartitionFetch, PartitionKey,
    ShareAcknowledgeRecords, ShareAcknowledgementBatch as ControlAcknowledgementBatch,
    ShareAcquireRequest, ShareAutoOffsetReset, ShareFetchSession, ShareFetchSessionUpdate,
    SharePartitionState, ShareSessionPartition,
};
use rutomq_protocol::records::Record;
use std::collections::{HashMap, HashSet};
use tokio::time::{Duration, Instant, sleep};
use tracing::warn;

type PartitionId = (uuid::Uuid, i32);
type AcknowledgeResult = HashMap<PartitionId, (i16, Option<String>)>;

struct ShareFetchState<'a> {
    identity: &'a ShareIdentity,
    session: &'a ShareFetchSession,
    acknowledgements: &'a AcknowledgeResult,
    group_config: &'a GroupRuntimeConfig,
    topic_accesses: &'a ShareTopicAccesses,
}

impl Broker {
    pub(super) async fn handle_share_fetch(
        &self,
        request: ShareFetchRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<ShareFetchResponse> {
        if let Some((code, message)) = self.share_feature_error().await {
            return Ok(top_error(code, &message, &self.config));
        }
        let identity = match identity(&request.group_id, &request.member_id) {
            Ok(identity) => identity,
            Err(message) => return Ok(top_error(INVALID_REQUEST, message, &self.config)),
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
            return Ok(top_error(
                error_code,
                backend_message
                    .as_deref()
                    .unwrap_or("share group authorization failed"),
                &self.config,
            ));
        }
        let group_config = match self.group_runtime_config(&identity.group_id).await {
            Ok(config) => config,
            Err(error) => {
                return Ok(top_error(
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    &self.config,
                ));
            }
        };
        let acquire_mode = match ShareAcquireMode::parse(version, request.share_acquire_mode) {
            Ok(mode) => mode,
            Err(message) => return Ok(top_error(INVALID_REQUEST, message, &self.config)),
        };
        if let Err(message) = validate_renew_fetch(
            request.is_renew_ack,
            request.max_wait_ms,
            request.min_bytes,
            request.max_bytes,
            request.max_records,
        ) {
            return Ok(top_error(INVALID_REQUEST, message, &self.config));
        }
        let session = match self
            .metadata
            .update_share_fetch_session(session_update(&request, &identity))
            .await
        {
            Ok(session) => session,
            Err(error) => {
                return Ok(top_error(
                    error_code(&error),
                    &error.to_string(),
                    &self.config,
                ));
            }
        };
        let topic_ids = request
            .topics
            .iter()
            .map(|topic| topic.topic_id)
            .chain(
                session
                    .partitions
                    .iter()
                    .map(|partition| partition.topic_id),
            )
            .collect::<Vec<_>>();
        let topic_accesses = match self.share_topic_accesses(context, topic_ids).await {
            Ok(accesses) => accesses,
            Err(error) => {
                return Ok(top_error(
                    UNKNOWN_SERVER_ERROR,
                    &error.to_string(),
                    &self.config,
                ));
            }
        };
        let acknowledgements = self
            .acknowledge_from_fetch(&request, version, &identity, &topic_accesses, &group_config)
            .await;
        if request.share_session_epoch == -1 || request.is_renew_ack {
            return Ok(acknowledgement_response(
                &acknowledgements,
                group_config.share_record_lock_duration_ms,
            ));
        }
        let min_bytes = usize::try_from(request.min_bytes.max(0)).unwrap_or(usize::MAX);
        let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms.max(0) as u64);
        loop {
            let (response, bytes, has_error) = self
                .share_fetch_once(
                    &request,
                    acquire_mode,
                    ShareFetchState {
                        identity: &identity,
                        session: &session,
                        acknowledgements: &acknowledgements,
                        group_config: &group_config,
                        topic_accesses: &topic_accesses,
                    },
                )
                .await?;
            if bytes > 0
                || bytes >= min_bytes
                || has_error
                || !acknowledgements.is_empty()
                || request.max_records <= 0
                || Instant::now() >= deadline
            {
                return Ok(response);
            }
            sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            )
            .await;
        }
    }

    async fn acknowledge_from_fetch(
        &self,
        request: &ShareFetchRequest,
        version: i16,
        identity: &ShareIdentity,
        topic_accesses: &ShareTopicAccesses,
        group_config: &GroupRuntimeConfig,
    ) -> AcknowledgeResult {
        let mut results = HashMap::new();
        for topic in &request.topics {
            let access = topic_accesses.get(&topic.topic_id);
            match access {
                Some(ShareTopicAccess::Missing) => {
                    for partition in &topic.partitions {
                        if !partition.acknowledgement_batches.is_empty() {
                            results.insert(
                                (topic.topic_id, partition.partition_index),
                                (UNKNOWN_TOPIC_ID, Some("topic ID was not found".to_owned())),
                            );
                        }
                    }
                    continue;
                }
                Some(ShareTopicAccess::MetadataError(error)) => {
                    for partition in &topic.partitions {
                        if !partition.acknowledgement_batches.is_empty() {
                            results.insert(
                                (topic.topic_id, partition.partition_index),
                                (UNKNOWN_SERVER_ERROR, Some(error.to_string())),
                            );
                        }
                    }
                    continue;
                }
                None => {
                    for partition in &topic.partitions {
                        if !partition.acknowledgement_batches.is_empty() {
                            results.insert(
                                (topic.topic_id, partition.partition_index),
                                (
                                    UNKNOWN_SERVER_ERROR,
                                    Some("topic authorization state is missing".to_owned()),
                                ),
                            );
                        }
                    }
                    continue;
                }
                Some(ShareTopicAccess::Allowed(_) | ShareTopicAccess::Denied) => {}
            }
            let authorized = matches!(access, Some(ShareTopicAccess::Allowed(_)));
            for partition in &topic.partitions {
                if partition.acknowledgement_batches.is_empty() {
                    continue;
                }
                let key = (topic.topic_id, partition.partition_index);
                if let Err(message) = validate_acknowledgement_types(
                    version,
                    request.is_renew_ack,
                    partition
                        .acknowledgement_batches
                        .iter()
                        .map(|batch| batch.acknowledge_types.as_slice()),
                ) {
                    results.insert(key, (INVALID_REQUEST, Some(message.to_owned())));
                    continue;
                }
                if !group_config.share_renew_acknowledge_enable
                    && has_renew_acknowledgement(
                        partition
                            .acknowledgement_batches
                            .iter()
                            .map(|batch| batch.acknowledge_types.as_slice()),
                    )
                {
                    results.insert(
                        key,
                        (
                            INVALID_RECORD_STATE,
                            Some(RENEW_DISABLED_MESSAGE.to_owned()),
                        ),
                    );
                    continue;
                }
                if !authorized {
                    results.insert(
                        key,
                        (
                            TOPIC_AUTHORIZATION_FAILED,
                            Some("topic authorization failed".to_owned()),
                        ),
                    );
                    continue;
                }
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
                results.insert(
                    key,
                    match result {
                        Ok(()) => (NO_ERROR, None),
                        Err(error) => (error_code(&error), Some(error.to_string())),
                    },
                );
            }
        }
        results
    }

    async fn share_fetch_once(
        &self,
        request: &ShareFetchRequest,
        acquire_mode: ShareAcquireMode,
        state: ShareFetchState<'_>,
    ) -> Result<(ShareFetchResponse, usize, bool)> {
        let max_bytes = usize::try_from(request.max_bytes.max(1))
            .unwrap_or(self.config.max_fetch_bytes)
            .min(self.config.max_fetch_bytes);
        let mut remaining_records =
            usize::try_from(request.max_records.max(0)).unwrap_or(usize::MAX);
        let mut remaining_bytes = max_bytes;
        let session_partitions = state
            .session
            .partitions
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut partitions = state.session.partitions.clone();
        for &(topic_id, partition) in state.acknowledgements.keys() {
            let candidate = ShareSessionPartition {
                topic_id,
                partition,
            };
            if !session_partitions.contains(&candidate) {
                partitions.push(candidate);
            }
        }
        partitions.sort_by_key(|partition| (partition.topic_id, partition.partition));
        partitions.dedup();

        let mut topics = HashMap::<uuid::Uuid, Vec<PartitionData>>::new();
        let mut returned_bytes = 0;
        let mut has_error = false;
        for partition in partitions {
            let ack = state
                .acknowledgements
                .get(&(partition.topic_id, partition.partition))
                .cloned()
                .unwrap_or((NO_ERROR, None));
            let mut response = partition_response(partition.partition, &ack);
            if !session_partitions.contains(&partition) || remaining_records == 0 {
                has_error |= ack.0 != NO_ERROR;
                topics.entry(partition.topic_id).or_default().push(response);
                continue;
            }
            let topic = match state.topic_accesses.get(&partition.topic_id) {
                Some(ShareTopicAccess::Allowed(topic)) => topic,
                Some(ShareTopicAccess::Missing) => {
                    response.error_code = UNKNOWN_TOPIC_ID;
                    response.error_message = Some(string("topic ID was not found"));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
                Some(ShareTopicAccess::MetadataError(error)) => {
                    response.error_code = UNKNOWN_SERVER_ERROR;
                    response.error_message = Some(string(error));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
                Some(ShareTopicAccess::Denied) => {
                    response.error_code = TOPIC_AUTHORIZATION_FAILED;
                    response.error_message = Some(string("topic authorization failed"));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
                None => {
                    response.error_code = UNKNOWN_SERVER_ERROR;
                    response.error_message = Some(string("topic authorization state is missing"));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
            };
            let partition_key = PartitionKey::new(&topic.name, partition.partition);
            let partition_state = match self
                .share_partition_state_for_fetch(
                    state.identity,
                    &partition,
                    &partition_key,
                    &state.group_config.share_auto_offset_reset,
                )
                .await
            {
                Ok(state) => state,
                Err((code, message)) => {
                    response.error_code = code;
                    response.error_message = Some(string(message));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
            };
            let fetched = self
                .metadata
                .fetch(
                    &partition_key,
                    partition_state.start_offset,
                    remaining_bytes.max(1),
                    if state.group_config.share_isolation_level == "read_committed" {
                        FetchIsolation::ReadCommitted
                    } else {
                        FetchIsolation::ReadUncommitted
                    },
                )
                .await;
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(error) => {
                    response.error_code = error_code(&error);
                    response.error_message = Some(string(error.to_string()));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
            };
            let candidate_limit = remaining_records.saturating_mul(16).clamp(1_024, 100_000);
            let decoded = match self
                .read_share_records(&fetched, partition_state.start_offset, candidate_limit)
                .await
            {
                Ok(decoded) => decoded,
                Err(error) => {
                    warn!(
                        topic = %topic.name,
                        partition = partition.partition,
                        %error,
                        "failed to read share fetch records"
                    );
                    response.error_code = KAFKA_STORAGE_ERROR;
                    response.error_message = Some(string(error.to_string()));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
            };
            let (candidates, acquisition_limit) =
                acquisition_candidates(&decoded, acquire_mode, remaining_records);
            if candidates.is_empty() {
                has_error |= ack.0 != NO_ERROR;
                topics.entry(partition.topic_id).or_default().push(response);
                continue;
            }
            let acquired = self
                .metadata
                .acquire_share_records(ShareAcquireRequest {
                    group_id: state.identity.group_id.clone(),
                    member_id: state.identity.member_id.clone(),
                    topic_id: partition.topic_id,
                    partition: partition.partition,
                    candidate_offsets: candidates,
                    max_records: acquisition_limit,
                    max_record_locks: usize::try_from(
                        state.group_config.share_partition_max_record_locks,
                    )
                    .unwrap_or(usize::MAX),
                    lock_duration_ms: state.group_config.share_record_lock_duration_ms,
                    delivery_count_limit: state.group_config.share_delivery_count_limit,
                })
                .await;
            let acquired = match acquired {
                Ok(acquired) => acquired,
                Err(error) => {
                    response.error_code = error_code(&error);
                    response.error_message = Some(string(error.to_string()));
                    has_error = true;
                    topics.entry(partition.topic_id).or_default().push(response);
                    continue;
                }
            };
            let selected = acquired
                .iter()
                .map(|record| (record.offset, record.delivery_count))
                .collect::<HashMap<_, _>>();
            let records = encode_selected(&decoded, &selected)?;
            remaining_records = remaining_records.saturating_sub(acquired.len());
            remaining_bytes = remaining_bytes.saturating_sub(records.len());
            returned_bytes += records.len();
            response.records = Some(records);
            let response_batch_size = match acquire_mode {
                ShareAcquireMode::BatchOptimized => {
                    usize::try_from(request.batch_size.max(1)).unwrap_or(1)
                }
                ShareAcquireMode::RecordLimit => usize::MAX,
            };
            response.acquired_records = acquired_ranges(&acquired, response_batch_size);
            has_error |= ack.0 != NO_ERROR;
            topics.entry(partition.topic_id).or_default().push(response);
        }
        let mut responses = topics
            .into_iter()
            .map(|(topic_id, mut partitions)| {
                partitions.sort_by_key(|partition| partition.partition_index);
                ShareFetchableTopicResponse::default()
                    .with_topic_id(topic_id)
                    .with_partitions(partitions)
            })
            .collect::<Vec<_>>();
        responses.sort_by_key(|topic| topic.topic_id);
        Ok((
            success_response(responses, state.group_config.share_record_lock_duration_ms),
            returned_bytes,
            has_error,
        ))
    }

    async fn share_partition_state_for_fetch(
        &self,
        identity: &ShareIdentity,
        partition: &ShareSessionPartition,
        partition_key: &PartitionKey,
        reset: &ShareOffsetReset,
    ) -> std::result::Result<SharePartitionState, (i16, String)> {
        let reset = match reset {
            ShareOffsetReset::Earliest => ShareAutoOffsetReset::Earliest,
            ShareOffsetReset::Latest => ShareAutoOffsetReset::Latest,
            ShareOffsetReset::ByDuration { .. } => {
                match self
                    .metadata
                    .existing_share_partition_state(
                        &identity.group_id,
                        &identity.member_id,
                        partition,
                    )
                    .await
                {
                    Ok(Some(state)) => return Ok(state),
                    Ok(None) => {}
                    Err(error) => return Err((error_code(&error), error.to_string())),
                }
                let target_timestamp = reset
                    .target_timestamp_ms(Utc::now())
                    .expect("duration reset has a target timestamp");
                let found = self
                    .lookup_offset(
                        partition_key,
                        target_timestamp,
                        FetchIsolation::ReadUncommitted,
                    )
                    .await
                    .map_err(|error| (lookup_error_code(&error), error.to_string()))?;
                if found.offset < 0 {
                    return Err((
                        OFFSET_NOT_AVAILABLE,
                        format!("no offset is available at or after timestamp {target_timestamp}"),
                    ));
                }
                ShareAutoOffsetReset::Exact(found.offset)
            }
        };
        self.metadata
            .share_partition_state(&identity.group_id, &identity.member_id, partition, reset)
            .await
            .map_err(|error| (error_code(&error), error.to_string()))
    }

    async fn read_share_records(
        &self,
        fetched: &PartitionFetch,
        start_offset: i64,
        candidate_limit: usize,
    ) -> Result<Vec<Vec<Record>>> {
        let mut decoded = Vec::new();
        let mut candidates = 0usize;
        for span in &fetched.spans {
            if candidates >= candidate_limit {
                break;
            }
            let raw = self
                .objects
                .get_range(&span.object_key, span.byte_start..span.byte_end)
                .await
                .context("read share records from object storage")?;
            let batches =
                decode_stored_record_batches(&raw, span.base_offset, span.offsets_preserved)?;
            for records in batches {
                let records = records
                    .into_iter()
                    .filter(|record| record.offset >= start_offset)
                    .collect::<Vec<_>>();
                candidates += records.len();
                if !records.is_empty() {
                    decoded.push(records);
                }
            }
        }
        Ok(decoded)
    }
}

fn session_update(
    request: &ShareFetchRequest,
    identity: &ShareIdentity,
) -> ShareFetchSessionUpdate {
    ShareFetchSessionUpdate {
        group_id: identity.group_id.clone(),
        member_id: identity.member_id.clone(),
        session_epoch: request.share_session_epoch,
        added: request
            .topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareSessionPartition {
                        topic_id: topic.topic_id,
                        partition: partition.partition_index,
                    })
            })
            .collect(),
        forgotten: request
            .forgotten_topics_data
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .copied()
                    .map(move |partition| ShareSessionPartition {
                        topic_id: topic.topic_id,
                        partition,
                    })
            })
            .collect(),
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

fn encode_selected(decoded: &[Vec<Record>], selected: &HashMap<i64, i16>) -> Result<Bytes> {
    let mut output = BytesMut::new();
    for records in decoded {
        let mut run = Vec::new();
        for record in records {
            if selected.contains_key(&record.offset) {
                if run
                    .last()
                    .is_some_and(|previous: &Record| previous.offset + 1 != record.offset)
                {
                    output.extend_from_slice(&encode_records(&run)?);
                    run.clear();
                }
                run.push(record.clone());
            } else if !run.is_empty() {
                output.extend_from_slice(&encode_records(&run)?);
                run.clear();
            }
        }
        if !run.is_empty() {
            output.extend_from_slice(&encode_records(&run)?);
        }
    }
    Ok(output.freeze())
}

fn acquired_ranges(
    records: &[rutomq_control::ShareAcquiredRecord],
    batch_size: usize,
) -> Vec<AcquiredRecords> {
    let mut ranges = Vec::new();
    for record in records {
        let can_extend = ranges.last().is_some_and(|range: &AcquiredRecords| {
            range.last_offset + 1 == record.offset
                && range.delivery_count == record.delivery_count
                && usize::try_from(range.last_offset - range.first_offset + 1)
                    .is_ok_and(|size| size < batch_size)
        });
        if can_extend {
            ranges.last_mut().expect("range exists").last_offset = record.offset;
        } else {
            ranges.push(
                AcquiredRecords::default()
                    .with_first_offset(record.offset)
                    .with_last_offset(record.offset)
                    .with_delivery_count(record.delivery_count),
            );
        }
    }
    ranges
}

fn partition_response(partition: i32, acknowledgement: &(i16, Option<String>)) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(NO_ERROR)
        .with_acknowledge_error_code(acknowledgement.0)
        .with_acknowledge_error_message(acknowledgement.1.as_deref().map(string))
        .with_current_leader(
            LeaderIdAndEpoch::default()
                .with_leader_id(0)
                .with_leader_epoch(VIRTUAL_LEADER_EPOCH),
        )
        .with_records(Some(Bytes::new()))
}

fn success_response(
    responses: Vec<ShareFetchableTopicResponse>,
    lock_duration_ms: i32,
) -> ShareFetchResponse {
    ShareFetchResponse::default()
        .with_error_code(NO_ERROR)
        .with_acquisition_lock_timeout_ms(lock_duration_ms)
        .with_responses(responses)
}

fn acknowledgement_response(
    acknowledgements: &AcknowledgeResult,
    lock_duration_ms: i32,
) -> ShareFetchResponse {
    let mut topics = HashMap::<uuid::Uuid, Vec<PartitionData>>::new();
    for (&(topic_id, partition), acknowledgement) in acknowledgements {
        topics
            .entry(topic_id)
            .or_default()
            .push(partition_response(partition, acknowledgement));
    }
    let mut responses = topics
        .into_iter()
        .map(|(topic_id, mut partitions)| {
            partitions.sort_by_key(|partition| partition.partition_index);
            ShareFetchableTopicResponse::default()
                .with_topic_id(topic_id)
                .with_partitions(partitions)
        })
        .collect::<Vec<_>>();
    responses.sort_by_key(|topic| topic.topic_id);
    success_response(responses, lock_duration_ms)
}

fn top_error(error: i16, message: &str, config: &crate::AgentConfig) -> ShareFetchResponse {
    ShareFetchResponse::default()
        .with_error_code(error)
        .with_error_message(Some(string(message)))
        .with_acquisition_lock_timeout_ms(config.share_record_lock_duration_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_control::ShareAcquiredRecord;

    #[test]
    fn groups_acquired_offsets_by_contiguity_delivery_and_batch_size() {
        let ranges = acquired_ranges(
            &[
                ShareAcquiredRecord {
                    offset: 1,
                    delivery_count: 1,
                },
                ShareAcquiredRecord {
                    offset: 2,
                    delivery_count: 1,
                },
                ShareAcquiredRecord {
                    offset: 4,
                    delivery_count: 2,
                },
            ],
            2,
        );
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].first_offset, ranges[0].last_offset), (1, 2));
        assert_eq!(ranges[1].delivery_count, 2);
    }
}
