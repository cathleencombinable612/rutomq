use super::Broker;
use super::authorization::AuthorizationContext;
use super::fetch_session::FetchSessionToken;
use super::partition_state_api::{VIRTUAL_LEADER_EPOCH, current_epoch_error};
use crate::kafka_error::{
    FENCED_LEADER_EPOCH, INVALID_REQUEST, KAFKA_STORAGE_ERROR, NO_ERROR, NOT_LEADER_OR_FOLLOWER,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION,
    control_error_code,
};
use crate::object_integrity;
use crate::records::materialize_records;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::{
    FetchableTopicResponse, LeaderIdAndEpoch, NodeEndpoint, PartitionData,
};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse};
use rutomq_control::{
    AclOperation, ControlError, FetchIsolation, PartitionFetch, PartitionKey, TopicInfo,
};
use tokio::time::{Duration, Instant, sleep};
use tracing::warn;

pub(super) struct FetchResult {
    pub(super) response: FetchResponse,
    pub(super) session: FetchSessionToken,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum FetchTopicKey {
    Name(String),
    Id(uuid::Uuid),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FetchPartitionKey {
    topic: FetchTopicKey,
    partition: i32,
}

#[derive(Clone)]
struct FetchTopicDescriptor {
    request: FetchTopic,
    info: TopicInfo,
}

struct PreparedFetchPartition {
    descriptor: FetchTopicDescriptor,
    request: FetchPartition,
}

struct DeferredFetchPartition {
    descriptor: FetchTopicDescriptor,
    response: PartitionData,
}

struct ConsumerFetchPlan {
    partitions: Vec<PreparedFetchPartition>,
    errors: Vec<DeferredFetchPartition>,
}

struct ResolvableFetchTopic {
    name: Option<String>,
    info: Option<TopicInfo>,
}

struct NormalizedFetchPartition {
    topic: FetchTopic,
    partition: FetchPartition,
}

impl Broker {
    pub(super) async fn handle_fetch(
        &self,
        request: FetchRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<FetchResult> {
        if is_follower_request(&request, version) {
            return Ok(FetchResult {
                response: self
                    .fetch_response(version, Vec::new())
                    .with_error_code(INVALID_REQUEST),
                session: FetchSessionToken::Sessionless,
            });
        }
        let error_request = request.clone();
        let prepared = match self.fetch_sessions.prepare(request, version) {
            Ok(prepared) => prepared,
            Err(error_code) => {
                return Ok(FetchResult {
                    response: self
                        .fetch_response(version, Vec::new())
                        .with_error_code(error_code),
                    session: FetchSessionToken::Sessionless,
                });
            }
        };
        let request = prepared.request;
        let plan = match self
            .prepare_consumer_fetch(&request, version, context)
            .await
        {
            Ok(plan) => plan,
            Err(error_code) => {
                self.fetch_sessions.abort_preflight(prepared.token);
                return Ok(FetchResult {
                    response: fetch_request_error(&error_request, version, error_code),
                    session: FetchSessionToken::Sessionless,
                });
            }
        };
        let min_bytes = usize::try_from(request.min_bytes.max(0)).unwrap_or(usize::MAX);
        let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms.max(0) as u64);
        loop {
            let (mut response, bytes, has_fetch_error) =
                self.fetch_once(&request, &plan, version).await?;
            if plan.partitions.is_empty()
                || bytes >= min_bytes
                || has_fetch_error
                || Instant::now() >= deadline
            {
                self.fetch_sessions
                    .shape_response(prepared.token, &mut response);
                return Ok(FetchResult {
                    response,
                    session: prepared.token,
                });
            }
            sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            )
            .await;
        }
    }

    async fn prepare_consumer_fetch(
        &self,
        request: &FetchRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> std::result::Result<ConsumerFetchPlan, i16> {
        let partitions = normalize_fetch_partitions(&request.topics, version);
        let mut topic_requests = IndexMap::new();
        for partition in &partitions {
            topic_requests
                .entry(fetch_topic_key(&partition.topic, version))
                .or_insert_with(|| partition.topic.clone());
        }

        let mut resolvable = IndexMap::with_capacity(topic_requests.len());
        for (key, topic) in topic_requests {
            if version >= 13 {
                match self.metadata.topic_by_id(topic.topic_id).await {
                    Ok(Some(info)) => {
                        resolvable.insert(
                            key,
                            ResolvableFetchTopic {
                                name: Some(info.name.clone()),
                                info: Some(info),
                            },
                        );
                    }
                    Ok(None) => {
                        resolvable.insert(
                            key,
                            ResolvableFetchTopic {
                                name: None,
                                info: None,
                            },
                        );
                    }
                    Err(_) => return Err(UNKNOWN_SERVER_ERROR),
                }
            } else {
                resolvable.insert(
                    key,
                    ResolvableFetchTopic {
                        name: Some(topic.topic.as_str().to_owned()),
                        info: None,
                    },
                );
            }
        }

        let names = resolvable
            .values()
            .filter_map(|topic| topic.name.as_deref())
            .collect::<Vec<_>>();
        let authorizations = self
            .topic_authorizations(context, &names, AclOperation::Read)
            .await
            .map_err(|_| UNKNOWN_SERVER_ERROR)?;

        let mut topic_infos = IndexMap::new();
        for (key, topic) in &resolvable {
            let Some(name) = topic.name.as_deref() else {
                continue;
            };
            if !authorizations.get(name).copied().unwrap_or(false) {
                continue;
            }
            let info = match topic.info.clone() {
                Some(info) => Some(info),
                None => self
                    .metadata
                    .topic(name)
                    .await
                    .map_err(|_| UNKNOWN_SERVER_ERROR)?,
            };
            topic_infos.insert(key.clone(), info);
        }

        let mut unknown_id_errors = Vec::new();
        let mut known_partitions = Vec::new();
        for partition in partitions {
            let key = fetch_topic_key(&partition.topic, version);
            let topic = resolvable
                .get(&key)
                .expect("normalized topic must have resolution state");
            if topic.name.is_none() {
                let descriptor = FetchTopicDescriptor {
                    info: unresolved_topic_info(&partition.topic),
                    request: partition.topic,
                };
                unknown_id_errors.push(DeferredFetchPartition {
                    descriptor,
                    response: partition_response(&partition.partition)
                        .with_error_code(UNKNOWN_TOPIC_ID),
                });
            } else {
                known_partitions.push(partition);
            }
        }

        let mut prepared = Vec::with_capacity(known_partitions.len());
        let mut errors = unknown_id_errors;
        for partition in known_partitions {
            let key = fetch_topic_key(&partition.topic, version);
            let topic = resolvable
                .get(&key)
                .expect("known topic must have resolution state");
            let name = topic.name.as_deref().expect("known topic must have name");
            if !authorizations.get(name).copied().unwrap_or(false) {
                let descriptor = FetchTopicDescriptor {
                    info: topic
                        .info
                        .clone()
                        .unwrap_or_else(|| unresolved_topic_info(&partition.topic)),
                    request: partition.topic,
                };
                errors.push(DeferredFetchPartition {
                    descriptor,
                    response: partition_response(&partition.partition)
                        .with_error_code(TOPIC_AUTHORIZATION_FAILED),
                });
                continue;
            }

            let Some(info) = topic_infos
                .get(&key)
                .expect("authorized topic must have metadata preflight")
                .clone()
            else {
                let descriptor = FetchTopicDescriptor {
                    info: unresolved_topic_info(&partition.topic),
                    request: partition.topic,
                };
                errors.push(DeferredFetchPartition {
                    descriptor,
                    response: partition_response(&partition.partition)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                });
                continue;
            };

            let descriptor = FetchTopicDescriptor {
                request: partition.topic,
                info,
            };
            if partition.partition.partition < 0
                || partition.partition.partition >= descriptor.info.partitions
            {
                errors.push(DeferredFetchPartition {
                    descriptor,
                    response: partition_response(&partition.partition)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                });
            } else {
                prepared.push(PreparedFetchPartition {
                    descriptor,
                    request: partition.partition,
                });
            }
        }

        Ok(ConsumerFetchPlan {
            partitions: prepared,
            errors,
        })
    }

    async fn fetch_once(
        &self,
        request: &FetchRequest,
        plan: &ConsumerFetchPlan,
        version: i16,
    ) -> Result<(FetchResponse, usize, bool)> {
        let max_bytes = usize::try_from(request.max_bytes.max(1))
            .unwrap_or(self.config.max_fetch_bytes)
            .min(self.config.max_fetch_bytes);
        let isolation = if request.isolation_level == 1 {
            FetchIsolation::ReadCommitted
        } else {
            FetchIsolation::ReadUncommitted
        };
        let mut remaining = max_bytes;
        let mut returned_bytes = 0;
        let mut has_fetch_error = false;
        let mut response_topics = Vec::with_capacity(plan.partitions.len() + plan.errors.len());
        let mut last_response_key = None;
        for partition in &plan.partitions {
            let requested = &partition.request;
            let epoch_error = current_epoch_error(requested.current_leader_epoch);
            if epoch_error != NO_ERROR {
                has_fetch_error = true;
                push_fetch_partition(
                    &mut response_topics,
                    &mut last_response_key,
                    &partition.descriptor,
                    version,
                    epoch_error_response(requested, epoch_error, version),
                );
                continue;
            }
            let partition_limit = usize::try_from(requested.partition_max_bytes.max(1))
                .unwrap_or(remaining)
                .min(remaining);
            let allow_records = remaining > 0;
            let fetched = self
                .metadata
                .fetch(
                    &PartitionKey::new(&partition.descriptor.info.name, requested.partition),
                    requested.fetch_offset,
                    partition_limit,
                    isolation,
                )
                .await;
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(error) => {
                    has_fetch_error = true;
                    push_fetch_partition(
                        &mut response_topics,
                        &mut last_response_key,
                        &partition.descriptor,
                        version,
                        fetch_control_error(requested, error),
                    );
                    continue;
                }
            };
            let records = if allow_records {
                match self.read_fetch_records(&fetched).await {
                    Ok(records) => records,
                    Err(error) => {
                        warn!(
                            topic = %partition.descriptor.info.name,
                            partition = requested.partition,
                            %error,
                            "failed to read fetch records"
                        );
                        has_fetch_error = true;
                        push_fetch_partition(
                            &mut response_topics,
                            &mut last_response_key,
                            &partition.descriptor,
                            version,
                            partition_response(requested)
                                .with_error_code(storage_error_code(version))
                                .with_high_watermark(fetched.high_watermark)
                                .with_last_stable_offset(fetched.last_stable_offset)
                                .with_log_start_offset(fetched.log_start_offset),
                        );
                        continue;
                    }
                }
            } else {
                Bytes::new()
            };
            returned_bytes += records.len();
            remaining = remaining.saturating_sub(records.len());
            push_fetch_partition(
                &mut response_topics,
                &mut last_response_key,
                &partition.descriptor,
                version,
                partition_response(requested)
                    .with_high_watermark(fetched.high_watermark)
                    .with_last_stable_offset(fetched.last_stable_offset)
                    .with_log_start_offset(fetched.log_start_offset)
                    .with_records(Some(records)),
            );
        }
        for deferred in &plan.errors {
            push_fetch_partition(
                &mut response_topics,
                &mut last_response_key,
                &deferred.descriptor,
                version,
                deferred.response.clone(),
            );
        }
        Ok((
            self.fetch_response(version, response_topics),
            returned_bytes,
            has_fetch_error,
        ))
    }

    fn fetch_response(
        &self,
        version: i16,
        responses: Vec<FetchableTopicResponse>,
    ) -> FetchResponse {
        let response = FetchResponse::default().with_responses(responses);
        if version >= 16
            && response.responses.iter().any(|topic| {
                topic.partitions.iter().any(|partition| {
                    matches!(
                        partition.error_code,
                        NOT_LEADER_OR_FOLLOWER | FENCED_LEADER_EPOCH
                    ) && partition.current_leader.leader_id.0 >= 0
                })
            })
        {
            response.with_node_endpoints(vec![
                NodeEndpoint::default()
                    .with_node_id(BrokerId::from(0))
                    .with_host(self.config.advertise_host.clone().into())
                    .with_port(self.config.advertise_port)
                    .with_rack(None),
            ])
        } else {
            response
        }
    }

    async fn read_fetch_records(&self, fetched: &PartitionFetch) -> Result<Bytes> {
        let mut records = BytesMut::new();
        for span in &fetched.spans {
            let range = span.byte_start..span.byte_end;
            let raw = if let Some(raw) = self.fetch_cache.get(&span.object_key, &range) {
                self.metrics.fetch_cache_hits.inc();
                self.verify_fetch_span(span, &raw)?;
                raw
            } else {
                self.metrics.fetch_cache_misses.inc();
                let raw = self
                    .objects
                    .get_range(&span.object_key, range.clone())
                    .await
                    .context("read Kafka records from object storage")?;
                self.verify_fetch_span(span, &raw)?;
                let update = self
                    .fetch_cache
                    .insert(&span.object_key, &range, raw.clone());
                self.metrics.fetch_cache_evictions.inc_by(update.evictions);
                self.metrics
                    .fetch_cache_bytes
                    .set(i64::try_from(update.bytes).unwrap_or(i64::MAX));
                raw
            };
            records.extend_from_slice(&materialize_records(
                &raw,
                span.base_offset,
                span.offsets_preserved,
            )?);
        }
        Ok(records.freeze())
    }

    fn verify_fetch_span(&self, span: &rutomq_control::StoredSpan, raw: &[u8]) -> Result<()> {
        object_integrity::verify(span, raw).inspect_err(|_| {
            self.metrics.object_integrity_failures.inc();
        })
    }
}

fn is_follower_request(request: &FetchRequest, version: i16) -> bool {
    if version >= 15 {
        request.replica_state.replica_id.0 >= 0
    } else {
        request.replica_id.0 >= 0
    }
}

fn partition_response(partition: &FetchPartition) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition.partition)
        .with_error_code(NO_ERROR)
        .with_high_watermark(-1)
}

fn epoch_error_response(partition: &FetchPartition, error: i16, version: i16) -> PartitionData {
    let response = partition_response(partition).with_error_code(error);
    if version >= 16 && matches!(error, NOT_LEADER_OR_FOLLOWER | FENCED_LEADER_EPOCH) {
        response.with_current_leader(
            LeaderIdAndEpoch::default()
                .with_leader_id(BrokerId::from(0))
                .with_leader_epoch(VIRTUAL_LEADER_EPOCH),
        )
    } else {
        response
    }
}

fn fetch_control_error(partition: &FetchPartition, error: ControlError) -> PartitionData {
    let response = partition_response(partition).with_error_code(control_error_code(&error));
    match error {
        ControlError::OffsetOutOfRange { start, end, .. } => response
            .with_high_watermark(end)
            .with_last_stable_offset(end)
            .with_log_start_offset(start),
        _ => response,
    }
}

fn fetch_topic_error(topic: &FetchTopic, version: i16, error: i16) -> FetchableTopicResponse {
    fetch_topic_response(
        topic,
        &unresolved_topic_info(topic),
        version,
        topic
            .partitions
            .iter()
            .map(|partition| partition_response(partition).with_error_code(error))
            .collect(),
    )
}

fn fetch_topic_response(
    request: &FetchTopic,
    topic: &TopicInfo,
    version: i16,
    partitions: Vec<PartitionData>,
) -> FetchableTopicResponse {
    let response = FetchableTopicResponse::default().with_partitions(partitions);
    if version >= 13 {
        response.with_topic_id(topic.id)
    } else {
        response.with_topic(request.topic.clone())
    }
}

fn fetch_request_error(request: &FetchRequest, version: i16, error: i16) -> FetchResponse {
    let responses = if version < 13 {
        request
            .topics
            .iter()
            .map(|topic| fetch_topic_error(topic, version, error))
            .collect()
    } else {
        Vec::new()
    };
    FetchResponse::default()
        .with_error_code(error)
        .with_session_id(request.session_id)
        .with_responses(responses)
}

fn normalize_fetch_partitions(
    topics: &[FetchTopic],
    version: i16,
) -> Vec<NormalizedFetchPartition> {
    let mut normalized = IndexMap::<FetchPartitionKey, NormalizedFetchPartition>::new();
    for topic in topics {
        for partition in &topic.partitions {
            let mut template = topic.clone();
            template.partitions.clear();
            normalized.insert(
                FetchPartitionKey {
                    topic: fetch_topic_key(topic, version),
                    partition: partition.partition,
                },
                NormalizedFetchPartition {
                    topic: template,
                    partition: partition.clone(),
                },
            );
        }
    }
    normalized.into_values().collect()
}

fn fetch_topic_key(topic: &FetchTopic, version: i16) -> FetchTopicKey {
    if version >= 13 {
        FetchTopicKey::Id(topic.topic_id)
    } else {
        FetchTopicKey::Name(topic.topic.as_str().to_owned())
    }
}

fn unresolved_topic_info(topic: &FetchTopic) -> TopicInfo {
    TopicInfo {
        id: topic.topic_id,
        name: topic.topic.as_str().to_owned(),
        partitions: topic.partitions.len() as i32,
    }
}

fn push_fetch_partition(
    topics: &mut Vec<FetchableTopicResponse>,
    last_key: &mut Option<FetchTopicKey>,
    descriptor: &FetchTopicDescriptor,
    version: i16,
    partition: PartitionData,
) {
    let key = fetch_topic_key(&descriptor.request, version);
    if last_key.as_ref() == Some(&key) {
        topics
            .last_mut()
            .expect("a response key requires a response topic")
            .partitions
            .push(partition);
        return;
    }
    topics.push(fetch_topic_response(
        &descriptor.request,
        &descriptor.info,
        version,
        vec![partition],
    ));
    *last_key = Some(key);
}

fn storage_error_code(version: i16) -> i16 {
    if version <= 5 {
        NOT_LEADER_OR_FOLLOWER
    } else {
        KAFKA_STORAGE_ERROR
    }
}
