use super::authorization::AuthorizationContext;
use super::*;
use crate::batcher::ProduceFlushPolicy;
use crate::kafka_error::{
    INVALID_RECORD, INVALID_REQUIRED_ACKS, INVALID_TXN_STATE, NOT_ENOUGH_REPLICAS,
    TOPIC_AUTHORIZATION_FAILED, TRANSACTIONAL_ID_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
    UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use crate::record_admission::admit_records_for_version;
use crate::records::analyze_records;
use chrono::Utc;
use kafka_protocol::messages::ProduceResponse;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use rutomq_control::{AclOperation, AclResourceType, PartitionKey, ProducerSession};

impl Broker {
    pub(super) async fn handle_produce(
        &self,
        mut request: ProduceRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<ProduceResponse> {
        if !matches!(request.acks, -1..=1) {
            return Ok(produce_error(
                &request.topic_data,
                version,
                INVALID_REQUIRED_ACKS,
                "required acks must be -1, 0, or 1",
            ));
        }
        let has_transactional_records = has_transactional_records(&request.topic_data);
        if has_transactional_records {
            let Some(transactional_id) = request.transactional_id.as_ref() else {
                return Ok(produce_error(
                    &request.topic_data,
                    version,
                    TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    "transactional records require a transactional ID",
                ));
            };
            let authorization = self
                .authorized(
                    context,
                    AclResourceType::TransactionalId,
                    transactional_id.as_str(),
                    AclOperation::Write,
                )
                .await;
            match authorization {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(produce_error(
                        &request.topic_data,
                        version,
                        TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                        "transactional ID authorization failed",
                    ));
                }
                Err(_) => {
                    return Ok(produce_error(
                        &request.topic_data,
                        version,
                        UNKNOWN_SERVER_ERROR,
                        "authorization backend failed",
                    ));
                }
            }
        }
        let verify_transaction_partition = if has_transactional_records && version < 12 {
            match self.transaction_partition_verification_enabled().await {
                Ok(enabled) => enabled,
                Err(_) => {
                    return Ok(produce_error(
                        &request.topic_data,
                        version,
                        UNKNOWN_SERVER_ERROR,
                        "broker configuration lookup failed",
                    ));
                }
            }
        } else {
            true
        };

        let original_topics = std::mem::take(&mut request.topic_data);
        let request_topics = original_topics.clone();
        let mut allowed = Vec::with_capacity(original_topics.len());
        let mut denied = Vec::new();
        let mut resolved = Vec::with_capacity(original_topics.len());
        for topic in original_topics {
            if version >= 13 {
                match self.metadata.topic_by_id(topic.topic_id).await {
                    Ok(Some(info)) => resolved.push((topic, info.name.clone(), Some(info))),
                    Ok(None) => denied.push(topic_error(
                        &topic,
                        version,
                        UNKNOWN_TOPIC_ID,
                        "unknown topic ID",
                    )),
                    Err(_) => {
                        return Ok(produce_error(
                            &request_topics,
                            version,
                            UNKNOWN_SERVER_ERROR,
                            "metadata lookup failed",
                        ));
                    }
                }
            } else {
                let name = topic.name.as_str().to_owned();
                resolved.push((topic, name, None));
            }
        }
        let topic_names = resolved
            .iter()
            .map(|(_, name, _)| name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Write)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(_) => {
                return Ok(produce_error(
                    &request_topics,
                    version,
                    UNKNOWN_SERVER_ERROR,
                    "authorization backend failed",
                ));
            }
        };

        let mut flush_policy = ProduceFlushPolicy::default();
        let mut transaction_producer = None;
        let mut transaction_partitions = Vec::new();
        let mut mixed_transaction_producers = false;
        let now_ms = Utc::now().timestamp_millis();
        for (mut topic, name, resolved_info) in resolved {
            if !authorizations.get(&name).copied().unwrap_or(false) {
                denied.push(topic_error(
                    &topic,
                    version,
                    TOPIC_AUTHORIZATION_FAILED,
                    "topic authorization failed",
                ));
                continue;
            }
            let topic_info = match resolved_info {
                Some(info) => Some(info),
                None => match self.metadata.topic(&name).await {
                    Ok(info) => info,
                    Err(_) => {
                        return Ok(produce_error(
                            &request_topics,
                            version,
                            UNKNOWN_SERVER_ERROR,
                            "metadata lookup failed",
                        ));
                    }
                },
            };
            let Some(topic_info) = topic_info else {
                denied.push(topic_error(
                    &topic,
                    version,
                    UNKNOWN_TOPIC_OR_PARTITION,
                    "unknown topic or partition",
                ));
                continue;
            };
            let config = match self.metadata.topic_config(&name).await {
                Ok(config) => config,
                Err(_) => {
                    return Ok(produce_error(
                        &request_topics,
                        version,
                        UNKNOWN_SERVER_ERROR,
                        "topic configuration lookup failed",
                    ));
                }
            };
            if request.acks == -1 && config.min_insync_replicas > 1 {
                denied.push(topic_error(
                    &topic,
                    version,
                    NOT_ENOUGH_REPLICAS,
                    "the single virtual ISR does not satisfy min.insync.replicas",
                ));
                continue;
            }
            let mut accepted_partitions = Vec::with_capacity(topic.partition_data.len());
            let mut rejected_partitions = Vec::new();
            for mut partition in std::mem::take(&mut topic.partition_data) {
                if partition.index < 0 || partition.index >= topic_info.partitions {
                    rejected_partitions.push(
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_base_offset(-1)
                            .with_log_append_time_ms(-1)
                            .with_log_start_offset(-1)
                            .with_error_message(Some(StrBytes::from_static_str(
                                "unknown topic or partition",
                            ))),
                    );
                    continue;
                }
                let records = partition.records.clone().unwrap_or_default();
                match admit_records_for_version(&records, &config, now_ms, version) {
                    Ok(records) => {
                        let metadata = match analyze_records(&records) {
                            Ok(metadata) => metadata,
                            Err(error) => {
                                rejected_partitions.push(
                                    PartitionProduceResponse::default()
                                        .with_index(partition.index)
                                        .with_error_code(INVALID_RECORD)
                                        .with_base_offset(-1)
                                        .with_log_append_time_ms(-1)
                                        .with_log_start_offset(-1)
                                        .with_error_message(Some(StrBytes::from_string(
                                            error.to_string(),
                                        ))),
                                );
                                continue;
                            }
                        };
                        let record_count = metadata.record_count;
                        flush_policy.add_partition(
                            PartitionKey::new(&name, partition.index),
                            record_count,
                            config.flush_messages,
                            config.flush_ms,
                        );
                        if metadata.transactional
                            && request.transactional_id.is_some()
                            && version >= 12
                            && let Some(producer) = metadata.producer
                        {
                            let producer = ProducerSession {
                                producer_id: producer.producer_id,
                                producer_epoch: producer.producer_epoch,
                            };
                            mixed_transaction_producers |=
                                transaction_producer.is_some_and(|current| current != producer);
                            transaction_producer.get_or_insert(producer);
                            transaction_partitions.push(PartitionKey::new(&name, partition.index));
                        }
                        partition.records = Some(records);
                        accepted_partitions.push(partition);
                    }
                    Err(error) => rejected_partitions.push(
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_error_code(error.code)
                            .with_base_offset(-1)
                            .with_log_append_time_ms(-1)
                            .with_log_start_offset(-1)
                            .with_error_message(Some(StrBytes::from_string(error.to_string()))),
                    ),
                }
            }
            if !rejected_partitions.is_empty() {
                denied.push(topic_response(&topic, version, rejected_partitions));
            }
            if !accepted_partitions.is_empty() {
                topic.partition_data = accepted_partitions;
                allowed.push(topic);
            }
        }

        request.topic_data = allowed;
        if mixed_transaction_producers {
            let mut response = produce_error(
                &request.topic_data,
                version,
                INVALID_TXN_STATE,
                "transactional records contain more than one producer identity",
            );
            merge_topic_errors(&mut response, denied, version);
            return Ok(response);
        }
        if let (Some(transactional_id), Some(producer)) =
            (request.transactional_id.as_ref(), transaction_producer)
            && let Err(error) = self
                .metadata
                .add_partitions_to_transaction(
                    transactional_id.as_str(),
                    producer,
                    &transaction_partitions,
                    false,
                )
                .await
        {
            let mut response = produce_error(
                &request.topic_data,
                version,
                control_error_code(&error),
                &error.to_string(),
            );
            merge_topic_errors(&mut response, denied, version);
            return Ok(response);
        }
        let mut response = if request.topic_data.is_empty() {
            ProduceResponse::default()
        } else {
            self.batcher
                .submit(request, version, flush_policy, verify_transaction_partition)
                .await?
        };
        merge_topic_errors(&mut response, denied, version);
        Ok(response)
    }
}

fn has_transactional_records(topics: &[TopicProduceData]) -> bool {
    topics.iter().any(|topic| {
        topic.partition_data.iter().any(|partition| {
            partition
                .records
                .as_ref()
                .and_then(|records| analyze_records(records).ok())
                .is_some_and(|metadata| metadata.transactional)
        })
    })
}

fn produce_error(
    topics: &[TopicProduceData],
    version: i16,
    error_code: i16,
    message: &str,
) -> ProduceResponse {
    ProduceResponse::default().with_responses(
        topics
            .iter()
            .map(|topic| topic_error(topic, version, error_code, message))
            .collect(),
    )
}

fn topic_error(
    topic: &TopicProduceData,
    version: i16,
    error_code: i16,
    message: &str,
) -> TopicProduceResponse {
    let response = TopicProduceResponse::default().with_partition_responses(
        topic
            .partition_data
            .iter()
            .map(|partition| {
                PartitionProduceResponse::default()
                    .with_index(partition.index)
                    .with_error_code(error_code)
                    .with_base_offset(-1)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(-1)
                    .with_error_message(Some(StrBytes::from_string(message.to_owned())))
            })
            .collect(),
    );
    if version >= 13 {
        response.with_topic_id(topic.topic_id)
    } else {
        response.with_name(topic.name.clone())
    }
}

fn topic_response(
    topic: &TopicProduceData,
    version: i16,
    partitions: Vec<PartitionProduceResponse>,
) -> TopicProduceResponse {
    let response = TopicProduceResponse::default().with_partition_responses(partitions);
    if version >= 13 {
        response.with_topic_id(topic.topic_id)
    } else {
        response.with_name(topic.name.clone())
    }
}

fn merge_topic_errors(
    response: &mut ProduceResponse,
    errors: Vec<TopicProduceResponse>,
    version: i16,
) {
    for mut error in errors {
        let existing = response.responses.iter_mut().find(|topic| {
            if version >= 13 {
                topic.topic_id == error.topic_id
            } else {
                topic.name == error.name
            }
        });
        if let Some(existing) = existing {
            existing
                .partition_responses
                .append(&mut error.partition_responses);
        } else {
            response.responses.push(error);
        }
    }
}
