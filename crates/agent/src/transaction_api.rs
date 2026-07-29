use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, GROUP_AUTHORIZATION_FAILED, ILLEGAL_GENERATION,
    INVALID_PRODUCER_EPOCH, INVALID_REQUEST, INVALID_TRANSACTION_TIMEOUT, NO_ERROR,
    OFFSET_METADATA_TOO_LARGE, OPERATION_NOT_ATTEMPTED, PRODUCER_FENCED,
    TOPIC_AUTHORIZATION_FAILED, TRANSACTION_COORDINATOR_FENCED,
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED, TRANSACTIONAL_ID_NOT_FOUND, UNKNOWN_SERVER_ERROR,
    UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::describe_transactions_response::{
    TopicData, TransactionState as DescribedTransactionState,
};
use kafka_protocol::messages::list_transactions_response::TransactionState as ListedTransactionState;
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};
use rutomq_control::{
    ControlError, OffsetCommit, PartitionKey, ProducerSession, TransactionDescription,
};
use std::collections::{BTreeMap, HashMap, HashSet};

impl Broker {
    pub(super) async fn handle_init_producer_id(
        &self,
        request: InitProducerIdRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> InitProducerIdResponse {
        let transactional_id = request
            .transactional_id
            .as_ref()
            .map(|value| value.as_str());
        let authorization = if let Some(transactional_id) = transactional_id {
            match self
                .authorized(
                    context,
                    AclResourceType::TransactionalId,
                    transactional_id,
                    AclOperation::Write,
                )
                .await
            {
                Ok(true)
                    if request.enable_2_pc && !self.config.transaction_two_phase_commit_enable =>
                {
                    Ok(TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                }
                Ok(true) if request.enable_2_pc => self
                    .authorized(
                        context,
                        AclResourceType::TransactionalId,
                        transactional_id,
                        AclOperation::TwoPhaseCommit,
                    )
                    .await
                    .map(|allowed| {
                        if allowed {
                            NO_ERROR
                        } else {
                            TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                        }
                    }),
                Ok(true) => Ok(NO_ERROR),
                Ok(false) => Ok(TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                Err(error) => Err(error),
            }
        } else {
            match self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    authorization::CLUSTER_RESOURCE_NAME,
                    AclOperation::IdempotentWrite,
                )
                .await
            {
                Ok(true) => Ok(NO_ERROR),
                Ok(false) => self
                    .authorized_by_resource_type(
                        context,
                        AclResourceType::Topic,
                        AclOperation::Write,
                    )
                    .await
                    .map(|allowed| {
                        if allowed {
                            NO_ERROR
                        } else {
                            CLUSTER_AUTHORIZATION_FAILED
                        }
                    }),
                Err(error) => Err(error),
            }
        };
        match authorization {
            Ok(NO_ERROR) => {}
            Ok(error_code) => {
                return InitProducerIdResponse::default().with_error_code(error_code);
            }
            Err(_) => {
                return InitProducerIdResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        }
        if transactional_id.is_some()
            && !request.enable_2_pc
            && request.transaction_timeout_ms > self.config.transaction_max_timeout_ms
        {
            return InitProducerIdResponse::default().with_error_code(INVALID_TRANSACTION_TIMEOUT);
        }
        let current = match (request.producer_id.0 == -1, request.producer_epoch == -1) {
            (true, true) => None,
            (true, false) | (false, true) => {
                return InitProducerIdResponse::default().with_error_code(INVALID_REQUEST);
            }
            (false, false) => Some(ProducerSession {
                producer_id: request.producer_id.0,
                producer_epoch: request.producer_epoch,
            }),
        };
        match self
            .metadata
            .init_producer_with_options(
                transactional_id,
                request.transaction_timeout_ms,
                current,
                request.enable_2_pc,
                request.keep_prepared_txn,
            )
            .await
        {
            Ok(initialization) => {
                let ongoing = initialization
                    .ongoing_transaction
                    .unwrap_or(ProducerSession {
                        producer_id: -1,
                        producer_epoch: -1,
                    });
                InitProducerIdResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_producer_id(initialization.producer.producer_id.into())
                    .with_producer_epoch(initialization.producer.producer_epoch)
                    .with_ongoing_txn_producer_id(ongoing.producer_id.into())
                    .with_ongoing_txn_producer_epoch(ongoing.producer_epoch)
            }
            Err(error) => InitProducerIdResponse::default()
                .with_error_code(versioned_producer_error_code(&error, version, 4)),
        }
    }

    pub(super) async fn handle_add_partitions_to_txn(
        &self,
        request: AddPartitionsToTxnRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> AddPartitionsToTxnResponse {
        if version <= 3 {
            let topics = request.v3_and_below_topics;
            let transactional_id = request.v3_and_below_transactional_id.as_str();
            let producer = ProducerSession {
                producer_id: request.v3_and_below_producer_id.0,
                producer_epoch: request.v3_and_below_producer_epoch,
            };
            let transaction_authorized = match self
                .authorized(
                    context,
                    AclResourceType::TransactionalId,
                    transactional_id,
                    AclOperation::Write,
                )
                .await
            {
                Ok(authorized) => authorized,
                Err(_) => {
                    return AddPartitionsToTxnResponse::default()
                        .with_results_by_topic_v3_and_below(partition_results(
                            &topics,
                            UNKNOWN_SERVER_ERROR,
                        ));
                }
            };
            if !transaction_authorized {
                return AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(
                    partition_results(&topics, TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                );
            }

            let topic_names = topics
                .iter()
                .map(|topic| topic.name.as_str())
                .filter(|topic| !is_kafka_internal_topic(topic))
                .collect::<Vec<_>>();
            let authorizations = match self
                .topic_authorizations(context, &topic_names, AclOperation::Write)
                .await
            {
                Ok(authorizations) => authorizations,
                Err(_) => {
                    return AddPartitionsToTxnResponse::default()
                        .with_results_by_topic_v3_and_below(partition_results(
                            &topics,
                            UNKNOWN_SERVER_ERROR,
                        ));
                }
            };
            let results = match self
                .add_partitions_transaction_result(
                    transactional_id,
                    producer,
                    &topics,
                    false,
                    Some(&authorizations),
                    version,
                )
                .await
            {
                Ok(results) => results,
                Err(()) => partition_results(&topics, UNKNOWN_SERVER_ERROR),
            };
            return AddPartitionsToTxnResponse::default()
                .with_results_by_topic_v3_and_below(results);
        }

        let cluster_authorized = match self
            .authorized(
                context,
                AclResourceType::Cluster,
                authorization::CLUSTER_RESOURCE_NAME,
                AclOperation::ClusterAction,
            )
            .await
        {
            Ok(authorized) => authorized,
            Err(_) => {
                return AddPartitionsToTxnResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };
        if !cluster_authorized {
            return AddPartitionsToTxnResponse::default()
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED);
        }

        let mut results = Vec::with_capacity(request.transactions.len());
        for transaction in request.transactions {
            let transactional_id = transaction.transactional_id.as_str();
            let producer = ProducerSession {
                producer_id: transaction.producer_id.0,
                producer_epoch: transaction.producer_epoch,
            };
            let topic_results = match self
                .add_partitions_transaction_result(
                    transactional_id,
                    producer,
                    &transaction.topics,
                    transaction.verify_only,
                    None,
                    version,
                )
                .await
            {
                Ok(results) => results,
                Err(()) => partition_results(&transaction.topics, UNKNOWN_SERVER_ERROR),
            };
            results.push(
                AddPartitionsToTxnResult::default()
                    .with_transactional_id(transaction.transactional_id)
                    .with_topic_results(topic_results),
            );
        }
        AddPartitionsToTxnResponse::default()
            .with_error_code(NO_ERROR)
            .with_results_by_transaction(results)
    }

    async fn add_partitions_transaction_result(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        topics: &[AddPartitionsToTxnTopic],
        verify_only: bool,
        authorizations: Option<&HashMap<String, bool>>,
        version: i16,
    ) -> std::result::Result<Vec<AddPartitionsToTxnTopicResult>, ()> {
        let mut topic_infos = HashMap::new();
        for topic in topics {
            let name = topic.name.as_str();
            let authorized = is_kafka_internal_topic(name)
                || authorizations.is_none_or(|authorizations| {
                    authorizations.get(name).copied().unwrap_or(false)
                });
            if authorized && !topic_infos.contains_key(name) {
                topic_infos.insert(
                    name.to_owned(),
                    self.metadata.topic(name).await.map_err(|_| ())?,
                );
            }
        }

        let mut codes = Vec::with_capacity(topics.len());
        let mut failed = false;
        for topic in topics {
            let name = topic.name.as_str();
            let authorized = is_kafka_internal_topic(name)
                || authorizations.is_none_or(|authorizations| {
                    authorizations.get(name).copied().unwrap_or(false)
                });
            let info = topic_infos.get(name).and_then(Option::as_ref);
            let topic_codes = topic
                .partitions
                .iter()
                .map(|partition| {
                    let code = if !authorized {
                        TOPIC_AUTHORIZATION_FAILED
                    } else if info
                        .is_none_or(|info| *partition < 0 || *partition >= info.partitions)
                    {
                        UNKNOWN_TOPIC_OR_PARTITION
                    } else {
                        NO_ERROR
                    };
                    failed |= code != NO_ERROR;
                    code
                })
                .collect::<Vec<_>>();
            codes.push(topic_codes);
        }

        if failed {
            for topic_codes in &mut codes {
                for code in topic_codes {
                    if *code == NO_ERROR {
                        *code = OPERATION_NOT_ATTEMPTED;
                    }
                }
            }
            return Ok(partition_results_with_codes(topics, &codes));
        }

        let code = self
            .add_transaction_partitions(transactional_id, producer, topics, verify_only)
            .await
            .as_ref()
            .err()
            .map_or(NO_ERROR, |error| {
                versioned_producer_error_code(error, version, 2)
            });
        Ok(partition_results(topics, code))
    }

    pub(super) async fn handle_add_offsets_to_txn(
        &self,
        request: AddOffsetsToTxnRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> AddOffsetsToTxnResponse {
        let transaction_authorized = self
            .authorized(
                context,
                AclResourceType::TransactionalId,
                request.transactional_id.as_str(),
                AclOperation::Write,
            )
            .await;
        if !matches!(transaction_authorized, Ok(true)) {
            return AddOffsetsToTxnResponse::default().with_error_code(
                if transaction_authorized.is_err() {
                    UNKNOWN_SERVER_ERROR
                } else {
                    TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                },
            );
        }
        let group_authorized = self
            .authorized(
                context,
                AclResourceType::Group,
                request.group_id.as_str(),
                AclOperation::Read,
            )
            .await;
        if !matches!(group_authorized, Ok(true)) {
            return AddOffsetsToTxnResponse::default().with_error_code(
                if group_authorized.is_err() {
                    UNKNOWN_SERVER_ERROR
                } else {
                    GROUP_AUTHORIZATION_FAILED
                },
            );
        }
        let result = self
            .metadata
            .add_offsets_to_transaction(
                request.transactional_id.as_str(),
                ProducerSession {
                    producer_id: request.producer_id.0,
                    producer_epoch: request.producer_epoch,
                },
                request.group_id.as_str(),
            )
            .await;
        AddOffsetsToTxnResponse::default().with_error_code(
            result.as_ref().err().map_or(NO_ERROR, |error| {
                versioned_producer_error_code(error, version, 2)
            }),
        )
    }

    pub(super) async fn handle_txn_offset_commit(
        &self,
        request: TxnOffsetCommitRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> TxnOffsetCommitResponse {
        let transaction_authorized = self
            .authorized(
                context,
                AclResourceType::TransactionalId,
                request.transactional_id.as_str(),
                AclOperation::Write,
            )
            .await;
        if !matches!(transaction_authorized, Ok(true)) {
            return txn_offset_error(
                &request.topics,
                if transaction_authorized.is_err() {
                    UNKNOWN_SERVER_ERROR
                } else {
                    TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                },
            );
        }
        let group_authorized = self
            .authorized(
                context,
                AclResourceType::Group,
                request.group_id.as_str(),
                AclOperation::Read,
            )
            .await;
        if !matches!(group_authorized, Ok(true)) {
            return txn_offset_error(
                &request.topics,
                if group_authorized.is_err() {
                    UNKNOWN_SERVER_ERROR
                } else {
                    GROUP_AUTHORIZATION_FAILED
                },
            );
        }
        let topic_names = request
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>();
        let authorizations = match self
            .topic_authorizations(context, &topic_names, AclOperation::Read)
            .await
        {
            Ok(authorizations) => authorizations,
            Err(_) => return txn_offset_error(&request.topics, UNKNOWN_SERVER_ERROR),
        };
        let mut topics = HashMap::new();
        for name in topic_names {
            if authorizations.get(name).copied().unwrap_or(false) && !topics.contains_key(name) {
                let topic = match self.metadata.topic(name).await {
                    Ok(topic) => topic,
                    Err(_) => return txn_offset_error(&request.topics, UNKNOWN_SERVER_ERROR),
                };
                topics.insert(name.to_owned(), topic);
            }
        }

        let mut offsets = Vec::new();
        let mut shape = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let name = topic.name.as_str();
            let topic_authorized = authorizations.get(name).copied().unwrap_or(false);
            let topic_info = topics.get(name).and_then(|topic| topic.as_ref());
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let metadata = partition
                    .committed_metadata
                    .map(|metadata| metadata.as_str().to_owned());
                let error_code = if !topic_authorized {
                    TOPIC_AUTHORIZATION_FAILED
                } else if topic_info.is_none_or(|info| {
                    partition.partition_index < 0 || partition.partition_index >= info.partitions
                }) {
                    UNKNOWN_TOPIC_OR_PARTITION
                } else if metadata.as_ref().is_some_and(|metadata| {
                    metadata.encode_utf16().count() > self.config.offset_metadata_max_bytes
                }) {
                    OFFSET_METADATA_TOO_LARGE
                } else {
                    offsets.push(OffsetCommit {
                        partition: PartitionKey::new(name, partition.partition_index),
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata,
                        retention_time_ms: None,
                    });
                    NO_ERROR
                };
                partitions.push((partition.partition_index, error_code));
            }
            shape.push((topic.name, partitions));
        }
        let producer = ProducerSession {
            producer_id: request.producer_id.0,
            producer_epoch: request.producer_epoch,
        };
        let group_id = request.group_id.as_str();
        let result = if offsets.is_empty() {
            Ok(())
        } else {
            self.metadata
                .commit_transaction_member_offsets(
                    request.transactional_id.as_str(),
                    producer,
                    group_id,
                    request.member_id.as_str(),
                    request
                        .group_instance_id
                        .as_ref()
                        .map(|value| value.as_str()),
                    request.generation_id,
                    version >= 5,
                    offsets,
                )
                .await
        };
        TxnOffsetCommitResponse::default().with_topics(
            shape
                .into_iter()
                .map(|(name, partitions)| {
                    TxnOffsetCommitResponseTopic::default()
                        .with_name(name)
                        .with_partitions(
                            partitions
                                .into_iter()
                                .map(|(partition_index, validation_code)| {
                                    let error_code = if validation_code == NO_ERROR {
                                        result
                                            .as_ref()
                                            .err()
                                            .map_or(NO_ERROR, txn_offset_commit_error)
                                    } else {
                                        validation_code
                                    };
                                    TxnOffsetCommitResponsePartition::default()
                                        .with_partition_index(partition_index)
                                        .with_error_code(error_code)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
    }

    pub(super) async fn handle_end_txn(
        &self,
        request: EndTxnRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> EndTxnResponse {
        let producer = ProducerSession {
            producer_id: request.producer_id.0,
            producer_epoch: request.producer_epoch,
        };
        let authorized = self
            .authorized(
                context,
                AclResourceType::TransactionalId,
                request.transactional_id.as_str(),
                AclOperation::Write,
            )
            .await;
        if !matches!(authorized, Ok(true)) {
            return EndTxnResponse::default().with_error_code(if authorized.is_err() {
                UNKNOWN_SERVER_ERROR
            } else {
                TRANSACTIONAL_ID_AUTHORIZATION_FAILED
            });
        }
        let result = if version >= 5 {
            self.metadata
                .end_transaction_with_epoch_bump(
                    request.transactional_id.as_str(),
                    producer,
                    request.committed,
                )
                .await
        } else {
            self.metadata
                .end_transaction(
                    request.transactional_id.as_str(),
                    producer,
                    request.committed,
                )
                .await
                .map(|()| producer)
        };
        if result.is_ok() {
            if request.committed {
                self.metrics.committed_transactions.inc();
            } else {
                self.metrics.aborted_transactions.inc();
            }
        }
        let mut response = EndTxnResponse::default().with_error_code(
            result.as_ref().err().map_or(NO_ERROR, |error| {
                versioned_producer_error_code(error, version, 2)
            }),
        );
        if version >= 5
            && let Ok(response_producer) = result
        {
            response = response
                .with_producer_id(response_producer.producer_id.into())
                .with_producer_epoch(response_producer.producer_epoch);
        }
        response
    }

    pub(super) async fn handle_write_txn_markers(
        &self,
        request: WriteTxnMarkersRequest,
        context: &AuthorizationContext,
    ) -> WriteTxnMarkersResponse {
        let authorization_error = match self
            .authorized(
                context,
                AclResourceType::Cluster,
                authorization::CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
        {
            Ok(true) => None,
            Ok(false) => match self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    authorization::CLUSTER_RESOURCE_NAME,
                    AclOperation::ClusterAction,
                )
                .await
            {
                Ok(true) => None,
                Ok(false) => Some(CLUSTER_AUTHORIZATION_FAILED),
                Err(_) => Some(UNKNOWN_SERVER_ERROR),
            },
            Err(_) => Some(UNKNOWN_SERVER_ERROR),
        };
        let mut results = Vec::with_capacity(request.markers.len());
        for marker in request.markers {
            let mut topics = Vec::with_capacity(marker.topics.len());
            if let Some(error_code) = authorization_error {
                for topic in marker.topics {
                    topics.push(
                        WritableTxnMarkerTopicResult::default()
                            .with_name(topic.name)
                            .with_partitions(
                                topic
                                    .partition_indexes
                                    .into_iter()
                                    .map(|partition| {
                                        WritableTxnMarkerPartitionResult::default()
                                            .with_partition_index(partition)
                                            .with_error_code(error_code)
                                    })
                                    .collect(),
                            ),
                    );
                }
                results.push(
                    WritableTxnMarkerResult::default()
                        .with_producer_id(marker.producer_id)
                        .with_topics(topics),
                );
                continue;
            }

            let producer = ProducerSession {
                producer_id: marker.producer_id.0,
                producer_epoch: marker.producer_epoch,
            };
            let mut valid_partitions = Vec::new();
            let mut metadata_failed = false;
            for topic in marker.topics {
                let topic_name = topic.name.as_str().to_owned();
                let partition_limit = if topic_name == "__consumer_offsets" {
                    None
                } else {
                    match self.metadata.topic(&topic_name).await {
                        Ok(Some(info)) => Some(info.partitions),
                        Ok(None) => Some(0),
                        Err(_) => {
                            metadata_failed = true;
                            Some(0)
                        }
                    }
                };
                let partitions = topic
                    .partition_indexes
                    .into_iter()
                    .map(|partition| {
                        let error_code = if partition < 0 {
                            UNKNOWN_TOPIC_OR_PARTITION
                        } else if topic_name == "__consumer_offsets"
                            || partition_limit.is_some_and(|limit| partition < limit)
                        {
                            valid_partitions.push(PartitionKey::new(&topic_name, partition));
                            NO_ERROR
                        } else if metadata_failed {
                            UNKNOWN_SERVER_ERROR
                        } else {
                            UNKNOWN_TOPIC_OR_PARTITION
                        };
                        WritableTxnMarkerPartitionResult::default()
                            .with_partition_index(partition)
                            .with_error_code(error_code)
                    })
                    .collect();
                topics.push(
                    WritableTxnMarkerTopicResult::default()
                        .with_name(topic.name)
                        .with_partitions(partitions),
                );
            }
            let result = if metadata_failed || valid_partitions.is_empty() {
                None
            } else {
                self.metadata
                    .write_transaction_marker(
                        producer,
                        &valid_partitions,
                        marker.transaction_result,
                        marker.coordinator_epoch,
                        marker.transaction_version,
                    )
                    .await
                    .err()
                    .map(|error| write_transaction_marker_error_code(&error))
            };
            let error_code = if metadata_failed {
                Some(UNKNOWN_SERVER_ERROR)
            } else {
                result
            };
            if let Some(error_code) = error_code {
                for topic in &mut topics {
                    for partition in &mut topic.partitions {
                        if partition.error_code == NO_ERROR {
                            partition.error_code = error_code;
                        }
                    }
                }
            }
            results.push(
                WritableTxnMarkerResult::default()
                    .with_producer_id(marker.producer_id)
                    .with_topics(topics),
            );
        }
        WriteTxnMarkersResponse::default().with_markers(results)
    }

    pub(super) async fn handle_describe_transactions(
        &self,
        request: DescribeTransactionsRequest,
        context: &AuthorizationContext,
    ) -> DescribeTransactionsResponse {
        let requested = request
            .transactional_ids
            .into_iter()
            .map(|transactional_id| transactional_id.as_str().to_owned())
            .collect::<Vec<_>>();
        let mut authorized = HashSet::new();
        let mut authorization_failures = HashSet::new();
        for transactional_id in &requested {
            match self
                .authorized(
                    context,
                    AclResourceType::TransactionalId,
                    transactional_id,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(true) => {
                    authorized.insert(transactional_id.clone());
                }
                Ok(false) => {
                    authorization_failures.insert(transactional_id.clone());
                }
                Err(_) => {}
            }
        }
        let authorized_ids = authorized
            .iter()
            .filter(|transactional_id| !transactional_id.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let descriptions = self.metadata.describe_transactions(&authorized_ids).await;
        let mut transaction_states = Vec::with_capacity(requested.len());
        for transactional_id in requested {
            let state = if authorization_failures.contains(&transactional_id) {
                transaction_error_state(
                    transactional_id.clone(),
                    TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                )
            } else if !authorized.contains(&transactional_id) {
                transaction_error_state(transactional_id.clone(), UNKNOWN_SERVER_ERROR)
            } else if transactional_id.is_empty() {
                transaction_error_state(transactional_id.clone(), INVALID_REQUEST)
            } else {
                match &descriptions {
                    Ok(descriptions) => match descriptions.get(&transactional_id) {
                        Some(description) => match self
                            .visible_transaction_description(context, description)
                            .await
                        {
                            Ok(description) => described_transaction_state(&description),
                            Err(_) => transaction_error_state(
                                transactional_id.clone(),
                                UNKNOWN_SERVER_ERROR,
                            ),
                        },
                        None => missing_transaction_state(transactional_id.clone()),
                    },
                    Err(error) => {
                        transaction_error_state(transactional_id.clone(), control_error_code(error))
                    }
                }
            };
            transaction_states.push(state);
        }
        DescribeTransactionsResponse::default().with_transaction_states(transaction_states)
    }

    pub(super) async fn handle_list_transactions(
        &self,
        request: ListTransactionsRequest,
        context: &AuthorizationContext,
    ) -> ListTransactionsResponse {
        let mut state_filters = Vec::new();
        let mut unknown_state_filters = Vec::new();
        for state in &request.state_filters {
            if is_known_transaction_state(state.as_str()) {
                state_filters.push(state.as_str().to_owned());
            } else {
                unknown_state_filters.push(state.clone());
            }
        }
        if !request.state_filters.is_empty() && state_filters.is_empty() {
            return ListTransactionsResponse::default()
                .with_error_code(NO_ERROR)
                .with_unknown_state_filters(unknown_state_filters);
        }
        let filter = TransactionFilter {
            state_filters,
            producer_id_filters: request
                .producer_id_filters
                .iter()
                .map(|producer_id| producer_id.0)
                .collect(),
            min_duration_ms: (request.duration_filter >= 0).then_some(request.duration_filter),
            transactional_id_pattern: request
                .transactional_id_pattern
                .map(|pattern| pattern.as_str().to_owned()),
        };
        match self.metadata.list_transactions(&filter).await {
            Ok(descriptions) => {
                let mut transaction_states = Vec::with_capacity(descriptions.len());
                for description in descriptions {
                    match self
                        .authorized(
                            context,
                            AclResourceType::TransactionalId,
                            &description.transactional_id,
                            AclOperation::Describe,
                        )
                        .await
                    {
                        Ok(true) => transaction_states.push(listed_transaction_state(&description)),
                        Ok(false) => {}
                        Err(_) => {
                            return ListTransactionsResponse::default()
                                .with_error_code(UNKNOWN_SERVER_ERROR)
                                .with_unknown_state_filters(unknown_state_filters);
                        }
                    }
                }
                ListTransactionsResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_unknown_state_filters(unknown_state_filters)
                    .with_transaction_states(transaction_states)
            }
            Err(error) => ListTransactionsResponse::default()
                .with_error_code(control_error_code(&error))
                .with_unknown_state_filters(unknown_state_filters),
        }
    }

    async fn visible_transaction_description(
        &self,
        context: &AuthorizationContext,
        description: &TransactionDescription,
    ) -> Result<TransactionDescription> {
        let topics = description
            .partitions
            .iter()
            .map(|partition| partition.topic.clone())
            .collect::<HashSet<_>>();
        let mut visible_topics = HashSet::with_capacity(topics.len());
        for topic in topics {
            if self
                .authorized(
                    context,
                    AclResourceType::Topic,
                    &topic,
                    AclOperation::Describe,
                )
                .await?
            {
                visible_topics.insert(topic);
            }
        }
        let mut visible = description.clone();
        visible
            .partitions
            .retain(|partition| visible_topics.contains(&partition.topic));
        Ok(visible)
    }

    async fn add_transaction_partitions(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        topics: &[AddPartitionsToTxnTopic],
        verify_only: bool,
    ) -> std::result::Result<(), ControlError> {
        let partitions = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionKey::new(topic.name.as_str(), *partition))
            })
            .collect::<Vec<_>>();
        self.metadata
            .add_partitions_to_transaction(transactional_id, producer, &partitions, verify_only)
            .await
    }
}

fn write_transaction_marker_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::ProducerFenced { .. } => INVALID_PRODUCER_EPOCH,
        ControlError::TransactionCoordinatorFenced { .. } => TRANSACTION_COORDINATOR_FENCED,
        _ => control_error_code(error),
    }
}

fn described_transaction_state(description: &TransactionDescription) -> DescribedTransactionState {
    let mut topics = BTreeMap::<String, Vec<i32>>::new();
    for partition in &description.partitions {
        topics
            .entry(partition.topic.clone())
            .or_default()
            .push(partition.partition);
    }
    DescribedTransactionState::default()
        .with_error_code(NO_ERROR)
        .with_transactional_id(kafka_transactional_id(description.transactional_id.clone()))
        .with_transaction_state(StrBytes::from_string(
            description.state.kafka_name().to_owned(),
        ))
        .with_transaction_timeout_ms(description.timeout_ms)
        .with_transaction_start_time_ms(description.start_time_ms)
        .with_producer_id(description.producer.producer_id.into())
        .with_producer_epoch(description.producer.producer_epoch)
        .with_topics(
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    TopicData::default()
                        .with_topic(topic_name(&topic))
                        .with_partitions(partitions)
                })
                .collect(),
        )
}

fn missing_transaction_state(transactional_id: String) -> DescribedTransactionState {
    DescribedTransactionState::default()
        .with_error_code(TRANSACTIONAL_ID_NOT_FOUND)
        .with_transactional_id(kafka_transactional_id(transactional_id))
        .with_transaction_timeout_ms(-1)
        .with_transaction_start_time_ms(-1)
        .with_producer_id((-1).into())
        .with_producer_epoch(-1)
}

fn transaction_error_state(transactional_id: String, error_code: i16) -> DescribedTransactionState {
    DescribedTransactionState::default()
        .with_error_code(error_code)
        .with_transactional_id(kafka_transactional_id(transactional_id))
        .with_transaction_timeout_ms(-1)
        .with_transaction_start_time_ms(-1)
        .with_producer_id((-1).into())
        .with_producer_epoch(-1)
}

fn txn_offset_error(
    topics: &[kafka_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequestTopic],
    error_code: i16,
) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse::default().with_topics(
        topics
            .iter()
            .map(|topic| {
                TxnOffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                TxnOffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(error_code)
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

fn txn_offset_commit_error(error: &ControlError) -> i16 {
    match error {
        ControlError::FencedMemberEpoch { .. } => ILLEGAL_GENERATION,
        error => control_error_code(error),
    }
}

fn listed_transaction_state(description: &TransactionDescription) -> ListedTransactionState {
    ListedTransactionState::default()
        .with_transactional_id(kafka_transactional_id(description.transactional_id.clone()))
        .with_producer_id(description.producer.producer_id.into())
        .with_transaction_state(StrBytes::from_string(
            description.state.kafka_name().to_owned(),
        ))
}

fn kafka_transactional_id(value: String) -> kafka_protocol::messages::TransactionalId {
    kafka_protocol::messages::TransactionalId::from(StrBytes::from_string(value))
}

fn is_known_transaction_state(state: &str) -> bool {
    matches!(
        state,
        "Empty"
            | "Ongoing"
            | "PrepareCommit"
            | "PrepareAbort"
            | "CompleteCommit"
            | "CompleteAbort"
            | "Dead"
            | "PrepareEpochFence"
    )
}

fn partition_results(
    topics: &[AddPartitionsToTxnTopic],
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|topic| {
            AddPartitionsToTxnTopicResult::default()
                .with_name(topic.name.clone())
                .with_results_by_partition(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(*partition)
                                .with_partition_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect()
}

fn partition_results_with_codes(
    topics: &[AddPartitionsToTxnTopic],
    codes: &[Vec<i16>],
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .zip(codes)
        .map(|(topic, codes)| {
            AddPartitionsToTxnTopicResult::default()
                .with_name(topic.name.clone())
                .with_results_by_partition(
                    topic
                        .partitions
                        .iter()
                        .zip(codes)
                        .map(|(partition, code)| {
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(*partition)
                                .with_partition_error_code(*code)
                        })
                        .collect(),
                )
        })
        .collect()
}

fn versioned_producer_error_code(
    error: &ControlError,
    version: i16,
    producer_fenced_min_version: i16,
) -> i16 {
    let code = control_error_code(error);
    if version < producer_fenced_min_version && code == PRODUCER_FENCED {
        INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

fn is_kafka_internal_topic(topic: &str) -> bool {
    matches!(
        topic,
        "__consumer_offsets" | "__transaction_state" | "__share_group_state"
    )
}
