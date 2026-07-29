use super::Broker;
use super::authorization::AuthorizationContext;
use crate::kafka_error::{
    INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT, INVALID_REQUEST, NO_ERROR,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
    control_error_code,
};
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
use kafka_protocol::messages::describe_topic_partitions_response::{
    Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
};
use kafka_protocol::messages::{
    BrokerId, CreatePartitionsRequest, CreatePartitionsResponse, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, TopicInfo};
use std::collections::{BTreeSet, HashMap, HashSet};

impl Broker {
    pub(super) async fn handle_create_partitions(
        &self,
        request: CreatePartitionsRequest,
        context: &AuthorizationContext,
    ) -> CreatePartitionsResponse {
        let mut topic_names = HashSet::with_capacity(request.topics.len());
        let mut duplicate_names = HashSet::new();
        for topic in &request.topics {
            let name = topic.name.as_str().to_owned();
            if !topic_names.insert(name.clone()) {
                duplicate_names.insert(name);
            }
        }

        let mut results = Vec::with_capacity(request.topics.len());
        let mut reported_duplicates = HashSet::with_capacity(duplicate_names.len());
        for topic in request.topics {
            let name = topic.name.as_str();
            if duplicate_names.contains(name) {
                if reported_duplicates.insert(name.to_owned()) {
                    results.push(partition_result(
                        &topic,
                        INVALID_REQUEST,
                        Some("Duplicate topic name."),
                    ));
                }
                continue;
            }
            let authorized = self
                .authorized(context, AclResourceType::Topic, name, AclOperation::Alter)
                .await;
            if !matches!(authorized, Ok(true)) {
                let (code, message) = if authorized.is_err() {
                    (UNKNOWN_SERVER_ERROR, "metadata authorization failed")
                } else {
                    (TOPIC_AUTHORIZATION_FAILED, "topic authorization failed")
                };
                results.push(partition_result(&topic, code, Some(message)));
                continue;
            }
            let current = match self.metadata.topic(name).await {
                Ok(Some(current)) => current,
                Ok(None) => {
                    results.push(partition_result(
                        &topic,
                        UNKNOWN_TOPIC_OR_PARTITION,
                        Some("topic was not found"),
                    ));
                    continue;
                }
                Err(error) => {
                    results.push(partition_result(
                        &topic,
                        control_error_code(&error),
                        Some(&error.to_string()),
                    ));
                    continue;
                }
            };
            if let Err((code, message)) = validate_expansion(&topic, &current) {
                results.push(partition_result(&topic, code, Some(&message)));
                continue;
            }
            let result = if request.validate_only {
                Ok(())
            } else {
                self.metadata
                    .create_partitions(name, topic.count)
                    .await
                    .map(|_| ())
            };
            results.push(match result {
                Ok(()) => partition_result(&topic, NO_ERROR, None),
                Err(error) => {
                    partition_result(&topic, control_error_code(&error), Some(&error.to_string()))
                }
            });
        }
        CreatePartitionsResponse::default().with_results(results)
    }

    pub(super) async fn handle_describe_topic_partitions(
        &self,
        request: DescribeTopicPartitionsRequest,
        context: &AuthorizationContext,
    ) -> DescribeTopicPartitionsResponse {
        let explicit = !request.topics.is_empty();
        let mut names = request
            .topics
            .iter()
            .map(|topic| topic.name.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        if invalid_describe_cursor(&request, &names, explicit) {
            return DescribeTopicPartitionsResponse::default().with_topics(
                request
                    .topics
                    .iter()
                    .map(|topic| topic_error(topic.name.as_str(), INVALID_REQUEST))
                    .collect(),
            );
        }
        let cursor = request.cursor.as_ref().map(|cursor| {
            (
                cursor.topic_name.as_str().to_owned(),
                cursor.partition_index,
            )
        });
        let mut stored = if explicit {
            Vec::new()
        } else {
            match self.metadata.topics(None).await {
                Ok(topics) => topics,
                Err(_) => return DescribeTopicPartitionsResponse::default(),
            }
        };
        if !explicit {
            names.extend(stored.iter().map(|topic| topic.name.clone()));
        }
        if let Some((cursor_topic, _)) = &cursor {
            names.retain(|name| name >= cursor_topic);
        }

        let mut authorized_names = Vec::with_capacity(names.len());
        let mut authorization_errors = Vec::new();
        for name in names {
            match self
                .authorized(
                    context,
                    AclResourceType::Topic,
                    &name,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(true) => authorized_names.push(name),
                Ok(false) if explicit => {
                    authorization_errors.push(topic_error(&name, TOPIC_AUTHORIZATION_FAILED));
                }
                Err(_) if explicit => {
                    authorization_errors.push(topic_error(&name, UNKNOWN_SERVER_ERROR));
                }
                Ok(false) | Err(_) => {}
            }
        }
        if explicit {
            stored = match self.metadata.topics(Some(&authorized_names)).await {
                Ok(topics) => topics,
                Err(error) => {
                    let error_code = control_error_code(&error);
                    authorization_errors.extend(
                        authorized_names
                            .into_iter()
                            .map(|name| topic_error(&name, error_code)),
                    );
                    return DescribeTopicPartitionsResponse::default()
                        .with_topics(authorization_errors);
                }
            };
        }
        let by_name = stored
            .into_iter()
            .map(|topic| (topic.name.clone(), topic))
            .collect::<HashMap<_, _>>();
        let limit = request
            .response_partition_limit
            .clamp(1, self.config.max_request_partition_size_limit) as usize;
        let mut partition_count = 0;
        let mut response_topics = Vec::new();
        let mut next_cursor = None;

        for name in authorized_names {
            let start = match &cursor {
                Some((cursor_topic, partition)) if name == *cursor_topic => (*partition).max(0),
                _ => 0,
            };
            let Some(topic) = by_name.get(&name) else {
                response_topics.push(topic_error(&name, UNKNOWN_TOPIC_OR_PARTITION));
                continue;
            };
            if start >= topic.partitions {
                continue;
            }
            let remaining = limit.saturating_sub(partition_count);
            if remaining == 0 {
                next_cursor = Some(response_cursor(&name, start));
                break;
            }
            let end = topic.partitions.min(start.saturating_add(remaining as i32));
            let operations = self.topic_authorized_operations(context, &name).await;
            response_topics.push(response_topic(topic, start, end, operations));
            partition_count += (end - start) as usize;
            if end < topic.partitions {
                next_cursor = Some(response_cursor(&name, end));
                break;
            }
        }
        response_topics.extend(authorization_errors);
        DescribeTopicPartitionsResponse::default()
            .with_topics(response_topics)
            .with_next_cursor(next_cursor)
    }

    pub(super) async fn topic_authorized_operations(
        &self,
        context: &AuthorizationContext,
        topic: &str,
    ) -> i32 {
        let mut bitfield = 0;
        for operation in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ] {
            if self
                .authorized(context, AclResourceType::Topic, topic, operation)
                .await
                .unwrap_or(false)
            {
                bitfield |= 1_i32 << operation as i8;
            }
        }
        bitfield
    }
}

fn invalid_describe_cursor(
    request: &DescribeTopicPartitionsRequest,
    topic_names: &BTreeSet<String>,
    explicit: bool,
) -> bool {
    request.cursor.as_ref().is_some_and(|cursor| {
        cursor.partition_index < 0
            || (explicit && !topic_names.contains(cursor.topic_name.as_str()))
    })
}

fn validate_expansion(
    request: &CreatePartitionsTopic,
    topic: &TopicInfo,
) -> Result<(), (i16, String)> {
    if request.count <= topic.partitions {
        return Err((
            INVALID_PARTITIONS,
            format!(
                "requested partition count {} must be greater than current count {}",
                request.count, topic.partitions
            ),
        ));
    }
    if let Some(assignments) = &request.assignments {
        let expected = (request.count - topic.partitions) as usize;
        let valid = assignments.len() == expected
            && assignments
                .iter()
                .all(|assignment| assignment.broker_ids == [BrokerId::from(0)]);
        if !valid {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                "new partitions must each be assigned to virtual broker 0".to_owned(),
            ));
        }
    }
    Ok(())
}

fn partition_result(
    request: &CreatePartitionsTopic,
    error_code: i16,
    message: Option<&str>,
) -> CreatePartitionsTopicResult {
    CreatePartitionsTopicResult::default()
        .with_name(request.name.clone())
        .with_error_code(error_code)
        .with_error_message(message.map(string))
}

fn response_topic(
    topic: &TopicInfo,
    start: i32,
    end: i32,
    authorized_operations: i32,
) -> DescribeTopicPartitionsResponseTopic {
    let broker = BrokerId::from(0);
    DescribeTopicPartitionsResponseTopic::default()
        .with_error_code(NO_ERROR)
        .with_name(Some(topic_name(&topic.name)))
        .with_topic_id(topic.id)
        .with_is_internal(topic.name.starts_with("__"))
        .with_partitions(
            (start..end)
                .map(|partition| {
                    DescribeTopicPartitionsResponsePartition::default()
                        .with_error_code(NO_ERROR)
                        .with_partition_index(partition)
                        .with_leader_id(broker)
                        .with_leader_epoch(0)
                        .with_replica_nodes(vec![broker])
                        .with_isr_nodes(vec![broker])
                        .with_eligible_leader_replicas(Some(Vec::new()))
                        .with_last_known_elr(Some(Vec::new()))
                        .with_offline_replicas(Vec::new())
                })
                .collect(),
        )
        .with_topic_authorized_operations(authorized_operations)
}

fn topic_error(name: &str, error_code: i16) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic::default()
        .with_error_code(error_code)
        .with_name(Some(topic_name(name)))
}

fn response_cursor(name: &str, partition: i32) -> Cursor {
    Cursor::default()
        .with_topic_name(topic_name(name))
        .with_partition_index(partition)
}

fn topic_name(value: &str) -> kafka_protocol::messages::TopicName {
    kafka_protocol::messages::TopicName::from(string(value))
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
