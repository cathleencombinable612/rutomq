use super::Broker;
use super::authorization::AuthorizationContext;
use super::topic_name;
use crate::kafka_error::{
    FENCED_MEMBER_EPOCH, GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, NO_ERROR,
    OFFSET_METADATA_TOO_LARGE, STALE_MEMBER_EPOCH, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_MEMBER_ID,
    UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION, control_error_code,
};
use anyhow::Result;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    GroupId, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, CommittedOffset, ControlError, OffsetCommit, PartitionKey,
};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone)]
struct FetchGroup {
    id: String,
    member_id: Option<String>,
    member_epoch: i32,
    topics: Option<Vec<FetchTopic>>,
}

#[derive(Clone)]
struct FetchTopic {
    id: Uuid,
    name: String,
    partitions: Vec<i32>,
}

struct ResolvedTopic {
    id: Uuid,
    name: String,
    partition_count: Option<i32>,
    partitions: Vec<i32>,
    error_code: i16,
}

impl Broker {
    pub(super) async fn handle_offset_commit(
        &self,
        request: OffsetCommitRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<OffsetCommitResponse> {
        let group_id = request.group_id.as_str().to_owned();
        let retention_time_ms = (2..=4)
            .contains(&version)
            .then_some(request.retention_time_ms)
            .filter(|retention| *retention != -1);
        let group_authorized = self
            .authorized(
                context,
                AclResourceType::Group,
                &group_id,
                AclOperation::Read,
            )
            .await;
        match group_authorized {
            Ok(true) => {}
            Ok(false) => {
                return Ok(offset_commit_error(
                    &request,
                    version,
                    GROUP_AUTHORIZATION_FAILED,
                ));
            }
            Err(_) => {
                return Ok(offset_commit_error(&request, version, UNKNOWN_SERVER_ERROR));
            }
        }

        let mut topic_infos = if version >= 10 {
            let mut infos = Vec::with_capacity(request.topics.len());
            for topic in &request.topics {
                match self.metadata.topic_by_id(topic.topic_id).await {
                    Ok(info) => infos.push(info),
                    Err(_) => {
                        return Ok(offset_commit_error(&request, version, UNKNOWN_SERVER_ERROR));
                    }
                }
            }
            infos
        } else {
            vec![None; request.topics.len()]
        };
        let authorizations = {
            let topic_names = if version >= 10 {
                topic_infos
                    .iter()
                    .filter_map(|topic| topic.as_ref().map(|topic| topic.name.as_str()))
                    .collect::<Vec<_>>()
            } else {
                request
                    .topics
                    .iter()
                    .map(|topic| topic.name.as_str())
                    .collect::<Vec<_>>()
            };
            match self
                .topic_authorizations(context, &topic_names, AclOperation::Read)
                .await
            {
                Ok(authorizations) => authorizations,
                Err(_) => {
                    return Ok(offset_commit_error(&request, version, UNKNOWN_SERVER_ERROR));
                }
            }
        };
        if version < 10 {
            let mut topics = HashMap::new();
            for topic in &request.topics {
                let name = topic.name.as_str();
                if authorizations.get(name).copied().unwrap_or(false) && !topics.contains_key(name)
                {
                    let info = match self.metadata.topic(name).await {
                        Ok(info) => info,
                        Err(_) => {
                            return Ok(offset_commit_error(
                                &request,
                                version,
                                UNKNOWN_SERVER_ERROR,
                            ));
                        }
                    };
                    topics.insert(name.to_owned(), info);
                }
            }
            topic_infos = request
                .topics
                .iter()
                .map(|topic| {
                    topics
                        .get(topic.name.as_str())
                        .and_then(|topic| topic.clone())
                })
                .collect();
        }

        let mut commits = Vec::new();
        let mut response_topics = Vec::with_capacity(request.topics.len());
        for (topic, topic_info) in request.topics.into_iter().zip(topic_infos) {
            let request_name = topic.name.as_str().to_owned();
            let resolved_name = topic_info
                .as_ref()
                .map_or(request_name.as_str(), |info| info.name.as_str());
            let topic_error = if version >= 10 && topic_info.is_none() {
                UNKNOWN_TOPIC_ID
            } else if !authorizations.get(resolved_name).copied().unwrap_or(false) {
                TOPIC_AUTHORIZATION_FAILED
            } else if topic_info.is_none() {
                UNKNOWN_TOPIC_OR_PARTITION
            } else {
                NO_ERROR
            };
            let partition_count = topic_info.as_ref().map(|info| info.partitions);
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let metadata = partition
                    .committed_metadata
                    .map(|metadata| metadata.as_str().to_owned());
                let mut error_code =
                    partition_error(topic_error, partition_count, partition.partition_index);
                if error_code == NO_ERROR
                    && metadata.as_ref().is_some_and(|metadata| {
                        metadata.encode_utf16().count() > self.config.offset_metadata_max_bytes
                    })
                {
                    error_code = OFFSET_METADATA_TOO_LARGE;
                }
                if error_code == NO_ERROR {
                    commits.push(OffsetCommit {
                        partition: PartitionKey::new(resolved_name, partition.partition_index),
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata,
                        retention_time_ms,
                    });
                }
                partitions.push(
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(error_code),
                );
            }
            response_topics.push(commit_response_topic(
                version,
                &request_name,
                topic.topic_id,
                partitions,
            ));
        }

        if !commits.is_empty() {
            match self
                .metadata
                .commit_member_offsets(
                    &group_id,
                    request.member_id.as_str(),
                    request
                        .group_instance_id
                        .as_ref()
                        .map(|value| value.as_str()),
                    request.generation_id_or_member_epoch,
                    version,
                    commits,
                )
                .await
            {
                Ok(validity) => {
                    for (partition, valid) in response_topics
                        .iter_mut()
                        .flat_map(|topic| &mut topic.partitions)
                        .filter(|partition| partition.error_code == NO_ERROR)
                        .zip(validity)
                    {
                        if !valid {
                            partition.error_code = STALE_MEMBER_EPOCH;
                        }
                    }
                }
                Err(error) => {
                    let error_code = offset_group_error(error, version);
                    for partition in response_topics
                        .iter_mut()
                        .flat_map(|topic| &mut topic.partitions)
                        .filter(|partition| partition.error_code == NO_ERROR)
                    {
                        partition.error_code = error_code;
                    }
                }
            }
        }
        Ok(OffsetCommitResponse::default()
            .with_topics(response_topics)
            .with_throttle_time_ms(0))
    }

    pub(super) async fn handle_offset_fetch(
        &self,
        request: OffsetFetchRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<OffsetFetchResponse> {
        let groups = fetch_groups(request, version);
        let mut old_topics = Vec::new();
        let mut old_group_error = NO_ERROR;
        let mut responses = Vec::with_capacity(groups.len());

        for group in &groups {
            match self
                .authorized(
                    context,
                    AclResourceType::Group,
                    &group.id,
                    AclOperation::Describe,
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    record_fetch_group_error(
                        group,
                        version,
                        GROUP_AUTHORIZATION_FAILED,
                        &mut old_topics,
                        &mut old_group_error,
                        &mut responses,
                    );
                    continue;
                }
                Err(_) => {
                    return Ok(offset_fetch_error(&groups, version, UNKNOWN_SERVER_ERROR));
                }
            }

            let fetch_all = group.topics.is_none();
            if fetch_all {
                let group_error = self.offset_fetch_member_error(group, version).await;
                if group_error != NO_ERROR {
                    record_fetch_group_error(
                        group,
                        version,
                        group_error,
                        &mut old_topics,
                        &mut old_group_error,
                        &mut responses,
                    );
                    continue;
                }
                let topic_infos = match self.metadata.topics(None).await {
                    Ok(topics) => topics,
                    Err(_) => {
                        return Ok(offset_fetch_error(&groups, version, UNKNOWN_SERVER_ERROR));
                    }
                };
                let mut topics = topic_infos
                    .into_iter()
                    .map(|topic| ResolvedTopic {
                        id: topic.id,
                        name: topic.name,
                        partition_count: Some(topic.partitions),
                        partitions: (0..topic.partitions).collect(),
                        error_code: NO_ERROR,
                    })
                    .collect::<Vec<_>>();
                let keys = valid_partition_keys(&topics);
                let stored = match self.metadata.fetch_offsets(&group.id, &keys).await {
                    Ok(stored) => stored,
                    Err(error) => {
                        record_fetch_group_error(
                            group,
                            version,
                            control_error_code(&error),
                            &mut old_topics,
                            &mut old_group_error,
                            &mut responses,
                        );
                        continue;
                    }
                };
                for topic in &mut topics {
                    topic.partitions.retain(|partition| {
                        stored.contains_key(&PartitionKey::new(&topic.name, *partition))
                    });
                }
                topics.retain(|topic| !topic.partitions.is_empty());
                let topic_names = topics
                    .iter()
                    .map(|topic| topic.name.as_str())
                    .collect::<Vec<_>>();
                let authorizations = match self
                    .topic_authorizations(context, &topic_names, AclOperation::Describe)
                    .await
                {
                    Ok(authorizations) => authorizations,
                    Err(_) => {
                        return Ok(offset_fetch_error(&groups, version, UNKNOWN_SERVER_ERROR));
                    }
                };
                topics.retain(|topic| authorizations.get(&topic.name).copied().unwrap_or(false));
                if version <= 7 {
                    old_topics.extend(old_fetch_topics(topics, &stored, NO_ERROR, true, version));
                } else {
                    responses.push(fetch_group_response(
                        group.id.clone(),
                        new_fetch_topics(topics, &stored, NO_ERROR, true, version),
                        NO_ERROR,
                    ));
                }
                continue;
            }

            let topics = match self
                .resolve_fetch_topics(
                    group.topics.as_ref().expect("checked explicit topics"),
                    version,
                    context,
                )
                .await
            {
                Ok(topics) => topics,
                Err(_) => {
                    return Ok(offset_fetch_error(&groups, version, UNKNOWN_SERVER_ERROR));
                }
            };
            let group_error = self.offset_fetch_member_error(group, version).await;
            if group_error != NO_ERROR {
                record_fetch_group_error(
                    group,
                    version,
                    group_error,
                    &mut old_topics,
                    &mut old_group_error,
                    &mut responses,
                );
                continue;
            }
            let keys = valid_partition_keys(&topics);
            let stored = match self.metadata.fetch_offsets(&group.id, &keys).await {
                Ok(stored) => stored,
                Err(error) => {
                    record_fetch_group_error(
                        group,
                        version,
                        control_error_code(&error),
                        &mut old_topics,
                        &mut old_group_error,
                        &mut responses,
                    );
                    continue;
                }
            };

            if version <= 7 {
                old_topics.extend(old_fetch_topics(topics, &stored, NO_ERROR, false, version));
            } else {
                responses.push(fetch_group_response(
                    group.id.clone(),
                    new_fetch_topics(topics, &stored, NO_ERROR, false, version),
                    NO_ERROR,
                ));
            }
        }

        if version <= 7 {
            Ok(OffsetFetchResponse::default()
                .with_topics(old_topics)
                .with_error_code(old_group_error))
        } else {
            Ok(OffsetFetchResponse::default().with_groups(responses))
        }
    }

    async fn offset_fetch_member_error(&self, group: &FetchGroup, version: i16) -> i16 {
        if version >= 9
            && (group.member_epoch >= 0
                || group
                    .member_id
                    .as_deref()
                    .is_some_and(|member| !member.is_empty()))
            && let Err(error) = self
                .metadata
                .validate_group_member(
                    &group.id,
                    group.member_id.as_deref().unwrap_or_default(),
                    None,
                    group.member_epoch,
                )
                .await
        {
            return offset_group_error(error, version);
        }
        NO_ERROR
    }

    async fn resolve_fetch_topics(
        &self,
        requested: &[FetchTopic],
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<Vec<ResolvedTopic>> {
        let mut topic_infos = if version >= 10 {
            let mut infos = Vec::with_capacity(requested.len());
            for topic in requested {
                infos.push(self.metadata.topic_by_id(topic.id).await?);
            }
            infos
        } else {
            vec![None; requested.len()]
        };
        let authorizations = {
            let topic_names = if version >= 10 {
                topic_infos
                    .iter()
                    .filter_map(|topic| topic.as_ref().map(|topic| topic.name.as_str()))
                    .collect::<Vec<_>>()
            } else {
                requested
                    .iter()
                    .map(|topic| topic.name.as_str())
                    .collect::<Vec<_>>()
            };
            self.topic_authorizations(context, &topic_names, AclOperation::Describe)
                .await?
        };
        if version < 10 {
            let mut topics = HashMap::new();
            for topic in requested {
                let name = topic.name.as_str();
                if authorizations.get(name).copied().unwrap_or(false) && !topics.contains_key(name)
                {
                    topics.insert(name.to_owned(), self.metadata.topic(name).await?);
                }
            }
            topic_infos = requested
                .iter()
                .map(|topic| {
                    topics
                        .get(topic.name.as_str())
                        .and_then(|topic| topic.clone())
                })
                .collect();
        }

        let mut authorized = Vec::with_capacity(requested.len());
        let mut errors = Vec::new();
        for (topic, info) in requested.iter().zip(topic_infos) {
            let name = info
                .as_ref()
                .map_or(topic.name.as_str(), |info| info.name.as_str());
            let error_code = if version >= 10 && info.is_none() {
                UNKNOWN_TOPIC_ID
            } else if !authorizations.get(name).copied().unwrap_or(false) {
                TOPIC_AUTHORIZATION_FAILED
            } else if info.is_none() {
                UNKNOWN_TOPIC_OR_PARTITION
            } else {
                NO_ERROR
            };
            let resolved = ResolvedTopic {
                id: topic.id,
                name: name.to_owned(),
                partition_count: info.as_ref().map(|info| info.partitions),
                partitions: topic.partitions.clone(),
                error_code,
            };
            if matches!(error_code, TOPIC_AUTHORIZATION_FAILED | UNKNOWN_TOPIC_ID) {
                errors.push(resolved);
            } else {
                authorized.push(resolved);
            }
        }
        authorized.extend(errors);
        Ok(authorized)
    }
}

fn fetch_groups(request: OffsetFetchRequest, version: i16) -> Vec<FetchGroup> {
    if version <= 7 {
        return vec![FetchGroup {
            id: request.group_id.as_str().to_owned(),
            member_id: None,
            member_epoch: -1,
            topics: request.topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| FetchTopic {
                        id: Uuid::nil(),
                        name: topic.name.as_str().to_owned(),
                        partitions: topic.partition_indexes,
                    })
                    .collect()
            }),
        }];
    }
    request
        .groups
        .into_iter()
        .map(|group| FetchGroup {
            id: group.group_id.as_str().to_owned(),
            member_id: group.member_id.map(|member| member.as_str().to_owned()),
            member_epoch: group.member_epoch,
            topics: group.topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| FetchTopic {
                        id: topic.topic_id,
                        name: topic.name.as_str().to_owned(),
                        partitions: topic.partition_indexes,
                    })
                    .collect()
            }),
        })
        .collect()
}

fn offset_fetch_error(groups: &[FetchGroup], version: i16, error_code: i16) -> OffsetFetchResponse {
    let response = if version < 2 {
        OffsetFetchResponse::default()
            .with_topics(old_fetch_group_error_topics(&groups[0], error_code))
    } else if version <= 7 {
        OffsetFetchResponse::default().with_error_code(error_code)
    } else {
        OffsetFetchResponse::default().with_groups(
            groups
                .iter()
                .map(|group| fetch_group_response(group.id.clone(), Vec::new(), error_code))
                .collect(),
        )
    };
    response.with_throttle_time_ms(0)
}

fn record_fetch_group_error(
    group: &FetchGroup,
    version: i16,
    error_code: i16,
    old_topics: &mut Vec<OffsetFetchResponseTopic>,
    old_group_error: &mut i16,
    responses: &mut Vec<OffsetFetchResponseGroup>,
) {
    if version < 2 {
        old_topics.extend(old_fetch_group_error_topics(group, error_code));
    } else if version <= 7 {
        old_topics.clear();
        *old_group_error = error_code;
    } else {
        responses.push(fetch_group_response(
            group.id.clone(),
            Vec::new(),
            error_code,
        ));
    }
}

fn old_fetch_group_error_topics(
    group: &FetchGroup,
    error_code: i16,
) -> Vec<OffsetFetchResponseTopic> {
    group
        .topics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|topic| {
            OffsetFetchResponseTopic::default()
                .with_name(topic_name(&topic.name))
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetFetchResponsePartition::default()
                                .with_partition_index(*partition)
                                .with_committed_offset(-1)
                                .with_metadata(None)
                                .with_error_code(error_code)
                        })
                        .collect(),
                )
        })
        .collect()
}

fn valid_partition_keys(topics: &[ResolvedTopic]) -> Vec<PartitionKey> {
    topics
        .iter()
        .filter(|topic| topic.error_code == NO_ERROR)
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .filter(|partition| valid_partition(topic.partition_count, **partition))
                .map(|partition| PartitionKey::new(&topic.name, *partition))
        })
        .collect()
}

fn old_fetch_topics(
    topics: Vec<ResolvedTopic>,
    stored: &HashMap<PartitionKey, CommittedOffset>,
    storage_error: i16,
    fetch_all: bool,
    version: i16,
) -> Vec<OffsetFetchResponseTopic> {
    topics
        .into_iter()
        .filter_map(|topic| {
            let partitions = topic
                .partitions
                .iter()
                .filter(|partition| include_partition(&topic, stored, **partition, fetch_all))
                .map(|partition| {
                    let key = PartitionKey::new(&topic.name, *partition);
                    let error = fetch_partition_error(&topic, *partition, storage_error);
                    old_fetch_partition(stored, &key, error, version)
                })
                .collect::<Vec<_>>();
            (!fetch_all || !partitions.is_empty()).then(|| {
                OffsetFetchResponseTopic::default()
                    .with_name(topic_name(&topic.name))
                    .with_partitions(partitions)
            })
        })
        .collect()
}

fn new_fetch_topics(
    topics: Vec<ResolvedTopic>,
    stored: &HashMap<PartitionKey, CommittedOffset>,
    storage_error: i16,
    fetch_all: bool,
    version: i16,
) -> Vec<OffsetFetchResponseTopics> {
    topics
        .into_iter()
        .filter_map(|topic| {
            let partitions = topic
                .partitions
                .iter()
                .filter(|partition| include_partition(&topic, stored, **partition, fetch_all))
                .map(|partition| {
                    let key = PartitionKey::new(&topic.name, *partition);
                    let error = fetch_partition_error(&topic, *partition, storage_error);
                    new_fetch_partition(stored, &key, error)
                })
                .collect::<Vec<_>>();
            if fetch_all && partitions.is_empty() {
                return None;
            }
            let response = OffsetFetchResponseTopics::default().with_partitions(partitions);
            Some(if version >= 10 {
                response.with_topic_id(topic.id)
            } else {
                response.with_name(topic_name(&topic.name))
            })
        })
        .collect()
}

fn include_partition(
    topic: &ResolvedTopic,
    stored: &HashMap<PartitionKey, CommittedOffset>,
    partition: i32,
    fetch_all: bool,
) -> bool {
    !fetch_all || stored.contains_key(&PartitionKey::new(&topic.name, partition))
}

fn fetch_partition_error(topic: &ResolvedTopic, partition: i32, storage_error: i16) -> i16 {
    let error = partition_error(topic.error_code, topic.partition_count, partition);
    if error == NO_ERROR {
        storage_error
    } else {
        error
    }
}

fn old_fetch_partition(
    stored: &HashMap<PartitionKey, CommittedOffset>,
    partition: &PartitionKey,
    error_code: i16,
    version: i16,
) -> OffsetFetchResponsePartition {
    let (offset, leader_epoch, metadata) = committed(stored, partition);
    let response = OffsetFetchResponsePartition::default()
        .with_partition_index(partition.partition)
        .with_committed_offset(offset)
        .with_metadata(metadata)
        .with_error_code(error_code);
    if version >= 5 {
        response.with_committed_leader_epoch(leader_epoch)
    } else {
        response
    }
}

fn new_fetch_partition(
    stored: &HashMap<PartitionKey, CommittedOffset>,
    partition: &PartitionKey,
    error_code: i16,
) -> OffsetFetchResponsePartitions {
    let (offset, leader_epoch, metadata) = committed(stored, partition);
    OffsetFetchResponsePartitions::default()
        .with_partition_index(partition.partition)
        .with_committed_offset(offset)
        .with_committed_leader_epoch(leader_epoch)
        .with_metadata(metadata)
        .with_error_code(error_code)
}

fn committed(
    stored: &HashMap<PartitionKey, CommittedOffset>,
    partition: &PartitionKey,
) -> (i64, i32, Option<StrBytes>) {
    let committed = stored.get(partition);
    (
        committed.map_or(-1, |offset| offset.offset),
        committed.map_or(-1, |offset| offset.leader_epoch),
        committed.and_then(|offset| offset.metadata.clone().map(StrBytes::from_string)),
    )
}

fn partition_error(topic_error: i16, partition_count: Option<i32>, partition: i32) -> i16 {
    if topic_error != NO_ERROR {
        topic_error
    } else if !valid_partition(partition_count, partition) {
        UNKNOWN_TOPIC_OR_PARTITION
    } else {
        NO_ERROR
    }
}

fn valid_partition(partition_count: Option<i32>, partition: i32) -> bool {
    partition_count.is_some_and(|count| partition >= 0 && partition < count)
}

fn offset_commit_error(
    request: &OffsetCommitRequest,
    version: i16,
    error_code: i16,
) -> OffsetCommitResponse {
    OffsetCommitResponse::default()
        .with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    commit_response_topic(
                        version,
                        topic.name.as_str(),
                        topic.topic_id,
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(error_code)
                            })
                            .collect(),
                    )
                })
                .collect(),
        )
        .with_throttle_time_ms(0)
}

fn commit_response_topic(
    version: i16,
    name: &str,
    id: Uuid,
    partitions: Vec<OffsetCommitResponsePartition>,
) -> OffsetCommitResponseTopic {
    let response = OffsetCommitResponseTopic::default().with_partitions(partitions);
    if version >= 10 {
        response.with_topic_id(id)
    } else {
        response.with_name(topic_name(name))
    }
}

fn fetch_group_response(
    group_id: String,
    topics: Vec<OffsetFetchResponseTopics>,
    error_code: i16,
) -> OffsetFetchResponseGroup {
    OffsetFetchResponseGroup::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group_id)))
        .with_topics(topics)
        .with_error_code(error_code)
}

fn offset_group_error(error: ControlError, version: i16) -> i16 {
    match error {
        ControlError::GroupNotFound(_) if version >= 9 => GROUP_ID_NOT_FOUND,
        ControlError::GroupNotFound(_) => UNKNOWN_MEMBER_ID,
        ControlError::FencedMemberEpoch { .. } if version >= 9 => STALE_MEMBER_EPOCH,
        ControlError::FencedMemberEpoch { .. } => FENCED_MEMBER_EPOCH,
        error => control_error_code(&error),
    }
}
