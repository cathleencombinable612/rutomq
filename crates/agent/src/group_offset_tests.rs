use super::acl_tests::handle_as;
use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, NO_ERROR, OFFSET_METADATA_TOO_LARGE, STALE_MEMBER_EPOCH,
    TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::{
    ApiKey, GroupId, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclPatternType, AclPermission, AclRule, ConsumerGroupHeartbeat, MemoryMetadataStore,
    OffsetCommit, PostgresMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn commit_request(topic_id: Uuid, offset: i64) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(group_id("workers"))
        .with_generation_id_or_member_epoch(-1)
        .with_member_id(StrBytes::from_string(String::new()))
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset)
                        .with_committed_leader_epoch(0)
                        .with_committed_metadata(Some(StrBytes::from_static_str("checkpoint"))),
                ]),
        ])
}

fn fetch_request(topic_id: Uuid) -> OffsetFetchRequest {
    OffsetFetchRequest::default().with_groups(vec![
        OffsetFetchRequestGroup::default()
            .with_group_id(group_id("workers"))
            .with_member_id(None)
            .with_member_epoch(-1)
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_topic_id(topic_id)
                    .with_partition_indexes(vec![0]),
            ])),
    ])
}

#[tokio::test]
async fn offset_commit_and_fetch_v10_round_trip_topic_ids() {
    let broker = broker();
    let topic = broker.metadata.create_topic("events-v10", 1).await.unwrap();

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetCommit,
            10,
            901,
            &commit_request(topic.id, 17),
        ))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 10, response);
    assert_eq!(response.topics[0].topic_id, topic.id);
    assert!(response.topics[0].name.is_empty());
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetFetch,
            10,
            902,
            &fetch_request(topic.id),
        ))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 10, response);
    let fetched = &response.groups[0].topics[0];
    assert_eq!(fetched.topic_id, topic.id);
    assert!(fetched.name.is_empty());
    assert_eq!(fetched.partitions[0].committed_offset, 17);
    assert_eq!(fetched.partitions[0].committed_leader_epoch, 0);
    assert_eq!(
        fetched.partitions[0]
            .metadata
            .as_ref()
            .map(StrBytes::as_str),
        Some("checkpoint")
    );
    assert_eq!(fetched.partitions[0].error_code, NO_ERROR);

    let legacy = legacy_fetch_request("workers", &[("events-v10", &[0])]);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 5, 903, &legacy))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 5, response);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 17);
    assert_eq!(response.topics[0].partitions[0].committed_leader_epoch, 0);

    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 4, 904, &legacy))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 4, response);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 17);
    assert_eq!(response.topics[0].partitions[0].committed_leader_epoch, -1);
    assert_eq!(
        broker
            .metrics
            .kafka_requests
            .with_label_values(&["OffsetCommit", "10"])
            .get(),
        1
    );
    assert_eq!(
        broker
            .metrics
            .kafka_requests
            .with_label_values(&["OffsetFetch", "10"])
            .get(),
        1
    );
}

#[tokio::test]
async fn offset_commit_v2_applies_legacy_custom_retention() {
    let broker = broker();
    broker
        .metadata
        .create_topic("legacy-retention", 1)
        .await
        .unwrap();
    let request = named_commit_request(
        "legacy-retention-workers",
        "",
        -1,
        &[("legacy-retention", &[(0, 12)])],
    )
    .with_retention_time_ms(1);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 2, 905, &request))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 2, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    broker
        .metadata
        .expire_consumer_offsets(
            chrono::Utc::now().timestamp_millis() + 100,
            7 * 24 * 60 * 60 * 1_000,
            100,
        )
        .await
        .unwrap();
    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetFetch,
            5,
            906,
            &legacy_fetch_request("legacy-retention-workers", &[("legacy-retention", &[0])]),
        ))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 5, response);
    assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
}

#[tokio::test]
async fn offset_commit_enforces_kafka_metadata_string_length_per_partition() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata
        .create_topic("offset-metadata-limit", 2)
        .await
        .unwrap();
    let config = AgentConfig {
        offset_metadata_max_bytes: 4,
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let mut request = named_commit_request(
        "offset-metadata-group",
        "",
        -1,
        &[("offset-metadata-limit", &[(0, 11), (1, 12), (9, 13)])],
    );
    request.topics[0].partitions[0].committed_metadata =
        Some(StrBytes::from_string("éééé".to_owned()));
    request.topics[0].partitions[1].committed_metadata =
        Some(StrBytes::from_string("ééééx".to_owned()));
    request.topics[0].partitions[2].committed_metadata =
        Some(StrBytes::from_string("also-too-large".to_owned()));

    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 9, 907, &request))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.topics[0].partitions[1].error_code,
        OFFSET_METADATA_TOO_LARGE
    );
    assert_eq!(
        response.topics[0].partitions[2].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );

    let accepted = PartitionKey::new("offset-metadata-limit", 0);
    let rejected = PartitionKey::new("offset-metadata-limit", 1);
    let committed = metadata
        .fetch_offsets(
            "offset-metadata-group",
            &[accepted.clone(), rejected.clone()],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[&accepted].offset, 11);
    assert_eq!(committed[&accepted].metadata.as_deref(), Some("éééé"));
    assert!(!committed.contains_key(&rejected));
}

#[tokio::test]
async fn offset_v10_preserves_unknown_topic_ids_in_errors() {
    let broker = broker();
    let missing = Uuid::new_v4();

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetCommit,
            10,
            903,
            &commit_request(missing, 3),
        ))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 10, response);
    assert_eq!(response.topics[0].topic_id, missing);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_ID
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::OffsetFetch,
            10,
            904,
            &fetch_request(missing),
        ))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 10, response);
    assert_eq!(response.groups[0].topics[0].topic_id, missing);
    assert_eq!(
        response.groups[0].topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_ID
    );
}

#[tokio::test]
async fn offset_commit_authorizer_failures_are_complete_and_non_mutating() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let user = "offset-commit-user";
    let denied_user = "offset-commit-denied";
    let group = "offset-commit-backend-group";
    for topic in ["offset-commit-backend-a", "offset-commit-backend-b"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    create_allow_rules(
        metadata.as_ref(),
        user,
        group,
        &["offset-commit-backend-a", "offset-commit-backend-b"],
    )
    .await;
    let broker = secured_broker(metadata.clone());
    let request = named_commit_request(
        group,
        "",
        -1,
        &[
            ("offset-commit-backend-a", &[(0, 11)]),
            ("offset-commit-backend-b", &[(0, 12)]),
        ],
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let response = handle_as(&broker, denied_user, ApiKey::OffsetCommit, 9, 910, &request).await;
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert!(response.topics.iter().all(|topic| {
        topic
            .partitions
            .iter()
            .all(|partition| partition.error_code == GROUP_AUTHORIZATION_FAILED)
    }));

    metadata.set_authorization_failure_for(Some(AclResourceType::Group));
    let response = handle_as(&broker, user, ApiKey::OffsetCommit, 9, 911, &request).await;
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert!(response.topics.iter().all(|topic| {
        topic
            .partitions
            .iter()
            .all(|partition| partition.error_code == UNKNOWN_SERVER_ERROR)
    }));

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let response = handle_as(&broker, user, ApiKey::OffsetCommit, 9, 912, &request).await;
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert!(response.topics.iter().all(|topic| {
        topic
            .partitions
            .iter()
            .all(|partition| partition.error_code == UNKNOWN_SERVER_ERROR)
    }));
    metadata.set_authorization_failure_for(None);
    let committed = metadata
        .fetch_offsets(
            group,
            &[
                PartitionKey::new("offset-commit-backend-a", 0),
                PartitionKey::new("offset-commit-backend-b", 0),
            ],
        )
        .await
        .unwrap();
    assert!(committed.is_empty());
}

#[tokio::test]
async fn offset_commit_preserves_mixed_preflight_results_in_memory() {
    assert_offset_commit_preflight(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn offset_commit_preserves_mixed_preflight_results_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_offset_commit_preflight(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn offset_fetch_authorizer_failures_return_versioned_request_errors() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let user = "offset-fetch-backend-user";
    let groups = ["offset-fetch-backend-a", "offset-fetch-backend-b"];
    let topic = "offset-fetch-backend-topic";
    metadata.create_topic(topic, 1).await.unwrap();
    for group in groups {
        metadata
            .create_acl(acl_rule(
                &format!("User:{user}"),
                AclResourceType::Group,
                group,
                AclOperation::Describe,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
    metadata
        .create_acl(acl_rule(
            &format!("User:{user}"),
            AclResourceType::Topic,
            topic,
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());

    metadata.set_authorization_failure_for(Some(AclResourceType::Group));
    let legacy = legacy_fetch_request(groups[0], &[(topic, &[0])]);
    let response = handle_as(&broker, user, ApiKey::OffsetFetch, 1, 930, &legacy).await;
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 1, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        UNKNOWN_SERVER_ERROR
    );
    assert_eq!(response.topics[0].partitions[0].committed_offset, -1);

    let batched = batched_fetch_request(&[
        (groups[0], Some(&[(topic, &[0])])),
        (groups[1], Some(&[(topic, &[0])])),
    ]);
    let response = handle_as(&broker, user, ApiKey::OffsetFetch, 8, 931, &batched).await;
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 8, response);
    assert_eq!(response.groups.len(), 2);
    assert!(
        response
            .groups
            .iter()
            .all(|group| { group.error_code == UNKNOWN_SERVER_ERROR && group.topics.is_empty() })
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let response = handle_as(&broker, user, ApiKey::OffsetFetch, 8, 932, &batched).await;
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 8, response);
    assert_eq!(response.groups.len(), 2);
    assert!(
        response
            .groups
            .iter()
            .all(|group| { group.error_code == UNKNOWN_SERVER_ERROR && group.topics.is_empty() })
    );
}

#[tokio::test]
async fn offset_fetch_preserves_explicit_privacy_and_all_topic_filtering_in_memory() {
    assert_offset_fetch_privacy(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn offset_fetch_preserves_explicit_privacy_and_all_topic_filtering_in_postgres() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    assert_offset_fetch_privacy(Arc::new(store), &Uuid::new_v4().simple().to_string()).await;
}

async fn assert_offset_fetch_privacy(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("offset-fetch-visible-{suffix}");
    let hidden = format!("offset-fetch-hidden-{suffix}");
    let missing = format!("offset-fetch-missing-{suffix}");
    let unrelated = format!("offset-fetch-unrelated-{suffix}");
    let group = format!("offset-fetch-group-{suffix}");
    let user = format!("offset-fetch-user-{suffix}");
    metadata.create_topic(&visible, 2).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    metadata.create_topic(&unrelated, 1).await.unwrap();
    metadata
        .commit_offsets(
            &group,
            vec![
                OffsetCommit {
                    partition: PartitionKey::new(&visible, 0),
                    offset: 7,
                    leader_epoch: -1,
                    metadata: Some("visible".to_owned()),
                    retention_time_ms: None,
                },
                OffsetCommit {
                    partition: PartitionKey::new(&hidden, 0),
                    offset: 8,
                    leader_epoch: -1,
                    metadata: Some("hidden".to_owned()),
                    retention_time_ms: None,
                },
            ],
        )
        .await
        .unwrap();
    let principal = format!("User:{user}");
    metadata
        .create_acl(acl_rule(
            &principal,
            AclResourceType::Group,
            &group,
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    for topic in [&visible, &missing] {
        metadata
            .create_acl(acl_rule(
                &principal,
                AclResourceType::Topic,
                topic,
                AclOperation::Describe,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
    metadata
        .create_acl(acl_rule(
            &principal,
            AclResourceType::Topic,
            &hidden,
            AclOperation::Describe,
            AclPermission::Deny,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);

    let explicit = batched_fetch_request(&[(
        &group,
        Some(&[
            (hidden.as_str(), &[0][..]),
            (missing.as_str(), &[0][..]),
            (visible.as_str(), &[9, 0][..]),
        ]),
    )]);
    let response = handle_as(&broker, &user, ApiKey::OffsetFetch, 9, 940, &explicit).await;
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 9, response);
    let topics = &response.groups[0].topics;
    assert_eq!(topics.len(), 3);
    assert_eq!(topics[0].name.as_str(), missing);
    assert_eq!(
        topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(topics[1].name.as_str(), visible);
    assert_eq!(
        topics[1].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(topics[1].partitions[1].error_code, NO_ERROR);
    assert_eq!(topics[1].partitions[1].committed_offset, 7);
    assert_eq!(topics[2].name.as_str(), hidden);
    assert_eq!(
        topics[2].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );

    let all = batched_fetch_request(&[(&group, None)]);
    let response = handle_as(&broker, &user, ApiKey::OffsetFetch, 9, 941, &all).await;
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 9, response);
    let topics = &response.groups[0].topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name.as_str(), visible);
    assert_eq!(topics[0].partitions.len(), 1);
    assert_eq!(topics[0].partitions[0].partition_index, 0);
    assert_eq!(topics[0].partitions[0].committed_offset, 7);
}

async fn assert_offset_commit_preflight(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let visible = format!("offset-commit-visible-{suffix}");
    let hidden = format!("offset-commit-hidden-{suffix}");
    let missing = format!("offset-commit-missing-{suffix}");
    let group = format!("offset-commit-group-{suffix}");
    let user = format!("offset-commit-user-{suffix}");
    metadata.create_topic(&visible, 2).await.unwrap();
    metadata.create_topic(&hidden, 1).await.unwrap();
    create_allow_rules(metadata.as_ref(), &user, &group, &[&visible, &missing]).await;
    metadata
        .create_acl(acl_rule(
            &format!("User:{user}"),
            AclResourceType::Topic,
            &hidden,
            AclOperation::Read,
            AclPermission::Deny,
        ))
        .await
        .unwrap();
    let member = metadata
        .consumer_group_heartbeat(ConsumerGroupHeartbeat {
            group_id: group.clone(),
            member_id: "offset-commit-member".to_owned(),
            member_epoch: 0,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec![visible.clone()]),
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".to_owned()),
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: Some(Vec::new()),
            client_id: "offset-commit-test".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        })
        .await
        .unwrap();
    let broker = secured_broker(metadata.clone());
    let topics = [
        (hidden.as_str(), &[(0, 20)][..]),
        (missing.as_str(), &[(0, 21)][..]),
        (visible.as_str(), &[(9, 22), (0, 23)][..]),
    ];
    let stale = named_commit_request(&group, &member.member_id, member.member_epoch + 1, &topics);
    let response = handle_as(&broker, &user, ApiKey::OffsetCommit, 9, 920, &stale).await;
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(
        response.topics[1].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        response.topics[2].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        response.topics[2].partitions[1].error_code,
        STALE_MEMBER_EPOCH
    );

    let valid = named_commit_request(&group, &member.member_id, member.member_epoch, &topics);
    let response = handle_as(&broker, &user, ApiKey::OffsetCommit, 9, 921, &valid).await;
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 9, response);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(
        response.topics[1].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        response.topics[2].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(response.topics[2].partitions[1].error_code, NO_ERROR);

    let visible_partition = PartitionKey::new(&visible, 0);
    let committed = metadata
        .fetch_offsets(
            &group,
            &[visible_partition.clone(), PartitionKey::new(&hidden, 0)],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[&visible_partition].offset, 23);
}

fn named_commit_request(
    group: &str,
    member_id: &str,
    member_epoch: i32,
    topics: &[(&str, &[(i32, i64)])],
) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id_or_member_epoch(member_epoch)
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
        .with_topics(
            topics
                .iter()
                .map(|(topic, partitions)| {
                    OffsetCommitRequestTopic::default()
                        .with_name(topic_name(topic))
                        .with_partitions(
                            partitions
                                .iter()
                                .map(|(partition, offset)| {
                                    OffsetCommitRequestPartition::default()
                                        .with_partition_index(*partition)
                                        .with_committed_offset(*offset)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}

fn legacy_fetch_request(group: &str, topics: &[(&str, &[i32])]) -> OffsetFetchRequest {
    OffsetFetchRequest::default()
        .with_group_id(group_id(group))
        .with_topics(Some(
            topics
                .iter()
                .map(|(topic, partitions)| {
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name(topic))
                        .with_partition_indexes(partitions.to_vec())
                })
                .collect(),
        ))
}

type RequestedFetchTopic<'a> = (&'a str, &'a [i32]);
type RequestedFetchGroup<'a> = (&'a str, Option<&'a [RequestedFetchTopic<'a>]>);

fn batched_fetch_request(groups: &[RequestedFetchGroup<'_>]) -> OffsetFetchRequest {
    OffsetFetchRequest::default().with_groups(
        groups
            .iter()
            .map(|(group, topics)| {
                OffsetFetchRequestGroup::default()
                    .with_group_id(group_id(group))
                    .with_member_id(None)
                    .with_member_epoch(-1)
                    .with_topics(topics.map(|topics| {
                        topics
                            .iter()
                            .map(|(topic, partitions)| {
                                OffsetFetchRequestTopics::default()
                                    .with_name(topic_name(topic))
                                    .with_partition_indexes(partitions.to_vec())
                            })
                            .collect()
                    }))
            })
            .collect(),
    )
}

async fn create_allow_rules(
    metadata: &dyn MetadataStore,
    user: &str,
    group: &str,
    topics: &[&str],
) {
    let principal = format!("User:{user}");
    metadata
        .create_acl(acl_rule(
            &principal,
            AclResourceType::Group,
            group,
            AclOperation::Read,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    for topic in topics {
        metadata
            .create_acl(acl_rule(
                &principal,
                AclResourceType::Topic,
                topic,
                AclOperation::Read,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
}

fn acl_rule(
    principal: &str,
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
    permission: AclPermission,
) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission,
    }
}

fn secured_broker(metadata: Arc<dyn MetadataStore>) -> Broker {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}
