use rutomq_control::{
    ConsumerGroupHeartbeat, ConsumerOwnedTopicPartitions, ControlError, GroupAssignmentCompletion,
    GroupMemberIdentity, MetadataStore, OffsetCommit, PartitionKey, PostgresMetadataStore,
};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn postgres_consumer_protocol_persists_epochs_and_assignments() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-topic-{suffix}");
    let group_id = format!("consumer-group-{suffix}");
    let topic = store.create_topic(&topic_name, 2).await.unwrap();
    let joined = store
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap();
    assert_eq!(joined.member_epoch, 2);
    assert_eq!(joined.assignment.as_ref().unwrap()[0].partitions, [0, 1]);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let acknowledged = reconnected
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            joined.member_epoch,
            None,
            vec![ConsumerOwnedTopicPartitions {
                topic_id: topic.id,
                partitions: vec![0, 1],
            }],
        ))
        .await
        .unwrap();
    assert!(acknowledged.assignment.is_none());

    let descriptions = reconnected
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap();
    let description = &descriptions[&group_id];
    assert_eq!(description.state, "Stable");
    assert_eq!(description.group_epoch, 2);
    assert_eq!(description.members[0].assignment[0].topic_name, topic_name);
}

#[tokio::test]
async fn postgres_consumer_group_max_size_serializes_cross_agent_joins() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-limit-topic-{suffix}");
    let group_id = format!("consumer-limit-group-{suffix}");
    first_agent.create_topic(&topic_name, 1).await.unwrap();

    let mut first = heartbeat(&group_id, "member-a", 0, Some(&topic_name), vec![]);
    first.max_size = 1;
    let mut second = heartbeat(&group_id, "member-b", 0, Some(&topic_name), vec![]);
    second.max_size = 1;
    let (first_result, second_result) = tokio::join!(
        first_agent.consumer_group_heartbeat(first),
        second_agent.consumer_group_heartbeat(second)
    );
    let results = [first_result, second_result];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControlError::GroupMaxSizeReached(_))))
            .count(),
        1
    );

    let description = first_agent
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap()
        .remove(&group_id)
        .unwrap();
    assert_eq!(description.members.len(), 1);
    let member = &description.members[0];
    let mut retry = heartbeat(
        &group_id,
        &member.member_id,
        member.member_epoch,
        None,
        vec![],
    );
    retry.max_size = 1;
    first_agent.consumer_group_heartbeat(retry).await.unwrap();
}

#[tokio::test]
async fn postgres_consumer_groups_persist_configured_default_assignor() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-assignor-topic-{suffix}");
    let group_id = format!("consumer-assignor-group-{suffix}");
    let deferred_group_id = format!("consumer-assignor-deferred-group-{suffix}");
    store.create_topic(&topic_name, 2).await.unwrap();

    let mut join = heartbeat(&group_id, "member-a", 0, Some(&topic_name), vec![]);
    join.configured_assignors = vec!["range".to_owned()];
    store.consumer_group_heartbeat(join).await.unwrap();

    let mut deferred_join = heartbeat(&deferred_group_id, "member-b", 0, Some(&topic_name), vec![]);
    deferred_join.configured_assignors = vec!["range".to_owned()];
    store
        .consumer_group_heartbeat_deferred(deferred_join)
        .await
        .unwrap();

    let descriptions = PostgresMetadataStore::connect(&database_url)
        .await
        .unwrap()
        .describe_consumer_groups(&[group_id.clone(), deferred_group_id.clone()])
        .await
        .unwrap();
    assert_eq!(descriptions[&group_id].assignor_name, "range");
    assert_eq!(descriptions[&group_id].members.len(), 1);
    assert_eq!(descriptions[&deferred_group_id].assignor_name, "range");
    assert_eq!(descriptions[&deferred_group_id].members.len(), 1);
}

#[tokio::test]
async fn postgres_regex_refresh_survives_reconnected_agents() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let first_topic_name = format!("regex-{suffix}-1");
    let second_topic_name = format!("regex-{suffix}-2");
    let group_id = format!("regex-group-{suffix}");
    let first_topic = first_agent
        .create_topic(&first_topic_name, 1)
        .await
        .unwrap();

    let mut join = heartbeat(&group_id, "member-a", 0, None, vec![]);
    join.subscribed_topic_names = Some(Vec::new());
    join.subscribed_topic_regex = Some(format!("regex-{suffix}-.*"));
    join.regex_refresh_interval_ms = 20_000;
    let joined = first_agent.consumer_group_heartbeat(join).await.unwrap();
    assert_eq!(
        joined.assignment.as_ref().unwrap()[0].topic_id,
        first_topic.id
    );

    let second_topic = first_agent
        .create_topic(&second_topic_name, 1)
        .await
        .unwrap();
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    sqlx::query(
        "UPDATE consumer_protocol_groups
         SET regex_refresh_timestamp = now() - interval '11 seconds',
             regex_refresh_pending = FALSE
         WHERE group_id = $1",
    )
    .bind(&group_id)
    .execute(&pool)
    .await
    .unwrap();

    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let mut refresh = heartbeat(
        &group_id,
        "member-a",
        joined.member_epoch,
        None,
        vec![ConsumerOwnedTopicPartitions {
            topic_id: first_topic.id,
            partitions: vec![0],
        }],
    );
    refresh.regex_refresh_interval_ms = 20_000;
    let task = second_agent
        .consumer_group_heartbeat_deferred(refresh)
        .await
        .unwrap()
        .assignment_task
        .unwrap();
    assert_eq!(
        PostgresMetadataStore::connect(&database_url)
            .await
            .unwrap()
            .complete_group_assignment(task)
            .await
            .unwrap(),
        GroupAssignmentCompletion::Published
    );

    let description = PostgresMetadataStore::connect(&database_url)
        .await
        .unwrap()
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap()
        .remove(&group_id)
        .unwrap();
    let target_topic_ids = description.members[0]
        .target_assignment
        .iter()
        .map(|assignment| assignment.topic_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        target_topic_ids,
        std::collections::BTreeSet::from([first_topic.id, second_topic.id])
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT NOT regex_refresh_pending
             AND regex_refresh_timestamp > now() - interval '5 seconds'
             FROM consumer_protocol_groups WHERE group_id = $1",
        )
        .bind(&group_id)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn postgres_converts_empty_classic_and_consumer_groups_across_agents() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("group-conversion-topic-{suffix}");
    let group_id = format!("group-conversion-{suffix}");
    let partition = PartitionKey::new(&topic_name, 0);
    first_agent.create_topic(&topic_name, 1).await.unwrap();
    first_agent
        .commit_offsets(
            &group_id,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 11,
                leader_epoch: -1,
                metadata: Some("preserved".to_owned()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    first_agent
        .alter_group_config(
            &group_id,
            BTreeMap::from([(
                "consumer.heartbeat.interval.ms".to_owned(),
                Some("6000".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    let classic = first_agent
        .join_group(
            &group_id,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                std::slice::from_ref(&topic_name),
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    assert!(matches!(
        first_agent
            .consumer_group_heartbeat(heartbeat(
                &group_id,
                "consumer-a",
                0,
                Some(&topic_name),
                vec![],
            ))
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
    first_agent
        .leave_group(
            &group_id,
            &[GroupMemberIdentity {
                member_id: classic.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();

    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let consumer = second_agent
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "consumer-a",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap();
    assert_eq!(
        second_agent
            .fetch_offsets(&group_id, std::slice::from_ref(&partition))
            .await
            .unwrap()[&partition]
            .offset,
        11
    );
    assert_eq!(
        second_agent.group_config(&group_id).await.unwrap()["consumer.heartbeat.interval.ms"],
        "6000"
    );
    assert!(matches!(
        second_agent
            .join_group(
                &group_id,
                "",
                None,
                "consumer",
                &[("range".to_owned(), vec![1])],
                (
                    "classic-client",
                    "127.0.0.1",
                    std::slice::from_ref(&topic_name),
                    45_000,
                ),
                3,
            )
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
    second_agent
        .consumer_group_heartbeat(heartbeat(&group_id, &consumer.member_id, -1, None, vec![]))
        .await
        .unwrap();

    let third_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    third_agent
        .join_group(
            &group_id,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                std::slice::from_ref(&topic_name),
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        third_agent
            .fetch_offsets(&group_id, std::slice::from_ref(&partition))
            .await
            .unwrap()[&partition]
            .offset,
        11
    );
    assert_eq!(
        third_agent.group_config(&group_id).await.unwrap()["consumer.heartbeat.interval.ms"],
        "6000"
    );

    let expired_group = format!("expired-group-conversion-{suffix}");
    let mut expired = heartbeat(
        &expired_group,
        "expired-member",
        0,
        Some(&topic_name),
        vec![],
    );
    expired.session_timeout_ms = 1;
    third_agent.consumer_group_heartbeat(expired).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    PostgresMetadataStore::connect(&database_url)
        .await
        .unwrap()
        .join_group(
            &expired_group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                std::slice::from_ref(&topic_name),
                45_000,
            ),
            3,
        )
        .await
        .unwrap();

    let static_group = format!("static-group-conversion-{suffix}");
    let mut static_join = heartbeat(&static_group, "static-member", 0, Some(&topic_name), vec![]);
    static_join.instance_id = Some("static-instance".to_owned());
    third_agent
        .consumer_group_heartbeat(static_join)
        .await
        .unwrap();
    let mut static_leave = heartbeat(&static_group, "static-member", -2, None, vec![]);
    static_leave.instance_id = Some("static-instance".to_owned());
    static_leave.owned_partitions = None;
    third_agent
        .consumer_group_heartbeat(static_leave)
        .await
        .unwrap();
    assert!(matches!(
        PostgresMetadataStore::connect(&database_url)
            .await
            .unwrap()
            .join_group(
                &static_group,
                "",
                None,
                "consumer",
                &[("range".to_owned(), vec![1])],
                (
                    "classic-client",
                    "127.0.0.1",
                    std::slice::from_ref(&topic_name),
                    45_000,
                ),
                3,
            )
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));

    let pending_group = format!("pending-group-conversion-{suffix}");
    let pending_member_id = match third_agent
        .begin_join_group(
            &pending_group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                std::slice::from_ref(&topic_name),
                45_000,
            ),
            45_000,
            0,
            i32::MAX,
            4,
        )
        .await
        .unwrap_err()
    {
        ControlError::MemberIdRequired { member_id } => member_id,
        error => panic!("unexpected pending-member result: {error}"),
    };
    let pending_consumer = third_agent
        .consumer_group_heartbeat_deferred(heartbeat(
            &pending_group,
            "pending-consumer",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap()
        .result;
    third_agent
        .consumer_group_heartbeat(heartbeat(
            &pending_group,
            &pending_consumer.member_id,
            -1,
            None,
            vec![],
        ))
        .await
        .unwrap();
    assert!(matches!(
        PostgresMetadataStore::connect(&database_url)
            .await
            .unwrap()
            .begin_join_group(
                &pending_group,
                &pending_member_id,
                None,
                "consumer",
                &[("range".to_owned(), vec![1])],
                (
                    "classic-client",
                    "127.0.0.1",
                    std::slice::from_ref(&topic_name),
                    45_000,
                ),
                45_000,
                0,
                i32::MAX,
                4,
            )
            .await,
        Err(ControlError::GroupMemberNotFound { .. })
    ));
}

#[tokio::test]
async fn postgres_deferred_assignment_is_single_flight_across_reconnected_agents() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-offload-topic-{suffix}");
    let group_id = format!("consumer-offload-group-{suffix}");
    first_agent.create_topic(&topic_name, 2).await.unwrap();

    let joined = first_agent
        .consumer_group_heartbeat_deferred(heartbeat(
            &group_id,
            "member-a",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap();
    assert_eq!(joined.result.member_epoch, 1);
    assert!(joined.result.assignment.unwrap().is_empty());
    let task = joined.assignment_task.unwrap();

    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        second_agent
            .complete_group_assignment(task.clone())
            .await
            .unwrap(),
        GroupAssignmentCompletion::Published
    );
    assert_eq!(
        first_agent.complete_group_assignment(task).await.unwrap(),
        GroupAssignmentCompletion::Stale
    );

    let assigned = first_agent
        .consumer_group_heartbeat_deferred(heartbeat(&group_id, "member-a", 1, None, vec![]))
        .await
        .unwrap()
        .result;
    assert_eq!(assigned.member_epoch, 2);
    assert_eq!(assigned.assignment.unwrap()[0].partitions, [0, 1]);
}

#[tokio::test]
async fn postgres_consumer_assignment_delay_survives_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-delay-topic-{suffix}");
    let group_id = format!("consumer-delay-group-{suffix}");
    store.create_topic(&topic_name, 2).await.unwrap();

    let mut first = heartbeat(&group_id, "member-a", 0, Some(&topic_name), vec![]);
    first.assignment_interval_ms = 15_000;
    let joined = store.consumer_group_heartbeat(first).await.unwrap();
    assert_eq!(joined.member_epoch, 2);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let mut second = heartbeat(&group_id, "member-b", 0, Some(&topic_name), vec![]);
    second.assignment_interval_ms = 15_000;
    let delayed = reconnected.consumer_group_heartbeat(second).await.unwrap();
    assert_eq!(delayed.member_epoch, 2);
    assert!(delayed.assignment.is_none());

    let description = PostgresMetadataStore::connect(&database_url)
        .await
        .unwrap()
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap()
        .remove(&group_id)
        .unwrap();
    assert_eq!(description.state, "Assigning");
    assert_eq!(description.group_epoch, 3);
    assert_eq!(description.assignment_epoch, 2);
}

#[tokio::test]
async fn postgres_consumer_previous_epoch_retry_survives_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-recovery-topic-{suffix}");
    let group_id = format!("consumer-recovery-group-{suffix}");
    let topic = store.create_topic(&topic_name, 2).await.unwrap();

    let first = store
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap();
    store
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            first.member_epoch,
            None,
            vec![ConsumerOwnedTopicPartitions {
                topic_id: topic.id,
                partitions: vec![0, 1],
            }],
        ))
        .await
        .unwrap();
    store
        .consumer_group_heartbeat(heartbeat(
            &group_id,
            "member-b",
            0,
            Some(&topic_name),
            vec![],
        ))
        .await
        .unwrap();

    let mut revoke = heartbeat(&group_id, "member-a", first.member_epoch, None, vec![]);
    revoke.owned_partitions = None;
    let revoking = store.consumer_group_heartbeat(revoke).await.unwrap();
    let lost_response = heartbeat(
        &group_id,
        "member-a",
        first.member_epoch,
        None,
        vec![ConsumerOwnedTopicPartitions {
            topic_id: topic.id,
            partitions: revoking.assignment.unwrap()[0].partitions.clone(),
        }],
    );
    let advanced = store
        .consumer_group_heartbeat(lost_response.clone())
        .await
        .unwrap();
    assert_eq!(advanced.member_epoch, 3);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let retained = PartitionKey::new(&topic_name, 0);
    let moved = PartitionKey::new(&topic_name, 1);
    let validity = reconnected
        .commit_member_offsets(
            &group_id,
            "member-a",
            None,
            first.member_epoch,
            9,
            vec![
                OffsetCommit {
                    partition: retained.clone(),
                    offset: 10,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
                OffsetCommit {
                    partition: moved.clone(),
                    offset: 11,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(validity, [true, false]);
    let committed = reconnected
        .fetch_offsets(&group_id, &[retained.clone(), moved.clone()])
        .await
        .unwrap();
    assert_eq!(committed[&retained].offset, 10);
    assert!(!committed.contains_key(&moved));

    let recovered = reconnected
        .consumer_group_heartbeat(lost_response.clone())
        .await
        .unwrap();
    assert_eq!(recovered.member_epoch, 3);
    let duplicate = reconnected
        .consumer_group_heartbeat(lost_response)
        .await
        .unwrap();
    assert_eq!(duplicate.member_epoch, 3);
}

#[tokio::test]
async fn postgres_consumer_static_temporary_leave_reconnects_with_assignment() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("consumer-static-topic-{suffix}");
    let group_id = format!("consumer-static-group-{suffix}");
    let topic = store.create_topic(&topic_name, 2).await.unwrap();

    let mut join = heartbeat(&group_id, "member-a", 0, Some(&topic_name), vec![]);
    join.instance_id = Some("instance-a".to_owned());
    let joined = store.consumer_group_heartbeat(join.clone()).await.unwrap();
    let mut acknowledge = heartbeat(
        &group_id,
        "member-a",
        joined.member_epoch,
        None,
        vec![ConsumerOwnedTopicPartitions {
            topic_id: topic.id,
            partitions: vec![0, 1],
        }],
    );
    acknowledge.instance_id = join.instance_id.clone();
    store.consumer_group_heartbeat(acknowledge).await.unwrap();

    let mut leave = heartbeat(&group_id, "member-a", -2, None, vec![]);
    leave.instance_id = join.instance_id;
    leave.owned_partitions = None;
    let left = store.consumer_group_heartbeat(leave).await.unwrap();
    assert_eq!(left.member_epoch, -2);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let mut rejoin = heartbeat(&group_id, "member-b", 0, Some(&topic_name), vec![]);
    rejoin.instance_id = Some("instance-a".to_owned());
    let rejoined = reconnected.consumer_group_heartbeat(rejoin).await.unwrap();
    assert_eq!(rejoined.member_epoch, joined.member_epoch);
    assert_eq!(rejoined.assignment.unwrap()[0].partitions, [0, 1]);
    let partition = PartitionKey::new(&topic_name, 0);
    assert_eq!(
        reconnected
            .commit_member_offsets(
                &group_id,
                "member-b",
                Some("instance-a"),
                0,
                9,
                vec![OffsetCommit {
                    partition: partition.clone(),
                    offset: 7,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                }],
            )
            .await
            .unwrap(),
        [true]
    );
    assert_eq!(
        reconnected
            .fetch_offsets(&group_id, &[partition.clone()])
            .await
            .unwrap()[&partition]
            .offset,
        7
    );
}

fn heartbeat(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
    owned_partitions: Vec<ConsumerOwnedTopicPartitions>,
) -> ConsumerGroupHeartbeat {
    ConsumerGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
        subscribed_topic_names: topic.map(|topic| vec![topic.to_owned()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions: Some(owned_partitions),
        client_id: "postgres-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        assignment_interval_ms: 0,
        max_size: i32::MAX,
    }
}
