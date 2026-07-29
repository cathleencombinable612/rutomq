use rutomq_control::{
    ControlError, GroupAssignmentCompletion, MetadataStore, PostgresMetadataStore,
    ShareGroupHeartbeat, StreamsGroupHeartbeat, StreamsInternalTopic, StreamsSubtopology,
    StreamsTaskAssignment, StreamsTopology,
};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn postgres_streams_group_survives_reconnect_and_group_admin() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("streams-topic-{suffix}");
    let group_id = format!("streams-group-{suffix}");
    store.create_topic(&topic_name, 2).await.unwrap();

    let joined = store
        .streams_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            0,
            Some(topology(&topic_name)),
            Some(StreamsTaskAssignment::default()),
        ))
        .await
        .unwrap();
    assert_eq!(joined.member_epoch, 2);
    assert_eq!(joined.assignment.as_ref().unwrap().active_tasks.len(), 2);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let acknowledged = reconnected
        .streams_group_heartbeat(heartbeat(
            &group_id,
            "member-a",
            joined.member_epoch,
            None,
            joined.assignment,
        ))
        .await
        .unwrap();
    assert!(acknowledged.assignment.is_none());
    reconnected
        .validate_group_member(&group_id, "member-a", None, acknowledged.member_epoch)
        .await
        .unwrap();

    let description = reconnected
        .describe_streams_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap()
        .remove(&group_id)
        .unwrap();
    assert_eq!(description.state, "Stable");
    assert_eq!(description.members[0].client_id, "postgres-streams-test");
    assert_eq!(
        description.topology.subtopologies[0].source_topics,
        [topic_name.clone()]
    );
    let summary = reconnected
        .list_groups()
        .await
        .unwrap()
        .into_iter()
        .find(|group| group.group_id == group_id)
        .unwrap();
    assert_eq!(summary.group_type, "Streams");
    assert_eq!(summary.protocol_type, "streams");
    assert!(matches!(
        reconnected.delete_group(&group_id).await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    assert!(matches!(
        reconnected
            .share_group_heartbeat(share_heartbeat(&group_id, 0, Some(&topic_name)))
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));

    reconnected
        .streams_group_heartbeat(heartbeat(&group_id, "member-a", -1, None, None))
        .await
        .unwrap();
    reconnected.delete_group(&group_id).await.unwrap();

    let share_group_id = format!("share-group-{suffix}");
    reconnected
        .share_group_heartbeat(share_heartbeat(&share_group_id, 0, Some(&topic_name)))
        .await
        .unwrap();
    assert!(matches!(
        reconnected
            .streams_group_heartbeat(heartbeat(
                &share_group_id,
                "streams-member",
                0,
                Some(topology("unused")),
                Some(StreamsTaskAssignment::default()),
            ))
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
    reconnected
        .share_group_heartbeat(share_heartbeat(&share_group_id, -1, None))
        .await
        .unwrap();
    reconnected.delete_group(&share_group_id).await.unwrap();
}

#[tokio::test]
async fn postgres_share_group_max_size_serializes_cross_agent_joins() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-limit-topic-{suffix}");
    let group_id = format!("share-limit-group-{suffix}");
    first_agent.create_topic(&topic_name, 1).await.unwrap();

    let mut first = share_heartbeat(&group_id, 0, Some(&topic_name));
    first.member_id = "share-a".to_owned();
    first.max_size = 1;
    let mut second = share_heartbeat(&group_id, 0, Some(&topic_name));
    second.member_id = "share-b".to_owned();
    second.max_size = 1;
    let (first_result, second_result) = tokio::join!(
        first_agent.share_group_heartbeat(first),
        second_agent.share_group_heartbeat(second)
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
        .describe_share_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap()
        .remove(&group_id)
        .unwrap();
    assert_eq!(description.members.len(), 1);
    let member = &description.members[0];
    let mut retry = share_heartbeat(&group_id, member.member_epoch, None);
    retry.member_id.clone_from(&member.member_id);
    retry.max_size = 1;
    first_agent.share_group_heartbeat(retry).await.unwrap();
}

#[tokio::test]
async fn postgres_streams_group_max_size_serializes_cross_agent_joins() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("streams-limit-topic-{suffix}");
    let group_id = format!("streams-limit-group-{suffix}");
    first_agent.create_topic(&topic_name, 1).await.unwrap();

    let mut first = heartbeat(
        &group_id,
        "streams-a",
        0,
        Some(topology(&topic_name)),
        Some(StreamsTaskAssignment::default()),
    );
    first.max_size = 1;
    first.process_id = Some("process-a".to_owned());
    let mut second = heartbeat(
        &group_id,
        "streams-b",
        0,
        Some(topology(&topic_name)),
        Some(StreamsTaskAssignment::default()),
    );
    second.max_size = 1;
    second.process_id = Some("process-b".to_owned());
    let (first_result, second_result) = tokio::join!(
        first_agent.streams_group_heartbeat(first),
        second_agent.streams_group_heartbeat(second)
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
        .describe_streams_groups(std::slice::from_ref(&group_id))
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
        Some(member.assignment.clone()),
    );
    retry.max_size = 1;
    first_agent.streams_group_heartbeat(retry).await.unwrap();
}

#[tokio::test]
async fn postgres_share_and_streams_deferred_assignments_publish_after_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("offload-protocol-topic-{suffix}");
    first_agent.create_topic(&topic_name, 2).await.unwrap();
    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();

    let share_group = format!("offload-share-{suffix}");
    let share_joined = first_agent
        .share_group_heartbeat_deferred(share_heartbeat(&share_group, 0, Some(&topic_name)))
        .await
        .unwrap();
    assert_eq!(share_joined.result.member_epoch, 1);
    assert!(share_joined.result.assignment.unwrap().is_empty());
    assert_eq!(
        second_agent
            .complete_group_assignment(share_joined.assignment_task.unwrap())
            .await
            .unwrap(),
        GroupAssignmentCompletion::Published
    );
    let share_assigned = first_agent
        .share_group_heartbeat_deferred(share_heartbeat(&share_group, 1, None))
        .await
        .unwrap()
        .result;
    assert_eq!(share_assigned.member_epoch, 2);
    assert_eq!(share_assigned.assignment.unwrap()[0].partitions, [0, 1]);

    let streams_group = format!("offload-streams-{suffix}");
    let streams_joined = first_agent
        .streams_group_heartbeat_deferred(heartbeat(
            &streams_group,
            "streams-a",
            0,
            Some(topology(&topic_name)),
            Some(StreamsTaskAssignment::default()),
        ))
        .await
        .unwrap();
    assert_eq!(streams_joined.result.member_epoch, 1);
    assert!(
        streams_joined
            .result
            .assignment
            .unwrap()
            .active_tasks
            .is_empty()
    );
    assert_eq!(
        second_agent
            .complete_group_assignment(streams_joined.assignment_task.unwrap())
            .await
            .unwrap(),
        GroupAssignmentCompletion::Published
    );
    let streams_assigned = first_agent
        .streams_group_heartbeat_deferred(heartbeat(
            &streams_group,
            "streams-a",
            1,
            None,
            Some(StreamsTaskAssignment::default()),
        ))
        .await
        .unwrap()
        .result;
    assert_eq!(streams_assigned.member_epoch, 2);
    assert_eq!(streams_assigned.assignment.unwrap().active_tasks.len(), 2);
}

#[tokio::test]
async fn postgres_share_and_streams_previous_epochs_survive_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("heartbeat-recovery-topic-{suffix}");
    store.create_topic(&topic_name, 2).await.unwrap();

    let share_group = format!("share-recovery-{suffix}");
    let mut share_first = share_heartbeat(&share_group, 0, Some(&topic_name));
    share_first.member_id = "share-a".to_owned();
    let share_joined = store.share_group_heartbeat(share_first).await.unwrap();
    let mut share_second = share_heartbeat(&share_group, 0, Some(&topic_name));
    share_second.member_id = "share-b".to_owned();
    store.share_group_heartbeat(share_second).await.unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let mut share_retry = share_heartbeat(&share_group, share_joined.member_epoch, None);
    share_retry.member_id = "share-a".to_owned();
    let share_recovered = reconnected
        .share_group_heartbeat(share_retry.clone())
        .await
        .unwrap();
    assert_eq!(share_recovered.member_epoch, 3);
    assert_eq!(
        reconnected
            .share_group_heartbeat(share_retry)
            .await
            .unwrap()
            .member_epoch,
        3
    );

    let streams_group = format!("streams-recovery-{suffix}");
    let first = store
        .streams_group_heartbeat(heartbeat(
            &streams_group,
            "streams-a",
            0,
            Some(topology(&topic_name)),
            Some(StreamsTaskAssignment::default()),
        ))
        .await
        .unwrap();
    store
        .streams_group_heartbeat(heartbeat(
            &streams_group,
            "streams-a",
            first.member_epoch,
            None,
            first.assignment,
        ))
        .await
        .unwrap();
    store
        .streams_group_heartbeat(heartbeat(
            &streams_group,
            "streams-b",
            0,
            Some(topology(&topic_name)),
            Some(StreamsTaskAssignment::default()),
        ))
        .await
        .unwrap();
    let revoking = store
        .streams_group_heartbeat(heartbeat(
            &streams_group,
            "streams-a",
            first.member_epoch,
            None,
            None,
        ))
        .await
        .unwrap();
    let lost_response = heartbeat(
        &streams_group,
        "streams-a",
        first.member_epoch,
        None,
        revoking.assignment,
    );
    assert_eq!(
        store
            .streams_group_heartbeat(lost_response.clone())
            .await
            .unwrap()
            .member_epoch,
        3
    );

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let recovered = reconnected
        .streams_group_heartbeat(lost_response.clone())
        .await
        .unwrap();
    assert_eq!(recovered.member_epoch, 3);
    assert_eq!(
        reconnected
            .streams_group_heartbeat(lost_response)
            .await
            .unwrap()
            .member_epoch,
        3
    );
}

#[tokio::test]
async fn postgres_streams_member_expires_before_offset_validation() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("streams-expiry-topic-{suffix}");
    let group_id = format!("streams-expiry-group-{suffix}");
    store.create_topic(&topic_name, 1).await.unwrap();
    let mut request = heartbeat(
        &group_id,
        "member-a",
        0,
        Some(topology(&topic_name)),
        Some(StreamsTaskAssignment::default()),
    );
    request.session_timeout_ms = 10;
    let joined = store.streams_group_heartbeat(request).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(matches!(
        store
            .validate_group_member(&group_id, "member-a", None, joined.member_epoch)
            .await,
        Err(ControlError::GroupMemberNotFound { .. })
    ));
    store.delete_group(&group_id).await.unwrap();
}

fn heartbeat(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    topology: Option<StreamsTopology>,
    owned_assignment: Option<StreamsTaskAssignment>,
) -> StreamsGroupHeartbeat {
    StreamsGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        endpoint_information_epoch: -1,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if topology.is_some() { 300_000 } else { -1 },
        topology,
        owned_assignment,
        process_id: (member_epoch == 0).then(|| "process-a".to_owned()),
        user_endpoint: None,
        client_tags: (member_epoch == 0).then(Vec::new),
        task_offsets: None,
        task_end_offsets: None,
        shutdown_application: false,
        client_id: "postgres-streams-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        max_size: i32::MAX,
        assignment_interval_ms: 0,
        num_standby_replicas: 0,
        initial_rebalance_delay_ms: 0,
        acceptable_recovery_lag: 10_000,
        task_offset_interval_ms: 10_000,
    }
}

fn topology(topic: &str) -> StreamsTopology {
    StreamsTopology {
        epoch: 0,
        subtopologies: vec![StreamsSubtopology {
            subtopology_id: "0".to_owned(),
            source_topics: vec![topic.to_owned()],
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::<StreamsInternalTopic>::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    }
}

fn share_heartbeat(group_id: &str, member_epoch: i32, topic: Option<&str>) -> ShareGroupHeartbeat {
    ShareGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: "share-member".to_owned(),
        member_epoch,
        rack_id: None,
        subscribed_topic_names: topic.map(|topic| vec![topic.to_owned()]),
        client_id: "postgres-streams-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        assignment_interval_ms: 0,
        max_size: 200,
    }
}
