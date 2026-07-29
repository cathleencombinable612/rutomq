use crate::consumer_groups::{
    ConsumerGroupHeartbeat, ConsumerOwnedTopicPartitions, ConsumerTopicAssignment,
};
use crate::share_groups::{
    ShareGroupHeartbeat, ShareGroupState, apply_heartbeat, apply_heartbeat_deferred,
};
use crate::streams_groups::{
    StreamsGroupHeartbeat, StreamsSubtopology, StreamsTaskAssignment, StreamsTaskId,
    StreamsTopology,
};
use crate::{ControlError, GroupAssignmentCompletion, TopicInfo, consumer_groups, streams_groups};
use chrono::{Duration, Utc};
use uuid::Uuid;

#[test]
fn consumer_previous_epoch_recovers_only_with_owned_assignment_subset() {
    let topic = topic("orders", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (state, joined) =
        consumer_groups::heartbeat(None, consumer_request("member-a", 0, &topic), topics, now)
            .unwrap();
    let mut acknowledge = consumer_request("member-a", joined.member_epoch, &topic);
    acknowledge.subscribed_topic_names = None;
    acknowledge.owned_partitions = joined.assignment.as_deref().map(consumer_owned);
    let (state, _) =
        consumer_groups::heartbeat(Some(state), acknowledge, topics, now + Duration::seconds(1))
            .unwrap();
    let (state, _) = consumer_groups::heartbeat(
        Some(state),
        consumer_request("member-b", 0, &topic),
        topics,
        now + Duration::seconds(2),
    )
    .unwrap();

    let mut revoke = consumer_request("member-a", joined.member_epoch, &topic);
    revoke.subscribed_topic_names = None;
    revoke.owned_partitions = None;
    let (state, revoking) =
        consumer_groups::heartbeat(Some(state), revoke, topics, now + Duration::seconds(3))
            .unwrap();
    let mut lost_response = consumer_request("member-a", joined.member_epoch, &topic);
    lost_response.subscribed_topic_names = None;
    lost_response.owned_partitions = revoking.assignment.as_deref().map(consumer_owned);
    let (state, advanced) = consumer_groups::heartbeat(
        Some(state),
        lost_response.clone(),
        topics,
        now + Duration::seconds(4),
    )
    .unwrap();
    assert_eq!(advanced.member_epoch, 3);
    assert_eq!(state.members["member-a"].previous_member_epoch, 2);

    let (state, recovered) = consumer_groups::heartbeat(
        Some(state),
        lost_response.clone(),
        topics,
        now + Duration::seconds(5),
    )
    .unwrap();
    assert_eq!(recovered.member_epoch, 3);
    assert_eq!(state.group_epoch, 3);
    assert_eq!(state.members["member-a"].previous_member_epoch, 2);

    let mut missing_owned = lost_response.clone();
    missing_owned.owned_partitions = None;
    assert!(matches!(
        consumer_groups::heartbeat(
            Some(state.clone()),
            missing_owned,
            topics,
            now + Duration::seconds(6)
        ),
        Err(ControlError::FencedMemberEpoch { .. })
    ));
    let mut over_owned = lost_response;
    over_owned.owned_partitions = joined.assignment.as_deref().map(consumer_owned);
    assert!(matches!(
        consumer_groups::heartbeat(Some(state), over_owned, topics, now + Duration::seconds(6)),
        Err(ControlError::FencedMemberEpoch { .. })
    ));
}

#[test]
fn consumer_static_temporary_leave_preserves_assignment_and_allows_reconnect() {
    let topic = topic("orders", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let mut join = consumer_request("member-a", 0, &topic);
    join.instance_id = Some("instance-a".to_owned());
    let (state, joined) = consumer_groups::heartbeat(None, join.clone(), topics, now).unwrap();
    let mut acknowledge = join.clone();
    acknowledge.member_epoch = joined.member_epoch;
    acknowledge.subscribed_topic_names = None;
    acknowledge.owned_partitions = joined.assignment.as_deref().map(consumer_owned);
    let (state, _) =
        consumer_groups::heartbeat(Some(state), acknowledge, topics, now + Duration::seconds(1))
            .unwrap();

    assert!(matches!(
        consumer_groups::heartbeat(
            Some(state.clone()),
            join,
            topics,
            now + Duration::seconds(2)
        ),
        Err(ControlError::UnreleasedInstanceId { .. })
    ));

    let mut leave = consumer_request("member-a", -2, &topic);
    leave.instance_id = Some("instance-a".to_owned());
    leave.subscribed_topic_names = None;
    leave.owned_partitions = None;
    let (state, left) =
        consumer_groups::heartbeat(Some(state), leave, topics, now + Duration::seconds(2)).unwrap();
    assert_eq!(left.member_epoch, -2);
    assert_eq!(left.heartbeat_interval_ms, 0);
    assert_eq!(state.group_epoch, 2);
    assert_eq!(state.members["member-a"].member_epoch, -2);
    assert_eq!(
        state.members["member-a"].current_assignment[0].partitions,
        [0, 1]
    );

    let mut reconnect = consumer_request("member-b", 0, &topic);
    reconnect.instance_id = Some("instance-a".to_owned());
    let (state, rejoined) =
        consumer_groups::heartbeat(Some(state), reconnect, topics, now + Duration::days(1))
            .unwrap();
    assert_eq!(rejoined.member_epoch, 2);
    assert_eq!(rejoined.assignment.unwrap()[0].partitions, [0, 1]);
    assert_eq!(state.group_epoch, 2);
    assert!(!state.members.contains_key("member-a"));
    assert_eq!(state.members["member-b"].previous_member_epoch, 0);
}

#[test]
fn share_previous_epoch_retry_is_idempotent_and_stale_epochs_are_fenced() {
    let topic = topic("jobs", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let mut group = ShareGroupState::new("workers");
    let first = apply_heartbeat(
        &mut group,
        share_request("one", 0, Some("jobs")),
        topics,
        now,
    )
    .unwrap();
    apply_heartbeat(
        &mut group,
        share_request("two", 0, Some("jobs")),
        topics,
        now + Duration::seconds(1),
    )
    .unwrap();
    assert_eq!(
        group.members["one"].previous_member_epoch,
        first.member_epoch
    );

    let retry = share_request("one", first.member_epoch, None);
    let recovered = apply_heartbeat(
        &mut group,
        retry.clone(),
        topics,
        now + Duration::seconds(2),
    )
    .unwrap();
    assert_eq!(recovered.member_epoch, 3);
    let duplicate = apply_heartbeat(&mut group, retry, topics, now + Duration::seconds(3)).unwrap();
    assert_eq!(duplicate.member_epoch, 3);
    assert_eq!(group.group_epoch, 3);

    apply_heartbeat(
        &mut group,
        share_request("three", 0, Some("jobs")),
        topics,
        now + Duration::seconds(4),
    )
    .unwrap();
    assert_eq!(group.members["one"].previous_member_epoch, 3);
    assert!(matches!(
        apply_heartbeat(
            &mut group,
            share_request("one", 1, None),
            topics,
            now + Duration::seconds(5)
        ),
        Err(ControlError::FencedMemberEpoch { .. })
    ));
}

#[test]
fn streams_previous_epoch_requires_owned_tasks_subset() {
    let topic = topic("input", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (state, first) = streams_groups::heartbeat(
        None,
        streams_request("member-a", 0, Some(streams_topology("input"))),
        topics,
        now,
    )
    .unwrap();
    let mut acknowledge = streams_request("member-a", first.member_epoch, None);
    acknowledge.owned_assignment = first.assignment.clone();
    let (state, _) =
        streams_groups::heartbeat(Some(state), acknowledge, topics, now + Duration::seconds(1))
            .unwrap();
    let (state, _) = streams_groups::heartbeat(
        Some(state),
        streams_request("member-b", 0, Some(streams_topology("input"))),
        topics,
        now + Duration::seconds(2),
    )
    .unwrap();

    let mut revoke = streams_request("member-a", first.member_epoch, None);
    revoke.owned_assignment = None;
    let (state, revoking) =
        streams_groups::heartbeat(Some(state), revoke, topics, now + Duration::seconds(3)).unwrap();
    let mut lost_response = streams_request("member-a", first.member_epoch, None);
    lost_response.owned_assignment = revoking.assignment;
    let (state, advanced) = streams_groups::heartbeat(
        Some(state),
        lost_response.clone(),
        topics,
        now + Duration::seconds(4),
    )
    .unwrap();
    assert_eq!(advanced.member_epoch, 3);
    assert_eq!(state.members["member-a"].previous_member_epoch, 2);

    let (state, recovered) = streams_groups::heartbeat(
        Some(state),
        lost_response.clone(),
        topics,
        now + Duration::seconds(5),
    )
    .unwrap();
    assert_eq!(recovered.member_epoch, 3);
    assert_eq!(state.group_epoch, 3);

    let mut missing_owned = lost_response.clone();
    missing_owned.owned_assignment = None;
    assert!(matches!(
        streams_groups::heartbeat(
            Some(state.clone()),
            missing_owned,
            topics,
            now + Duration::seconds(6)
        ),
        Err(ControlError::FencedMemberEpoch { .. })
    ));
    let mut over_owned = lost_response;
    over_owned.owned_assignment = Some(streams_tasks(&[0, 1]));
    assert!(matches!(
        streams_groups::heartbeat(Some(state), over_owned, topics, now + Duration::seconds(6)),
        Err(ControlError::FencedMemberEpoch { .. })
    ));
}

#[test]
fn deferred_consumer_assignment_starts_at_epoch_one_and_publishes_on_next_heartbeat() {
    let topic = topic("orders", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (mut state, joined, task) = consumer_groups::heartbeat_deferred(
        None,
        consumer_request("member-a", 0, &topic),
        topics,
        now,
    )
    .unwrap();
    assert_eq!(state.group_epoch, 2);
    assert_eq!(state.assignment_epoch, 1);
    assert_eq!(joined.member_epoch, 1);
    assert!(joined.assignment.as_ref().unwrap().is_empty());
    let task = task.unwrap();
    assert_eq!(
        consumer_groups::complete_assignment(
            &mut state,
            topics,
            &task,
            now + Duration::milliseconds(1),
        )
        .unwrap(),
        GroupAssignmentCompletion::Published
    );

    let (state, assigned, next_task) = consumer_groups::heartbeat_deferred(
        Some(state),
        consumer_request("member-a", 1, &topic),
        topics,
        now + Duration::milliseconds(2),
    )
    .unwrap();
    assert_eq!(state.assignment_epoch, 2);
    assert_eq!(assigned.member_epoch, 2);
    assert_eq!(assigned.assignment.unwrap()[0].partitions, [0, 1]);
    assert!(next_task.is_some());
}

#[test]
fn deferred_consumer_assignment_fences_a_task_after_membership_changes() {
    let topic = topic("orders", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (state, _, old_task) = consumer_groups::heartbeat_deferred(
        None,
        consumer_request("member-a", 0, &topic),
        topics,
        now,
    )
    .unwrap();
    let (mut state, _, replacement_task) = consumer_groups::heartbeat_deferred(
        Some(state),
        consumer_request("member-b", 0, &topic),
        topics,
        now + Duration::milliseconds(1),
    )
    .unwrap();
    assert_eq!(replacement_task.unwrap().group_epoch, 3);
    assert_eq!(
        consumer_groups::complete_assignment(
            &mut state,
            topics,
            &old_task.unwrap(),
            now + Duration::milliseconds(2),
        )
        .unwrap(),
        GroupAssignmentCompletion::Stale
    );
    assert_eq!(state.assignment_epoch, 1);
}

#[test]
fn deferred_consumer_rebalance_recovers_the_last_published_epoch() {
    let topic = topic("orders", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (mut state, joined, task) = consumer_groups::heartbeat_deferred(
        None,
        consumer_request("member-a", 0, &topic),
        topics,
        now,
    )
    .unwrap();
    assert_eq!(joined.member_epoch, 1);
    consumer_groups::complete_assignment(
        &mut state,
        topics,
        &task.unwrap(),
        now + Duration::milliseconds(1),
    )
    .unwrap();

    let (state, assigned, _) = consumer_groups::heartbeat_deferred(
        Some(state),
        consumer_request("member-a", joined.member_epoch, &topic),
        topics,
        now + Duration::milliseconds(2),
    )
    .unwrap();
    assert_eq!(assigned.member_epoch, 2);
    let assigned_partitions = assigned.assignment.as_deref().unwrap();

    let mut acknowledge = consumer_request("member-a", assigned.member_epoch, &topic);
    acknowledge.owned_partitions = Some(consumer_owned(assigned_partitions));
    let (state, _, _) = consumer_groups::heartbeat_deferred(
        Some(state),
        acknowledge,
        topics,
        now + Duration::milliseconds(3),
    )
    .unwrap();
    let (mut state, _, task) = consumer_groups::heartbeat_deferred(
        Some(state),
        consumer_request("member-b", 0, &topic),
        topics,
        now + Duration::milliseconds(4),
    )
    .unwrap();
    consumer_groups::complete_assignment(
        &mut state,
        topics,
        &task.unwrap(),
        now + Duration::milliseconds(5),
    )
    .unwrap();

    let mut revoke = consumer_request("member-a", assigned.member_epoch, &topic);
    revoke.owned_partitions = None;
    let (state, revoking, _) = consumer_groups::heartbeat_deferred(
        Some(state),
        revoke,
        topics,
        now + Duration::milliseconds(6),
    )
    .unwrap();
    assert_eq!(revoking.member_epoch, assigned.member_epoch);
    let retained = revoking.assignment.as_deref().unwrap();

    let mut lost_response = consumer_request("member-a", assigned.member_epoch, &topic);
    lost_response.owned_partitions = Some(consumer_owned(retained));
    let (state, advanced, _) = consumer_groups::heartbeat_deferred(
        Some(state),
        lost_response.clone(),
        topics,
        now + Duration::milliseconds(7),
    )
    .unwrap();
    assert_eq!(advanced.member_epoch, 3);
    let (_, recovered, _) = consumer_groups::heartbeat_deferred(
        Some(state),
        lost_response,
        topics,
        now + Duration::milliseconds(8),
    )
    .unwrap();
    assert_eq!(recovered.member_epoch, advanced.member_epoch);
}

#[test]
fn deferred_share_assignment_publishes_after_the_epoch_one_join() {
    let topic = topic("jobs", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let mut group = ShareGroupState::new("workers");
    let (joined, task) = apply_heartbeat_deferred(
        &mut group,
        share_request("one", 0, Some("jobs")),
        topics,
        now,
    )
    .unwrap();
    assert_eq!(joined.member_epoch, 1);
    assert!(joined.assignment.unwrap().is_empty());
    assert_eq!(
        crate::share_groups::complete_assignment(
            &mut group,
            topics,
            &task.unwrap(),
            now + Duration::milliseconds(1),
        ),
        GroupAssignmentCompletion::Published
    );
    let (assigned, _) = apply_heartbeat_deferred(
        &mut group,
        share_request("one", 1, None),
        topics,
        now + Duration::milliseconds(2),
    )
    .unwrap();
    assert_eq!(assigned.member_epoch, 2);
    assert_eq!(assigned.assignment.unwrap()[0].partitions, [0, 1]);
}

#[test]
fn deferred_streams_assignment_publishes_after_the_epoch_one_join() {
    let topic = topic("input", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let (mut state, joined, task) = streams_groups::heartbeat_deferred(
        None,
        streams_request("member-a", 0, Some(streams_topology("input"))),
        topics,
        now,
    )
    .unwrap();
    assert_eq!(joined.member_epoch, 1);
    assert!(joined.assignment.unwrap().active_tasks.is_empty());
    assert_eq!(
        streams_groups::complete_assignment(
            &mut state,
            topics,
            &task.unwrap(),
            now + Duration::milliseconds(1),
        )
        .unwrap(),
        GroupAssignmentCompletion::Published
    );
    let (state, assigned, _) = streams_groups::heartbeat_deferred(
        Some(state),
        streams_request("member-a", 1, None),
        topics,
        now + Duration::milliseconds(2),
    )
    .unwrap();
    assert_eq!(state.assignment_epoch, 2);
    assert_eq!(assigned.member_epoch, 2);
    assert_eq!(
        assigned.assignment.unwrap().active_tasks,
        streams_tasks(&[0, 1]).active_tasks
    );
}

#[test]
fn stable_deferred_streams_group_does_not_report_assignment_delay() {
    let topic = topic("input", 2);
    let topics = std::slice::from_ref(&topic);
    let now = Utc::now();
    let mut join = streams_request("member-a", 0, Some(streams_topology("input")));
    join.assignment_interval_ms = 1_000;
    let (mut state, joined, task) =
        streams_groups::heartbeat_deferred(None, join, topics, now).unwrap();
    streams_groups::complete_assignment(
        &mut state,
        topics,
        &task.unwrap(),
        now + Duration::milliseconds(1),
    )
    .unwrap();

    let mut follow_up = streams_request("member-a", joined.member_epoch, None);
    follow_up.assignment_interval_ms = 1_000;
    let (_, assigned, task) = streams_groups::heartbeat_deferred(
        Some(state),
        follow_up,
        topics,
        now + Duration::milliseconds(2),
    )
    .unwrap();
    assert_eq!(assigned.member_epoch, 2);
    assert!(
        assigned
            .statuses
            .iter()
            .all(|status| status.code != crate::STREAMS_STATUS_ASSIGNMENT_DELAYED)
    );
    assert!(task.is_none());
}

fn topic(name: &str, partitions: i32) -> TopicInfo {
    TopicInfo {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        partitions,
    }
}

fn consumer_request(
    member_id: &str,
    member_epoch: i32,
    topic: &TopicInfo,
) -> ConsumerGroupHeartbeat {
    ConsumerGroupHeartbeat {
        group_id: "consumers".to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
        subscribed_topic_names: (member_epoch == 0).then(|| vec![topic.name.clone()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions: Some(Vec::new()),
        client_id: member_id.to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        max_size: i32::MAX,
        assignment_interval_ms: 0,
    }
}

fn consumer_owned(assignment: &[ConsumerTopicAssignment]) -> Vec<ConsumerOwnedTopicPartitions> {
    assignment
        .iter()
        .map(|topic| ConsumerOwnedTopicPartitions {
            topic_id: topic.topic_id,
            partitions: topic.partitions.clone(),
        })
        .collect()
}

fn share_request(member_id: &str, member_epoch: i32, topic: Option<&str>) -> ShareGroupHeartbeat {
    ShareGroupHeartbeat {
        group_id: "workers".to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        rack_id: None,
        subscribed_topic_names: topic.map(|topic| vec![topic.to_owned()]),
        client_id: member_id.to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        assignment_interval_ms: 0,
        max_size: 200,
    }
}

fn streams_request(
    member_id: &str,
    member_epoch: i32,
    topology: Option<StreamsTopology>,
) -> StreamsGroupHeartbeat {
    StreamsGroupHeartbeat {
        group_id: "streams".to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        endpoint_information_epoch: -1,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if topology.is_some() { 300_000 } else { -1 },
        topology,
        owned_assignment: Some(StreamsTaskAssignment::default()),
        process_id: (member_epoch == 0).then(|| format!("process-{member_id}")),
        user_endpoint: None,
        client_tags: (member_epoch == 0).then(Vec::new),
        task_offsets: None,
        task_end_offsets: None,
        shutdown_application: false,
        client_id: member_id.to_owned(),
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

fn streams_topology(topic: &str) -> StreamsTopology {
    StreamsTopology {
        epoch: 0,
        subtopologies: vec![StreamsSubtopology {
            subtopology_id: "0".to_owned(),
            source_topics: vec![topic.to_owned()],
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    }
}

fn streams_tasks(partitions: &[i32]) -> StreamsTaskAssignment {
    StreamsTaskAssignment {
        active_tasks: partitions
            .iter()
            .map(|partition| StreamsTaskId {
                subtopology_id: "0".to_owned(),
                partition: *partition,
            })
            .collect(),
        standby_tasks: Vec::new(),
        warmup_tasks: Vec::new(),
    }
}
