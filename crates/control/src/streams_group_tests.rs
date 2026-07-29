use crate::TopicInfo;
use crate::streams_groups::{
    STREAMS_STATUS_ASSIGNMENT_DELAYED, STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS,
    STREAMS_STATUS_MISSING_SOURCE_TOPICS, STREAMS_STATUS_STALE_TOPOLOGY, StreamsCopartitionGroup,
    StreamsGroupHeartbeat, StreamsInternalTopic, StreamsKeyValue, StreamsSubtopology,
    StreamsTaskAssignment, StreamsTopology, describe, expire_and_describe, heartbeat,
    validate_member,
};
use crate::streams_topology::{self, streams_internal_topic_requirements};
use chrono::{Duration, Utc};
use uuid::Uuid;

#[test]
fn member_joins_and_acknowledges_active_tasks() {
    let topics = vec![topic("input", 3)];
    let now = Utc::now();
    let (state, joined) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology("input"))),
        &topics,
        now,
    )
    .unwrap();
    assert_eq!(joined.member_epoch, 2);
    assert_eq!(
        joined.assignment.as_ref().unwrap().active_tasks,
        tasks("0", &[0, 1, 2]).active_tasks
    );
    assert!(joined.statuses.is_empty());

    let mut acknowledge = request("streams", "member-a", joined.member_epoch, None);
    acknowledge.owned_assignment = joined.assignment.clone();
    let (state, acknowledged) = heartbeat(
        Some(state),
        acknowledge,
        &topics,
        now + Duration::seconds(1),
    )
    .unwrap();
    assert!(acknowledged.assignment.is_none());
    let resolved = streams_topology::resolve(&state.topology, &topics).unwrap();
    assert_eq!(describe(&state, &resolved).state, "Stable");
}

#[test]
fn active_task_moves_only_after_old_owner_revokes_it() {
    let topics = vec![topic("input", 2)];
    let now = Utc::now();
    let (state, first) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology("input"))),
        &topics,
        now,
    )
    .unwrap();
    let mut first_ack = request("streams", "member-a", first.member_epoch, None);
    first_ack.owned_assignment = first.assignment.clone();
    let (state, _) =
        heartbeat(Some(state), first_ack, &topics, now + Duration::seconds(1)).unwrap();

    let (state, second) = heartbeat(
        Some(state),
        request("streams", "member-b", 0, Some(topology("input"))),
        &topics,
        now + Duration::seconds(2),
    )
    .unwrap();
    assert_eq!(
        second.assignment.as_ref().unwrap(),
        &StreamsTaskAssignment::default()
    );

    let mut revoke = request("streams", "member-a", first.member_epoch, None);
    revoke.owned_assignment = None;
    let (state, revoke_response) =
        heartbeat(Some(state), revoke, &topics, now + Duration::seconds(3)).unwrap();
    assert_eq!(
        revoke_response.assignment.as_ref().unwrap(),
        &tasks("0", &[0])
    );
    assert_eq!(revoke_response.member_epoch, first.member_epoch);

    let mut revoked = request("streams", "member-a", first.member_epoch, None);
    revoked.owned_assignment = revoke_response.assignment.clone();
    let (state, advanced) =
        heartbeat(Some(state), revoked, &topics, now + Duration::seconds(4)).unwrap();
    assert_eq!(advanced.member_epoch, 3);

    let mut acquire = request("streams", "member-b", second.member_epoch, None);
    acquire.owned_assignment = Some(StreamsTaskAssignment::default());
    let (_, acquired) =
        heartbeat(Some(state), acquire, &topics, now + Duration::seconds(5)).unwrap();
    assert_eq!(acquired.assignment.as_ref().unwrap(), &tasks("0", &[1]));
}

#[test]
fn adding_a_member_moves_only_the_tasks_needed_for_balance() {
    let topics = vec![topic("input", 6)];
    let now = Utc::now();
    let (state, _) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology("input"))),
        &topics,
        now,
    )
    .unwrap();
    let (state, _) = heartbeat(
        Some(state),
        request("streams", "member-b", 0, Some(topology("input"))),
        &topics,
        now + Duration::seconds(1),
    )
    .unwrap();
    let before = state
        .members
        .iter()
        .map(|(member, state)| {
            (
                member.clone(),
                state
                    .target_assignment
                    .active_tasks
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>(),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(before["member-a"].len(), 3);
    assert_eq!(before["member-b"].len(), 3);

    let (state, _) = heartbeat(
        Some(state),
        request("streams", "member-c", 0, Some(topology("input"))),
        &topics,
        now + Duration::seconds(2),
    )
    .unwrap();
    let retained = ["member-a", "member-b"]
        .iter()
        .map(|member| {
            state.members[*member]
                .target_assignment
                .active_tasks
                .iter()
                .filter(|task| before[*member].contains(*task))
                .count()
        })
        .sum::<usize>();
    assert_eq!(retained, 4);
    assert_eq!(
        state.members["member-c"]
            .target_assignment
            .active_tasks
            .len(),
        2
    );
}

#[test]
fn stateful_tasks_receive_standbys_on_a_different_process() {
    let topics = vec![topic("input", 2), topic("app-store-changelog", 2)];
    let mut stateful = topology("input");
    stateful.subtopologies[0]
        .state_changelog_topics
        .push(StreamsInternalTopic {
            name: "app-store-changelog".to_owned(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: Vec::new(),
        });
    let now = Utc::now();
    let mut first = request("streams", "member-a", 0, Some(stateful.clone()));
    first.num_standby_replicas = 1;
    let (state, _) = heartbeat(None, first, &topics, now).unwrap();
    let mut second = request("streams", "member-b", 0, Some(stateful));
    second.num_standby_replicas = 1;
    let (state, _) = heartbeat(Some(state), second, &topics, now + Duration::seconds(1)).unwrap();

    for member in state.members.values() {
        assert_eq!(member.target_assignment.active_tasks.len(), 1);
        assert_eq!(member.target_assignment.standby_tasks.len(), 1);
        assert_ne!(
            member.target_assignment.active_tasks,
            member.target_assignment.standby_tasks
        );
    }
    for member in state.members.values() {
        let standby = &member.target_assignment.standby_tasks[0];
        let active_owner = state
            .members
            .values()
            .find(|candidate| candidate.target_assignment.active_tasks.contains(standby))
            .unwrap();
        assert_ne!(member.process_id, active_owner.process_id);
    }
}

#[test]
fn first_assignment_waits_for_the_configured_initial_delay() {
    let topics = vec![topic("input", 2)];
    let now = Utc::now();
    let mut join = request("streams", "member-a", 0, Some(topology("input")));
    join.initial_rebalance_delay_ms = 3_000;
    let (state, joined) = heartbeat(None, join, &topics, now).unwrap();
    assert_eq!(joined.assignment.unwrap(), StreamsTaskAssignment::default());
    assert_eq!(joined.statuses[0].code, STREAMS_STATUS_ASSIGNMENT_DELAYED);
    assert_eq!(
        joined.statuses[0].detail,
        "Assignment delayed due to the configured initial rebalance delay."
    );
    let resolved = streams_topology::resolve(&state.topology, &topics).unwrap();
    assert_eq!(describe(&state, &resolved).state, "Assigning");

    let heartbeat_request = request("streams", "member-a", joined.member_epoch, None);
    let (_, assigned) = heartbeat(
        Some(state),
        heartbeat_request,
        &topics,
        now + Duration::seconds(3),
    )
    .unwrap();
    assert_eq!(assigned.assignment.unwrap().active_tasks.len(), 2);
}

#[test]
fn assignment_updates_are_batched_until_the_interval_elapses() {
    let topics = vec![topic("input", 2)];
    let now = Utc::now();
    let mut first_join = request("streams", "member-a", 0, Some(topology("input")));
    first_join.assignment_interval_ms = 1_000;
    let (state, first) = heartbeat(None, first_join, &topics, now).unwrap();
    assert_eq!(state.group_epoch, 2);
    assert_eq!(state.assignment_epoch, 2);
    assert_eq!(state.assignment_timestamp, Some(now));

    let mut acknowledge = request("streams", "member-a", first.member_epoch, None);
    acknowledge.assignment_interval_ms = 1_000;
    acknowledge.owned_assignment = first.assignment;
    let (state, _) = heartbeat(
        Some(state),
        acknowledge,
        &topics,
        now + Duration::milliseconds(50),
    )
    .unwrap();

    let mut second_join = request("streams", "member-b", 0, Some(topology("input")));
    second_join.assignment_interval_ms = 1_000;
    let (state, second) = heartbeat(
        Some(state),
        second_join,
        &topics,
        now + Duration::milliseconds(100),
    )
    .unwrap();
    assert_eq!(state.group_epoch, 3);
    assert_eq!(state.assignment_epoch, 2);
    assert_eq!(second.member_epoch, 2);
    assert_eq!(second.statuses[0].code, STREAMS_STATUS_ASSIGNMENT_DELAYED);
    assert_eq!(
        second.statuses[0].detail,
        "Assignment delayed due to the configured assignment interval."
    );

    let mut poll = request("streams", "member-b", second.member_epoch, None);
    poll.assignment_interval_ms = 1_000;
    let (state, delayed) = heartbeat(
        Some(state),
        poll.clone(),
        &topics,
        now + Duration::milliseconds(999),
    )
    .unwrap();
    assert_eq!(state.assignment_epoch, 2);
    assert_eq!(delayed.statuses[0].code, STREAMS_STATUS_ASSIGNMENT_DELAYED);

    let (state, assigned) = heartbeat(
        Some(state),
        poll,
        &topics,
        now + Duration::milliseconds(1_000),
    )
    .unwrap();
    assert_eq!(state.group_epoch, 3);
    assert_eq!(state.assignment_epoch, 3);
    assert_eq!(
        state.assignment_timestamp,
        Some(now + Duration::milliseconds(1_000))
    );
    assert_eq!(assigned.member_epoch, 3);
    assert!(
        assigned
            .statuses
            .iter()
            .all(|status| status.code != STREAMS_STATUS_ASSIGNMENT_DELAYED)
    );
}

#[test]
fn missing_source_is_not_ready_with_empty_assignment() {
    let now = Utc::now();
    let (state, response) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology("missing"))),
        &[],
        now,
    )
    .unwrap();
    assert_eq!(
        response.statuses[0].code,
        STREAMS_STATUS_MISSING_SOURCE_TOPICS
    );
    assert_eq!(
        response.assignment.unwrap(),
        StreamsTaskAssignment::default()
    );
    let resolved = streams_topology::resolve(&state.topology, &[]).unwrap();
    assert_eq!(describe(&state, &resolved).state, "NotReady");
}

#[test]
fn copartition_mismatch_is_reported() {
    let mut topology = topology("left");
    topology.subtopologies[0]
        .source_topics
        .push("right".to_owned());
    topology.subtopologies[0]
        .copartition_groups
        .push(StreamsCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: Vec::new(),
            repartition_source_topics: Vec::new(),
        });
    let (_, response) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology)),
        &[topic("left", 2), topic("right", 3)],
        Utc::now(),
    )
    .unwrap();
    assert_eq!(
        response.statuses[0].code,
        STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS
    );
}

#[test]
fn internal_topic_requirements_derive_changelog_partitions() {
    let topics = vec![topic("input", 3)];
    let mut stateful = topology("input");
    stateful.subtopologies[0]
        .state_changelog_topics
        .push(StreamsInternalTopic {
            name: "app-store-changelog".to_owned(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: vec![StreamsKeyValue {
                key: "cleanup.policy".to_owned(),
                value: "compact".to_owned(),
            }],
        });
    let requirements = streams_internal_topic_requirements(&stateful, &topics).unwrap();
    assert_eq!(requirements.len(), 1);
    assert_eq!(requirements[0].partitions, 3);

    let (_, response) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(stateful)),
        &topics,
        Utc::now(),
    )
    .unwrap();
    assert_eq!(
        response.statuses[0].code,
        crate::STREAMS_STATUS_MISSING_INTERNAL_TOPICS
    );
}

#[test]
fn internal_topic_requirements_propagate_across_repartition_subtopologies() {
    let topics = vec![topic("input", 3)];
    let repartition = StreamsInternalTopic {
        name: "app-counts-repartition".to_owned(),
        partitions: 0,
        replication_factor: 1,
        topic_configs: vec![StreamsKeyValue {
            key: "cleanup.policy".to_owned(),
            value: "delete".to_owned(),
        }],
    };
    let changelog = StreamsInternalTopic {
        name: "app-counts-changelog".to_owned(),
        partitions: 0,
        replication_factor: 1,
        topic_configs: vec![StreamsKeyValue {
            key: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        }],
    };
    let topology = StreamsTopology {
        epoch: 0,
        subtopologies: vec![
            StreamsSubtopology {
                subtopology_id: "0".to_owned(),
                source_topics: vec!["input".to_owned()],
                source_topic_regex: Vec::new(),
                state_changelog_topics: Vec::new(),
                repartition_sink_topics: vec![repartition.name.clone()],
                repartition_source_topics: Vec::new(),
                copartition_groups: Vec::new(),
            },
            StreamsSubtopology {
                subtopology_id: "1".to_owned(),
                source_topics: Vec::new(),
                source_topic_regex: Vec::new(),
                state_changelog_topics: vec![changelog],
                repartition_sink_topics: Vec::new(),
                repartition_source_topics: vec![repartition],
                copartition_groups: Vec::new(),
            },
        ],
    };

    let requirements = streams_internal_topic_requirements(&topology, &topics).unwrap();
    assert_eq!(
        requirements
            .iter()
            .map(|requirement| (requirement.topic.name.as_str(), requirement.partitions))
            .collect::<Vec<_>>(),
        vec![("app-counts-changelog", 3), ("app-counts-repartition", 3),]
    );
    let resolved = streams_topology::resolve(&topology, &topics).unwrap();
    assert_eq!(
        resolved
            .tasks
            .iter()
            .filter(|task| task.subtopology_id == "1")
            .count(),
        3
    );
}

#[test]
fn missing_regex_and_bad_changelog_partitioning_follow_status_precedence() {
    let mut stateful = topology("missing");
    stateful.subtopologies[0].source_topic_regex = vec!["events-.*".to_owned()];
    stateful.subtopologies[0]
        .state_changelog_topics
        .push(StreamsInternalTopic {
            name: "app-store-changelog".to_owned(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: Vec::new(),
        });
    let (_, response) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(stateful)),
        &[],
        Utc::now(),
    )
    .unwrap();
    assert_eq!(response.statuses.len(), 1);
    assert_eq!(
        response.statuses[0].code,
        STREAMS_STATUS_MISSING_SOURCE_TOPICS
    );
    assert!(response.statuses[0].detail.contains("regex:events-.*"));

    let mut stateful = topology("input");
    stateful.subtopologies[0]
        .state_changelog_topics
        .push(StreamsInternalTopic {
            name: "app-store-changelog".to_owned(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: Vec::new(),
        });
    let (_, response) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(stateful)),
        &[topic("input", 2), topic("app-store-changelog", 1)],
        Utc::now(),
    )
    .unwrap();
    assert_eq!(response.statuses.len(), 1);
    assert_eq!(
        response.statuses[0].code,
        STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS
    );
}

#[test]
fn active_group_rejects_a_different_topology_epoch() {
    let topics = vec![topic("input", 1)];
    let now = Utc::now();
    let (state, _) = heartbeat(
        None,
        request("streams", "member-a", 0, Some(topology("input"))),
        &topics,
        now,
    )
    .unwrap();
    let mut changed = topology("input");
    changed.epoch = 1;
    let (_, response) = heartbeat(
        Some(state),
        request("streams", "member-b", 0, Some(changed)),
        &topics,
        now + Duration::seconds(1),
    )
    .unwrap();
    assert!(
        response
            .statuses
            .iter()
            .any(|status| status.code == STREAMS_STATUS_STALE_TOPOLOGY)
    );
    assert_eq!(
        response.assignment.unwrap(),
        StreamsTaskAssignment::default()
    );
}

#[test]
fn expired_member_is_removed_before_description_and_offset_validation() {
    let topics = vec![topic("input", 1)];
    let now = Utc::now();
    let mut join = request("streams", "member-a", 0, Some(topology("input")));
    join.session_timeout_ms = 10;
    let (mut state, joined) = heartbeat(None, join, &topics, now).unwrap();

    let (changed, description) =
        expire_and_describe(&mut state, &topics, now + Duration::milliseconds(11)).unwrap();
    assert!(changed);
    assert_eq!(description.state, "Empty");
    assert!(description.members.is_empty());
    assert!(validate_member(&state, "member-a", joined.member_epoch).is_err());
}

#[test]
fn expired_member_releases_streams_group_capacity_before_a_new_join() {
    let topics = vec![topic("input", 1)];
    let now = Utc::now();
    let mut first = request("streams", "member-a", 0, Some(topology("input")));
    first.session_timeout_ms = 10;
    first.max_size = 1;
    let (state, _) = heartbeat(None, first, &topics, now).unwrap();

    let mut second = request("streams", "member-b", 0, Some(topology("input")));
    second.max_size = 1;
    let (state, _) = heartbeat(
        Some(state),
        second,
        &topics,
        now + Duration::milliseconds(11),
    )
    .unwrap();
    assert_eq!(state.members.len(), 1);
    assert!(state.members.contains_key("member-b"));
}

fn request(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    topology: Option<StreamsTopology>,
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
        owned_assignment: Some(StreamsTaskAssignment::default()),
        process_id: (member_epoch == 0).then(|| format!("process-{member_id}")),
        user_endpoint: None,
        client_tags: (member_epoch == 0).then(Vec::new),
        task_offsets: None,
        task_end_offsets: None,
        shutdown_application: false,
        client_id: "test-client".to_owned(),
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

fn topology(source: &str) -> StreamsTopology {
    StreamsTopology {
        epoch: 0,
        subtopologies: vec![StreamsSubtopology {
            subtopology_id: "0".to_owned(),
            source_topics: vec![source.to_owned()],
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::<StreamsInternalTopic>::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    }
}

fn topic(name: &str, partitions: i32) -> TopicInfo {
    TopicInfo {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        partitions,
    }
}

fn tasks(subtopology_id: &str, partitions: &[i32]) -> StreamsTaskAssignment {
    StreamsTaskAssignment {
        active_tasks: partitions
            .iter()
            .map(|partition| crate::StreamsTaskId {
                subtopology_id: subtopology_id.to_owned(),
                partition: *partition,
            })
            .collect(),
        standby_tasks: Vec::new(),
        warmup_tasks: Vec::new(),
    }
}
