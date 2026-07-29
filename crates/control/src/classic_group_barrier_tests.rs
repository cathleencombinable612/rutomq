use crate::{
    ConsumerGroupHeartbeat, ControlError, GroupAssignment, GroupMemberIdentity, JoinGroupResult,
    MemoryMetadataStore, MetadataStore,
};
use std::time::Duration;
use tokio::time::sleep;

const PROTOCOL_TYPE: &str = "consumer";

#[tokio::test]
async fn consumer_conversion_discards_pending_classic_member_ids() {
    let store = MemoryMetadataStore::new();
    store
        .create_topic("pending-conversion-topic", 1)
        .await
        .unwrap();
    let pending_member_id = match store
        .begin_join_group(
            "pending-conversion",
            "",
            None,
            PROTOCOL_TYPE,
            &[("range".to_owned(), vec![1])],
            ("barrier-test", "127.0.0.1", &[], 1_000),
            1_000,
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
    let heartbeat = |member_epoch| ConsumerGroupHeartbeat {
        group_id: "pending-conversion".to_owned(),
        member_id: "consumer-member".to_owned(),
        member_epoch,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
        subscribed_topic_names: (member_epoch == 0)
            .then(|| vec!["pending-conversion-topic".to_owned()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions: (member_epoch >= 0).then(Vec::new),
        client_id: "consumer-client".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        assignment_interval_ms: 0,
        max_size: i32::MAX,
    };
    store.consumer_group_heartbeat(heartbeat(0)).await.unwrap();
    store.consumer_group_heartbeat(heartbeat(-1)).await.unwrap();

    assert!(matches!(
        store
            .begin_join_group(
                "pending-conversion",
                &pending_member_id,
                None,
                PROTOCOL_TYPE,
                &[("range".to_owned(), vec![1])],
                ("barrier-test", "127.0.0.1", &[], 1_000),
                1_000,
                0,
                i32::MAX,
                4,
            )
            .await,
        Err(ControlError::GroupMemberNotFound { .. })
    ));
}

#[tokio::test]
async fn initial_join_gathers_members_into_one_generation() {
    let store = MemoryMetadataStore::new();
    let first = begin(&store, "initial-barrier", "", None, 200, 40).await;
    let rebalance_id = first.pending_rebalance.expect("first join is parked");
    assert_eq!(first.generation_id, 0);

    let second = begin(&store, "initial-barrier", "", None, 200, 40).await;
    assert_eq!(second.pending_rebalance, Some(rebalance_id));
    assert_eq!(second.generation_id, 0);

    let early = store
        .poll_join_group("initial-barrier", &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    assert_eq!(early.pending_rebalance, Some(rebalance_id));

    sleep(Duration::from_millis(50)).await;
    let completed = store
        .poll_join_group("initial-barrier", &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    let peer = store
        .poll_join_group("initial-barrier", &second.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    assert_eq!(completed.pending_rebalance, None);
    assert_eq!(completed.generation_id, 1);
    assert_eq!(peer.generation_id, 1);
    assert_eq!(completed.leader, peer.leader);
    assert_eq!(completed.members.len(), 2);
}

#[tokio::test]
async fn existing_members_rejoin_before_generation_advances_once() {
    let store = MemoryMetadataStore::new();
    let (first, second) = ready_pair(&store, "existing-barrier").await;
    make_stable(&store, "existing-barrier", &first).await;

    let (leader, follower) = if first.member_id == first.leader {
        (&first, &second)
    } else {
        (&second, &first)
    };
    let leader_join = begin(&store, "existing-barrier", &leader.member_id, None, 200, 0).await;
    let rebalance_id = leader_join
        .pending_rebalance
        .expect("leader starts a rebalance");
    assert_eq!(leader_join.generation_id, 1);

    let follower_join = begin(
        &store,
        "existing-barrier",
        &follower.member_id,
        None,
        200,
        0,
    )
    .await;
    assert_eq!(follower_join.pending_rebalance, None);
    assert_eq!(follower_join.generation_id, 2);
    let leader_result = store
        .poll_join_group("existing-barrier", &leader.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    assert_eq!(leader_result.generation_id, 2);
    assert_eq!(leader_result.members.len(), 2);
}

#[tokio::test]
async fn timeout_removes_dynamic_non_joiners_but_retains_static_members() {
    let store = MemoryMetadataStore::new();
    let first = begin(&store, "timeout-barrier", "", None, 60, 20).await;
    let second = begin(&store, "timeout-barrier", "", Some("static-b"), 60, 20).await;
    sleep(Duration::from_millis(25)).await;
    let ready = store
        .poll_join_group(
            "timeout-barrier",
            &first.member_id,
            None,
            first.pending_rebalance.unwrap(),
            3,
        )
        .await
        .unwrap();
    make_stable(&store, "timeout-barrier", &ready).await;

    let leader = ready
        .members
        .iter()
        .find(|member| member.member_id == ready.leader)
        .unwrap();
    let parked = begin(
        &store,
        "timeout-barrier",
        &leader.member_id,
        leader.group_instance_id.as_deref(),
        40,
        0,
    )
    .await;
    let rebalance_id = parked.pending_rebalance.unwrap();
    sleep(Duration::from_millis(50)).await;
    let completed = store
        .poll_join_group(
            "timeout-barrier",
            &leader.member_id,
            leader.group_instance_id.as_deref(),
            rebalance_id,
            3,
        )
        .await
        .unwrap();

    let static_member = completed
        .members
        .iter()
        .find(|member| member.group_instance_id.as_deref() == Some("static-b"));
    if second.member_id == leader.member_id {
        assert_eq!(completed.members.len(), 1);
    } else {
        assert!(static_member.is_some());
        assert_eq!(completed.members.len(), 2);
    }
}

#[tokio::test]
async fn session_expiry_releases_barrier_before_rebalance_timeout() {
    let store = MemoryMetadataStore::new();
    let first = begin_with_session(&store, "session-expiry", "", None, 150, 500, 20).await;
    let second = begin_with_session(&store, "session-expiry", "", None, 150, 500, 20).await;
    let rebalance_id = first.pending_rebalance.unwrap();
    sleep(Duration::from_millis(25)).await;
    let ready = store
        .poll_join_group("session-expiry", &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    make_stable(&store, "session-expiry", &ready).await;

    let replacement = begin_with_session(&store, "session-expiry", "", None, 1_000, 500, 0).await;
    let rebalance_id = replacement.pending_rebalance.unwrap();
    sleep(Duration::from_millis(150)).await;
    let completed = store
        .poll_join_group(
            "session-expiry",
            &replacement.member_id,
            None,
            rebalance_id,
            3,
        )
        .await
        .unwrap();

    assert_eq!(completed.pending_rebalance, None);
    assert_eq!(completed.generation_id, 2);
    assert_eq!(completed.members.len(), 1);
    assert_eq!(completed.members[0].member_id, replacement.member_id);
    assert_ne!(replacement.member_id, second.member_id);
}

#[tokio::test]
async fn expired_classic_member_releases_group_capacity() {
    let store = MemoryMetadataStore::new();
    let first =
        begin_with_session_and_max_size(&store, "classic-capacity-expiry", "", None, 10, 100, 0, 1)
            .await;
    sleep(Duration::from_millis(15)).await;
    let second = begin_with_session_and_max_size(
        &store,
        "classic-capacity-expiry",
        "",
        None,
        1_000,
        100,
        0,
        1,
    )
    .await;

    assert_ne!(first.member_id, second.member_id);
    assert_eq!(second.members.len(), 1);
    assert_eq!(second.members[0].member_id, second.member_id);
}

#[tokio::test]
async fn capacity_rejection_preserves_a_valid_pending_member_id() {
    let store = MemoryMetadataStore::new();
    let allocated = store
        .begin_join_group(
            "classic-pending-capacity",
            "",
            None,
            PROTOCOL_TYPE,
            &[("range".to_owned(), vec![1])],
            ("pending-client", "127.0.0.1", &[], 1_000),
            100,
            0,
            1,
            4,
        )
        .await
        .unwrap_err();
    let ControlError::MemberIdRequired { member_id } = allocated else {
        panic!("expected a pending member ID");
    };

    let blocker = begin_with_session_and_max_size(
        &store,
        "classic-pending-capacity",
        "",
        None,
        10,
        100,
        0,
        1,
    )
    .await;
    assert!(matches!(
        store
            .begin_join_group(
                "classic-pending-capacity",
                &member_id,
                None,
                PROTOCOL_TYPE,
                &[("range".to_owned(), vec![1])],
                ("pending-client", "127.0.0.1", &[], 1_000),
                100,
                0,
                1,
                4,
            )
            .await,
        Err(ControlError::GroupMaxSizeReached(_))
    ));

    sleep(Duration::from_millis(15)).await;
    let joined = store
        .begin_join_group(
            "classic-pending-capacity",
            &member_id,
            None,
            PROTOCOL_TYPE,
            &[("range".to_owned(), vec![1])],
            ("pending-client", "127.0.0.1", &[], 1_000),
            100,
            0,
            1,
            4,
        )
        .await
        .unwrap();
    assert_ne!(joined.member_id, blocker.member_id);
    assert_eq!(joined.member_id, member_id);
    assert_eq!(joined.members.len(), 1);
}

#[tokio::test]
async fn leave_during_rebalance_completes_one_generation() {
    let store = MemoryMetadataStore::new();
    let (first, second) = ready_pair(&store, "leave-barrier").await;
    make_stable(&store, "leave-barrier", &first).await;
    let (leader, follower) = if first.member_id == first.leader {
        (&first, &second)
    } else {
        (&second, &first)
    };
    let parked = begin(&store, "leave-barrier", &leader.member_id, None, 500, 0).await;
    let rebalance_id = parked.pending_rebalance.unwrap();

    store
        .leave_group(
            "leave-barrier",
            &[GroupMemberIdentity {
                member_id: follower.member_id.clone(),
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let completed = store
        .poll_join_group("leave-barrier", &leader.member_id, None, rebalance_id, 3)
        .await
        .unwrap();

    assert_eq!(completed.pending_rebalance, None);
    assert_eq!(completed.generation_id, 2);
    assert_eq!(completed.members.len(), 1);
    store
        .heartbeat_group(
            "leave-barrier",
            completed.generation_id,
            &leader.member_id,
            None,
        )
        .await
        .unwrap();
}

async fn ready_pair(
    store: &MemoryMetadataStore,
    group_id: &str,
) -> (JoinGroupResult, JoinGroupResult) {
    let first = begin(store, group_id, "", None, 200, 20).await;
    let second = begin(store, group_id, "", None, 200, 20).await;
    let rebalance_id = first.pending_rebalance.unwrap();
    sleep(Duration::from_millis(25)).await;
    let first = store
        .poll_join_group(group_id, &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    let second = store
        .poll_join_group(group_id, &second.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    (first, second)
}

async fn make_stable(store: &MemoryMetadataStore, group_id: &str, joined: &JoinGroupResult) {
    let assignments = joined
        .members
        .iter()
        .map(|member| GroupAssignment {
            member_id: member.member_id.clone(),
            assignment: vec![1],
        })
        .collect();
    store
        .sync_group(
            group_id,
            joined.generation_id,
            &joined.leader,
            joined
                .members
                .iter()
                .find(|member| member.member_id == joined.leader)
                .and_then(|member| member.group_instance_id.as_deref()),
            assignments,
        )
        .await
        .unwrap();
}

async fn begin(
    store: &MemoryMetadataStore,
    group_id: &str,
    member_id: &str,
    instance_id: Option<&str>,
    rebalance_timeout_ms: i32,
    initial_delay_ms: i32,
) -> JoinGroupResult {
    begin_with_session(
        store,
        group_id,
        member_id,
        instance_id,
        1_000,
        rebalance_timeout_ms,
        initial_delay_ms,
    )
    .await
}

async fn begin_with_session(
    store: &MemoryMetadataStore,
    group_id: &str,
    member_id: &str,
    instance_id: Option<&str>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    initial_delay_ms: i32,
) -> JoinGroupResult {
    begin_with_session_and_max_size(
        store,
        group_id,
        member_id,
        instance_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        initial_delay_ms,
        i32::MAX,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn begin_with_session_and_max_size(
    store: &MemoryMetadataStore,
    group_id: &str,
    member_id: &str,
    instance_id: Option<&str>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    initial_delay_ms: i32,
    max_size: i32,
) -> JoinGroupResult {
    store
        .begin_join_group(
            group_id,
            member_id,
            instance_id,
            PROTOCOL_TYPE,
            &[("range".to_owned(), vec![1])],
            ("barrier-test", "127.0.0.1", &[], session_timeout_ms),
            rebalance_timeout_ms,
            initial_delay_ms,
            max_size,
            3,
        )
        .await
        .unwrap_or_else(|error| match error {
            ControlError::MemberIdRequired { .. } => {
                panic!("version 3 must not require a member-id handshake")
            }
            error => panic!("join failed: {error}"),
        })
}
