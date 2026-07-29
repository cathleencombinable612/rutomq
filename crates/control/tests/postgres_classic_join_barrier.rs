use rutomq_control::{
    ControlError, GroupAssignment, GroupMemberIdentity, JoinGroupResult, MetadataStore,
    PostgresMetadataStore,
};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::test]
async fn postgres_classic_join_barrier_is_shared_across_store_instances() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_store.migrate().await.unwrap();
    let second_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let recovery_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let group_id = format!("classic-barrier-{}", Uuid::new_v4().simple());

    let first = begin(&first_store, &group_id, "", 500, 50).await;
    let rebalance_id = first.pending_rebalance.expect("initial join is parked");
    let second = begin(&second_store, &group_id, "", 500, 50).await;
    assert_eq!(second.pending_rebalance, Some(rebalance_id));
    assert_eq!(second.generation_id, 0);

    let description = recovery_store
        .describe_classic_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap();
    assert_eq!(description[&group_id].state, "PreparingRebalance");

    sleep(Duration::from_millis(60)).await;
    let first = recovery_store
        .poll_join_group(&group_id, &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    let second = second_store
        .poll_join_group(&group_id, &second.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    assert_ready_pair(&first, &second, 1);
    make_stable(&first_store, &group_id, &first).await;

    let (leader, follower) = if first.member_id == first.leader {
        (&first, &second)
    } else {
        (&second, &first)
    };
    let parked = begin(&first_store, &group_id, &leader.member_id, 500, 0).await;
    let next_rebalance = parked
        .pending_rebalance
        .expect("stable leader starts the next join phase");
    assert_eq!(parked.generation_id, 1);
    let follower_result = begin(&second_store, &group_id, &follower.member_id, 500, 0).await;
    let leader_result = recovery_store
        .poll_join_group(&group_id, &leader.member_id, None, next_rebalance, 3)
        .await
        .unwrap();
    assert_ready_pair(&leader_result, &follower_result, 2);
}

#[tokio::test]
async fn postgres_classic_group_max_size_serializes_cross_agent_joins() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_store.migrate().await.unwrap();
    let second_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let group_id = format!("classic-limit-{}", Uuid::new_v4().simple());
    let protocols = vec![("range".to_owned(), vec![1])];
    let subscribed_topics = Vec::<String>::new();

    let (first, second) = tokio::join!(
        first_store.begin_join_group(
            &group_id,
            "",
            None,
            "consumer",
            &protocols,
            ("postgres-limit-a", "127.0.0.1", &subscribed_topics, 1_000,),
            1_000,
            0,
            1,
            3,
        ),
        second_store.begin_join_group(
            &group_id,
            "",
            None,
            "consumer",
            &protocols,
            ("postgres-limit-b", "127.0.0.1", &subscribed_topics, 1_000,),
            1_000,
            0,
            1,
            3,
        )
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControlError::GroupMaxSizeReached(_))))
            .count(),
        1
    );

    let description = first_store
        .describe_classic_groups(std::slice::from_ref(&group_id))
        .await
        .unwrap();
    let member = &description[&group_id].members[0];
    assert_eq!(description[&group_id].members.len(), 1);
    first_store
        .begin_join_group(
            &group_id,
            &member.member_id,
            None,
            "consumer",
            &protocols,
            (
                "postgres-limit-retry",
                "127.0.0.1",
                &subscribed_topics,
                1_000,
            ),
            1_000,
            0,
            1,
            3,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_poll_expires_old_members_before_rebalance_timeout() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_store.migrate().await.unwrap();
    let second_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let group_id = format!("classic-session-expiry-{}", Uuid::new_v4().simple());

    let first = begin_with_session(&first_store, &group_id, "", 2_000, 8_000, 30).await;
    let second = begin_with_session(&second_store, &group_id, "", 2_000, 8_000, 30).await;
    let rebalance_id = first.pending_rebalance.unwrap();
    sleep(Duration::from_millis(40)).await;
    let ready = first_store
        .poll_join_group(&group_id, &first.member_id, None, rebalance_id, 3)
        .await
        .unwrap();
    make_stable(&first_store, &group_id, &ready).await;

    let replacement = begin_with_session(&first_store, &group_id, "", 10_000, 8_000, 0).await;
    let rebalance_id = replacement.pending_rebalance.unwrap();
    sleep(Duration::from_millis(2_050)).await;
    let completed = second_store
        .poll_join_group(&group_id, &replacement.member_id, None, rebalance_id, 3)
        .await
        .unwrap();

    assert_eq!(completed.pending_rebalance, None);
    assert_eq!(completed.generation_id, 2);
    assert_eq!(completed.members.len(), 1);
    assert_eq!(completed.members[0].member_id, replacement.member_id);
    assert_ne!(replacement.member_id, second.member_id);
}

#[tokio::test]
async fn postgres_leave_during_rebalance_completes_one_generation() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_store.migrate().await.unwrap();
    let second_store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let group_id = format!("classic-leave-barrier-{}", Uuid::new_v4().simple());

    let first = begin(&first_store, &group_id, "", 500, 30).await;
    let second = begin(&second_store, &group_id, "", 500, 30).await;
    sleep(Duration::from_millis(40)).await;
    let first = first_store
        .poll_join_group(
            &group_id,
            &first.member_id,
            None,
            first.pending_rebalance.unwrap(),
            3,
        )
        .await
        .unwrap();
    make_stable(&first_store, &group_id, &first).await;
    let (leader, follower) = if first.member_id == first.leader {
        (&first, &second)
    } else {
        (&second, &first)
    };
    let parked = begin(&first_store, &group_id, &leader.member_id, 500, 0).await;
    let rebalance_id = parked.pending_rebalance.unwrap();

    second_store
        .leave_group(
            &group_id,
            &[GroupMemberIdentity {
                member_id: follower.member_id.clone(),
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let completed = first_store
        .poll_join_group(&group_id, &leader.member_id, None, rebalance_id, 3)
        .await
        .unwrap();

    assert_eq!(completed.pending_rebalance, None);
    assert_eq!(completed.generation_id, 2);
    assert_eq!(completed.members.len(), 1);
    second_store
        .heartbeat_group(&group_id, completed.generation_id, &leader.member_id, None)
        .await
        .unwrap();
}

async fn begin(
    store: &PostgresMetadataStore,
    group_id: &str,
    member_id: &str,
    rebalance_timeout_ms: i32,
    initial_delay_ms: i32,
) -> JoinGroupResult {
    begin_with_session(
        store,
        group_id,
        member_id,
        1_000,
        rebalance_timeout_ms,
        initial_delay_ms,
    )
    .await
}

async fn begin_with_session(
    store: &PostgresMetadataStore,
    group_id: &str,
    member_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    initial_delay_ms: i32,
) -> JoinGroupResult {
    store
        .begin_join_group(
            group_id,
            member_id,
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            ("postgres-barrier", "127.0.0.1", &[], session_timeout_ms),
            rebalance_timeout_ms,
            initial_delay_ms,
            i32::MAX,
            3,
        )
        .await
        .unwrap()
}

async fn make_stable(store: &PostgresMetadataStore, group_id: &str, joined: &JoinGroupResult) {
    store
        .sync_group(
            group_id,
            joined.generation_id,
            &joined.leader,
            None,
            joined
                .members
                .iter()
                .map(|member| GroupAssignment {
                    member_id: member.member_id.clone(),
                    assignment: vec![1],
                })
                .collect(),
        )
        .await
        .unwrap();
}

fn assert_ready_pair(first: &JoinGroupResult, second: &JoinGroupResult, generation_id: i32) {
    assert_eq!(first.pending_rebalance, None);
    assert_eq!(second.pending_rebalance, None);
    assert_eq!(first.generation_id, generation_id);
    assert_eq!(second.generation_id, generation_id);
    assert_eq!(first.leader, second.leader);
    assert_eq!(first.members.len(), 2);
    assert_eq!(second.members.len(), 2);
}
