use rutomq_control::{
    BatchDraft, ControlError, GroupAssignment, GroupMemberIdentity, MetadataStore, ObjectRef,
    OffsetCommit, PartitionKey, PostgresMetadataStore,
};
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::Barrier;
use uuid::Uuid;

#[tokio::test]
async fn postgres_deletes_records_and_unsubscribed_group_offsets() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("offset-admin-topic-{suffix}");
    let group = format!("offset-admin-group-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();
    let object = ObjectRef {
        key: format!("objects/offset-admin-{suffix}"),
        size: 20,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![
                batch(partition.clone(), 0, 10),
                batch(partition.clone(), 10, 20),
            ],
        )
        .await
        .unwrap();
    assert_eq!(store.delete_records(&partition, 2).await.unwrap(), 2);
    assert_eq!(store.list_offset(&partition, -2).await.unwrap(), 2);
    assert_eq!(store.list_offset(&partition, -1).await.unwrap(), 4);

    store
        .commit_offsets(
            &group,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 3,
                leader_epoch: 0,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let committed = store
        .fetch_offsets(&group, std::slice::from_ref(&partition))
        .await
        .unwrap();
    assert_eq!(committed[&partition].leader_epoch, 0);
    let joined = store
        .join_group(
            &group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "postgres-offset-admin",
                "127.0.0.1",
                std::slice::from_ref(&topic),
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .delete_offsets(&group, std::slice::from_ref(&partition))
            .await
            .unwrap(),
        [partition.clone()].into_iter().collect()
    );
    store
        .leave_group(
            &group,
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    assert!(
        store
            .delete_offsets(&group, std::slice::from_ref(&partition))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .fetch_offsets(&group, std::slice::from_ref(&partition))
            .await
            .unwrap()
            .is_empty()
    );

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(reconnected.list_offset(&partition, -2).await.unwrap(), 2);
}

#[tokio::test]
async fn postgres_persists_and_applies_custom_offset_retention() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("offset-explicit-topic-{suffix}");
    let group = format!("offset-explicit-group-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();
    store
        .commit_offsets(
            &group,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 3,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: Some(1),
            }],
        )
        .await
        .unwrap();
    let lifetime = sqlx::query(
        "SELECT commit_timestamp_ms, expire_timestamp_ms
         FROM consumer_offsets WHERE group_id = $1",
    )
    .bind(&group)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        lifetime.get::<i64, _>("expire_timestamp_ms"),
        lifetime.get::<i64, _>("commit_timestamp_ms") + 1
    );

    store
        .expire_consumer_offsets(
            chrono::Utc::now().timestamp_millis() + 100,
            7 * 24 * 60 * 60 * 1_000,
            100,
        )
        .await
        .unwrap();
    assert!(
        store
            .fetch_offsets(&group, std::slice::from_ref(&partition))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn postgres_offset_expiration_observes_classic_subscriptions_and_empty_time() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let subscribed_topic = format!("offset-subscribed-{suffix}");
    let retired_topic = format!("offset-retired-{suffix}");
    let group = format!("offset-managed-group-{suffix}");
    let subscribed = PartitionKey::new(&subscribed_topic, 0);
    let retired = PartitionKey::new(&retired_topic, 0);
    store.create_topic(&subscribed_topic, 1).await.unwrap();
    store.create_topic(&retired_topic, 1).await.unwrap();
    let joined = store
        .join_group(
            &group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            (
                "postgres-offset-expiry",
                "127.0.0.1",
                std::slice::from_ref(&subscribed_topic),
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    store
        .sync_group(
            &group,
            joined.generation_id,
            &joined.member_id,
            None,
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![1],
            }],
        )
        .await
        .unwrap();
    store
        .commit_offsets(
            &group,
            vec![
                OffsetCommit {
                    partition: subscribed.clone(),
                    offset: 10,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
                OffsetCommit {
                    partition: retired.clone(),
                    offset: 11,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
            ],
        )
        .await
        .unwrap();
    let now_ms = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "UPDATE consumer_offsets
         SET commit_timestamp_ms = $2, expiration_checked_at_ms = -1
         WHERE group_id = $1",
    )
    .bind(&group)
    .bind(now_ms - 20_000)
    .execute(&pool)
    .await
    .unwrap();
    store
        .expire_consumer_offsets(now_ms, 10_000, 100)
        .await
        .unwrap();
    let offsets = store
        .fetch_offsets(&group, &[subscribed.clone(), retired.clone()])
        .await
        .unwrap();
    assert!(offsets.contains_key(&subscribed));
    assert!(!offsets.contains_key(&retired));

    store
        .leave_group(
            &group,
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    store
        .expire_consumer_offsets(now_ms + 100, 10_000, 100)
        .await
        .unwrap();
    assert!(
        store
            .fetch_offsets(&group, std::slice::from_ref(&subscribed))
            .await
            .unwrap()
            .contains_key(&subscribed)
    );
    sqlx::query("UPDATE consumer_groups SET empty_since_ms = $2 WHERE group_id = $1")
        .bind(&group)
        .bind(now_ms - 20_000)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE consumer_offsets SET expiration_checked_at_ms = -1
         WHERE group_id = $1",
    )
    .bind(&group)
    .execute(&pool)
    .await
    .unwrap();
    store
        .expire_consumer_offsets(now_ms, 10_000, 100)
        .await
        .unwrap();
    assert!(
        store
            .fetch_offsets(&group, std::slice::from_ref(&subscribed))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .describe_classic_groups(std::slice::from_ref(&group))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn postgres_offset_commits_do_not_deadlock_on_reverse_partition_order() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("offset-order-topic-{suffix}");
    let group = format!("offset-order-group-{suffix}");
    let partition_count = 128;
    store.create_topic(&topic, partition_count).await.unwrap();

    let commits = (0..partition_count)
        .map(|partition| OffsetCommit {
            partition: PartitionKey::new(&topic, partition),
            offset: 1,
            leader_epoch: -1,
            metadata: None,
            retention_time_ms: None,
        })
        .collect::<Vec<_>>();
    store.commit_offsets(&group, commits.clone()).await.unwrap();
    let mut reverse = commits.clone();
    reverse.reverse();

    let barrier = Arc::new(Barrier::new(3));
    let ascending_store = store.clone();
    let ascending_group = group.clone();
    let ascending_barrier = barrier.clone();
    let ascending = tokio::spawn(async move {
        ascending_barrier.wait().await;
        ascending_store
            .commit_offsets(&ascending_group, commits)
            .await
    });
    let descending_store = store.clone();
    let descending_barrier = barrier.clone();
    let descending = tokio::spawn(async move {
        descending_barrier.wait().await;
        descending_store.commit_offsets(&group, reverse).await
    });
    barrier.wait().await;

    let (ascending, descending) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(ascending, descending)
    })
    .await
    .expect("offset commits must not wait indefinitely");
    let ascending = ascending.unwrap();
    let descending = descending.unwrap();
    assert!(
        !matches!(ascending, Err(ControlError::Database(_))),
        "ascending offset commit exposed a database concurrency error: {ascending:?}"
    );
    assert!(
        !matches!(descending, Err(ControlError::Database(_))),
        "descending offset commit exposed a database concurrency error: {descending:?}"
    );
}

#[tokio::test]
async fn postgres_offset_delete_and_commit_share_partition_lock_order() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("offset-delete-order-topic-{suffix}");
    let group = format!("offset-delete-order-group-{suffix}");
    let partition_count = 128;
    store.create_topic(&topic, partition_count).await.unwrap();

    let commits = (0..partition_count)
        .map(|partition| OffsetCommit {
            partition: PartitionKey::new(&topic, partition),
            offset: 1,
            leader_epoch: -1,
            metadata: None,
            retention_time_ms: None,
        })
        .collect::<Vec<_>>();
    let partitions = commits
        .iter()
        .map(|commit| commit.partition.clone())
        .collect::<Vec<_>>();
    store.commit_offsets(&group, commits.clone()).await.unwrap();
    let mut reverse = commits;
    reverse.reverse();

    let barrier = Arc::new(Barrier::new(3));
    let commit_store = store.clone();
    let commit_group = group.clone();
    let commit_barrier = barrier.clone();
    let commit = tokio::spawn(async move {
        commit_barrier.wait().await;
        commit_store.commit_offsets(&commit_group, reverse).await
    });
    let delete_store = store.clone();
    let delete_barrier = barrier.clone();
    let delete = tokio::spawn(async move {
        delete_barrier.wait().await;
        delete_store.delete_offsets(&group, &partitions).await
    });
    barrier.wait().await;

    let (commit, delete) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(commit, delete)
    })
    .await
    .expect("offset commit and delete must not wait indefinitely");
    let commit = commit.unwrap();
    let delete = delete.unwrap();
    assert!(
        !matches!(commit, Err(ControlError::Database(_))),
        "offset commit exposed a database concurrency error: {commit:?}"
    );
    assert!(
        !matches!(delete, Err(ControlError::Database(_))),
        "offset delete exposed a database concurrency error: {delete:?}"
    );
}

fn batch(partition: PartitionKey, byte_start: u64, byte_end: u64) -> BatchDraft {
    BatchDraft {
        partition,
        byte_start,
        byte_end,
        record_count: 2,
        timestamp_ms: 1,
        checksum: None,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}
