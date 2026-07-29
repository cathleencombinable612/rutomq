use rutomq_control::{
    BatchDraft, CompactedObject, CompactedSpanDraft, FetchIsolation, MetadataStore, ObjectRef,
    PartitionKey, PostgresMetadataStore, TopicConfig,
};
use tokio::sync::Mutex;
use uuid::Uuid;

static COMPACTION_TEST_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test]
async fn postgres_compaction_replaces_spans_and_preserves_shared_objects() {
    let _guard = COMPACTION_TEST_LOCK.lock().await;
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    cleanup_scheduling_topics(&store).await;
    let suffix = Uuid::new_v4().simple().to_string();
    let compact_topic = "000-rutomq-postgres-compaction".to_owned();
    let shared_topic = "000-rutomq-postgres-compaction-shared".to_owned();
    let compact_partition = PartitionKey::new(&compact_topic, 0);
    let shared_partition = PartitionKey::new(&shared_topic, 0);
    let _ = store.delete_topic(&compact_topic).await;
    let _ = store.delete_topic(&shared_topic).await;
    store.create_topic(&compact_topic, 1).await.unwrap();
    store.create_topic(&shared_topic, 1).await.unwrap();
    store
        .set_topic_config(
            &compact_topic,
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                delete_retention_ms: 100,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();

    let source_key = format!("objects/compact-source-{suffix}");
    let source_object = ObjectRef {
        key: source_key.clone(),
        size: 30,
    };
    store.stage_object(source_object.clone()).await.unwrap();
    store
        .commit_object(
            source_object,
            vec![
                batch(compact_partition.clone(), 0, 10),
                batch(compact_partition.clone(), 10, 20),
                batch(shared_partition.clone(), 20, 30),
            ],
        )
        .await
        .unwrap();
    let plan = store.claim_compaction(100, 1_000).await.unwrap().unwrap();
    assert_eq!(plan.partition, compact_partition);
    assert_eq!(plan.spans.len(), 2);

    let retained = plan.spans.last().unwrap();
    let compacted_key = format!("objects/compact-result-{suffix}");
    let compacted_object = ObjectRef {
        key: compacted_key.clone(),
        size: 10,
    };
    store.stage_object(compacted_object.clone()).await.unwrap();
    assert!(
        store
            .commit_compaction(
                &plan,
                vec![CompactedObject {
                    object: compacted_object,
                    spans: vec![CompactedSpanDraft {
                        source_id: retained.id,
                        byte_start: 0,
                        byte_end: 10,
                        base_offset: 1,
                        last_offset: 1,
                        record_count: 1,
                        checksum: [7; 32],
                        producer: None,
                    }],
                }],
                Some(200),
                100,
            )
            .await
            .unwrap()
    );

    let compacted = store
        .fetch(
            &compact_partition,
            0,
            usize::MAX,
            FetchIsolation::ReadUncommitted,
        )
        .await
        .unwrap();
    assert_eq!(compacted.high_watermark, 2);
    assert_eq!(compacted.log_start_offset, 0);
    assert_eq!(compacted.spans.len(), 1);
    assert_eq!(compacted.spans[0].object_key, compacted_key);
    assert_eq!(compacted.spans[0].base_offset, 1);
    assert!(compacted.spans[0].offsets_preserved);
    assert_eq!(
        compacted.spans[0].integrity,
        rutomq_control::SpanIntegrity::current([7; 32])
    );

    let shared = store
        .fetch(
            &shared_partition,
            0,
            usize::MAX,
            FetchIsolation::ReadUncommitted,
        )
        .await
        .unwrap();
    assert_eq!(shared.spans[0].object_key, source_key);
    assert!(store.object_committed(&source_key).await.unwrap());
    let recheck = store.claim_compaction(200, 1_000).await.unwrap().unwrap();
    assert_eq!(recheck.partition, compact_partition);
    store
        .release_compaction(&recheck.partition, recheck.lease_id)
        .await
        .unwrap();
    store.delete_topic(&compact_topic).await.unwrap();
    store.delete_topic(&shared_topic).await.unwrap();
}

#[tokio::test]
async fn postgres_compaction_honors_dirty_ratio_and_maximum_lag() {
    let _guard = COMPACTION_TEST_LOCK.lock().await;
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    cleanup_scheduling_topics(&store).await;
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("000-rutomq-compaction-scheduling-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store
        .create_topic_with_config(
            &topic,
            1,
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.9,
                max_compaction_lag_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();

    let initial_key = format!("objects/compaction-scheduling-initial-{suffix}");
    let initial = ObjectRef {
        key: initial_key,
        size: 100,
    };
    store.stage_object(initial.clone()).await.unwrap();
    store
        .commit_object(
            initial,
            vec![
                batch_at(partition.clone(), 0, 90, 1),
                batch_at(partition.clone(), 90, 100, 1),
            ],
        )
        .await
        .unwrap();
    let first = store.claim_compaction(100, 1_000).await.unwrap().unwrap();
    assert_eq!(first.partition, partition);
    let retained = first.spans.last().unwrap();
    let compacted = ObjectRef {
        key: format!("objects/compaction-scheduling-clean-{suffix}"),
        size: 90,
    };
    store.stage_object(compacted.clone()).await.unwrap();
    assert!(
        store
            .commit_compaction(
                &first,
                vec![CompactedObject {
                    object: compacted,
                    spans: vec![CompactedSpanDraft {
                        source_id: retained.id,
                        byte_start: 0,
                        byte_end: 90,
                        base_offset: 1,
                        last_offset: 1,
                        record_count: 1,
                        checksum: [9; 32],
                        producer: None,
                    }],
                }],
                None,
                100,
            )
            .await
            .unwrap()
    );

    let dirty = ObjectRef {
        key: format!("objects/compaction-scheduling-dirty-{suffix}"),
        size: 10,
    };
    store.stage_object(dirty.clone()).await.unwrap();
    store
        .commit_object(dirty, vec![batch_at(partition.clone(), 0, 10, 200)])
        .await
        .unwrap();
    let premature = store.claim_compaction(699, 1_000).await.unwrap();
    assert!(
        premature.is_none(),
        "unexpected early compaction for {:?}",
        premature.map(|plan| plan.partition)
    );

    store
        .set_topic_config(
            &topic,
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.05,
                max_compaction_lag_ms: 10_000,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let ratio = store.claim_compaction(699, 1_000).await.unwrap().unwrap();
    assert_eq!(ratio.partition, partition);
    store
        .release_compaction(&ratio.partition, ratio.lease_id)
        .await
        .unwrap();

    store
        .set_topic_config(
            &topic,
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.9,
                max_compaction_lag_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(store.claim_compaction(699, 1_000).await.unwrap().is_none());
    let maximum_lag = store.claim_compaction(700, 1_000).await.unwrap().unwrap();
    assert_eq!(maximum_lag.partition, partition);
    store
        .release_compaction(&maximum_lag.partition, maximum_lag.lease_id)
        .await
        .unwrap();
    store.delete_topic(&topic).await.unwrap();
}

async fn cleanup_scheduling_topics(store: &PostgresMetadataStore) {
    for existing in store.topics(None).await.unwrap() {
        if existing
            .name
            .starts_with("000-rutomq-compaction-scheduling-")
        {
            store.delete_topic(&existing.name).await.unwrap();
        }
    }
}

fn batch(partition: PartitionKey, byte_start: u64, byte_end: u64) -> BatchDraft {
    batch_at(partition, byte_start, byte_end, 1)
}

fn batch_at(
    partition: PartitionKey,
    byte_start: u64,
    byte_end: u64,
    timestamp_ms: i64,
) -> BatchDraft {
    BatchDraft {
        partition,
        byte_start,
        byte_end,
        record_count: 1,
        timestamp_ms,
        checksum: None,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}
