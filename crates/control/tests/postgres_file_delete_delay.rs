use chrono::Utc;
use rutomq_control::{
    BatchDraft, MetadataStore, ObjectRef, PartitionKey, PostgresMetadataStore, TopicConfig,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_file_delete_delay_is_durable_and_shared_object_safe() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let now_ms = Utc::now().timestamp_millis();

    let topic_a = format!("file-delay-a-{suffix}");
    let topic_b = format!("file-delay-b-{suffix}");
    store
        .create_topic_with_config(
            &topic_a,
            1,
            TopicConfig {
                retention_ms: 0,
                file_delete_delay_ms: 200,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    store
        .create_topic_with_config(
            &topic_b,
            1,
            TopicConfig {
                retention_ms: 1_000_000,
                file_delete_delay_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let shared_key = format!("objects/file-delay-shared-{suffix}");
    commit(
        &store,
        &shared_key,
        vec![
            draft(&topic_a, 0, 0, 10, now_ms),
            draft(&topic_b, 0, 10, 20, now_ms),
        ],
    )
    .await;

    let first = store.apply_retention(now_ms, 0).await.unwrap();
    assert!(first.removed_spans >= 1);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new(&topic_a, 0), -2)
            .await
            .unwrap(),
        1
    );
    assert!(store.object_committed(&shared_key).await.unwrap());
    store
        .set_topic_config(
            &topic_b,
            TopicConfig {
                retention_ms: 0,
                file_delete_delay_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let second = store.apply_retention(now_ms, 0).await.unwrap();
    assert!(second.removed_spans >= 1);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new(&topic_b, 0), -2)
            .await
            .unwrap(),
        1
    );
    assert!(store.object_committed(&shared_key).await.unwrap());
    assert!(
        !store
            .apply_retention(now_ms + 499, 0)
            .await
            .unwrap()
            .deletable_objects
            .contains(&shared_key)
    );
    let matured = store.apply_retention(now_ms + 500, 0).await.unwrap();
    assert!(matured.deletable_objects.contains(&shared_key));
    assert!(store.object_committed(&shared_key).await.unwrap());
    let retry = store.apply_retention(now_ms + 500, 0).await.unwrap();
    assert!(retry.deletable_objects.contains(&shared_key));
    assert!(store.complete_object_deletion(&shared_key).await.unwrap());
    assert!(!store.object_committed(&shared_key).await.unwrap());

    let records_topic = format!("file-delay-records-{suffix}");
    store
        .create_topic_with_config(
            &records_topic,
            1,
            TopicConfig {
                retention_ms: -1,
                file_delete_delay_ms: 50,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let records_key = format!("objects/file-delay-records-{suffix}");
    commit(
        &store,
        &records_key,
        vec![draft(&records_topic, 0, 0, 10, now_ms)],
    )
    .await;
    let records_before = Utc::now().timestamp_millis();
    store
        .delete_records(&PartitionKey::new(&records_topic, 0), -1)
        .await
        .unwrap();
    let records_after = Utc::now().timestamp_millis();
    assert!(
        !store
            .apply_retention(records_before + 49, 0)
            .await
            .unwrap()
            .deletable_objects
            .contains(&records_key)
    );
    let records_matured = store.apply_retention(records_after + 50, 0).await.unwrap();
    assert!(records_matured.deletable_objects.contains(&records_key));
    assert!(store.complete_object_deletion(&records_key).await.unwrap());

    let deleted_topic = format!("file-delay-topic-delete-{suffix}");
    let deleted_info = store
        .create_topic_with_config(
            &deleted_topic,
            1,
            TopicConfig {
                file_delete_delay_ms: 40,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let deleted_key = format!("objects/file-delay-topic-delete-{suffix}");
    commit(
        &store,
        &deleted_key,
        vec![draft(&deleted_topic, 0, 0, 10, now_ms)],
    )
    .await;
    let delete_before = Utc::now().timestamp_millis();
    let deleted = store
        .delete_topic_by_id(deleted_info.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deleted.id, deleted_info.id);
    assert_eq!(deleted.name, deleted_info.name);
    let delete_after = Utc::now().timestamp_millis();
    assert!(
        !store
            .apply_retention(delete_before + 39, 0)
            .await
            .unwrap()
            .deletable_objects
            .contains(&deleted_key)
    );
    let deleted_matured = store.apply_retention(delete_after + 40, 0).await.unwrap();
    assert!(deleted_matured.deletable_objects.contains(&deleted_key));
    assert!(store.complete_object_deletion(&deleted_key).await.unwrap());

    store.delete_topic(&topic_a).await.unwrap();
    store.delete_topic(&topic_b).await.unwrap();
    store.delete_topic(&records_topic).await.unwrap();
}

async fn commit(store: &PostgresMetadataStore, key: &str, drafts: Vec<BatchDraft>) {
    let object = ObjectRef {
        key: key.to_owned(),
        size: drafts.iter().map(|draft| draft.byte_end).max().unwrap_or(0),
    };
    store.stage_object(object.clone()).await.unwrap();
    store.commit_object(object, drafts).await.unwrap();
}

fn draft(
    topic: &str,
    partition: i32,
    byte_start: u64,
    byte_end: u64,
    timestamp_ms: i64,
) -> BatchDraft {
    BatchDraft {
        partition: PartitionKey::new(topic, partition),
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
