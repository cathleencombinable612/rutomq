use rutomq_control::{
    BatchDraft, CURRENT_OBJECT_FORMAT_VERSION, FetchIsolation, MetadataStore, ObjectRef,
    PartitionKey, PostgresMetadataStore, SpanIntegrity,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_round_trips_current_and_legacy_span_integrity() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("span-integrity-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();

    let checksum = [42; 32];
    let current = object(&suffix, "current");
    store.stage_object(current.clone()).await.unwrap();
    let committed = store
        .commit_object(current, vec![draft(partition.clone(), 0, Some(checksum))])
        .await
        .unwrap();
    assert_eq!(committed[0].integrity, SpanIntegrity::current(checksum));

    let legacy = object(&suffix, "legacy");
    store.stage_object(legacy.clone()).await.unwrap();
    store
        .commit_object(legacy, vec![draft(partition.clone(), 4, None)])
        .await
        .unwrap();
    let fetched = store
        .fetch(&partition, 0, usize::MAX, FetchIsolation::ReadUncommitted)
        .await
        .unwrap();
    assert_eq!(
        fetched.spans[0].integrity.format_version,
        CURRENT_OBJECT_FORMAT_VERSION
    );
    assert_eq!(fetched.spans[0].integrity.checksum, Some(checksum));
    assert_eq!(fetched.spans[1].integrity, SpanIntegrity::legacy());

    store.delete_topic(&topic).await.unwrap();
}

fn object(suffix: &str, kind: &str) -> ObjectRef {
    ObjectRef {
        key: format!("objects/{suffix}-{kind}"),
        size: 4,
    }
}

fn draft(partition: PartitionKey, byte_start: u64, checksum: Option<[u8; 32]>) -> BatchDraft {
    BatchDraft {
        partition,
        byte_start,
        byte_end: byte_start + 4,
        record_count: 1,
        timestamp_ms: 1,
        checksum,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}
