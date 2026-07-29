use rutomq_control::{
    BatchDraft, MetadataStore, ObjectRef, OffsetCommit, PartitionKey, PostgresMetadataStore,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_reports_exact_consumer_lag_and_latest_transaction_states() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("observability-{suffix}");
    let group = format!("observability-group-{suffix}");
    let transactional_id = format!("observability-tx-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();

    let object = ObjectRef {
        key: format!("objects/observability-{suffix}"),
        size: 2,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: partition.clone(),
                byte_start: 0,
                byte_end: 2,
                record_count: 2,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let mut config = store.topic_config(&topic).await.unwrap();
    config.retention_bytes = 1;
    store.set_topic_config(&topic, config).await.unwrap();
    let retention = store
        .partition_retention_sizes(10_000)
        .await
        .unwrap()
        .into_iter()
        .find(|observation| observation.partition == partition)
        .unwrap();
    assert_eq!(retention.size_bytes, 2);
    assert_eq!(retention.retention_bytes, 1);
    assert_eq!(retention.percent(), 200);
    store
        .commit_offsets(
            &group,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 1,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let lag = store
        .consumer_lags(10_000)
        .await
        .unwrap()
        .into_iter()
        .find(|lag| lag.group_id == group)
        .unwrap();
    assert_eq!(lag.committed_offset, 1);
    assert_eq!(lag.high_watermark, 2);
    assert_eq!(lag.lag, 1);

    let producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &transactional_id,
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    assert!(store.transaction_state_counts().await.unwrap().ongoing >= 1);
    store
        .end_transaction(&transactional_id, producer, false)
        .await
        .unwrap();
    assert!(
        store
            .transaction_state_counts()
            .await
            .unwrap()
            .complete_abort
            >= 1
    );

    store.apply_retention(1, 0).await.unwrap();
    let retention = store
        .partition_retention_sizes(10_000)
        .await
        .unwrap()
        .into_iter()
        .find(|observation| observation.partition == partition)
        .unwrap();
    assert_eq!(retention.size_bytes, 0);
    assert_eq!(retention.percent(), 0);

    store.delete_topic(&topic).await.unwrap();
}
