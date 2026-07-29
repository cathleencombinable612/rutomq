use rutomq_control::{
    ControlError, MetadataStore, PartitionKey, PostgresMetadataStore, TopicConfig,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_topic_expansion_is_atomic_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let topic = format!("expanded-topic-{}", Uuid::new_v4().simple());
    store.create_topic(&topic, 1).await.unwrap();
    let mut config = TopicConfig {
        max_message_bytes: 4_096,
        file_delete_delay_ms: 321,
        flush_messages: 7,
        flush_ms: 11,
        compression_type: "zstd".to_owned(),
        message_timestamp_type: "LogAppendTime".to_owned(),
        message_timestamp_before_max_ms: 100,
        message_timestamp_after_max_ms: 200,
        min_compaction_lag_ms: 10,
        max_compaction_lag_ms: 1_000,
        min_cleanable_dirty_ratio: 0.75,
        min_insync_replicas: 2,
        compression_gzip_level: 9,
        compression_lz4_level: 17,
        compression_zstd_level: 22,
        ..TopicConfig::default()
    };
    config.mark_dynamic("max.message.bytes");
    config.mark_dynamic("compression.type");
    store
        .set_topic_config(&topic, config.clone())
        .await
        .unwrap();
    let expanded = store.create_partitions(&topic, 4).await.unwrap();
    assert_eq!(expanded.partitions, 4);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new(&topic, 3), -1)
            .await
            .unwrap(),
        0
    );

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected.topic(&topic).await.unwrap().unwrap().partitions,
        4
    );
    assert_eq!(reconnected.topic_config(&topic).await.unwrap(), config);
    assert!(
        reconnected
            .topic_config(&topic)
            .await
            .unwrap()
            .is_dynamic("compression.type")
    );

    let created_with_config = format!("configured-topic-{}", Uuid::new_v4().simple());
    let mut create_config = TopicConfig {
        retention_ms: 123,
        ..TopicConfig::default()
    };
    create_config.mark_dynamic("retention.ms");
    store
        .create_topic_with_config(&created_with_config, 1, create_config.clone())
        .await
        .unwrap();
    assert_eq!(
        reconnected
            .topic_config(&created_with_config)
            .await
            .unwrap(),
        create_config
    );
    assert!(matches!(
        reconnected.create_partitions(&topic, 2).await,
        Err(ControlError::InvalidPartitionCount { .. })
    ));

    let collision = format!("collision_{}", Uuid::new_v4().simple());
    let dotted = collision.replacen('_', ".", 1);
    store.create_topic(&collision, 1).await.unwrap();
    assert!(matches!(
        store.create_topic(&dotted, 1).await,
        Err(ControlError::InvalidTopic(_))
    ));
    assert!(matches!(
        store.create_topic(&collision, 1).await,
        Err(ControlError::TopicAlreadyExists(_))
    ));
    assert!(matches!(
        store.create_topic("invalid/topic", 1).await,
        Err(ControlError::InvalidTopic(_))
    ));

    let id_delete_name = format!("id-delete-{}", Uuid::new_v4().simple());
    let id_delete = store.create_topic(&id_delete_name, 2).await.unwrap();
    let deleted = store
        .delete_topic_by_id(id_delete.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deleted.id, id_delete.id);
    assert_eq!(deleted.name, id_delete.name);
    assert_eq!(deleted.partitions, id_delete.partitions);
    assert!(store.topic(&id_delete_name).await.unwrap().is_none());
    assert!(
        store
            .delete_topic_by_id(id_delete.id)
            .await
            .unwrap()
            .is_none()
    );

    let race_suffix = Uuid::new_v4().simple().to_string();
    let dotted_race = format!("race.{race_suffix}");
    let underscored_race = format!("race_{race_suffix}");
    let dotted_store = store.clone();
    let underscored_store = store.clone();
    let (dotted_result, underscored_result) = tokio::join!(
        dotted_store.create_topic(&dotted_race, 1),
        underscored_store.create_topic(&underscored_race, 1),
    );
    let results = [dotted_result, underscored_result];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControlError::InvalidTopic(_))))
            .count(),
        1
    );
}
