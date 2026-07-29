use rutomq_control::{
    ACKNOWLEDGED_DELIVERY_STATE, AVAILABLE_DELIVERY_STATE, BatchDraft, ControlError, MetadataStore,
    ObjectRef, PartitionKey, PostgresMetadataStore, ShareAcknowledgeRecords,
    ShareAcknowledgementBatch, ShareAcquireRequest, ShareAutoOffsetReset, ShareFetchSessionUpdate,
    ShareGroupHeartbeat, ShareSessionPartition, ShareStateBatch, ShareStateInitialization,
    ShareStateKey, ShareStateRead, ShareStateSnapshot, ShareStateSummary, ShareStateWrite,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_share_records_persist_sessions_locks_and_acknowledgements() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-topic-{suffix}");
    let group_id = format!("share-group-{suffix}");
    let topic = store.create_topic(&topic_name, 1).await.unwrap();
    let object = ObjectRef {
        key: format!("share-object-{suffix}"),
        size: 3,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: PartitionKey::new(&topic_name, 0),
                byte_start: 0,
                byte_end: 3,
                record_count: 3,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .share_group_heartbeat(heartbeat(&group_id, &topic_name))
        .await
        .unwrap();
    let partition = ShareSessionPartition {
        topic_id: topic.id,
        partition: 0,
    };
    let opened = store
        .update_share_fetch_session(ShareFetchSessionUpdate {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            session_epoch: 0,
            added: vec![partition.clone()],
            forgotten: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(opened.next_epoch, 1);
    let state = store
        .share_partition_state(
            &group_id,
            "member-a",
            &partition,
            ShareAutoOffsetReset::Earliest,
        )
        .await
        .unwrap();
    assert_eq!(state.start_offset, 0);

    let acquired = store
        .acquire_share_records(acquire(&group_id, topic.id, vec![0, 1, 2], 2))
        .await
        .unwrap();
    assert_eq!(
        acquired
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let mut constrained = acquire(&group_id, topic.id, vec![2], 1);
    constrained.max_record_locks = 1;
    assert!(
        store
            .acquire_share_records(constrained)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .acknowledge_share_records(ShareAcknowledgeRecords {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            topic_id: topic.id,
            partition: 0,
            batches: vec![ShareAcknowledgementBatch {
                first_offset: 0,
                last_offset: 1,
                types: vec![2, 1],
            }],
            lock_duration_ms: 30_000,
            delivery_count_limit: 5,
        })
        .await
        .unwrap();
    let offset = store
        .describe_share_group_offsets(&group_id, Some(&[PartitionKey::new(&topic_name, 0)]))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        (
            offset.start_offset,
            offset.high_watermark,
            offset.delivery_complete_count,
            offset.lag(),
        ),
        (0, 3, 1, 2)
    );

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let continued = reconnected
        .update_share_fetch_session(ShareFetchSessionUpdate {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            session_epoch: 1,
            added: Vec::new(),
            forgotten: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(continued.next_epoch, 2);
    assert!(matches!(
        reconnected
            .update_share_fetch_session(ShareFetchSessionUpdate {
                group_id: group_id.clone(),
                member_id: "member-a".into(),
                session_epoch: 1,
                added: Vec::new(),
                forgotten: Vec::new(),
            })
            .await,
        Err(ControlError::InvalidShareSessionEpoch { .. })
    ));
    let retried = reconnected
        .acquire_share_records(acquire(&group_id, topic.id, vec![0, 1, 2], 2))
        .await
        .unwrap();
    assert_eq!(
        retried
            .iter()
            .map(|record| (record.offset, record.delivery_count))
            .collect::<Vec<_>>(),
        [(0, 2), (2, 1)]
    );
    reconnected
        .acknowledge_share_records(ShareAcknowledgeRecords {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            topic_id: topic.id,
            partition: 0,
            batches: vec![
                ShareAcknowledgementBatch {
                    first_offset: 0,
                    last_offset: 0,
                    types: vec![2],
                },
                ShareAcknowledgementBatch {
                    first_offset: 2,
                    last_offset: 2,
                    types: vec![1],
                },
            ],
            lock_duration_ms: 30_000,
            delivery_count_limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        reconnected
            .share_partition_state(
                &group_id,
                "member-a",
                &partition,
                ShareAutoOffsetReset::Earliest,
            )
            .await
            .unwrap()
            .start_offset,
        3
    );
}

#[tokio::test]
async fn postgres_share_coordinator_state_survives_reconnect_and_fences_epochs() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-state-topic-{suffix}");
    let group_id = format!("share-state-group-{suffix}");
    let topic = store.create_topic(&topic_name, 1).await.unwrap();
    let key = ShareStateKey {
        group_id: group_id.clone(),
        topic_id: topic.id,
        partition: 0,
    };

    store
        .initialize_share_group_state(ShareStateInitialization {
            key: key.clone(),
            state_epoch: 2,
            start_offset: 100,
        })
        .await
        .unwrap();
    assert!(
        store
            .list_groups()
            .await
            .unwrap()
            .iter()
            .all(|group| group.group_id != group_id)
    );

    let second_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let initialized = second_agent
        .read_share_group_state(ShareStateRead {
            key: key.clone(),
            leader_epoch: 4,
        })
        .await
        .unwrap();
    assert_eq!(
        initialized,
        ShareStateSnapshot {
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: 100,
            delivery_complete_count: 0,
            state_batches: Vec::new(),
        }
    );
    second_agent
        .write_share_group_state(ShareStateWrite {
            key: key.clone(),
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: -1,
            delivery_complete_count: 2,
            state_batches: vec![
                ShareStateBatch {
                    first_offset: 100,
                    last_offset: 105,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                },
                ShareStateBatch {
                    first_offset: 102,
                    last_offset: 103,
                    delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                    delivery_count: 2,
                },
            ],
        })
        .await
        .unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected.summarize_share_group_state(&key).await.unwrap(),
        Some(ShareStateSummary {
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: 100,
            delivery_complete_count: 2,
        })
    );
    assert_eq!(
        reconnected
            .read_share_group_state(ShareStateRead {
                key: key.clone(),
                leader_epoch: -1,
            })
            .await
            .unwrap()
            .state_batches,
        [
            ShareStateBatch {
                first_offset: 100,
                last_offset: 101,
                delivery_state: AVAILABLE_DELIVERY_STATE,
                delivery_count: 1,
            },
            ShareStateBatch {
                first_offset: 102,
                last_offset: 103,
                delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                delivery_count: 2,
            },
            ShareStateBatch {
                first_offset: 104,
                last_offset: 105,
                delivery_state: AVAILABLE_DELIVERY_STATE,
                delivery_count: 1,
            },
        ]
    );
    assert!(matches!(
        reconnected
            .read_share_group_state(ShareStateRead {
                key: key.clone(),
                leader_epoch: 3,
            })
            .await,
        Err(ControlError::FencedShareLeaderEpoch {
            current: 4,
            requested: 3
        })
    ));
    assert!(matches!(
        reconnected
            .initialize_share_group_state(ShareStateInitialization {
                key: key.clone(),
                state_epoch: 1,
                start_offset: 0,
            })
            .await,
        Err(ControlError::FencedShareStateEpoch {
            current: 2,
            requested: 1
        })
    ));

    reconnected.delete_share_group_state(&key).await.unwrap();
    reconnected.delete_share_group_state(&key).await.unwrap();
    assert_eq!(store.summarize_share_group_state(&key).await.unwrap(), None);
    store.delete_topic(&topic_name).await.unwrap();
}

#[tokio::test]
async fn postgres_persisted_share_state_controls_record_acquisition() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-state-fetch-topic-{suffix}");
    let group_id = format!("share-state-fetch-group-{suffix}");
    let topic = store.create_topic(&topic_name, 1).await.unwrap();
    let object = ObjectRef {
        key: format!("share-state-fetch-object-{suffix}"),
        size: 4,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: PartitionKey::new(&topic_name, 0),
                byte_start: 0,
                byte_end: 4,
                record_count: 4,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .share_group_heartbeat(heartbeat(&group_id, &topic_name))
        .await
        .unwrap();
    let partition = ShareSessionPartition {
        topic_id: topic.id,
        partition: 0,
    };
    store
        .update_share_fetch_session(ShareFetchSessionUpdate {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            session_epoch: 0,
            added: vec![partition.clone()],
            forgotten: Vec::new(),
        })
        .await
        .unwrap();
    store
        .share_partition_state(
            &group_id,
            "member-a",
            &partition,
            ShareAutoOffsetReset::Earliest,
        )
        .await
        .unwrap();
    let key = ShareStateKey {
        group_id: group_id.clone(),
        topic_id: topic.id,
        partition: 0,
    };
    store
        .write_share_group_state(ShareStateWrite {
            key: key.clone(),
            state_epoch: 0,
            leader_epoch: -1,
            start_offset: -1,
            delivery_complete_count: 1,
            state_batches: vec![
                ShareStateBatch {
                    first_offset: 0,
                    last_offset: 0,
                    delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                    delivery_count: 1,
                },
                ShareStateBatch {
                    first_offset: 1,
                    last_offset: 2,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 2,
                },
            ],
        })
        .await
        .unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let offset = reconnected
        .describe_share_group_offsets(&group_id, Some(&[PartitionKey::new(&topic_name, 0)]))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        (
            offset.start_offset,
            offset.delivery_complete_count,
            offset.lag(),
        ),
        (0, 1, 3)
    );

    let mut request = acquire(&group_id, topic.id, vec![0, 1, 2, 3], 3);
    request.max_record_locks = 3;
    let acquired = store.acquire_share_records(request).await.unwrap();
    assert_eq!(
        acquired
            .iter()
            .map(|record| (record.offset, record.delivery_count))
            .collect::<Vec<_>>(),
        [(1, 3), (2, 3), (3, 1)]
    );
    let snapshot = store
        .read_share_group_state(ShareStateRead {
            key,
            leader_epoch: -1,
        })
        .await
        .unwrap();
    assert_eq!(snapshot.start_offset, 1);
    assert_eq!(snapshot.delivery_complete_count, 0);

    store
        .acknowledge_share_records(ShareAcknowledgeRecords {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            topic_id: topic.id,
            partition: 0,
            batches: vec![ShareAcknowledgementBatch {
                first_offset: 2,
                last_offset: 2,
                types: vec![1],
            }],
            lock_duration_ms: 30_000,
            delivery_count_limit: 5,
        })
        .await
        .unwrap();
    let after_ack = reconnected
        .describe_share_group_offsets(&group_id, Some(&[PartitionKey::new(&topic_name, 0)]))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        (
            after_ack.start_offset,
            after_ack.delivery_complete_count,
            after_ack.lag(),
        ),
        (1, 1, 2)
    );
}

#[tokio::test]
async fn postgres_share_latest_is_only_applied_when_partition_state_is_created() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-latest-topic-{suffix}");
    let group_id = format!("share-latest-group-{suffix}");
    let topic = store.create_topic(&topic_name, 1).await.unwrap();
    store
        .share_group_heartbeat(heartbeat(&group_id, &topic_name))
        .await
        .unwrap();
    let partition = ShareSessionPartition {
        topic_id: topic.id,
        partition: 0,
    };
    store
        .update_share_fetch_session(ShareFetchSessionUpdate {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            session_epoch: 0,
            added: vec![partition.clone()],
            forgotten: Vec::new(),
        })
        .await
        .unwrap();

    assert_eq!(
        store
            .share_partition_state(
                &group_id,
                "member-a",
                &partition,
                ShareAutoOffsetReset::Latest,
            )
            .await
            .unwrap()
            .start_offset,
        0
    );
    let object = ObjectRef {
        key: format!("share-latest-object-{suffix}"),
        size: 1,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: PartitionKey::new(&topic_name, 0),
                byte_start: 0,
                byte_end: 1,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .share_partition_state(
                &group_id,
                "member-a",
                &partition,
                ShareAutoOffsetReset::Latest,
            )
            .await
            .unwrap()
            .start_offset,
        0
    );
}

#[tokio::test]
async fn postgres_share_exact_offset_is_initialized_once_and_probeable() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-exact-topic-{suffix}");
    let group_id = format!("share-exact-group-{suffix}");
    let topic = store.create_topic(&topic_name, 1).await.unwrap();
    let object = ObjectRef {
        key: format!("share-exact-object-{suffix}"),
        size: 3,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: PartitionKey::new(&topic_name, 0),
                byte_start: 0,
                byte_end: 3,
                record_count: 3,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .share_group_heartbeat(heartbeat(&group_id, &topic_name))
        .await
        .unwrap();
    let partition = ShareSessionPartition {
        topic_id: topic.id,
        partition: 0,
    };
    store
        .update_share_fetch_session(ShareFetchSessionUpdate {
            group_id: group_id.clone(),
            member_id: "member-a".into(),
            session_epoch: 0,
            added: vec![partition.clone()],
            forgotten: Vec::new(),
        })
        .await
        .unwrap();

    assert_eq!(
        store
            .existing_share_partition_state(&group_id, "member-a", &partition)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .share_partition_state(
                &group_id,
                "member-a",
                &partition,
                ShareAutoOffsetReset::Exact(2),
            )
            .await
            .unwrap()
            .start_offset,
        2
    );
    assert_eq!(
        store
            .existing_share_partition_state(&group_id, "member-a", &partition)
            .await
            .unwrap()
            .unwrap()
            .start_offset,
        2
    );
    assert_eq!(
        store
            .share_partition_state(
                &group_id,
                "member-a",
                &partition,
                ShareAutoOffsetReset::Exact(0),
            )
            .await
            .unwrap()
            .start_offset,
        2
    );
}

fn heartbeat(group_id: &str, topic: &str) -> ShareGroupHeartbeat {
    ShareGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: "member-a".to_owned(),
        member_epoch: 0,
        rack_id: None,
        subscribed_topic_names: Some(vec![topic.to_owned()]),
        client_id: "postgres-share-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        assignment_interval_ms: 0,
        max_size: 200,
    }
}

fn acquire(
    group_id: &str,
    topic_id: Uuid,
    candidate_offsets: Vec<i64>,
    max_records: usize,
) -> ShareAcquireRequest {
    ShareAcquireRequest {
        group_id: group_id.to_owned(),
        member_id: "member-a".into(),
        topic_id,
        partition: 0,
        candidate_offsets,
        max_records,
        max_record_locks: 2,
        lock_duration_ms: 30_000,
        delivery_count_limit: 5,
    }
}
