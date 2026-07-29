use chrono::Utc;
use rutomq_control::{
    BatchDraft, ControlError, FetchIsolation, MetadataStore, ObjectRef, OffsetCommit, PartitionKey,
    PostgresMetadataStore, ProducerBatch, ProducerSession, TopicConfig, TransactionFilter,
    TransactionState,
};
use std::sync::Arc;
use tokio::sync::{Barrier, Mutex};
use uuid::Uuid;

// Producer expiration is cluster-wide, so topic suffixes cannot isolate these
// tests when Cargo runs this PostgreSQL test binary concurrently.
static PRODUCER_STATE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test]
async fn postgres_transaction_partition_verification_is_per_batch_and_preserves_fencing() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("transaction-verification-{suffix}");
    let transactional_id = format!("transaction-verification-{suffix}");
    store.create_topic(&topic, 2).await.unwrap();
    let registered = PartitionKey::new(&topic, 0);
    let unregistered = PartitionKey::new(&topic, 1);
    let producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &transactional_id,
            producer,
            std::slice::from_ref(&registered),
            false,
        )
        .await
        .unwrap();
    let draft = BatchDraft {
        partition: unregistered.clone(),
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: 0,
            last_sequence: 0,
        }),
        transactional_id: Some(transactional_id.clone()),
        verify_transaction_partition: true,
    };

    let rejected_object = ObjectRef {
        key: format!("objects/{suffix}-verification-required"),
        size: 8,
    };
    store.stage_object(rejected_object.clone()).await.unwrap();
    assert!(matches!(
        store
            .commit_object(rejected_object, vec![draft.clone()])
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));

    let accepted_object = ObjectRef {
        key: format!("objects/{suffix}-verification-disabled"),
        size: 16,
    };
    store.stage_object(accepted_object.clone()).await.unwrap();
    let mut verified = draft.clone();
    verified.partition = registered;
    let mut relaxed = draft.clone();
    relaxed.byte_start = 8;
    relaxed.byte_end = 16;
    relaxed.verify_transaction_partition = false;
    let committed = store
        .commit_object(accepted_object, vec![verified, relaxed])
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert!(committed.iter().any(|span| span.partition == unregistered));

    let fenced_object = ObjectRef {
        key: format!("objects/{suffix}-verification-fenced"),
        size: 8,
    };
    store.stage_object(fenced_object.clone()).await.unwrap();
    let mut fenced = draft;
    fenced.verify_transaction_partition = false;
    fenced.producer.as_mut().unwrap().producer_epoch += 1;
    fenced.producer.as_mut().unwrap().first_sequence = 1;
    fenced.producer.as_mut().unwrap().last_sequence = 1;
    assert!(matches!(
        store.commit_object(fenced_object, vec![fenced]).await,
        Err(ControlError::ProducerFenced { .. })
    ));
}

#[tokio::test]
async fn postgres_transactional_id_expiration_preserves_ongoing_transactions() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("transaction-id-expiration-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    let idle_id = format!("idle-{suffix}");
    let completed_id = format!("completed-{suffix}");
    let ongoing_id = format!("ongoing-{suffix}");
    store.create_topic(&topic, 1).await.unwrap();

    let idle = store
        .init_producer(Some(&idle_id), 60_000, None)
        .await
        .unwrap();
    let completed = store
        .init_producer(Some(&completed_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &completed_id,
            completed,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    let completed_object = ObjectRef {
        key: format!("objects/{suffix}-completed-transactional-id"),
        size: 8,
    };
    store.stage_object(completed_object.clone()).await.unwrap();
    store
        .commit_object(
            completed_object,
            vec![BatchDraft {
                partition: partition.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: completed.producer_id,
                    producer_epoch: completed.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(completed_id.clone()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .end_transaction(&completed_id, completed, true)
        .await
        .unwrap();
    let ongoing = store
        .init_producer(Some(&ongoing_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &ongoing_id,
            ongoing,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();

    let expired = store
        .expire_transactional_ids(Utc::now().timestamp_millis() + 60_000, 1, 100_000)
        .await
        .unwrap();
    assert!(expired >= 2);
    let descriptions = store
        .describe_transactions(&[idle_id.clone(), completed_id.clone(), ongoing_id.clone()])
        .await
        .unwrap();
    assert!(!descriptions.contains_key(&idle_id));
    assert!(!descriptions.contains_key(&completed_id));
    assert_eq!(descriptions[&ongoing_id].state, TransactionState::Ongoing);
    assert_eq!(descriptions[&ongoing_id].producer, ongoing);
    assert_eq!(
        store
            .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .len(),
        1
    );

    let replacement_idle = store
        .init_producer(Some(&idle_id), 60_000, None)
        .await
        .unwrap();
    let replacement_completed = store
        .init_producer(Some(&completed_id), 60_000, None)
        .await
        .unwrap();
    assert_ne!(replacement_idle.producer_id, idle.producer_id);
    assert_ne!(replacement_completed.producer_id, completed.producer_id);
}

#[tokio::test]
async fn postgres_idempotence_and_transaction_visibility() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("events-{suffix}");
    let transactional_id = format!("tx-{suffix}");
    let group_id = format!("group-{suffix}");
    store.create_topic(&topic, 1).await.unwrap();
    let partition = PartitionKey::new(&topic, 0);
    let producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    let shadow_transactional_id = format!("x{transactional_id}");
    store
        .init_producer(Some(&shadow_transactional_id), 60_000, None)
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
    let active = store
        .describe_transactions(std::slice::from_ref(&transactional_id))
        .await
        .unwrap();
    assert_eq!(active[&transactional_id].state, TransactionState::Ongoing);
    assert_eq!(active[&transactional_id].partitions, [partition.clone()]);
    let draft = BatchDraft {
        partition: partition.clone(),
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: 0,
            last_sequence: 0,
        }),
        transactional_id: Some(transactional_id.clone()),
        verify_transaction_partition: true,
    };
    let first_object = ObjectRef {
        key: format!("objects/{suffix}-first"),
        size: 8,
    };
    store.stage_object(first_object.clone()).await.unwrap();
    let first = store
        .commit_object(first_object, vec![draft.clone()])
        .await
        .unwrap();
    let retry_key = format!("objects/{suffix}-retry");
    let retry_object = ObjectRef {
        key: retry_key.clone(),
        size: 8,
    };
    store.stage_object(retry_object.clone()).await.unwrap();
    let retry = store
        .commit_object(retry_object, vec![draft])
        .await
        .unwrap();
    assert_eq!(first[0].base_offset, retry[0].base_offset);
    assert!(!store.object_committed(&retry_key).await.unwrap());
    let active_producers = store.describe_producers(&partition).await.unwrap();
    assert_eq!(active_producers.len(), 1);
    assert_eq!(active_producers[0].producer_id, producer.producer_id);
    assert_eq!(active_producers[0].producer_epoch, producer.producer_epoch);
    assert_eq!(active_producers[0].last_sequence, 0);
    assert_eq!(active_producers[0].last_timestamp, 1);
    assert_eq!(active_producers[0].current_transaction_start_offset, 0);

    let pending = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert!(pending.spans.is_empty());
    assert_eq!(pending.last_stable_offset, 0);
    let pending_watermarks = store.partition_watermarks(&partition).await.unwrap();
    assert_eq!(pending_watermarks.high_watermark, 1);
    assert_eq!(pending_watermarks.last_stable_offset, 0);
    assert_eq!(pending_watermarks.log_start_offset, 0);

    store
        .add_offsets_to_transaction(&transactional_id, producer, &group_id)
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            &transactional_id,
            producer,
            &group_id,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 1,
                leader_epoch: 0,
                metadata: Some("transactional".into()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    store
        .end_transaction(&transactional_id, producer, true)
        .await
        .unwrap();
    assert_eq!(
        store.describe_producers(&partition).await.unwrap()[0].current_transaction_start_offset,
        -1
    );
    let completed = store
        .list_transactions(&TransactionFilter {
            state_filters: vec!["CompleteCommit".to_owned()],
            producer_id_filters: vec![producer.producer_id],
            ..TransactionFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].transactional_id, transactional_id);
    let exact = store
        .list_transactions(&TransactionFilter {
            transactional_id_pattern: Some(transactional_id.clone()),
            ..TransactionFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].transactional_id, transactional_id);
    assert!(matches!(
        store
            .list_transactions(&TransactionFilter {
                transactional_id_pattern: Some("[".to_owned()),
                ..TransactionFilter::default()
            })
            .await,
        Err(rutomq_control::ControlError::InvalidRegularExpression(_))
    ));

    let committed = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert_eq!(committed.spans.len(), 1);
    assert_eq!(committed.last_stable_offset, 1);
    let committed_watermarks = store.partition_watermarks(&partition).await.unwrap();
    assert_eq!(committed_watermarks.high_watermark, 1);
    assert_eq!(committed_watermarks.last_stable_offset, 1);
    let committed_offset = store
        .fetch_offsets(&group_id, std::slice::from_ref(&partition))
        .await
        .unwrap();
    assert_eq!(committed_offset[&partition].offset, 1);
    assert_eq!(committed_offset[&partition].leader_epoch, 0);

    let expiring_id = format!("expiring-{suffix}");
    let expiring = store
        .init_producer(Some(&expiring_id), 1, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &expiring_id,
            expiring,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert!(store.abort_expired_transactions().await.unwrap() >= 1);
    assert_eq!(
        store
            .describe_transactions(std::slice::from_ref(&expiring_id))
            .await
            .unwrap()[&expiring_id]
            .state,
        TransactionState::CompleteAbort
    );

    let concurrent_topic = format!("concurrent-{suffix}");
    store.create_topic(&concurrent_topic, 2).await.unwrap();
    let first_store = store.clone();
    let second_store = store.clone();
    let first_key = format!("objects/{suffix}-concurrent-a");
    let second_key = format!("objects/{suffix}-concurrent-b");
    let first_object_key = first_key.clone();
    let second_object_key = second_key.clone();
    store
        .stage_object(ObjectRef {
            key: first_key.clone(),
            size: 16,
        })
        .await
        .unwrap();
    store
        .stage_object(ObjectRef {
            key: second_key.clone(),
            size: 16,
        })
        .await
        .unwrap();
    let first = tokio::spawn(async move {
        first_store
            .commit_object(
                ObjectRef {
                    key: first_key,
                    size: 16,
                },
                vec![
                    plain_draft(&concurrent_topic, 1, 0),
                    plain_draft(&concurrent_topic, 0, 8),
                ],
            )
            .await
    });
    let second_topic = format!("concurrent-{suffix}");
    let second = tokio::spawn(async move {
        second_store
            .commit_object(
                ObjectRef {
                    key: second_key,
                    size: 16,
                },
                vec![
                    plain_draft(&second_topic, 0, 0),
                    plain_draft(&second_topic, 1, 8),
                ],
            )
            .await
    });
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first[0].partition.partition, 1);
    assert_eq!(first[1].partition.partition, 0);
    let mut partition_zero_offsets = vec![
        first
            .iter()
            .find(|span| span.partition.partition == 0)
            .unwrap()
            .base_offset,
        second
            .iter()
            .find(|span| span.partition.partition == 0)
            .unwrap()
            .base_offset,
    ];
    partition_zero_offsets.sort_unstable();
    assert_eq!(partition_zero_offsets, vec![0, 1]);

    store
        .set_topic_config(
            &format!("concurrent-{suffix}"),
            TopicConfig {
                retention_ms: 0,
                file_delete_delay_ms: 0,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let retained = store.apply_retention(10, 0).await.unwrap();
    assert!(retained.removed_spans >= 4);
    let mut test_objects = retained
        .deletable_objects
        .into_iter()
        .filter(|key| key == &first_object_key || key == &second_object_key)
        .collect::<Vec<_>>();
    test_objects.sort();
    let mut expected_objects = vec![first_object_key.clone(), second_object_key.clone()];
    expected_objects.sort();
    assert_eq!(test_objects, expected_objects);
    assert!(store.object_committed(&first_object_key).await.unwrap());
    assert!(store.object_committed(&second_object_key).await.unwrap());
    assert!(
        store
            .complete_object_deletion(&first_object_key)
            .await
            .unwrap()
    );
    assert!(
        store
            .complete_object_deletion(&second_object_key)
            .await
            .unwrap()
    );
    assert!(!store.object_committed(&first_object_key).await.unwrap());
    assert!(!store.object_committed(&second_object_key).await.unwrap());
}

#[tokio::test]
async fn postgres_transaction_marker_persists_visibility_and_retry_outcome() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("marker-events-{suffix}");
    let transactional_id = format!("marker-tx-{suffix}");
    let first = PartitionKey::new(&topic, 0);
    let second = PartitionKey::new(&topic, 1);
    store.create_topic(&topic, 2).await.unwrap();
    let producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &transactional_id,
            producer,
            &[first.clone(), second.clone()],
            false,
        )
        .await
        .unwrap();
    let object = ObjectRef {
        key: format!("objects/{suffix}-marker"),
        size: 8,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: first.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(transactional_id.clone()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let group = format!("marker-group-{suffix}");
    store
        .add_offsets_to_transaction(&transactional_id, producer, &group)
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            &transactional_id,
            producer,
            &group,
            vec![OffsetCommit {
                partition: first.clone(),
                offset: 1,
                leader_epoch: 0,
                metadata: Some("marker".into()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .write_transaction_marker(producer, std::slice::from_ref(&first), true, 5, 0)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
    assert!(
        store
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .is_empty()
    );

    let marker_partitions = [first.clone(), second];
    store
        .write_transaction_marker(producer, &marker_partitions, true, 5, 0)
        .await
        .unwrap();
    drop(store);

    let recovered = PostgresMetadataStore::connect(&database_url).await.unwrap();
    recovered
        .write_transaction_marker(producer, &marker_partitions, true, 5, 0)
        .await
        .unwrap();
    assert_eq!(
        recovered
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .len(),
        1
    );
    assert_eq!(
        recovered
            .fetch_offsets(&group, std::slice::from_ref(&first))
            .await
            .unwrap()[&first]
            .offset,
        1
    );
    assert!(matches!(
        recovered
            .write_transaction_marker(producer, &marker_partitions, false, 5, 0)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));

    recovered
        .add_partitions_to_transaction(&transactional_id, producer, &marker_partitions, false)
        .await
        .unwrap();
    assert!(matches!(
        recovered
            .write_transaction_marker(producer, &marker_partitions, false, 6, 2)
            .await,
        Err(ControlError::ProducerFenced {
            expected_epoch: 0,
            actual_epoch: 0,
            ..
        })
    ));
    let marker_epoch_one = ProducerSession {
        producer_id: producer.producer_id,
        producer_epoch: 1,
    };
    assert!(matches!(
        recovered
            .write_transaction_marker(marker_epoch_one, &marker_partitions, false, 4, 2)
            .await,
        Err(ControlError::TransactionCoordinatorFenced {
            current_epoch: 5,
            requested_epoch: 4,
            ..
        })
    ));
    recovered
        .write_transaction_marker(marker_epoch_one, &marker_partitions, false, 6, 2)
        .await
        .unwrap();
    drop(recovered);

    let recovered = PostgresMetadataStore::connect(&database_url).await.unwrap();
    recovered
        .write_transaction_marker(marker_epoch_one, &marker_partitions, false, 6, 2)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_producer_sequences_roll_over_from_i32_max_to_zero() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("producer-sequence-rollover-{suffix}");
    store.create_topic(&topic, 1).await.unwrap();
    let partition = PartitionKey::new(&topic, 0);
    let producer = store.init_producer(None, 60_000, None).await.unwrap();
    let draft = |sequence, timestamp_ms| BatchDraft {
        partition: partition.clone(),
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: sequence,
            last_sequence: sequence,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };

    let before = ObjectRef {
        key: format!("objects/{suffix}-before-sequence-rollover"),
        size: 8,
    };
    store.stage_object(before.clone()).await.unwrap();
    let before = store
        .commit_object(before, vec![draft(i32::MAX, 1)])
        .await
        .unwrap();
    assert_eq!(before[0].base_offset, 0);

    let after = ObjectRef {
        key: format!("objects/{suffix}-after-sequence-rollover"),
        size: 8,
    };
    store.stage_object(after.clone()).await.unwrap();
    let after = store.commit_object(after, vec![draft(0, 2)]).await.unwrap();
    assert_eq!(after[0].base_offset, 1);

    let retry = ObjectRef {
        key: format!("objects/{suffix}-sequence-rollover-retry"),
        size: 8,
    };
    store.stage_object(retry.clone()).await.unwrap();
    let retry = store
        .commit_object(retry, vec![draft(i32::MAX, 1)])
        .await
        .unwrap();
    assert_eq!(retry[0].base_offset, 0);
    assert_eq!(store.list_offset(&partition, -1).await.unwrap(), 2);
    let active = store.describe_producers(&partition).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].last_sequence, 0);
}

#[tokio::test]
async fn postgres_timeout_sweep_does_not_deadlock_multi_producer_object_commit() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("producer-timeout-race-{suffix}");
    let producer_count = 12;
    store.create_topic(&topic, producer_count).await.unwrap();
    let mut drafts = Vec::with_capacity(producer_count as usize);
    for partition_index in (0..producer_count).rev() {
        let transactional_id = format!("producer-timeout-race-{partition_index}-{suffix}");
        let partition = PartitionKey::new(&topic, partition_index);
        let producer = store
            .init_producer(Some(&transactional_id), 1, None)
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
        drafts.push(BatchDraft {
            partition,
            byte_start: partition_index as u64 * 8,
            byte_end: partition_index as u64 * 8 + 8,
            record_count: 1,
            timestamp_ms: 1,
            checksum: None,
            producer: Some(ProducerBatch {
                producer_id: producer.producer_id,
                producer_epoch: producer.producer_epoch,
                first_sequence: 0,
                last_sequence: 0,
            }),
            transactional_id: Some(transactional_id),
            verify_transaction_partition: true,
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let object = ObjectRef {
        key: format!("objects/{suffix}-producer-timeout-race"),
        size: producer_count as u64 * 8,
    };
    store.stage_object(object.clone()).await.unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let commit_store = store.clone();
    let commit_barrier = barrier.clone();
    let commit = tokio::spawn(async move {
        commit_barrier.wait().await;
        commit_store.commit_object(object, drafts).await
    });
    let sweep_store = store.clone();
    let sweep_barrier = barrier.clone();
    let sweep = tokio::spawn(async move {
        sweep_barrier.wait().await;
        sweep_store.abort_expired_transactions().await
    });
    barrier.wait().await;

    let (commit, sweep) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(commit, sweep)
    })
    .await
    .expect("metadata operations must not wait indefinitely");
    let commit = commit.unwrap();
    let sweep = sweep.unwrap();
    assert!(
        !matches!(commit, Err(ControlError::Database(_))),
        "object commit exposed a database concurrency error: {commit:?}"
    );
    assert!(
        !matches!(sweep, Err(ControlError::Database(_))),
        "timeout sweep exposed a database concurrency error: {sweep:?}"
    );
}

#[tokio::test]
async fn postgres_end_transactions_publish_offsets_in_consumer_key_order() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("transaction-offset-order-topic-{suffix}");
    let group = format!("transaction-offset-order-group-{suffix}");
    let partition_count = 128;
    store.create_topic(&topic, partition_count).await.unwrap();
    let partitions = (0..partition_count)
        .map(|partition| PartitionKey::new(&topic, partition))
        .collect::<Vec<_>>();
    store
        .commit_offsets(
            &group,
            partitions
                .iter()
                .cloned()
                .map(|partition| OffsetCommit {
                    partition,
                    offset: 1,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                })
                .collect(),
        )
        .await
        .unwrap();

    let ascending_id = format!("transaction-offset-order-a-{suffix}");
    let ascending_producer = store
        .init_producer(Some(&ascending_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_offsets_to_transaction(&ascending_id, ascending_producer, &group)
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            &ascending_id,
            ascending_producer,
            &group,
            partitions
                .iter()
                .cloned()
                .map(|partition| OffsetCommit {
                    partition,
                    offset: 2,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                })
                .collect(),
        )
        .await
        .unwrap();

    let descending_id = format!("transaction-offset-order-b-{suffix}");
    let descending_producer = store
        .init_producer(Some(&descending_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_offsets_to_transaction(&descending_id, descending_producer, &group)
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            &descending_id,
            descending_producer,
            &group,
            partitions
                .iter()
                .rev()
                .cloned()
                .map(|partition| OffsetCommit {
                    partition,
                    offset: 3,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                })
                .collect(),
        )
        .await
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let ascending_store = store.clone();
    let ascending_barrier = barrier.clone();
    let ascending = tokio::spawn(async move {
        ascending_barrier.wait().await;
        ascending_store
            .end_transaction(&ascending_id, ascending_producer, true)
            .await
    });
    let descending_store = store.clone();
    let descending_barrier = barrier.clone();
    let descending = tokio::spawn(async move {
        descending_barrier.wait().await;
        descending_store
            .end_transaction(&descending_id, descending_producer, true)
            .await
    });
    barrier.wait().await;

    let (ascending, descending) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(ascending, descending)
    })
    .await
    .expect("transaction offset publication must not wait indefinitely");
    let ascending = ascending.unwrap();
    let descending = descending.unwrap();
    assert!(
        !matches!(ascending, Err(ControlError::Database(_))),
        "ascending EndTxn exposed a database concurrency error: {ascending:?}"
    );
    assert!(
        !matches!(descending, Err(ControlError::Database(_))),
        "descending EndTxn exposed a database concurrency error: {descending:?}"
    );
    let committed = store.fetch_offsets(&group, &partitions).await.unwrap();
    let committed_values = committed
        .values()
        .map(|offset| offset.offset)
        .collect::<std::collections::HashSet<_>>();
    assert!(
        committed_values == [2].into_iter().collect()
            || committed_values == [3].into_iter().collect(),
        "one atomic EndTxn must win every offset, got {committed_values:?}"
    );
}

fn plain_draft(topic: &str, partition: i32, byte_start: u64) -> BatchDraft {
    BatchDraft {
        partition: PartitionKey::new(topic, partition),
        byte_start,
        byte_end: byte_start + 8,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}

#[tokio::test]
async fn postgres_producer_state_expiration_forgets_retries_but_preserves_pending_transactions() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("producer-expiration-{suffix}");
    let transactional_id = format!("producer-expiration-tx-{suffix}");
    let plain_partition = PartitionKey::new(&topic, 0);
    let transaction_partition = PartitionKey::new(&topic, 1);
    let producer_timestamp_ms = 1_000;
    let sweep_now_ms = 2_000_000;
    store.create_topic(&topic, 2).await.unwrap();

    let plain_producer = store.init_producer(None, 60_000, None).await.unwrap();
    let plain_draft = BatchDraft {
        partition: plain_partition.clone(),
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms: producer_timestamp_ms,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: plain_producer.producer_id,
            producer_epoch: plain_producer.producer_epoch,
            first_sequence: 0,
            last_sequence: 0,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };
    let first_object = ObjectRef {
        key: format!("objects/{suffix}-producer-expiration-first"),
        size: 8,
    };
    store.stage_object(first_object.clone()).await.unwrap();
    let first = store
        .commit_object(first_object, vec![plain_draft.clone()])
        .await
        .unwrap();
    let retry_object = ObjectRef {
        key: format!("objects/{suffix}-producer-expiration-retry"),
        size: 8,
    };
    store.stage_object(retry_object.clone()).await.unwrap();
    let retry = store
        .commit_object(retry_object, vec![plain_draft.clone()])
        .await
        .unwrap();
    assert_eq!(first[0].base_offset, retry[0].base_offset);

    let sequence_batches = (1..=5)
        .enumerate()
        .map(|(index, sequence)| {
            let mut next = plain_draft.clone();
            next.byte_start = (index * 8) as u64;
            next.byte_end = ((index + 1) * 8) as u64;
            next.timestamp_ms = producer_timestamp_ms + i64::from(sequence);
            next.producer = Some(ProducerBatch {
                producer_id: plain_producer.producer_id,
                producer_epoch: plain_producer.producer_epoch,
                first_sequence: sequence,
                last_sequence: sequence,
            });
            next
        })
        .collect::<Vec<_>>();
    let sequence_object = ObjectRef {
        key: format!("objects/{suffix}-producer-sequences"),
        size: 40,
    };
    store.stage_object(sequence_object.clone()).await.unwrap();
    let committed = store
        .commit_object(sequence_object, sequence_batches)
        .await
        .unwrap();
    for (index, span) in committed.iter().enumerate() {
        assert_eq!(span.base_offset, (index + 1) as i64);
    }

    let mut recent_retry_draft = plain_draft.clone();
    recent_retry_draft.timestamp_ms = producer_timestamp_ms + 1;
    recent_retry_draft.producer = Some(ProducerBatch {
        producer_id: plain_producer.producer_id,
        producer_epoch: plain_producer.producer_epoch,
        first_sequence: 1,
        last_sequence: 1,
    });
    let recent_retry_object = ObjectRef {
        key: format!("objects/{suffix}-producer-recent-retry"),
        size: 8,
    };
    store
        .stage_object(recent_retry_object.clone())
        .await
        .unwrap();
    let recent_retry = store
        .commit_object(recent_retry_object, vec![recent_retry_draft])
        .await
        .unwrap();
    assert_eq!(recent_retry[0].base_offset, 1);

    let evicted_retry_object = ObjectRef {
        key: format!("objects/{suffix}-producer-evicted-retry"),
        size: 8,
    };
    store
        .stage_object(evicted_retry_object.clone())
        .await
        .unwrap();
    let evicted_retry = store
        .commit_object(evicted_retry_object, vec![plain_draft.clone()])
        .await
        .unwrap_err();
    assert!(matches!(
        evicted_retry,
        ControlError::OutOfOrderSequence {
            expected: 6,
            actual: 0,
            ..
        }
    ));

    let transaction_producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            &transactional_id,
            transaction_producer,
            std::slice::from_ref(&transaction_partition),
            false,
        )
        .await
        .unwrap();
    let transaction_object = ObjectRef {
        key: format!("objects/{suffix}-producer-expiration-pending"),
        size: 8,
    };
    store
        .stage_object(transaction_object.clone())
        .await
        .unwrap();
    store
        .commit_object(
            transaction_object,
            vec![BatchDraft {
                partition: transaction_partition.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: producer_timestamp_ms,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: transaction_producer.producer_id,
                    producer_epoch: transaction_producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(transactional_id.clone()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .delete_records(&transaction_partition, -1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .describe_producers(&transaction_partition)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .expire_producer_sequences(sweep_now_ms, 1_000_000, 10_000)
            .await
            .unwrap()
            >= 1
    );
    assert!(
        store
            .describe_producers(&plain_partition)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .describe_producers(&transaction_partition)
            .await
            .unwrap()
            .len(),
        1
    );

    let after_expiration_object = ObjectRef {
        key: format!("objects/{suffix}-producer-expiration-new"),
        size: 8,
    };
    store
        .stage_object(after_expiration_object.clone())
        .await
        .unwrap();
    let after_expiration = store
        .commit_object(after_expiration_object, vec![plain_draft.clone()])
        .await
        .unwrap();
    assert_eq!(after_expiration[0].base_offset, 6);

    let mut sequence_one_after_expiration = plain_draft;
    sequence_one_after_expiration.timestamp_ms = producer_timestamp_ms + 1;
    sequence_one_after_expiration.producer = Some(ProducerBatch {
        producer_id: plain_producer.producer_id,
        producer_epoch: plain_producer.producer_epoch,
        first_sequence: 1,
        last_sequence: 1,
    });
    let sequence_one_object = ObjectRef {
        key: format!("objects/{suffix}-producer-sequence-one-after-expiration"),
        size: 8,
    };
    store
        .stage_object(sequence_one_object.clone())
        .await
        .unwrap();
    let sequence_one_after_expiration = store
        .commit_object(sequence_one_object, vec![sequence_one_after_expiration])
        .await
        .unwrap();
    assert_eq!(sequence_one_after_expiration[0].base_offset, 7);

    store
        .end_transaction(&transactional_id, transaction_producer, false)
        .await
        .unwrap();
    assert!(
        store
            .expire_producer_sequences(sweep_now_ms, 1_000_000, 10_000)
            .await
            .unwrap()
            >= 2
    );
    assert!(
        store
            .describe_producers(&transaction_partition)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn postgres_producer_state_follows_delete_records_and_retention() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let _guard = PRODUCER_STATE_TEST_LOCK.lock().await;
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("producer-truncation-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();
    let producer = store.init_producer(None, 60_000, None).await.unwrap();
    let draft = |sequence: i32, timestamp_ms: i64| BatchDraft {
        partition: partition.clone(),
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: sequence,
            last_sequence: sequence,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };
    for sequence in 0..=5 {
        let object = ObjectRef {
            key: format!("objects/{suffix}-producer-truncation-{sequence}"),
            size: 8,
        };
        store.stage_object(object.clone()).await.unwrap();
        store
            .commit_object(object, vec![draft(sequence, i64::from(sequence))])
            .await
            .unwrap();
    }

    assert_eq!(store.delete_records(&partition, 2).await.unwrap(), 2);
    assert_eq!(
        store.describe_producers(&partition).await.unwrap()[0].last_sequence,
        5
    );
    let removed_retry_object = ObjectRef {
        key: format!("objects/{suffix}-producer-truncation-removed-retry"),
        size: 8,
    };
    store
        .stage_object(removed_retry_object.clone())
        .await
        .unwrap();
    let removed_retry = store
        .commit_object(removed_retry_object, vec![draft(1, 1)])
        .await
        .unwrap_err();
    assert!(matches!(
        removed_retry,
        ControlError::OutOfOrderSequence {
            expected: 6,
            actual: 1,
            ..
        }
    ));
    let retained_retry_object = ObjectRef {
        key: format!("objects/{suffix}-producer-truncation-retained-retry"),
        size: 8,
    };
    store
        .stage_object(retained_retry_object.clone())
        .await
        .unwrap();
    let retained_retry = store
        .commit_object(retained_retry_object, vec![draft(2, 2)])
        .await
        .unwrap();
    assert_eq!(retained_retry[0].base_offset, 2);

    assert_eq!(store.delete_records(&partition, -1).await.unwrap(), 6);
    assert!(
        store
            .describe_producers(&partition)
            .await
            .unwrap()
            .is_empty()
    );
    let new_state_object = ObjectRef {
        key: format!("objects/{suffix}-producer-truncation-new-state"),
        size: 8,
    };
    store.stage_object(new_state_object.clone()).await.unwrap();
    let new_state = store
        .commit_object(new_state_object, vec![draft(5, 10)])
        .await
        .unwrap();
    assert_eq!(new_state[0].base_offset, 6);

    let retention_topic = format!("producer-retention-{suffix}");
    let retention_partition = PartitionKey::new(&retention_topic, 0);
    store
        .create_topic_with_config(
            &retention_topic,
            1,
            TopicConfig {
                retention_ms: 0,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let retention_producer = store.init_producer(None, 60_000, None).await.unwrap();
    let retention_object = ObjectRef {
        key: format!("objects/{suffix}-producer-retention"),
        size: 8,
    };
    store.stage_object(retention_object.clone()).await.unwrap();
    store
        .commit_object(
            retention_object,
            vec![BatchDraft {
                partition: retention_partition.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: -1_000_000_000_000,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: retention_producer.producer_id,
                    producer_epoch: retention_producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store.apply_retention(-1_000_000_000_000, 0).await.unwrap();
    assert!(
        store
            .describe_producers(&retention_partition)
            .await
            .unwrap()
            .is_empty()
    );
}
