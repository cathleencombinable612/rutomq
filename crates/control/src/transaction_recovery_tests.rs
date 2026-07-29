use super::*;

#[tokio::test]
async fn two_phase_transaction_survives_timeout_and_reinitialization() {
    let store = MemoryMetadataStore::new();
    store.create_topic("two-phase", 1).await.unwrap();
    let partition = PartitionKey::new("two-phase", 0);
    let initialized = store
        .init_producer_with_options(Some("two-phase-tx"), 1, None, false, false)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "two-phase-tx",
            initialized.producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "two-phase-object".to_owned(),
                size: 8,
            },
            vec![transactional_draft(
                &partition,
                "two-phase-tx",
                initialized.producer,
            )],
        )
        .await
        .unwrap();

    let recovered = store
        .init_producer_with_options(Some("two-phase-tx"), 1, None, true, true)
        .await
        .unwrap();
    assert_eq!(recovered.ongoing_transaction, Some(initialized.producer));
    assert_eq!(
        recovered.producer.producer_epoch,
        initialized.producer.producer_epoch + 1
    );
    assert!(matches!(
        store
            .add_partitions_to_transaction(
                "two-phase-tx",
                recovered.producer,
                std::slice::from_ref(&partition),
                false,
            )
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert_eq!(store.abort_expired_transactions().await.unwrap(), 0);
    assert!(matches!(
        store
            .end_transaction("two-phase-tx", initialized.producer, true)
            .await,
        Err(ControlError::ProducerFenced { .. })
    ));
    store
        .end_transaction("two-phase-tx", recovered.producer, true)
        .await
        .unwrap();

    let fetched = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert_eq!(fetched.spans.len(), 1);
}

#[tokio::test]
async fn normal_reinitialization_still_aborts_an_ongoing_transaction() {
    let store = MemoryMetadataStore::new();
    store.create_topic("normal-reinit", 1).await.unwrap();
    let partition = PartitionKey::new("normal-reinit", 0);
    let initialized = store
        .init_producer(Some("normal-reinit-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "normal-reinit-tx",
            initialized,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "normal-reinit-object".to_owned(),
                size: 8,
            },
            vec![transactional_draft(
                &partition,
                "normal-reinit-tx",
                initialized,
            )],
        )
        .await
        .unwrap();

    let next = store
        .init_producer(Some("normal-reinit-tx"), 60_000, None)
        .await
        .unwrap();
    assert_eq!(next.producer_epoch, initialized.producer_epoch + 1);
    let fetched = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert!(fetched.spans.is_empty());
    assert_eq!(fetched.last_stable_offset, 1);
}

fn transactional_draft(
    partition: &PartitionKey,
    transactional_id: &str,
    producer: ProducerSession,
) -> BatchDraft {
    BatchDraft {
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
        transactional_id: Some(transactional_id.to_owned()),
        verify_transaction_partition: true,
    }
}
