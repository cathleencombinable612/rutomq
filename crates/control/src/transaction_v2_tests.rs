use crate::{ControlError, MemoryMetadataStore, MetadataStore, PartitionKey};

#[tokio::test]
async fn end_txn_v2_bumps_once_and_retries_exactly() {
    let store = MemoryMetadataStore::new();
    store.create_topic("txn-v2", 1).await.unwrap();
    let partition = PartitionKey::new("txn-v2", 0);
    let producer = store
        .init_producer(Some("txn-v2-id"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "txn-v2-id",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();

    let bumped = store
        .end_transaction_with_epoch_bump("txn-v2-id", producer, true)
        .await
        .unwrap();
    assert_eq!(bumped.producer_id, producer.producer_id);
    assert_eq!(bumped.producer_epoch, producer.producer_epoch + 1);
    assert_eq!(
        store
            .end_transaction_with_epoch_bump("txn-v2-id", producer, true)
            .await
            .unwrap(),
        bumped
    );
    assert!(matches!(
        store
            .end_transaction_with_epoch_bump("txn-v2-id", producer, false)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));

    store
        .add_partitions_to_transaction("txn-v2-id", bumped, std::slice::from_ref(&partition), false)
        .await
        .unwrap();
    let next = store
        .end_transaction_with_epoch_bump("txn-v2-id", bumped, false)
        .await
        .unwrap();
    assert_eq!(next.producer_epoch, bumped.producer_epoch + 1);
}

#[tokio::test]
async fn end_txn_v2_allows_empty_abort_but_rejects_empty_commit() {
    let store = MemoryMetadataStore::new();
    let producer = store
        .init_producer(Some("empty-txn-v2"), 60_000, None)
        .await
        .unwrap();
    assert!(matches!(
        store
            .end_transaction_with_epoch_bump("empty-txn-v2", producer, true)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));

    let first = store
        .end_transaction_with_epoch_bump("empty-txn-v2", producer, false)
        .await
        .unwrap();
    assert_eq!(first.producer_epoch, producer.producer_epoch + 1);
    assert_eq!(
        store
            .end_transaction_with_epoch_bump("empty-txn-v2", producer, false)
            .await
            .unwrap(),
        first
    );

    let second = store
        .end_transaction_with_epoch_bump("empty-txn-v2", first, false)
        .await
        .unwrap();
    assert_eq!(second.producer_epoch, first.producer_epoch + 1);
}
