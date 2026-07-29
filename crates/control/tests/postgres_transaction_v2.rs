use rutomq_control::{ControlError, MetadataStore, PartitionKey, PostgresMetadataStore};
use uuid::Uuid;

#[tokio::test]
async fn postgres_end_txn_v2_bump_and_retry_survive_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple();
    let topic = format!("txn-v2-{suffix}");
    let transactional_id = format!("txn-v2-id-{suffix}");
    store.create_topic(&topic, 1).await.unwrap();
    let partition = PartitionKey::new(topic, 0);
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

    let bumped = store
        .end_transaction_with_epoch_bump(&transactional_id, producer, true)
        .await
        .unwrap();
    assert_eq!(bumped.producer_id, producer.producer_id);
    assert_eq!(bumped.producer_epoch, producer.producer_epoch + 1);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected
            .end_transaction_with_epoch_bump(&transactional_id, producer, true)
            .await
            .unwrap(),
        bumped
    );
    assert!(matches!(
        reconnected
            .end_transaction_with_epoch_bump(&transactional_id, producer, false)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
    reconnected
        .add_partitions_to_transaction(
            &transactional_id,
            bumped,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    let next = reconnected
        .end_transaction_with_epoch_bump(&transactional_id, bumped, false)
        .await
        .unwrap();
    assert_eq!(next.producer_epoch, bumped.producer_epoch + 1);
}

#[tokio::test]
async fn postgres_end_txn_v2_empty_abort_bumps_and_retries() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let transactional_id = format!("empty-txn-v2-{}", Uuid::new_v4().simple());
    let producer = store
        .init_producer(Some(&transactional_id), 60_000, None)
        .await
        .unwrap();
    assert!(matches!(
        store
            .end_transaction_with_epoch_bump(&transactional_id, producer, true)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));

    let bumped = store
        .end_transaction_with_epoch_bump(&transactional_id, producer, false)
        .await
        .unwrap();
    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected
            .end_transaction_with_epoch_bump(&transactional_id, producer, false)
            .await
            .unwrap(),
        bumped
    );
    let next = reconnected
        .end_transaction_with_epoch_bump(&transactional_id, bumped, false)
        .await
        .unwrap();
    assert_eq!(next.producer_epoch, bumped.producer_epoch + 1);
}
