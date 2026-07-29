use rutomq_control::{
    BatchDraft, ControlError, FetchIsolation, MetadataStore, ObjectRef, PartitionKey,
    PostgresMetadataStore, ProducerBatch,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_recovers_a_two_phase_transaction_after_agent_loss() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let first_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    first_agent.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("two-phase-{suffix}");
    let transactional_id = format!("two-phase-tx-{suffix}");
    first_agent.create_topic(&topic, 1).await.unwrap();
    let partition = PartitionKey::new(&topic, 0);
    let initialized = first_agent
        .init_producer_with_options(Some(&transactional_id), 1, None, false, false)
        .await
        .unwrap();
    first_agent
        .add_partitions_to_transaction(
            &transactional_id,
            initialized.producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    let object = ObjectRef {
        key: format!("objects/{suffix}-prepared"),
        size: 8,
    };
    first_agent.stage_object(object.clone()).await.unwrap();
    first_agent
        .commit_object(
            object,
            vec![BatchDraft {
                partition: partition.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: initialized.producer.producer_id,
                    producer_epoch: initialized.producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(transactional_id.clone()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    drop(first_agent);

    let recovered_agent = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let recovered = recovered_agent
        .init_producer_with_options(Some(&transactional_id), 1, None, true, true)
        .await
        .unwrap();
    assert_eq!(recovered.ongoing_transaction, Some(initialized.producer));
    assert!(matches!(
        recovered_agent
            .add_partitions_to_transaction(
                &transactional_id,
                recovered.producer,
                std::slice::from_ref(&partition),
                false,
            )
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    recovered_agent.abort_expired_transactions().await.unwrap();
    assert!(matches!(
        recovered_agent
            .end_transaction(&transactional_id, initialized.producer, true)
            .await,
        Err(ControlError::ProducerFenced { .. })
    ));
    recovered_agent
        .end_transaction(&transactional_id, recovered.producer, true)
        .await
        .unwrap();
    let fetched = recovered_agent
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert_eq!(fetched.spans.len(), 1);
}
