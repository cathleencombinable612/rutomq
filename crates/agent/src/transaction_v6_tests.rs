use super::tests::{broker, decode_response, request_frame};
use crate::kafka_error::{NO_ERROR, TRANSACTIONAL_ID_AUTHORIZATION_FAILED};
use kafka_protocol::messages::{
    ApiKey, InitProducerIdRequest, InitProducerIdResponse, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{PartitionKey, ProducerSession};

#[tokio::test]
async fn init_producer_id_v6_preserves_and_reports_the_ongoing_transaction() {
    let mut broker = broker();
    broker.config.transaction_two_phase_commit_enable = true;
    broker.metadata.create_topic("v6-topic", 1).await.unwrap();
    let initial = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id()))
        .with_transaction_timeout_ms(1);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 1, &initial))
        .await
        .unwrap();
    let initial: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(initial.error_code, 0);
    assert_eq!(initial.ongoing_txn_producer_id.0, -1);
    let initial_session = ProducerSession {
        producer_id: initial.producer_id.0,
        producer_epoch: initial.producer_epoch,
    };
    broker
        .metadata
        .add_partitions_to_transaction(
            "v6-tx",
            initial_session,
            &[PartitionKey::new("v6-topic", 0)],
            false,
        )
        .await
        .unwrap();

    let recover = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id()))
        .with_transaction_timeout_ms(900_001)
        .with_enable_2_pc(true)
        .with_keep_prepared_txn(true);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 2, &recover))
        .await
        .unwrap();
    let recovered: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(recovered.error_code, 0);
    assert_eq!(
        recovered.ongoing_txn_producer_id.0,
        initial_session.producer_id
    );
    assert_eq!(
        recovered.ongoing_txn_producer_epoch,
        initial_session.producer_epoch
    );
    assert_eq!(recovered.producer_epoch, initial_session.producer_epoch + 1);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert_eq!(
        broker.metadata.abort_expired_transactions().await.unwrap(),
        0
    );
}

#[tokio::test]
async fn init_producer_id_v6_requires_the_broker_two_phase_feature_gate() {
    let broker = broker();
    let two_phase = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id()))
        .with_transaction_timeout_ms(60_000)
        .with_enable_2_pc(true);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 10, &two_phase))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    assert!(
        broker
            .metadata
            .describe_transactions(&["v6-tx".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );

    let keep_only = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id()))
        .with_transaction_timeout_ms(60_000)
        .with_keep_prepared_txn(true);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 6, 11, &keep_only))
        .await
        .unwrap();
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, NO_ERROR);
}

fn transactional_id() -> TransactionalId {
    TransactionalId::from(StrBytes::from_string("v6-tx".to_owned()))
}
