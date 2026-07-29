use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::INVALID_REGULAR_EXPRESSION;
use kafka_protocol::messages::TransactionalId;

async fn list_transactions(
    broker: &Broker,
    correlation_id: i32,
    request: &ListTransactionsRequest,
) -> ListTransactionsResponse {
    let frame = broker
        .handle_request(request_frame(
            ApiKey::ListTransactions,
            2,
            correlation_id,
            request,
        ))
        .await
        .unwrap();
    decode_response(ApiKey::ListTransactions, 2, frame)
}

#[tokio::test]
async fn list_transactions_distinguishes_unknown_only_from_no_state_filter() {
    let broker = broker();
    broker
        .metadata
        .init_producer(Some("orders"), 60_000, None)
        .await
        .unwrap();

    let response = list_transactions(
        &broker,
        1,
        &ListTransactionsRequest::default()
            .with_state_filters(vec![StrBytes::from_static_str("Bogus")]),
    )
    .await;
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response
            .unknown_state_filters
            .iter()
            .map(StrBytes::as_str)
            .collect::<Vec<_>>(),
        ["Bogus"]
    );
    assert!(response.transaction_states.is_empty());

    let unfiltered = list_transactions(&broker, 2, &ListTransactionsRequest::default()).await;
    assert_eq!(unfiltered.transaction_states.len(), 1);
}

#[tokio::test]
async fn list_transactions_matches_complete_ids_and_reports_invalid_regex() {
    let broker = broker();
    for transactional_id in ["orders", "xorders"] {
        broker
            .metadata
            .init_producer(Some(transactional_id), 60_000, None)
            .await
            .unwrap();
    }

    let exact = list_transactions(
        &broker,
        10,
        &ListTransactionsRequest::default()
            .with_transactional_id_pattern(Some(StrBytes::from_static_str("orders"))),
    )
    .await;
    assert_eq!(exact.error_code, NO_ERROR);
    assert_eq!(exact.transaction_states.len(), 1);
    assert_eq!(
        exact.transaction_states[0].transactional_id.as_str(),
        "orders"
    );

    let malformed = list_transactions(
        &broker,
        11,
        &ListTransactionsRequest::default()
            .with_transactional_id_pattern(Some(StrBytes::from_static_str("["))),
    )
    .await;
    assert_eq!(malformed.error_code, INVALID_REGULAR_EXPRESSION);
    assert!(malformed.transaction_states.is_empty());
}

#[tokio::test]
async fn describe_transactions_rejects_an_authorized_empty_id() {
    let broker = broker();
    let request = DescribeTransactionsRequest::default()
        .with_transactional_ids(vec![TransactionalId::from(StrBytes::from_static_str(""))]);
    let frame = broker
        .handle_request(request_frame(ApiKey::DescribeTransactions, 0, 20, &request))
        .await
        .unwrap();
    let response: DescribeTransactionsResponse =
        decode_response(ApiKey::DescribeTransactions, 0, frame);
    assert_eq!(response.transaction_states.len(), 1);
    assert_eq!(response.transaction_states[0].error_code, INVALID_REQUEST);
    assert_eq!(response.transaction_states[0].transactional_id.as_str(), "");
}
