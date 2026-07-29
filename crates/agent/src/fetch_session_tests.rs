use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    FETCH_SESSION_ID_NOT_FOUND, FETCH_SESSION_TOPIC_ID_ERROR, INVALID_FETCH_SESSION_EPOCH, NO_ERROR,
};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};

fn partition(index: i32) -> FetchPartition {
    FetchPartition::default()
        .with_partition(index)
        .with_fetch_offset(0)
        .with_partition_max_bytes(1024 * 1024)
}

fn topic_by_id(topic_id: Uuid, partitions: Vec<FetchPartition>) -> FetchTopic {
    FetchTopic::default()
        .with_topic_id(topic_id)
        .with_partitions(partitions)
}

fn topic_by_name(name: &str, partitions: Vec<FetchPartition>) -> FetchTopic {
    FetchTopic::default()
        .with_topic(TopicName::from(StrBytes::from_string(name.to_owned())))
        .with_partitions(partitions)
}

fn request(session_id: i32, session_epoch: i32, topics: Vec<FetchTopic>) -> FetchRequest {
    FetchRequest::default()
        .with_session_id(session_id)
        .with_session_epoch(session_epoch)
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(1024 * 1024)
        .with_topics(topics)
}

async fn fetch(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    request: &FetchRequest,
) -> FetchResponse {
    let frame = broker
        .handle_request(request_frame(
            ApiKey::Fetch,
            version,
            correlation_id,
            request,
        ))
        .await
        .unwrap();
    decode_response(ApiKey::Fetch, version, frame)
}

#[tokio::test]
async fn full_fetch_creates_session_and_unchanged_increment_is_empty() {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic("fetch-session", 1)
        .await
        .unwrap();

    let full = fetch(
        &broker,
        18,
        1,
        &request(0, 0, vec![topic_by_id(topic.id, vec![partition(0)])]),
    )
    .await;
    assert_eq!(full.error_code, NO_ERROR);
    assert!(full.session_id > 0);
    assert_eq!(full.responses[0].partitions[0].partition_index, 0);

    let incremental = fetch(&broker, 18, 2, &request(full.session_id, 1, Vec::new())).await;
    assert_eq!(incremental.error_code, NO_ERROR);
    assert_eq!(incremental.session_id, full.session_id);
    assert!(incremental.responses.is_empty());
}

#[tokio::test]
async fn incremental_fetch_returns_only_new_or_changed_partitions() {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic("fetch-session-add", 2)
        .await
        .unwrap();

    let full = fetch(
        &broker,
        18,
        10,
        &request(0, 0, vec![topic_by_id(topic.id, vec![partition(0)])]),
    )
    .await;
    let incremental = fetch(
        &broker,
        18,
        11,
        &request(
            full.session_id,
            1,
            vec![topic_by_id(topic.id, vec![partition(1)])],
        ),
    )
    .await;

    assert_eq!(incremental.error_code, NO_ERROR);
    assert_eq!(incremental.session_id, full.session_id);
    assert_eq!(incremental.responses.len(), 1);
    assert_eq!(incremental.responses[0].partitions.len(), 1);
    assert_eq!(incremental.responses[0].partitions[0].partition_index, 1);

    let unchanged = fetch(&broker, 18, 12, &request(full.session_id, 2, Vec::new())).await;
    assert!(unchanged.responses.is_empty());
}

#[tokio::test]
async fn forgotten_partitions_close_empty_session_and_epochs_are_fenced() {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic("fetch-session-forget", 2)
        .await
        .unwrap();
    let full = fetch(
        &broker,
        18,
        20,
        &request(
            0,
            0,
            vec![topic_by_id(topic.id, vec![partition(0), partition(1)])],
        ),
    )
    .await;

    let forget_zero = request(full.session_id, 1, Vec::new()).with_forgotten_topics_data(vec![
        ForgottenTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(vec![0]),
    ]);
    let response = fetch(&broker, 18, 21, &forget_zero).await;
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.session_id, full.session_id);

    let stale = fetch(&broker, 18, 22, &request(full.session_id, 99, Vec::new())).await;
    assert_eq!(stale.error_code, INVALID_FETCH_SESSION_EPOCH);
    assert_eq!(stale.session_id, 0);
    assert!(stale.responses.is_empty());

    let forget_one = request(full.session_id, 2, Vec::new()).with_forgotten_topics_data(vec![
        ForgottenTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(vec![1]),
    ]);
    let closed = fetch(&broker, 18, 23, &forget_one).await;
    assert_eq!(closed.error_code, NO_ERROR);
    assert_eq!(closed.session_id, 0);
    assert!(closed.responses.is_empty());

    let missing = fetch(&broker, 18, 24, &request(full.session_id, 3, Vec::new())).await;
    assert_eq!(missing.error_code, FETCH_SESSION_ID_NOT_FOUND);
    assert_eq!(missing.session_id, 0);
}

#[tokio::test]
async fn final_epoch_closes_session_and_topic_id_mode_cannot_change() {
    let broker = broker();
    broker
        .metadata
        .create_topic("fetch-session-mode", 1)
        .await
        .unwrap();
    let full = fetch(
        &broker,
        12,
        30,
        &request(
            0,
            0,
            vec![topic_by_name("fetch-session-mode", vec![partition(0)])],
        ),
    )
    .await;

    let mode_mismatch = fetch(&broker, 18, 31, &request(full.session_id, 1, Vec::new())).await;
    assert_eq!(mode_mismatch.error_code, FETCH_SESSION_TOPIC_ID_ERROR);
    assert_eq!(mode_mismatch.session_id, 0);

    let closed = fetch(&broker, 12, 32, &request(full.session_id, -1, Vec::new())).await;
    assert_eq!(closed.error_code, NO_ERROR);
    assert_eq!(closed.session_id, 0);

    let missing = fetch(&broker, 12, 33, &request(full.session_id, 1, Vec::new())).await;
    assert_eq!(missing.error_code, FETCH_SESSION_ID_NOT_FOUND);
}
