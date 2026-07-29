use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{INVALID_REQUEST, UNKNOWN_TOPIC_ID};
use kafka_protocol::messages::DeleteTopicsResponse;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;

#[tokio::test]
async fn delete_topics_preserves_legacy_names_and_v6_response_ids() {
    let broker = broker();
    for version in 1..=5 {
        let name = format!("delete-name-v{version}");
        broker.metadata.create_topic(&name, 1).await.unwrap();
        let request = DeleteTopicsRequest::default().with_topic_names(vec![topic_name(&name)]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::DeleteTopics,
                version,
                160 + i32::from(version),
                &request,
            ))
            .await
            .unwrap();
        let response: DeleteTopicsResponse =
            decode_response(ApiKey::DeleteTopics, version, response);
        assert_eq!(response.responses[0].error_code, NO_ERROR);
        assert_eq!(response.responses[0].name.as_ref().unwrap().as_str(), name);
        assert!(response.responses[0].topic_id.is_nil());
    }

    let by_name = broker
        .metadata
        .create_topic("delete-v6-name", 1)
        .await
        .unwrap();
    let request = DeleteTopicsRequest::default().with_topics(vec![
        DeleteTopicState::default()
            .with_name(Some(topic_name(&by_name.name)))
            .with_topic_id(Uuid::nil()),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteTopics, 6, 166, &request))
        .await
        .unwrap();
    let response: DeleteTopicsResponse = decode_response(ApiKey::DeleteTopics, 6, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(response.responses[0].topic_id, by_name.id);

    let by_id = broker
        .metadata
        .create_topic("delete-v6-id", 2)
        .await
        .unwrap();
    let request = DeleteTopicsRequest::default().with_topics(vec![
        DeleteTopicState::default()
            .with_name(None)
            .with_topic_id(by_id.id),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteTopics, 6, 167, &request))
        .await
        .unwrap();
    let response: DeleteTopicsResponse = decode_response(ApiKey::DeleteTopics, 6, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(response.responses[0].topic_id, by_id.id);
    assert_eq!(
        response.responses[0].name.as_ref().unwrap().as_str(),
        by_id.name
    );
    assert!(
        broker
            .metadata
            .topic_by_id(by_id.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_topics_v6_rejects_identity_conflicts_before_mutation() {
    let broker = broker();
    let duplicate_name = broker
        .metadata
        .create_topic("delete-duplicate-name", 1)
        .await
        .unwrap();
    let duplicate_id = broker
        .metadata
        .create_topic("delete-duplicate-id", 1)
        .await
        .unwrap();
    let cross_identity = broker
        .metadata
        .create_topic("delete-cross-identity", 1)
        .await
        .unwrap();
    let mixed_identity = broker
        .metadata
        .create_topic("delete-mixed-identity", 1)
        .await
        .unwrap();
    let request = DeleteTopicsRequest::default().with_topics(vec![
        by_name(&duplicate_name.name),
        by_name(&duplicate_name.name),
        by_id(duplicate_id.id),
        by_id(duplicate_id.id),
        by_name(&cross_identity.name),
        by_id(cross_identity.id),
        DeleteTopicState::default()
            .with_name(Some(topic_name(&mixed_identity.name)))
            .with_topic_id(mixed_identity.id),
        DeleteTopicState::default(),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteTopics, 6, 168, &request))
        .await
        .unwrap();
    let response: DeleteTopicsResponse = decode_response(ApiKey::DeleteTopics, 6, response);
    assert_eq!(response.responses.len(), 5);
    assert!(
        response
            .responses
            .iter()
            .all(|response| response.error_code == INVALID_REQUEST)
    );
    for topic in [duplicate_name, duplicate_id, cross_identity, mixed_identity] {
        assert!(
            broker
                .metadata
                .topic_by_id(topic.id)
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn delete_topics_v6_reports_unknown_topic_ids() {
    let broker = broker();
    let missing = Uuid::new_v4();
    let request = DeleteTopicsRequest::default().with_topics(vec![by_id(missing)]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteTopics, 6, 169, &request))
        .await
        .unwrap();
    let response: DeleteTopicsResponse = decode_response(ApiKey::DeleteTopics, 6, response);
    assert_eq!(response.responses[0].error_code, UNKNOWN_TOPIC_ID);
    assert_eq!(response.responses[0].topic_id, missing);
    assert!(response.responses[0].name.is_none());
}

fn by_name(name: &str) -> DeleteTopicState {
    DeleteTopicState::default()
        .with_name(Some(topic_name(name)))
        .with_topic_id(Uuid::nil())
}

fn by_id(id: Uuid) -> DeleteTopicState {
    DeleteTopicState::default()
        .with_name(None)
        .with_topic_id(id)
}
