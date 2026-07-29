use super::acl_tests::{acl_broker, decode_response, handle_as, topic_rule};
use super::*;
use crate::kafka_error::{INVALID_REQUEST, UNKNOWN_TOPIC_OR_PARTITION};
use kafka_protocol::messages::DeleteTopicsResponse;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
use rutomq_control::{AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule};

#[tokio::test]
async fn delete_topics_v6_hides_id_names_and_honors_cluster_delete() {
    let (broker, metadata) = acl_broker();
    let topic = metadata.create_topic("delete-id-acl", 1).await.unwrap();
    let request = DeleteTopicsRequest::default().with_topics(vec![by_id(topic.id)]);

    let response = delete_as(&broker, "alice", 5901, &request).await;
    assert_eq!(response.responses[0].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert_eq!(response.responses[0].topic_id, topic.id);
    assert!(response.responses[0].name.is_none());
    assert!(metadata.topic_by_id(topic.id).await.unwrap().is_some());

    metadata
        .create_acl(topic_rule(
            "User:alice",
            &topic.name,
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = delete_as(&broker, "alice", 5902, &request).await;
    assert_eq!(response.responses[0].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert_eq!(
        response.responses[0].name.as_ref().unwrap().as_str(),
        topic.name
    );

    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Cluster,
            resource_name: "kafka-cluster".to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Delete,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    let response = delete_as(&broker, "alice", 5903, &request).await;
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(response.responses[0].topic_id, topic.id);
    assert!(metadata.topic_by_id(topic.id).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_topics_name_existence_is_hidden_until_describe_is_allowed() {
    let (broker, metadata) = acl_broker();
    let existing = metadata.create_topic("private-existing", 1).await.unwrap();
    let missing = "private-missing";
    let request =
        DeleteTopicsRequest::default().with_topics(vec![by_name(&existing.name), by_name(missing)]);

    let response = delete_as(&broker, "alice", 5904, &request).await;
    assert_eq!(response.responses.len(), 2);
    assert!(response.responses.iter().all(|result| {
        result.error_code == TOPIC_AUTHORIZATION_FAILED && result.topic_id.is_nil()
    }));

    for name in [&existing.name, missing] {
        metadata
            .create_acl(topic_rule(
                "User:alice",
                name,
                AclOperation::Describe,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
    let response = delete_as(&broker, "alice", 5905, &request).await;
    let existing_result = by_response_name(&response, &existing.name);
    let missing_result = by_response_name(&response, missing);
    assert_eq!(existing_result.error_code, TOPIC_AUTHORIZATION_FAILED);
    assert_eq!(missing_result.error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert!(metadata.topic_by_id(existing.id).await.unwrap().is_some());
}

#[tokio::test]
async fn delete_topics_alias_conflict_is_only_reported_after_authorization() {
    let (broker, metadata) = acl_broker();
    let topic = metadata.create_topic("private-alias", 1).await.unwrap();
    let request =
        DeleteTopicsRequest::default().with_topics(vec![by_name(&topic.name), by_id(topic.id)]);

    let response = delete_as(&broker, "alice", 5906, &request).await;
    assert_eq!(response.responses.len(), 2);
    assert!(
        response
            .responses
            .iter()
            .all(|result| result.error_code == TOPIC_AUTHORIZATION_FAILED)
    );
    assert!(
        response
            .responses
            .iter()
            .any(|result| { result.name.is_none() && result.topic_id == topic.id })
    );
    assert!(response.responses.iter().any(|result| {
        result.name.as_ref().map(|name| name.as_str()) == Some(topic.name.as_str())
            && result.topic_id.is_nil()
    }));

    metadata
        .create_acl(topic_rule(
            "User:alice",
            &topic.name,
            AclOperation::Delete,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = delete_as(&broker, "alice", 5907, &request).await;
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
    assert_eq!(response.responses[0].topic_id, topic.id);
    assert_eq!(
        response.responses[0].name.as_ref().unwrap().as_str(),
        topic.name
    );
    assert!(metadata.topic_by_id(topic.id).await.unwrap().is_some());
}

async fn delete_as(
    broker: &Broker,
    username: &str,
    correlation_id: i32,
    request: &DeleteTopicsRequest,
) -> DeleteTopicsResponse {
    let response = handle_as(
        broker,
        username,
        ApiKey::DeleteTopics,
        6,
        correlation_id,
        request,
    )
    .await;
    decode_response(ApiKey::DeleteTopics, 6, response)
}

fn by_response_name<'a>(
    response: &'a DeleteTopicsResponse,
    name: &str,
) -> &'a kafka_protocol::messages::delete_topics_response::DeletableTopicResult {
    response
        .responses
        .iter()
        .find(|result| result.name.as_ref().map(|value| value.as_str()) == Some(name))
        .unwrap()
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
