use super::acl_tests::{acl_broker, decode_response as decode_acl_response, handle_as};
use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{INVALID_TOPIC_EXCEPTION, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse};
use rutomq_control::{AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule};
use std::collections::HashSet;

async fn metadata(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    request: &MetadataRequest,
) -> MetadataResponse {
    let frame = broker
        .handle_request(request_frame(
            ApiKey::Metadata,
            version,
            correlation_id,
            request,
        ))
        .await
        .unwrap();
    decode_response(ApiKey::Metadata, version, frame)
}

fn named_topic(name: &str) -> MetadataRequestTopic {
    MetadataRequestTopic::default().with_name(Some(topic_name(name)))
}

fn acl_rule(
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
    permission: AclPermission,
) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: "User:alice".to_owned(),
        host: "*".to_owned(),
        operation,
        permission,
    }
}

#[tokio::test]
async fn metadata_v0_empty_topics_means_all_but_later_empty_lists_mean_none() {
    let broker = broker();
    broker.metadata.create_topic("metadata-a", 1).await.unwrap();
    broker.metadata.create_topic("metadata-b", 1).await.unwrap();
    let request = MetadataRequest::default().with_topics(Some(Vec::new()));

    let response = metadata(&broker, 0, 1, &request).await;
    let names = response
        .topics
        .iter()
        .map(|topic| topic.name.as_ref().unwrap().as_str())
        .collect::<HashSet<_>>();
    assert_eq!(names, HashSet::from(["metadata-a", "metadata-b"]));

    let response = metadata(&broker, 1, 2, &request).await;
    assert!(response.topics.is_empty());
    assert_eq!(response.brokers.len(), 1);
}

#[tokio::test]
async fn metadata_v10_v11_reject_topic_ids_and_null_names_request_wide() {
    let broker = broker();
    let unknown_id = Uuid::new_v4();
    for version in [10, 11] {
        let request = MetadataRequest::default().with_topics(Some(vec![
            named_topic(&format!("must-not-create-{version}")),
            named_topic("id-is-invalid").with_topic_id(unknown_id),
        ]));
        let response = metadata(&broker, version, version as i32, &request).await;
        assert_eq!(response.topics.len(), 2);
        assert!(
            response
                .topics
                .iter()
                .all(|topic| topic.error_code == INVALID_REQUEST)
        );
        assert!(response.brokers.is_empty());
        assert!(
            broker
                .metadata
                .topic(&format!("must-not-create-{version}"))
                .await
                .unwrap()
                .is_none()
        );
    }

    let request = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(None)]));
    let response = metadata(&broker, 11, 20, &request).await;
    assert_eq!(response.topics[0].error_code, INVALID_REQUEST);
    assert_eq!(response.topics[0].name.as_ref().unwrap().as_str(), "");
}

#[tokio::test]
async fn metadata_v12_topic_ids_take_precedence_and_hide_unknown_names() {
    let broker = broker();
    let known = broker.metadata.create_topic("known-id", 2).await.unwrap();
    let unknown_id = Uuid::new_v4();
    let request = MetadataRequest::default()
        .with_topics(Some(vec![
            named_topic("misleading-name").with_topic_id(known.id),
            named_topic("also-misleading").with_topic_id(unknown_id),
            named_topic("ignored-zero-id"),
        ]))
        .with_allow_auto_topic_creation(true);

    let response = metadata(&broker, 12, 30, &request).await;
    assert_eq!(response.topics.len(), 2);
    let known_response = response
        .topics
        .iter()
        .find(|topic| topic.topic_id == known.id)
        .unwrap();
    assert_eq!(known_response.error_code, NO_ERROR);
    assert_eq!(known_response.name.as_ref().unwrap().as_str(), "known-id");
    let unknown_response = response
        .topics
        .iter()
        .find(|topic| topic.topic_id == unknown_id)
        .unwrap();
    assert_eq!(unknown_response.error_code, UNKNOWN_TOPIC_ID);
    assert!(unknown_response.name.is_none());
    for ignored in ["misleading-name", "also-misleading", "ignored-zero-id"] {
        assert!(broker.metadata.topic(ignored).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn metadata_authorizes_before_auto_creation_and_validates_names() {
    let (broker, metadata_store) = acl_broker();
    for rule in [
        acl_rule(
            AclResourceType::Topic,
            "describe-denied",
            AclOperation::Describe,
            AclPermission::Deny,
        ),
        acl_rule(
            AclResourceType::Topic,
            "describe-denied",
            AclOperation::Create,
            AclPermission::Allow,
        ),
        acl_rule(
            AclResourceType::Topic,
            "create-denied",
            AclOperation::Describe,
            AclPermission::Allow,
        ),
        acl_rule(
            AclResourceType::Topic,
            "create-denied",
            AclOperation::Create,
            AclPermission::Deny,
        ),
    ] {
        metadata_store.create_acl(rule).await.unwrap();
    }

    for (correlation_id, name) in [(40, "describe-denied"), (41, "create-denied")] {
        let request = MetadataRequest::default().with_topics(Some(vec![named_topic(name)]));
        let frame = handle_as(
            &broker,
            "alice",
            ApiKey::Metadata,
            13,
            correlation_id,
            &request,
        )
        .await;
        let response: MetadataResponse = decode_acl_response(ApiKey::Metadata, 13, frame);
        assert_eq!(response.topics[0].error_code, TOPIC_AUTHORIZATION_FAILED);
        assert!(metadata_store.topic(name).await.unwrap().is_none());
    }

    let invalid = MetadataRequest::default().with_topics(Some(vec![named_topic("bad/name")]));
    let frame = handle_as(&broker, "admin", ApiKey::Metadata, 13, 42, &invalid).await;
    let response: MetadataResponse = decode_acl_response(ApiKey::Metadata, 13, frame);
    assert_eq!(response.topics[0].error_code, INVALID_TOPIC_EXCEPTION);
    assert!(metadata_store.topic("bad/name").await.unwrap().is_none());

    let valid = MetadataRequest::default().with_topics(Some(vec![named_topic("auto-created")]));
    let frame = handle_as(&broker, "admin", ApiKey::Metadata, 13, 43, &valid).await;
    let response: MetadataResponse = decode_acl_response(ApiKey::Metadata, 13, frame);
    assert_eq!(response.topics[0].error_code, NO_ERROR);
    assert!(
        metadata_store
            .topic("auto-created")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn metadata_auto_creation_uses_the_controller_default_and_static_switch() {
    let mut enabled = broker();
    enabled.config.num_partitions = 3;
    let request = MetadataRequest::default()
        .with_topics(Some(vec![named_topic("configured-auto-created")]))
        .with_allow_auto_topic_creation(true);
    let response = metadata(&enabled, 13, 44, &request).await;
    assert_eq!(response.topics[0].error_code, NO_ERROR);
    assert_eq!(response.topics[0].partitions.len(), 3);
    assert_eq!(
        enabled
            .metadata
            .topic("configured-auto-created")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        3
    );

    let mut disabled = broker();
    disabled.config.auto_create_topics_enable = false;
    let request = MetadataRequest::default()
        .with_topics(Some(vec![named_topic("disabled-auto-create")]))
        .with_allow_auto_topic_creation(true);
    let response = metadata(&disabled, 13, 45, &request).await;
    assert_eq!(response.topics[0].error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert!(
        disabled
            .metadata
            .topic("disabled-auto-create")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn metadata_populates_requested_topic_and_cluster_authorized_operations() {
    let (broker, metadata_store) = acl_broker();
    metadata_store
        .create_topic("operation-bits", 1)
        .await
        .unwrap();
    for rule in [
        acl_rule(
            AclResourceType::Topic,
            "operation-bits",
            AclOperation::Read,
            AclPermission::Allow,
        ),
        acl_rule(
            AclResourceType::Cluster,
            authorization::CLUSTER_RESOURCE_NAME,
            AclOperation::Describe,
            AclPermission::Allow,
        ),
    ] {
        metadata_store.create_acl(rule).await.unwrap();
    }
    let request = MetadataRequest::default()
        .with_topics(Some(vec![named_topic("operation-bits")]))
        .with_include_cluster_authorized_operations(true)
        .with_include_topic_authorized_operations(true);
    let frame = handle_as(&broker, "alice", ApiKey::Metadata, 10, 50, &request).await;
    let response: MetadataResponse = decode_acl_response(ApiKey::Metadata, 10, frame);

    let read_and_describe =
        (1_i32 << AclOperation::Read.code()) | (1_i32 << AclOperation::Describe.code());
    assert_eq!(
        response.topics[0].topic_authorized_operations,
        read_and_describe
    );
    assert_eq!(
        response.cluster_authorized_operations,
        1_i32 << AclOperation::Describe.code()
    );

    let empty = MetadataRequest::default()
        .with_topics(Some(Vec::new()))
        .with_include_cluster_authorized_operations(true);
    let frame = handle_as(&broker, "bob", ApiKey::Metadata, 10, 51, &empty).await;
    let response: MetadataResponse = decode_acl_response(ApiKey::Metadata, 10, frame);
    assert_eq!(response.cluster_authorized_operations, 0);
}
