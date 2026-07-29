use super::acl_tests::{acl_broker, decode_response, handle_as, topic_rule};
use super::*;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
};
use kafka_protocol::messages::share_acknowledge_request::{
    AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch,
};
use kafka_protocol::messages::share_fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    GroupId, ShareAcknowledgeRequest, ShareAcknowledgeResponse, ShareFetchRequest,
    ShareFetchResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
    ShareGroupHeartbeat, TopicInfo,
};

const USERNAME: &str = "share-reader";
const MEMBER_ID: &str = "share-member";

#[tokio::test]
async fn share_group_authorization_backend_failures_are_top_level_server_errors() {
    let (broker, metadata) = acl_broker();
    metadata.set_authorization_failure(true);

    let fetch = share_fetch("share-group-failure", 0);
    let response = handle_as(&broker, USERNAME, ApiKey::ShareFetch, 2, 8001, &fetch).await;
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());

    let acknowledge = share_acknowledge("share-group-failure", 0, uuid::Uuid::nil());
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ShareAcknowledge,
        2,
        8002,
        &acknowledge,
    )
    .await;
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());
}

#[tokio::test]
async fn share_topic_authorization_backend_failures_are_top_level_and_empty() {
    let (broker, metadata, _) = seeded_share_session("fetch-topic-failure", true).await;
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let fetch = share_fetch("fetch-topic-failure", 1);
    let response = handle_as(&broker, USERNAME, ApiKey::ShareFetch, 2, 8011, &fetch).await;
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());

    let (broker, metadata, topic) = seeded_share_session("ack-topic-failure", true).await;
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let acknowledge = share_acknowledge("ack-topic-failure", 1, topic.id);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ShareAcknowledge,
        2,
        8012,
        &acknowledge,
    )
    .await;
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());
}

#[tokio::test]
async fn explicit_share_denials_keep_group_and_partition_error_shapes() {
    let (broker, _, topic) = seeded_share_session("topic-denial", false).await;
    let fetch = share_fetch("topic-denial", 1);
    let response = handle_as(&broker, USERNAME, ApiKey::ShareFetch, 2, 8021, &fetch).await;
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );

    let acknowledge = share_acknowledge("topic-denial", 2, topic.id);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ShareAcknowledge,
        2,
        8022,
        &acknowledge,
    )
    .await;
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );

    let (broker, _) = acl_broker();
    let fetch = share_fetch("group-denial", 0);
    let response = handle_as(&broker, USERNAME, ApiKey::ShareFetch, 2, 8023, &fetch).await;
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, GROUP_AUTHORIZATION_FAILED);
}

async fn seeded_share_session(
    group: &str,
    allow_topic: bool,
) -> (Broker, Arc<MemoryMetadataStore>, TopicInfo) {
    let (broker, metadata) = acl_broker();
    let topic = metadata
        .create_topic(&format!("{group}-topic"), 1)
        .await
        .unwrap();
    metadata.create_acl(group_read_rule(group)).await.unwrap();
    if allow_topic {
        metadata
            .create_acl(topic_rule(
                &format!("User:{USERNAME}"),
                &topic.name,
                AclOperation::Read,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
    metadata
        .share_group_heartbeat(ShareGroupHeartbeat {
            group_id: group.to_owned(),
            member_id: MEMBER_ID.to_owned(),
            member_epoch: 0,
            rack_id: None,
            subscribed_topic_names: Some(vec![topic.name.clone()]),
            client_id: "share-authorization-test".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            assignment_interval_ms: 0,
            max_size: 200,
        })
        .await
        .unwrap();
    let initial = share_fetch(group, 0).with_topics(vec![
        FetchTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(vec![FetchPartition::default().with_partition_index(0)]),
    ]);
    let response = handle_as(&broker, USERNAME, ApiKey::ShareFetch, 2, 8031, &initial).await;
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, NO_ERROR);

    (broker, metadata, topic)
}

fn share_fetch(group: &str, epoch: i32) -> ShareFetchRequest {
    ShareFetchRequest::default()
        .with_group_id(Some(group_id(group)))
        .with_member_id(Some(string(MEMBER_ID)))
        .with_share_session_epoch(epoch)
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_max_records(10)
        .with_batch_size(10)
}

fn share_acknowledge(group: &str, epoch: i32, topic_id: uuid::Uuid) -> ShareAcknowledgeRequest {
    ShareAcknowledgeRequest::default()
        .with_group_id(Some(group_id(group)))
        .with_member_id(Some(string(MEMBER_ID)))
        .with_share_session_epoch(epoch)
        .with_topics(vec![
            AcknowledgeTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    AcknowledgePartition::default()
                        .with_partition_index(0)
                        .with_acknowledgement_batches(vec![
                            AcknowledgementBatch::default()
                                .with_first_offset(0)
                                .with_last_offset(0)
                                .with_acknowledge_types(vec![1]),
                        ]),
                ]),
        ])
}

fn group_read_rule(group: &str) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Group,
        resource_name: group.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: format!("User:{USERNAME}"),
        host: "*".to_owned(),
        operation: AclOperation::Read,
        permission: AclPermission::Allow,
    }
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
