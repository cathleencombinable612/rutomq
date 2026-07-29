use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::*;
use crate::kafka_error::{NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR};
use kafka_protocol::messages::alter_share_group_offsets_request::{
    AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic;
use kafka_protocol::messages::describe_share_group_offsets_request::{
    DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse, DeleteShareGroupOffsetsRequest,
    DeleteShareGroupOffsetsResponse, DescribeShareGroupOffsetsRequest,
    DescribeShareGroupOffsetsResponse, GroupId,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
    MetadataStore, PartitionKey, PostgresMetadataStore, ShareOffsetUpdate,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

const USERNAME: &str = "share-offset-reader";

#[tokio::test]
async fn share_offset_authorization_backend_failures_use_request_error_shapes() {
    let (broker, metadata) = acl_broker();
    metadata
        .create_topic("share-offset-failure-topic", 1)
        .await
        .unwrap();
    metadata.set_authorization_failure(true);

    let request = DescribeShareGroupOffsetsRequest::default().with_groups(vec![
        describe_group("failure-a", None),
        describe_group("failure-b", None),
    ]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeShareGroupOffsets,
        1,
        8301,
        &request,
    )
    .await;
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    assert_eq!(response.groups.len(), 2);
    assert!(
        response
            .groups
            .iter()
            .all(|group| group.error_code == UNKNOWN_SERVER_ERROR && group.topics.is_empty())
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::AlterShareGroupOffsets,
        0,
        8302,
        &alter("failure-a", &[("share-offset-failure-topic", 0, 0)]),
    )
    .await;
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DeleteShareGroupOffsets,
        0,
        8303,
        &delete("failure-a", &["share-offset-failure-topic"]),
    )
    .await;
    let response: DeleteShareGroupOffsetsResponse =
        decode_response(ApiKey::DeleteShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());

    metadata.set_authorization_failure(false);
    for (group, operation) in [
        ("failure-a", AclOperation::Describe),
        ("failure-b", AclOperation::Describe),
        ("failure-a", AclOperation::Read),
        ("failure-a", AclOperation::Delete),
    ] {
        metadata
            .create_acl(allow_rule(AclResourceType::Group, group, operation))
            .await
            .unwrap();
    }
    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));

    let request = DescribeShareGroupOffsetsRequest::default().with_groups(vec![
        describe_group(
            "failure-a",
            Some(vec![describe_topic("share-offset-failure-topic", &[0])]),
        ),
        describe_group(
            "failure-b",
            Some(vec![describe_topic("share-offset-failure-topic", &[0])]),
        ),
    ]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeShareGroupOffsets,
        1,
        8304,
        &request,
    )
    .await;
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    assert!(
        response
            .groups
            .iter()
            .all(|group| group.error_code == UNKNOWN_SERVER_ERROR && group.topics.is_empty())
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::AlterShareGroupOffsets,
        0,
        8305,
        &alter("failure-a", &[("share-offset-failure-topic", 0, 0)]),
    )
    .await;
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DeleteShareGroupOffsets,
        0,
        8306,
        &delete("failure-a", &["share-offset-failure-topic"]),
    )
    .await;
    let response: DeleteShareGroupOffsetsResponse =
        decode_response(ApiKey::DeleteShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.responses.is_empty());
}

#[tokio::test]
async fn all_offset_describe_hides_denied_topics_and_empty_topics_succeed() {
    assert_describe_privacy(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn postgres_all_offset_describe_hides_denied_topics_and_empty_topics_succeed() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let metadata = PostgresMetadataStore::connect(&database_url).await.unwrap();
    metadata.migrate().await.unwrap();
    assert_describe_privacy(Arc::new(metadata), &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn denied_alter_identity_and_delete_order_match_kafka() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let allowed = metadata
        .create_topic("share-offset-allowed", 1)
        .await
        .unwrap();
    let denied = metadata
        .create_topic("share-offset-denied", 1)
        .await
        .unwrap();
    let group = "share-offset-mutation";
    seed_offsets(
        metadata.as_ref(),
        group,
        &[(&allowed.name, 0, 0), (&denied.name, 0, 0)],
    )
    .await;
    for operation in [AclOperation::Read, AclOperation::Delete] {
        metadata
            .create_acl(allow_rule(AclResourceType::Group, group, operation))
            .await
            .unwrap();
    }
    metadata
        .create_acl(allow_rule(
            AclResourceType::Topic,
            &allowed.name,
            AclOperation::Read,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::AlterShareGroupOffsets,
        0,
        8321,
        &alter(group, &[(&allowed.name, 0, 1), (&denied.name, 0, 1)]),
    )
    .await;
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    let denied_response = response
        .responses
        .iter()
        .find(|topic| topic.topic_name.as_str() == denied.name)
        .unwrap();
    assert_eq!(denied_response.topic_id, denied.id);
    assert_eq!(
        denied_response.partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DeleteShareGroupOffsets,
        0,
        8322,
        &delete(group, &[&allowed.name, &denied.name]),
    )
    .await;
    let response: DeleteShareGroupOffsetsResponse =
        decode_response(ApiKey::DeleteShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.responses[0].topic_name.as_str(), denied.name);
    assert_eq!(response.responses[0].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert_eq!(response.responses[1].topic_name.as_str(), allowed.name);
    assert_eq!(response.responses[1].error_code, NO_ERROR);
}

async fn assert_describe_privacy(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let group = format!("share-offset-private-{suffix}");
    let empty_group = format!("share-offset-empty-{suffix}");
    let visible = metadata
        .create_topic(&format!("share-offset-visible-{suffix}"), 1)
        .await
        .unwrap();
    let hidden = metadata
        .create_topic(&format!("share-offset-hidden-{suffix}"), 1)
        .await
        .unwrap();
    seed_offsets(
        metadata.as_ref(),
        &group,
        &[(&visible.name, 0, 4), (&hidden.name, 0, 7)],
    )
    .await;
    for group_name in [&group, &empty_group] {
        metadata
            .create_acl(allow_rule(
                AclResourceType::Group,
                group_name,
                AclOperation::Describe,
            ))
            .await
            .unwrap();
    }
    metadata
        .create_acl(allow_rule(
            AclResourceType::Topic,
            &visible.name,
            AclOperation::Describe,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);

    let request =
        DescribeShareGroupOffsetsRequest::default().with_groups(vec![describe_group(&group, None)]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeShareGroupOffsets,
        1,
        8311,
        &request,
    )
    .await;
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    assert_eq!(response.groups[0].error_code, NO_ERROR);
    assert_eq!(response.groups[0].topics.len(), 1);
    assert_eq!(
        response.groups[0].topics[0].topic_name.as_str(),
        visible.name
    );

    let request = DescribeShareGroupOffsetsRequest::default().with_groups(vec![describe_group(
        &group,
        Some(vec![
            describe_topic(&hidden.name, &[0]),
            describe_topic(&visible.name, &[0]),
        ]),
    )]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeShareGroupOffsets,
        1,
        8312,
        &request,
    )
    .await;
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    assert_eq!(
        response.groups[0].topics[0].topic_name.as_str(),
        visible.name
    );
    assert_eq!(
        response.groups[0].topics[1].topic_name.as_str(),
        hidden.name
    );
    assert_eq!(
        response.groups[0].topics[1].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );

    let request = DescribeShareGroupOffsetsRequest::default()
        .with_groups(vec![describe_group(&empty_group, Some(Vec::new()))]);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::DescribeShareGroupOffsets,
        1,
        8313,
        &request,
    )
    .await;
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    assert_eq!(response.groups[0].error_code, NO_ERROR);
    assert!(response.groups[0].topics.is_empty());
}

async fn seed_offsets(metadata: &dyn MetadataStore, group: &str, offsets: &[(&str, i32, i64)]) {
    metadata
        .alter_share_group_offsets(
            group,
            &offsets
                .iter()
                .map(|(topic, partition, offset)| ShareOffsetUpdate {
                    partition: PartitionKey::new(*topic, *partition),
                    start_offset: *offset,
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
}

fn secured_broker(metadata: Arc<dyn MetadataStore>) -> Broker {
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn allow_rule(
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: format!("User:{USERNAME}"),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}

fn describe_group(
    group: &str,
    topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
) -> DescribeShareGroupOffsetsRequestGroup {
    DescribeShareGroupOffsetsRequestGroup::default()
        .with_group_id(group_id(group))
        .with_topics(topics)
}

fn describe_topic(name: &str, partitions: &[i32]) -> DescribeShareGroupOffsetsRequestTopic {
    DescribeShareGroupOffsetsRequestTopic::default()
        .with_topic_name(topic_name(name))
        .with_partitions(partitions.to_vec())
}

fn alter(group: &str, offsets: &[(&str, i32, i64)]) -> AlterShareGroupOffsetsRequest {
    let mut topics = Vec::new();
    for (name, partition, offset) in offsets {
        topics.push(
            AlterShareGroupOffsetsRequestTopic::default()
                .with_topic_name(topic_name(name))
                .with_partitions(vec![
                    AlterShareGroupOffsetsRequestPartition::default()
                        .with_partition_index(*partition)
                        .with_start_offset(*offset),
                ]),
        );
    }
    AlterShareGroupOffsetsRequest::default()
        .with_group_id(group_id(group))
        .with_topics(topics)
}

fn delete(group: &str, topics: &[&str]) -> DeleteShareGroupOffsetsRequest {
    DeleteShareGroupOffsetsRequest::default()
        .with_group_id(group_id(group))
        .with_topics(
            topics
                .iter()
                .map(|name| {
                    DeleteShareGroupOffsetsRequestTopic::default().with_topic_name(topic_name(name))
                })
                .collect(),
        )
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}
