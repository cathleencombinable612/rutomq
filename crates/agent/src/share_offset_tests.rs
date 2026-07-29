use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, NON_EMPTY_GROUP, TOPIC_AUTHORIZATION_FAILED,
};
use kafka_protocol::messages::alter_share_group_offsets_request::{
    AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic;
use kafka_protocol::messages::describe_share_group_offsets_request::{
    DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse, DeleteGroupsRequest,
    DeleteGroupsResponse, DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse,
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse, GroupId,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse,
};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn alter(group: &str, offsets: &[(i32, i64)]) -> AlterShareGroupOffsetsRequest {
    AlterShareGroupOffsetsRequest::default()
        .with_group_id(group_id(group))
        .with_topics(vec![
            AlterShareGroupOffsetsRequestTopic::default()
                .with_topic_name(topic_name("share-admin"))
                .with_partitions(
                    offsets
                        .iter()
                        .map(|(partition, offset)| {
                            AlterShareGroupOffsetsRequestPartition::default()
                                .with_partition_index(*partition)
                                .with_start_offset(*offset)
                        })
                        .collect(),
                ),
        ])
}

fn describe(group: &str, partitions: Option<Vec<i32>>) -> DescribeShareGroupOffsetsRequest {
    DescribeShareGroupOffsetsRequest::default().with_groups(vec![
        DescribeShareGroupOffsetsRequestGroup::default()
            .with_group_id(group_id(group))
            .with_topics(partitions.map(|partitions| {
                vec![
                    DescribeShareGroupOffsetsRequestTopic::default()
                        .with_topic_name(topic_name("share-admin"))
                        .with_partitions(partitions),
                ]
            })),
    ])
}

#[tokio::test]
async fn share_offset_admin_apis_support_partial_results_and_empty_group_rules() {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic("share-admin", 2)
        .await
        .unwrap();

    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterShareGroupOffsets,
            0,
            600,
            &alter("share-offsets", &[(0, 0), (9, 0)]),
        ))
        .await
        .unwrap();
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.responses[0].topic_id, topic.id);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(
        response.responses[0].partitions[1].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            0,
            601,
            &describe("share-offsets", Some(vec![0, 1, 9])),
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 0, response);
    let partitions = &response.groups[0].topics[0].partitions;
    assert_eq!(response.groups[0].error_code, NO_ERROR);
    assert_eq!(
        (partitions[0].start_offset, partitions[0].error_code),
        (0, 0)
    );
    assert_eq!(
        (partitions[1].start_offset, partitions[1].error_code),
        (-1, 0)
    );
    assert_eq!(partitions[2].error_code, UNKNOWN_TOPIC_OR_PARTITION);

    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            0,
            602,
            &describe("share-offsets", None),
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 0, response);
    assert_eq!(response.groups[0].topics[0].partitions.len(), 1);
    assert_eq!(
        response.groups[0].topics[0].partitions[0].partition_index,
        0
    );

    let join = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("share-offsets"))
        .with_member_id(StrBytes::from_static_str("member"))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name("share-admin")]));
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 603, &join))
        .await
        .unwrap();
    let joined: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(joined.error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterShareGroupOffsets,
            0,
            604,
            &alter("share-offsets", &[(1, 0)]),
        ))
        .await
        .unwrap();
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, NON_EMPTY_GROUP);

    let leave = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("share-offsets"))
        .with_member_id(StrBytes::from_static_str("member"))
        .with_member_epoch(-1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 605, &leave))
        .await
        .unwrap();
    let left: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(left.error_code, NO_ERROR);

    let delete = DeleteShareGroupOffsetsRequest::default()
        .with_group_id(group_id("share-offsets"))
        .with_topics(vec![
            DeleteShareGroupOffsetsRequestTopic::default()
                .with_topic_name(topic_name("share-admin")),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DeleteShareGroupOffsets,
            0,
            606,
            &delete,
        ))
        .await
        .unwrap();
    let response: DeleteShareGroupOffsetsResponse =
        decode_response(ApiKey::DeleteShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.responses[0].error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            0,
            607,
            &describe("share-offsets", None),
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 0, response);
    assert!(response.groups[0].topics.is_empty());

    let delete_group =
        DeleteGroupsRequest::default().with_groups_names(vec![group_id("share-offsets")]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, 608, &delete_group))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            0,
            609,
            &describe("share-offsets", None),
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 0, response);
    assert_eq!(response.groups[0].error_code, GROUP_ID_NOT_FOUND);
}

#[tokio::test]
async fn share_offset_admin_uses_group_and_topic_acls() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("share-admin", 1).await.unwrap();
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );

    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterShareGroupOffsets,
            0,
            610,
            &alter("secured-share", &[(0, 0)]),
        ))
        .await
        .unwrap();
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, GROUP_AUTHORIZATION_FAILED);

    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Group,
            resource_name: "secured-share".to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:ANONYMOUS".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterShareGroupOffsets,
            0,
            611,
            &alter("secured-share", &[(0, 0)]),
        ))
        .await
        .unwrap();
    let response: AlterShareGroupOffsetsResponse =
        decode_response(ApiKey::AlterShareGroupOffsets, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
}
