use super::acl_tests::{decode_response, handle_as};
use super::*;
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, GROUP_ID_NOT_FOUND, TOPIC_AUTHORIZATION_FAILED,
    UNKNOWN_SERVER_ERROR,
};
use kafka_protocol::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, GroupId, ShareGroupDescribeRequest, ShareGroupDescribeResponse,
    StreamsGroupDescribeRequest, StreamsGroupDescribeResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, ConsumerGroupHeartbeat,
    MemoryMetadataStore, PostgresMetadataStore, ShareGroupHeartbeat,
};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

#[tokio::test]
async fn duplicate_group_descriptions_preserve_every_authorization_result() {
    assert_duplicate_authorization_results(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn postgres_duplicate_group_descriptions_preserve_every_authorization_result() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let metadata = PostgresMetadataStore::connect(&database_url).await.unwrap();
    metadata.migrate().await.unwrap();
    assert_duplicate_authorization_results(
        Arc::new(metadata),
        &Uuid::new_v4().simple().to_string(),
    )
    .await;
}

#[tokio::test]
async fn authorization_backend_failures_are_not_acl_denials() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let broker = secured_broker(metadata.clone());
    metadata.set_authorization_failure(true);
    let ids = vec![group_id("backend-failure"), group_id("backend-failure")];

    let classic = DescribeGroupsRequest::default().with_groups(ids.clone());
    let response = handle_as(&broker, "reader", ApiKey::DescribeGroups, 6, 7701, &classic).await;
    let response: DescribeGroupsResponse = decode_response(ApiKey::DescribeGroups, 6, response);
    assert_all_errors(
        "DescribeGroups",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        UNKNOWN_SERVER_ERROR,
    );

    let consumer = ConsumerGroupDescribeRequest::default().with_group_ids(ids.clone());
    let response = handle_as(
        &broker,
        "reader",
        ApiKey::ConsumerGroupDescribe,
        1,
        7702,
        &consumer,
    )
    .await;
    let response: ConsumerGroupDescribeResponse =
        decode_response(ApiKey::ConsumerGroupDescribe, 1, response);
    assert_all_errors(
        "ConsumerGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        UNKNOWN_SERVER_ERROR,
    );

    let streams = StreamsGroupDescribeRequest::default().with_group_ids(ids.clone());
    let response = handle_as(
        &broker,
        "reader",
        ApiKey::StreamsGroupDescribe,
        0,
        7703,
        &streams,
    )
    .await;
    let response: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_all_errors(
        "StreamsGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        UNKNOWN_SERVER_ERROR,
    );

    let share = ShareGroupDescribeRequest::default().with_group_ids(ids);
    let response = handle_as(
        &broker,
        "reader",
        ApiKey::ShareGroupDescribe,
        1,
        7704,
        &share,
    )
    .await;
    let response: ShareGroupDescribeResponse =
        decode_response(ApiKey::ShareGroupDescribe, 1, response);
    assert_all_errors(
        "ShareGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        UNKNOWN_SERVER_ERROR,
    );
}

#[tokio::test]
async fn consumer_and_share_descriptions_hide_unauthorized_topics() {
    assert_topic_privacy(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn postgres_consumer_and_share_descriptions_hide_unauthorized_topics() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let metadata = PostgresMetadataStore::connect(&database_url).await.unwrap();
    metadata.migrate().await.unwrap();
    assert_topic_privacy(Arc::new(metadata), &Uuid::new_v4().simple().to_string()).await;
}

async fn assert_duplicate_authorization_results(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let username = format!("describe-reader-{suffix}");
    let allowed = format!("describe-allowed-{suffix}");
    let denied = format!("describe-denied-{suffix}");
    metadata
        .create_acl(allow_describe(&username, AclResourceType::Group, &allowed))
        .await
        .unwrap();
    let broker = secured_broker(metadata);
    let ids = vec![
        group_id(&allowed),
        group_id(&denied),
        group_id(&denied),
        group_id(&allowed),
    ];
    let expected = [
        (denied.as_str(), GROUP_AUTHORIZATION_FAILED),
        (denied.as_str(), GROUP_AUTHORIZATION_FAILED),
        (allowed.as_str(), GROUP_ID_NOT_FOUND),
        (allowed.as_str(), GROUP_ID_NOT_FOUND),
    ];

    let classic = DescribeGroupsRequest::default().with_groups(ids.clone());
    let response = handle_as(
        &broker,
        &username,
        ApiKey::DescribeGroups,
        6,
        7711,
        &classic,
    )
    .await;
    let response: DescribeGroupsResponse = decode_response(ApiKey::DescribeGroups, 6, response);
    assert_results(
        "DescribeGroups",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        &expected,
    );

    let consumer = ConsumerGroupDescribeRequest::default().with_group_ids(ids.clone());
    let response = handle_as(
        &broker,
        &username,
        ApiKey::ConsumerGroupDescribe,
        1,
        7712,
        &consumer,
    )
    .await;
    let response: ConsumerGroupDescribeResponse =
        decode_response(ApiKey::ConsumerGroupDescribe, 1, response);
    assert_results(
        "ConsumerGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        &expected,
    );

    let streams = StreamsGroupDescribeRequest::default().with_group_ids(ids.clone());
    let response = handle_as(
        &broker,
        &username,
        ApiKey::StreamsGroupDescribe,
        0,
        7713,
        &streams,
    )
    .await;
    let response: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_results(
        "StreamsGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        &expected,
    );

    let share = ShareGroupDescribeRequest::default().with_group_ids(ids);
    let response = handle_as(
        &broker,
        &username,
        ApiKey::ShareGroupDescribe,
        1,
        7714,
        &share,
    )
    .await;
    let response: ShareGroupDescribeResponse =
        decode_response(ApiKey::ShareGroupDescribe, 1, response);
    assert_results(
        "ShareGroupDescribe",
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code)),
        &expected,
    );
}

async fn assert_topic_privacy(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let username = format!("topic-reader-{suffix}");
    let visible_topic = format!("describe-visible-{suffix}");
    let hidden_topic = format!("describe-hidden-{suffix}");
    let consumer_group = format!("consumer-describe-{suffix}");
    let share_group = format!("share-describe-{suffix}");
    metadata.create_topic(&visible_topic, 1).await.unwrap();
    metadata.create_topic(&hidden_topic, 1).await.unwrap();
    metadata
        .consumer_group_heartbeat(ConsumerGroupHeartbeat {
            group_id: consumer_group.clone(),
            member_id: "consumer-member".to_owned(),
            member_epoch: 0,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: Some(vec![visible_topic.clone(), hidden_topic.clone()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: Some(Vec::new()),
            client_id: "privacy-test".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        })
        .await
        .unwrap();
    metadata
        .share_group_heartbeat(ShareGroupHeartbeat {
            group_id: share_group.clone(),
            member_id: "share-member".to_owned(),
            member_epoch: 0,
            rack_id: None,
            subscribed_topic_names: Some(vec![visible_topic.clone(), hidden_topic]),
            client_id: "privacy-test".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            assignment_interval_ms: 0,
            max_size: 200,
        })
        .await
        .unwrap();
    for group in [&consumer_group, &share_group] {
        metadata
            .create_acl(allow_describe(&username, AclResourceType::Group, group))
            .await
            .unwrap();
    }
    metadata
        .create_acl(allow_describe(
            &username,
            AclResourceType::Topic,
            &visible_topic,
        ))
        .await
        .unwrap();
    let broker = secured_broker(metadata);

    let request =
        ConsumerGroupDescribeRequest::default().with_group_ids(vec![group_id(&consumer_group)]);
    let response = handle_as(
        &broker,
        &username,
        ApiKey::ConsumerGroupDescribe,
        1,
        7721,
        &request,
    )
    .await;
    let response: ConsumerGroupDescribeResponse =
        decode_response(ApiKey::ConsumerGroupDescribe, 1, response);
    assert_eq!(response.groups[0].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert!(response.groups[0].members.is_empty());

    let request = ShareGroupDescribeRequest::default().with_group_ids(vec![group_id(&share_group)]);
    let response = handle_as(
        &broker,
        &username,
        ApiKey::ShareGroupDescribe,
        1,
        7722,
        &request,
    )
    .await;
    let response: ShareGroupDescribeResponse =
        decode_response(ApiKey::ShareGroupDescribe, 1, response);
    assert_eq!(response.groups[0].error_code, TOPIC_AUTHORIZATION_FAILED);
    assert!(response.groups[0].members.is_empty());
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

fn allow_describe(username: &str, resource_type: AclResourceType, resource_name: &str) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: format!("User:{username}"),
        host: "*".to_owned(),
        operation: AclOperation::Describe,
        permission: AclPermission::Allow,
    }
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn assert_all_errors<'a>(
    label: &str,
    actual: impl IntoIterator<Item = (&'a str, i16)>,
    error_code: i16,
) {
    let actual = actual.into_iter().collect::<Vec<_>>();
    assert_eq!(actual.len(), 2, "{label}: {actual:?}");
    assert!(
        actual.iter().all(|(_, code)| *code == error_code),
        "{label}: {actual:?}"
    );
}

fn assert_results<'a>(
    label: &str,
    actual: impl IntoIterator<Item = (&'a str, i16)>,
    expected: &[(&str, i16)],
) {
    let actual = actual.into_iter().collect::<Vec<_>>();
    assert_eq!(actual, expected, "{label}");
}
