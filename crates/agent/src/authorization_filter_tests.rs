use super::acl_tests::{decode_response, handle_as};
use super::authorization::CLUSTER_RESOURCE_NAME;
use super::*;
use crate::kafka_error::{NO_ERROR, UNKNOWN_SERVER_ERROR};
use kafka_protocol::messages::describe_topic_partitions_request::TopicRequest;
use kafka_protocol::messages::{
    ApiKey, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
    DescribeTransactionsRequest, DescribeTransactionsResponse, ListGroupsRequest,
    ListGroupsResponse, ListTransactionsRequest, ListTransactionsResponse, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclPatternType, AclPermission, AclRule, MemoryMetadataStore, OffsetCommit,
    PostgresMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;

struct VisibilityScenario {
    username: String,
    visible_transaction: String,
    hidden_transaction: String,
    visible_topic: String,
    hidden_topic: String,
    visible_group: String,
    hidden_group: String,
}

#[tokio::test]
async fn list_and_describe_filter_resources_without_cluster_access() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let scenario = seed_scenario(metadata.as_ref(), "memory").await;
    assert_visibility(metadata, &scenario).await;
}

#[tokio::test]
async fn postgres_acl_filters_are_visible_to_a_fresh_broker() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let setup = PostgresMetadataStore::connect(&database_url).await.unwrap();
    setup.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let scenario = seed_scenario(&setup, &suffix).await;

    let fresh: Arc<dyn MetadataStore> =
        Arc::new(PostgresMetadataStore::connect(&database_url).await.unwrap());
    assert_visibility(fresh, &scenario).await;
}

#[tokio::test]
async fn authorization_backend_errors_return_no_resource_names() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let scenario = seed_scenario(metadata.as_ref(), "authorization-error").await;
    let broker = secured_broker(metadata.clone());
    metadata.set_authorization_failure(true);

    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListTransactions,
        2,
        10,
        &ListTransactionsRequest::default(),
    )
    .await;
    let response: ListTransactionsResponse = decode_response(ApiKey::ListTransactions, 2, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.transaction_states.is_empty());

    let describe =
        DescribeTransactionsRequest::default().with_transactional_ids(vec![TransactionalId::from(
            StrBytes::from_string(scenario.visible_transaction.clone()),
        )]);
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::DescribeTransactions,
        0,
        11,
        &describe,
    )
    .await;
    let response: DescribeTransactionsResponse =
        decode_response(ApiKey::DescribeTransactions, 0, response);
    assert_eq!(
        response.transaction_states[0].error_code,
        UNKNOWN_SERVER_ERROR
    );
    assert!(response.transaction_states[0].topics.is_empty());

    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListGroups,
        5,
        12,
        &ListGroupsRequest::default(),
    )
    .await;
    let response: ListGroupsResponse = decode_response(ApiKey::ListGroups, 5, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    assert!(response.groups.is_empty());
}

async fn seed_scenario(metadata: &dyn MetadataStore, suffix: &str) -> VisibilityScenario {
    let scenario = VisibilityScenario {
        username: format!("visibility-{suffix}"),
        visible_transaction: format!("visible-tx-{suffix}"),
        hidden_transaction: format!("hidden-tx-{suffix}"),
        visible_topic: format!("visible-topic-{suffix}"),
        hidden_topic: format!("hidden-topic-{suffix}"),
        visible_group: format!("visible-group-{suffix}"),
        hidden_group: format!("hidden-group-{suffix}"),
    };
    metadata
        .create_topic(&scenario.visible_topic, 1)
        .await
        .unwrap();
    metadata
        .create_topic(&scenario.hidden_topic, 1)
        .await
        .unwrap();

    let visible_producer = metadata
        .init_producer(Some(&scenario.visible_transaction), 60_000, None)
        .await
        .unwrap();
    metadata
        .add_partitions_to_transaction(
            &scenario.visible_transaction,
            visible_producer,
            &[
                PartitionKey::new(&scenario.visible_topic, 0),
                PartitionKey::new(&scenario.hidden_topic, 0),
            ],
            false,
        )
        .await
        .unwrap();
    metadata
        .init_producer(Some(&scenario.hidden_transaction), 60_000, None)
        .await
        .unwrap();

    for group in [&scenario.visible_group, &scenario.hidden_group] {
        metadata
            .commit_offsets(
                group,
                vec![OffsetCommit {
                    partition: PartitionKey::new(&scenario.visible_topic, 0),
                    offset: 0,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                }],
            )
            .await
            .unwrap();
    }

    let principal = format!("User:{}", scenario.username);
    for (resource_type, resource_name) in [
        (
            AclResourceType::TransactionalId,
            scenario.visible_transaction.as_str(),
        ),
        (AclResourceType::Topic, scenario.visible_topic.as_str()),
        (AclResourceType::Group, scenario.visible_group.as_str()),
    ] {
        metadata
            .create_acl(allow_describe(&principal, resource_type, resource_name))
            .await
            .unwrap();
    }
    scenario
}

async fn assert_visibility(metadata: Arc<dyn MetadataStore>, scenario: &VisibilityScenario) {
    let broker = secured_broker(metadata.clone());

    let list_transactions = ListTransactionsRequest::default();
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListTransactions,
        2,
        1,
        &list_transactions,
    )
    .await;
    let response: ListTransactionsResponse = decode_response(ApiKey::ListTransactions, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.transaction_states.len(), 1);
    assert_eq!(
        response.transaction_states[0].transactional_id.as_str(),
        scenario.visible_transaction
    );

    let describe =
        DescribeTransactionsRequest::default().with_transactional_ids(vec![TransactionalId::from(
            StrBytes::from_string(scenario.visible_transaction.clone()),
        )]);
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::DescribeTransactions,
        0,
        2,
        &describe,
    )
    .await;
    let response: DescribeTransactionsResponse =
        decode_response(ApiKey::DescribeTransactions, 0, response);
    assert_eq!(response.transaction_states[0].error_code, NO_ERROR);
    assert_eq!(response.transaction_states[0].topics.len(), 1);
    assert_eq!(
        response.transaction_states[0].topics[0].topic.as_str(),
        scenario.visible_topic
    );

    let missing_topic = format!("missing-topic-{}", scenario.username);
    let describe_topics = DescribeTopicPartitionsRequest::default()
        .with_topics(vec![
            topic_request(&scenario.visible_topic),
            topic_request(&scenario.hidden_topic),
            topic_request(&missing_topic),
        ])
        .with_response_partition_limit(10);
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::DescribeTopicPartitions,
        0,
        6,
        &describe_topics,
    )
    .await;
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    let topic_results = response
        .topics
        .iter()
        .map(|topic| {
            (
                topic.name.as_ref().unwrap().as_str(),
                (topic.error_code, topic.topic_id),
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(topic_results[scenario.visible_topic.as_str()].0, NO_ERROR);
    for hidden in [&scenario.hidden_topic, &missing_topic] {
        assert_eq!(
            topic_results[hidden.as_str()],
            (crate::kafka_error::TOPIC_AUTHORIZATION_FAILED, Uuid::nil())
        );
    }

    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::DescribeTopicPartitions,
        0,
        7,
        &DescribeTopicPartitionsRequest::default(),
    )
    .await;
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].name.as_ref().unwrap().as_str(),
        scenario.visible_topic
    );

    let list_groups = ListGroupsRequest::default();
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListGroups,
        5,
        3,
        &list_groups,
    )
    .await;
    let response: ListGroupsResponse = decode_response(ApiKey::ListGroups, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.groups.len(), 1);
    assert_eq!(response.groups[0].group_id.as_str(), scenario.visible_group);

    metadata
        .create_acl(allow_describe(
            &format!("User:{}", scenario.username),
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListGroups,
        5,
        4,
        &list_groups,
    )
    .await;
    let response: ListGroupsResponse = decode_response(ApiKey::ListGroups, 5, response);
    let group_ids = response
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<HashSet<_>>();
    assert!(group_ids.contains(scenario.visible_group.as_str()));
    assert!(group_ids.contains(scenario.hidden_group.as_str()));

    let response = handle_as(
        &broker,
        &scenario.username,
        ApiKey::ListTransactions,
        2,
        5,
        &list_transactions,
    )
    .await;
    let response: ListTransactionsResponse = decode_response(ApiKey::ListTransactions, 2, response);
    assert_eq!(response.transaction_states.len(), 1);
    assert_eq!(
        response.transaction_states[0].transactional_id.as_str(),
        scenario.visible_transaction
    );
    assert_ne!(
        response.transaction_states[0].transactional_id.as_str(),
        scenario.hidden_transaction
    );
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

fn allow_describe(principal: &str, resource_type: AclResourceType, resource_name: &str) -> AclRule {
    AclRule {
        resource_type,
        resource_name: resource_name.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::Describe,
        permission: AclPermission::Allow,
    }
}

fn topic_request(name: &str) -> TopicRequest {
    TopicRequest::default().with_name(topic_name(name))
}
