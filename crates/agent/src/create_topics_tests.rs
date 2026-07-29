use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT, INVALID_REPLICATION_FACTOR, INVALID_REQUEST,
    INVALID_TOPIC_EXCEPTION, POLICY_VIOLATION, TOPIC_ALREADY_EXISTS,
};
use kafka_protocol::messages::create_topics_request::{
    CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
};

#[tokio::test]
async fn create_topics_returns_versioned_effective_configs() {
    let broker = broker();

    for version in 5..=7 {
        let name = format!("response-configs-v{version}");
        let request = CreateTopicsRequest::default().with_topics(vec![
            automatic(&name, 2, 1).with_configs(vec![create_config("retention.ms", "123")]),
        ]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::CreateTopics,
                version,
                100 + i32::from(version),
                &request,
            ))
            .await
            .unwrap();
        let response: CreateTopicsResponse =
            decode_response(ApiKey::CreateTopics, version, response);
        let result = &response.topics[0];

        assert_eq!(result.error_code, NO_ERROR);
        assert_eq!(result.error_message, None);
        assert_eq!(result.num_partitions, 2);
        assert_eq!(result.replication_factor, 1);
        assert_eq!(result.topic_config_error_code, NO_ERROR);
        assert_eq!(result.topic_id.is_nil(), version < 7);

        let configs = result.configs.as_ref().unwrap();
        assert_eq!(configs.len(), 19);
        assert!(
            configs
                .windows(2)
                .all(|pair| pair[0].name.as_str() < pair[1].name.as_str())
        );
        assert!(
            configs
                .iter()
                .all(|config| !config.read_only && !config.is_sensitive)
        );
        let retention = response_config(configs, "retention.ms");
        assert_eq!(retention.value.as_ref().unwrap().as_str(), "123");
        assert_eq!(retention.config_source, 1);
        assert_eq!(response_config(configs, "cleanup.policy").config_source, 5);
    }
}

#[tokio::test]
async fn create_topics_v4_preserves_legacy_response_shape() {
    let broker = broker();
    let request =
        CreateTopicsRequest::default().with_topics(vec![automatic("legacy-response-v4", 2, 1)]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 4, 104, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 4, response);
    let result = &response.topics[0];

    assert_eq!(result.error_code, NO_ERROR);
    assert_eq!(result.error_message, None);
    assert!(result.topic_id.is_nil());
    assert_eq!(result.num_partitions, -1);
    assert_eq!(result.replication_factor, -1);
    assert!(result.configs.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn create_topics_rejects_excessive_partition_batches_atomically() {
    let broker = broker();
    let request = CreateTopicsRequest::default().with_topics(vec![
        automatic("batch-limit-large", 10_000, 1),
        automatic("batch-limit-small", 1, 1),
    ]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 105, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);

    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>(),
        [POLICY_VIOLATION, POLICY_VIOLATION]
    );
    assert_eq!(stored_partitions(&broker, "batch-limit-large").await, None);
    assert_eq!(stored_partitions(&broker, "batch-limit-small").await, None);
}

#[tokio::test]
async fn create_topics_honors_explicit_and_default_topology() {
    let mut broker = broker();
    broker.config.num_partitions = 3;
    let request = CreateTopicsRequest::default().with_topics(vec![
        automatic("explicit-topology", 2, 1),
        automatic("default-topology", -1, -1),
    ]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 70, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);

    assert_eq!(response.topics[0].error_code, NO_ERROR);
    assert_eq!(response.topics[0].num_partitions, 2);
    assert_eq!(response.topics[0].replication_factor, 1);
    assert_eq!(response.topics[1].error_code, NO_ERROR);
    assert_eq!(response.topics[1].num_partitions, 3);
    assert_eq!(response.topics[1].replication_factor, 1);
    assert_eq!(
        stored_partitions(&broker, "explicit-topology").await,
        Some(2)
    );
    assert_eq!(
        stored_partitions(&broker, "default-topology").await,
        Some(3)
    );
}

#[tokio::test]
async fn create_topics_rejects_invalid_automatic_topology() {
    let broker = broker();
    let cases = [
        ("zero-partitions", 0, 1, INVALID_PARTITIONS),
        ("negative-partitions", -2, 1, INVALID_PARTITIONS),
        ("zero-replicas", 1, 0, INVALID_REPLICATION_FACTOR),
        ("negative-replicas", 1, -2, INVALID_REPLICATION_FACTOR),
        ("physical-replicas", 1, 2, INVALID_REPLICATION_FACTOR),
    ];

    for (correlation, (name, partitions, replicas, expected)) in (71..).zip(cases.into_iter()) {
        let request =
            CreateTopicsRequest::default().with_topics(vec![automatic(name, partitions, replicas)]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::CreateTopics,
                7,
                correlation,
                &request,
            ))
            .await
            .unwrap();
        let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
        assert_eq!(response.topics[0].error_code, expected, "{name}");
        assert_eq!(stored_partitions(&broker, name).await, None, "{name}");
    }
}

#[tokio::test]
async fn create_topics_validates_manual_virtual_assignments() {
    let broker = broker();
    let request = CreateTopicsRequest::default().with_topics(vec![
        manual(
            "manual-valid",
            -1,
            -1,
            vec![assignment(0, &[0]), assignment(1, &[0])],
        ),
        manual(
            "manual-count-conflict",
            2,
            -1,
            vec![assignment(0, &[0]), assignment(1, &[0])],
        ),
        manual("manual-factor-conflict", -1, 1, vec![assignment(0, &[0])]),
        manual(
            "manual-duplicate-index",
            -1,
            -1,
            vec![assignment(0, &[0]), assignment(0, &[0])],
        ),
        manual(
            "manual-index-gap",
            -1,
            -1,
            vec![assignment(0, &[0]), assignment(2, &[0])],
        ),
        manual("manual-unknown-broker", -1, -1, vec![assignment(0, &[1])]),
        manual(
            "manual-duplicate-broker",
            -1,
            -1,
            vec![assignment(0, &[0, 0])],
        ),
        manual("manual-empty-replicas", -1, -1, vec![assignment(0, &[])]),
    ]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 80, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    let errors = response
        .topics
        .iter()
        .map(|topic| topic.error_code)
        .collect::<Vec<_>>();
    assert_eq!(
        errors,
        [
            NO_ERROR,
            INVALID_REQUEST,
            INVALID_REQUEST,
            INVALID_REPLICA_ASSIGNMENT,
            INVALID_REPLICA_ASSIGNMENT,
            INVALID_REPLICA_ASSIGNMENT,
            INVALID_REPLICA_ASSIGNMENT,
            INVALID_REPLICA_ASSIGNMENT,
        ]
    );
    assert_eq!(stored_partitions(&broker, "manual-valid").await, Some(2));
    for name in [
        "manual-count-conflict",
        "manual-factor-conflict",
        "manual-duplicate-index",
        "manual-index-gap",
        "manual-unknown-broker",
        "manual-duplicate-broker",
        "manual-empty-replicas",
    ] {
        assert_eq!(stored_partitions(&broker, name).await, None, "{name}");
    }
}

#[tokio::test]
async fn create_topics_validate_only_checks_topology_without_mutation() {
    let broker = broker();
    let request = CreateTopicsRequest::default()
        .with_validate_only(true)
        .with_topics(vec![
            automatic("validate-default", -1, -1),
            manual(
                "validate-manual",
                -1,
                -1,
                vec![assignment(0, &[0]), assignment(1, &[0])],
            ),
        ]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 90, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == NO_ERROR)
    );
    assert_eq!(response.topics[0].num_partitions, 1);
    assert_eq!(response.topics[1].num_partitions, 2);
    assert_eq!(stored_partitions(&broker, "validate-default").await, None);
    assert_eq!(stored_partitions(&broker, "validate-manual").await, None);
}

#[tokio::test]
async fn create_topics_validates_names_existing_topics_and_duplicates() {
    let broker = broker();
    broker
        .metadata
        .create_topic("already-exists", 1)
        .await
        .unwrap();
    broker.metadata.create_topic("metrics_v1", 1).await.unwrap();
    let long_name = "a".repeat(250);
    let request = CreateTopicsRequest::default()
        .with_validate_only(true)
        .with_topics(vec![
            automatic("", 1, 1),
            automatic(".", 1, 1),
            automatic("..", 1, 1),
            automatic("bad/name", 1, 1),
            automatic(&long_name, 1, 1),
            automatic("already-exists", 1, 1),
            automatic("metrics.v1", 1, 1),
            automatic("duplicate", 1, 1),
            automatic("duplicate", 1, 1),
        ]);

    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 91, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>(),
        [
            INVALID_TOPIC_EXCEPTION,
            INVALID_TOPIC_EXCEPTION,
            INVALID_TOPIC_EXCEPTION,
            INVALID_TOPIC_EXCEPTION,
            INVALID_TOPIC_EXCEPTION,
            TOPIC_ALREADY_EXISTS,
            INVALID_TOPIC_EXCEPTION,
            INVALID_REQUEST,
            INVALID_REQUEST,
        ]
    );
    assert_eq!(stored_partitions(&broker, "already-exists").await, Some(1));
    assert_eq!(stored_partitions(&broker, "metrics.v1").await, None);
    assert_eq!(stored_partitions(&broker, "duplicate").await, None);
}

fn automatic(name: &str, partitions: i32, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(topic_name(name))
        .with_num_partitions(partitions)
        .with_replication_factor(replication_factor)
}

fn manual(
    name: &str,
    partitions: i32,
    replication_factor: i16,
    assignments: Vec<CreatableReplicaAssignment>,
) -> CreatableTopic {
    automatic(name, partitions, replication_factor).with_assignments(assignments)
}

fn assignment(partition: i32, brokers: &[i32]) -> CreatableReplicaAssignment {
    CreatableReplicaAssignment::default()
        .with_partition_index(partition)
        .with_broker_ids(brokers.iter().copied().map(BrokerId::from).collect())
}

fn create_config(name: &str, value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(Some(StrBytes::from_string(value.to_owned())))
}

fn response_config<'a>(
    configs: &'a [kafka_protocol::messages::create_topics_response::CreatableTopicConfigs],
    name: &str,
) -> &'a kafka_protocol::messages::create_topics_response::CreatableTopicConfigs {
    configs
        .iter()
        .find(|config| config.name.as_str() == name)
        .unwrap()
}

async fn stored_partitions(broker: &Broker, name: &str) -> Option<i32> {
    broker
        .metadata
        .topic(name)
        .await
        .unwrap()
        .map(|topic| topic.partitions)
}
