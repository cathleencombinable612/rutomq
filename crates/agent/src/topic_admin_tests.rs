use super::*;
use crate::kafka_error::{INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT, INVALID_REQUEST};
use bytes::Buf;
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsTopic,
};
use kafka_protocol::messages::describe_topic_partitions_request::{
    Cursor as RequestCursor, TopicRequest,
};
use kafka_protocol::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn test_broker() -> Broker {
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    )
}

fn request_frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(73)
        .with_client_id(Some(StrBytes::from_string("topic-admin-test".to_owned())))
        .encode(&mut payload, api_key.request_header_version(version))
        .unwrap();
    body.encode(&mut payload, version).unwrap();
    payload.freeze()
}

fn decode_response<T: Decodable>(api_key: ApiKey, version: i16, mut frame: Bytes) -> T {
    let frame_size = frame.get_i32() as usize;
    assert_eq!(frame_size, frame.remaining());
    ResponseHeader::decode(&mut frame, api_key.response_header_version(version)).unwrap();
    T::decode(&mut frame, version).unwrap()
}

#[tokio::test]
async fn create_partitions_supports_validation_and_virtual_assignments() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 2).await.unwrap();

    let validate = CreatePartitionsRequest::default()
        .with_topics(vec![expansion("events", 3, None)])
        .with_validate_only(true);
    let response = broker
        .handle_request(request_frame(ApiKey::CreatePartitions, 3, &validate))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .topic("events")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );

    let assignments = Some(vec![assignment(0), assignment(0)]);
    let create =
        CreatePartitionsRequest::default().with_topics(vec![expansion("events", 4, assignments)]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreatePartitions, 3, &create))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .topic("events")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        4
    );

    let invalid_count =
        CreatePartitionsRequest::default().with_topics(vec![expansion("events", 4, None)]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreatePartitions, 3, &invalid_count))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results[0].error_code, INVALID_PARTITIONS);

    let invalid_assignment = CreatePartitionsRequest::default().with_topics(vec![expansion(
        "events",
        5,
        Some(vec![assignment(1)]),
    )]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreatePartitions,
            3,
            &invalid_assignment,
        ))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results[0].error_code, INVALID_REPLICA_ASSIGNMENT);
}

#[tokio::test]
async fn create_partitions_rejects_duplicate_topics_before_mutation() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 2).await.unwrap();
    broker.metadata.create_topic("unique", 1).await.unwrap();

    let create = CreatePartitionsRequest::default().with_topics(vec![
        expansion("events", 3, None),
        expansion("unique", 2, None),
        expansion("events", 4, None),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreatePartitions, 3, &create))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results.len(), 2);
    let duplicate = response
        .results
        .iter()
        .find(|result| result.name.as_str() == "events")
        .unwrap();
    assert_eq!(duplicate.error_code, INVALID_REQUEST);
    assert_eq!(
        duplicate
            .error_message
            .as_ref()
            .map(|message| message.as_str()),
        Some("Duplicate topic name.")
    );
    assert_eq!(
        response
            .results
            .iter()
            .find(|result| result.name.as_str() == "unique")
            .unwrap()
            .error_code,
        NO_ERROR
    );
    assert_eq!(
        broker
            .metadata
            .topic("events")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );
    assert_eq!(
        broker
            .metadata
            .topic("unique")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );

    let validate = CreatePartitionsRequest::default()
        .with_topics(vec![
            expansion("events", 5, None),
            expansion("unique", 3, None),
            expansion("events", 6, None),
        ])
        .with_validate_only(true);
    let response = broker
        .handle_request(request_frame(ApiKey::CreatePartitions, 3, &validate))
        .await
        .unwrap();
    let response: CreatePartitionsResponse = decode_response(ApiKey::CreatePartitions, 3, response);
    assert_eq!(response.results.len(), 2);
    assert_eq!(
        response
            .results
            .iter()
            .find(|result| result.name.as_str() == "events")
            .unwrap()
            .error_code,
        INVALID_REQUEST
    );
    assert_eq!(
        broker
            .metadata
            .topic("events")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );
    assert_eq!(
        broker
            .metadata
            .topic("unique")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );
}

#[tokio::test]
async fn describe_topic_partitions_paginates_in_topic_order() {
    let broker = test_broker();
    broker.metadata.create_topic("alpha", 3).await.unwrap();
    broker.metadata.create_topic("beta", 2).await.unwrap();
    let topics = vec![
        topic_request("beta"),
        topic_request("missing"),
        topic_request("alpha"),
    ];
    let first = DescribeTopicPartitionsRequest::default()
        .with_topics(topics.clone())
        .with_response_partition_limit(4);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeTopicPartitions, 0, &first))
        .await
        .unwrap();
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.name.as_ref().unwrap().as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert_eq!(response.topics[0].partitions.len(), 3);
    assert_eq!(response.topics[1].partitions.len(), 1);
    assert_eq!(
        response.topics[0].partitions[0].leader_id,
        BrokerId::from(0)
    );
    assert_eq!(
        response.topics[0].partitions[0].replica_nodes,
        [BrokerId::from(0)]
    );
    assert_eq!(
        response.topics[0].partitions[0]
            .eligible_leader_replicas
            .as_deref(),
        Some([].as_slice())
    );
    assert_eq!(
        response.topics[0].partitions[0].last_known_elr.as_deref(),
        Some([].as_slice())
    );
    assert_ne!(response.topics[0].topic_authorized_operations, i32::MIN);
    let cursor = response.next_cursor.unwrap();
    assert_eq!(cursor.topic_name.as_str(), "beta");
    assert_eq!(cursor.partition_index, 1);

    let second = DescribeTopicPartitionsRequest::default()
        .with_topics(topics)
        .with_response_partition_limit(4)
        .with_cursor(Some(
            RequestCursor::default()
                .with_topic_name(cursor.topic_name)
                .with_partition_index(cursor.partition_index),
        ));
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeTopicPartitions, 0, &second))
        .await
        .unwrap();
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(response.topics.len(), 2);
    assert_eq!(response.topics[0].name.as_ref().unwrap().as_str(), "beta");
    assert_eq!(response.topics[0].partitions[0].partition_index, 1);
    assert_eq!(response.topics[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert!(response.next_cursor.is_none());

    let all = DescribeTopicPartitionsRequest::default().with_response_partition_limit(10);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeTopicPartitions, 0, &all))
        .await
        .unwrap();
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(response.topics.len(), 2);
    assert_eq!(
        response
            .topics
            .iter()
            .map(|topic| topic.partitions.len())
            .sum::<usize>(),
        5
    );
}

#[tokio::test]
async fn describe_topic_partitions_validates_cursor_and_clamps_page_size() {
    let config = AgentConfig {
        max_request_partition_size_limit: 2,
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    broker.metadata.create_topic("bounded", 3).await.unwrap();
    broker.metadata.create_topic("second", 1).await.unwrap();
    let topics = vec![topic_request("bounded"), topic_request("second")];

    for cursor in [
        RequestCursor::default()
            .with_topic_name(topic_name("missing-cursor"))
            .with_partition_index(0),
        RequestCursor::default()
            .with_topic_name(topic_name("bounded"))
            .with_partition_index(-1),
    ] {
        let request = DescribeTopicPartitionsRequest::default()
            .with_topics(topics.clone())
            .with_cursor(Some(cursor));
        let response = broker
            .handle_request(request_frame(ApiKey::DescribeTopicPartitions, 0, &request))
            .await
            .unwrap();
        let response: DescribeTopicPartitionsResponse =
            decode_response(ApiKey::DescribeTopicPartitions, 0, response);
        assert_eq!(response.topics.len(), 2);
        assert!(
            response
                .topics
                .iter()
                .all(|topic| topic.error_code == INVALID_REQUEST)
        );
        assert!(response.next_cursor.is_none());
    }

    let zero = DescribeTopicPartitionsRequest::default()
        .with_topics(vec![topic_request("bounded")])
        .with_response_partition_limit(0);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeTopicPartitions, 0, &zero))
        .await
        .unwrap();
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(response.topics[0].partitions.len(), 1);
    assert_eq!(response.next_cursor.unwrap().partition_index, 1);

    let oversized = DescribeTopicPartitionsRequest::default()
        .with_topics(vec![topic_request("bounded")])
        .with_response_partition_limit(i32::MAX);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeTopicPartitions,
            0,
            &oversized,
        ))
        .await
        .unwrap();
    let response: DescribeTopicPartitionsResponse =
        decode_response(ApiKey::DescribeTopicPartitions, 0, response);
    assert_eq!(response.topics[0].partitions.len(), 2);
    assert_eq!(response.next_cursor.unwrap().partition_index, 2);
}

fn expansion(
    name: &str,
    count: i32,
    assignments: Option<Vec<CreatePartitionsAssignment>>,
) -> CreatePartitionsTopic {
    CreatePartitionsTopic::default()
        .with_name(topic_name(name))
        .with_count(count)
        .with_assignments(assignments)
}

fn assignment(broker: i32) -> CreatePartitionsAssignment {
    CreatePartitionsAssignment::default().with_broker_ids(vec![BrokerId::from(broker)])
}

fn topic_request(name: &str) -> TopicRequest {
    TopicRequest::default().with_name(topic_name(name))
}
