use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    ELECTION_NOT_NEEDED, INVALID_REPLICA_ASSIGNMENT, INVALID_REQUEST, NO_REASSIGNMENT_IN_PROGRESS,
};
use kafka_protocol::messages::alter_partition_reassignments_request::{
    ReassignablePartition, ReassignableTopic,
};
use kafka_protocol::messages::elect_leaders_request::TopicPartitions;

fn election_request(
    election_type: i8,
    topic_partitions: Option<Vec<TopicPartitions>>,
) -> ElectLeadersRequest {
    ElectLeadersRequest::default()
        .with_election_type(election_type)
        .with_topic_partitions(topic_partitions)
}

fn reassignment_request(
    partition: i32,
    replicas: Option<Vec<BrokerId>>,
) -> AlterPartitionReassignmentsRequest {
    AlterPartitionReassignmentsRequest::default()
        .with_allow_replication_factor_change(true)
        .with_topics(vec![
            ReassignableTopic::default()
                .with_name(topic_name("leadership-topic"))
                .with_partitions(vec![
                    ReassignablePartition::default()
                        .with_partition_index(partition)
                        .with_replicas(replicas),
                ]),
        ])
}

#[tokio::test]
async fn elect_leaders_reports_virtual_leader_state() {
    let broker = broker();
    broker
        .metadata
        .create_topic("leadership-topic", 2)
        .await
        .unwrap();

    let request = election_request(
        0,
        Some(vec![
            TopicPartitions::default()
                .with_topic(topic_name("leadership-topic"))
                .with_partitions(vec![0, 9]),
        ]),
    );
    let response = broker
        .handle_request(request_frame(ApiKey::ElectLeaders, 2, 80, &request))
        .await
        .unwrap();
    let response: ElectLeadersResponse = decode_response(ApiKey::ElectLeaders, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    let partitions = &response.replica_election_results[0].partition_result;
    assert_eq!(partitions[0].error_code, ELECTION_NOT_NEEDED);
    assert_eq!(partitions[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);

    let request = election_request(0, None);
    let response = broker
        .handle_request(request_frame(ApiKey::ElectLeaders, 2, 81, &request))
        .await
        .unwrap();
    let response: ElectLeadersResponse = decode_response(ApiKey::ElectLeaders, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.replica_election_results.len(), 1);
    assert!(
        response.replica_election_results[0]
            .partition_result
            .is_empty()
    );

    let request = election_request(2, None);
    let response = broker
        .handle_request(request_frame(ApiKey::ElectLeaders, 2, 82, &request))
        .await
        .unwrap();
    let response: ElectLeadersResponse = decode_response(ApiKey::ElectLeaders, 2, response);
    assert_eq!(response.error_code, INVALID_REQUEST);
}

#[tokio::test]
async fn partition_reassignment_preserves_virtual_replica_set() {
    let broker = broker();
    broker
        .metadata
        .create_topic("leadership-topic", 1)
        .await
        .unwrap();

    let cases = [
        (Some(vec![BrokerId::from(0)]), 0, NO_ERROR),
        (None, 0, NO_REASSIGNMENT_IN_PROGRESS),
        (Some(vec![BrokerId::from(1)]), 0, INVALID_REPLICA_ASSIGNMENT),
        (Some(vec![BrokerId::from(0)]), 9, UNKNOWN_TOPIC_OR_PARTITION),
    ];
    for (index, (replicas, partition, expected)) in cases.into_iter().enumerate() {
        let request = reassignment_request(partition, replicas);
        let response = broker
            .handle_request(request_frame(
                ApiKey::AlterPartitionReassignments,
                1,
                90 + index as i32,
                &request,
            ))
            .await
            .unwrap();
        let response: AlterPartitionReassignmentsResponse =
            decode_response(ApiKey::AlterPartitionReassignments, 1, response);
        assert_eq!(response.error_code, NO_ERROR);
        assert!(response.allow_replication_factor_change);
        assert_eq!(response.responses[0].partitions[0].error_code, expected);
    }

    let request = ListPartitionReassignmentsRequest::default();
    let response = broker
        .handle_request(request_frame(
            ApiKey::ListPartitionReassignments,
            0,
            100,
            &request,
        ))
        .await
        .unwrap();
    let response: ListPartitionReassignmentsResponse =
        decode_response(ApiKey::ListPartitionReassignments, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert!(response.topics.is_empty());
}
