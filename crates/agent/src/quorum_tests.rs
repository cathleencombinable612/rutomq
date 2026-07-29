use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{CLUSTER_AUTHORIZATION_FAILED, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION};
use kafka_protocol::messages::DescribeQuorumResponse;
use kafka_protocol::messages::describe_quorum_request::{
    PartitionData as RequestPartition, TopicData as RequestTopic,
};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn request(topic: &str, partition: i32) -> DescribeQuorumRequest {
    DescribeQuorumRequest::default().with_topics(vec![
        RequestTopic::default()
            .with_topic_name(topic_name(topic))
            .with_partitions(vec![
                RequestPartition::default().with_partition_index(partition),
            ]),
    ])
}

#[tokio::test]
async fn describes_single_virtual_metadata_voter_across_versions() {
    let broker = broker();
    for version in 0..=2 {
        let request = request("__cluster_metadata", 0);
        let response = broker
            .handle_request(request_frame(
                ApiKey::DescribeQuorum,
                version,
                120 + i32::from(version),
                &request,
            ))
            .await
            .unwrap();
        let response: DescribeQuorumResponse =
            decode_response(ApiKey::DescribeQuorum, version, response);
        assert_eq!(response.error_code, NO_ERROR);
        assert_eq!(response.topics.len(), 1);
        let partition = &response.topics[0].partitions[0];
        assert_eq!(partition.error_code, NO_ERROR);
        assert_eq!(partition.leader_id, BrokerId::from(0));
        assert_eq!(partition.leader_epoch, 0);
        assert_eq!(partition.high_watermark, 0);
        assert_eq!(partition.current_voters.len(), 1);
        assert_eq!(partition.current_voters[0].replica_id, BrokerId::from(0));
        assert_eq!(partition.current_voters[0].log_end_offset, 0);
        assert!(partition.observers.is_empty());
        if version >= 1 {
            assert_eq!(partition.current_voters[0].last_fetch_timestamp, -1);
            assert!(partition.current_voters[0].last_caught_up_timestamp > 0);
        }
        if version == 2 {
            assert_eq!(
                partition.current_voters[0].replica_directory_id,
                quorum_api::VIRTUAL_DIRECTORY_ID
            );
            assert_eq!(response.nodes.len(), 1);
            assert_eq!(response.nodes[0].node_id, BrokerId::from(0));
            assert_eq!(response.nodes[0].listeners[0].name.as_str(), "CONTROLLER");
            assert_eq!(response.nodes[0].listeners[0].host.as_str(), "127.0.0.1");
            assert_eq!(response.nodes[0].listeners[0].port, 9092);
        } else {
            assert!(response.nodes.is_empty());
        }
    }
}

#[tokio::test]
async fn rejects_unknown_metadata_partition_and_missing_cluster_acl() {
    let broker = broker();
    let invalid = request("__cluster_metadata", 1);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeQuorum, 2, 130, &invalid))
        .await
        .unwrap();
    let response: DescribeQuorumResponse = decode_response(ApiKey::DescribeQuorum, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert!(response.nodes.is_empty());

    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let valid = request("__cluster_metadata", 0);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeQuorum, 2, 131, &valid))
        .await
        .unwrap();
    let response: DescribeQuorumResponse = decode_response(ApiKey::DescribeQuorum, 2, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);
    assert!(response.topics.is_empty());
}
