use super::log_dir_api::VIRTUAL_LOG_DIR;
use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{KAFKA_STORAGE_ERROR, REPLICA_NOT_AVAILABLE};
use kafka_protocol::messages::alter_replica_log_dirs_request::{
    AlterReplicaLogDir, AlterReplicaLogDirTopic,
};
use rutomq_control::{BatchDraft, ObjectRef};

fn alter_request(path: &str, partition: i32) -> AlterReplicaLogDirsRequest {
    AlterReplicaLogDirsRequest::default().with_dirs(vec![
        AlterReplicaLogDir::default()
            .with_path(StrBytes::from_string(path.to_owned()))
            .with_topics(vec![
                AlterReplicaLogDirTopic::default()
                    .with_name(topic_name("log-dir-topic"))
                    .with_partitions(vec![partition]),
            ]),
    ])
}

#[tokio::test]
async fn describes_object_store_as_one_virtual_log_directory() {
    let broker = broker();
    broker
        .metadata
        .create_topic("log-dir-topic", 2)
        .await
        .unwrap();
    broker
        .metadata
        .commit_object(
            ObjectRef {
                key: "objects/log-dir-test".to_owned(),
                size: 12,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("log-dir-topic", 0),
                byte_start: 2,
                byte_end: 12,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();

    let request = DescribeLogDirsRequest::default().with_topics(None);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeLogDirs, 5, 70, &request))
        .await
        .unwrap();
    let response: DescribeLogDirsResponse = decode_response(ApiKey::DescribeLogDirs, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.results.len(), 1);
    let directory = &response.results[0];
    assert_eq!(directory.log_dir.as_str(), VIRTUAL_LOG_DIR);
    assert_eq!(directory.total_bytes, -1);
    assert_eq!(directory.usable_bytes, -1);
    assert!(!directory.is_cordoned);
    let topic = directory
        .topics
        .iter()
        .find(|topic| topic.name.as_str() == "log-dir-topic")
        .unwrap();
    assert_eq!(topic.partitions.len(), 2);
    assert_eq!(topic.partitions[0].partition_size, 10);
    assert_eq!(topic.partitions[1].partition_size, 0);
    assert!(
        topic
            .partitions
            .iter()
            .all(|partition| partition.offset_lag == 0 && !partition.is_future_key)
    );
}

#[tokio::test]
async fn alter_replica_log_dirs_accepts_only_the_virtual_directory() {
    let broker = broker();
    broker
        .metadata
        .create_topic("log-dir-topic", 1)
        .await
        .unwrap();

    let request = alter_request(VIRTUAL_LOG_DIR, 0);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterReplicaLogDirs, 2, 71, &request))
        .await
        .unwrap();
    let response: AlterReplicaLogDirsResponse =
        decode_response(ApiKey::AlterReplicaLogDirs, 2, response);
    assert_eq!(response.results[0].partitions[0].error_code, NO_ERROR);

    let request = alter_request("/tmp/local-wal", 0);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterReplicaLogDirs, 2, 72, &request))
        .await
        .unwrap();
    let response: AlterReplicaLogDirsResponse =
        decode_response(ApiKey::AlterReplicaLogDirs, 2, response);
    assert_eq!(
        response.results[0].partitions[0].error_code,
        KAFKA_STORAGE_ERROR
    );

    let request = alter_request(VIRTUAL_LOG_DIR, 9);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterReplicaLogDirs, 2, 73, &request))
        .await
        .unwrap();
    let response: AlterReplicaLogDirsResponse =
        decode_response(ApiKey::AlterReplicaLogDirs, 2, response);
    assert_eq!(
        response.results[0].partitions[0].error_code,
        REPLICA_NOT_AVAILABLE
    );
}
