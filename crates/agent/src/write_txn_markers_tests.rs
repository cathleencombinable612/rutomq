use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_PRODUCER_EPOCH, INVALID_TXN_STATE, NO_ERROR,
    TRANSACTION_COORDINATOR_FENCED, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic,
};
use kafka_protocol::messages::{ApiKey, WriteTxnMarkersRequest, WriteTxnMarkersResponse};
use rutomq_control::{
    AclPatternType, AclPermission, AclRule, BatchDraft, FetchIsolation, MemoryMetadataStore,
    ObjectRef, ProducerBatch, ProducerSession, TransactionState,
};

#[tokio::test]
async fn write_txn_markers_enforces_cluster_acl_and_atomic_visibility() {
    let (broker, metadata): (Broker, Arc<MemoryMetadataStore>) = acl_broker();
    let topic = "marker-wire";
    let transactional_id = "marker-wire-tx";
    metadata.create_topic(topic, 2).await.unwrap();
    let first = PartitionKey::new(topic, 0);
    let second = PartitionKey::new(topic, 1);
    let producer = metadata
        .init_producer(Some(transactional_id), 60_000, None)
        .await
        .unwrap();
    metadata
        .add_partitions_to_transaction(
            transactional_id,
            producer,
            &[first.clone(), second.clone()],
            false,
        )
        .await
        .unwrap();
    metadata
        .commit_object(
            ObjectRef {
                key: "objects/marker-wire".into(),
                size: 8,
            },
            vec![BatchDraft {
                partition: first.clone(),
                byte_start: 0,
                byte_end: 8,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(transactional_id.to_owned()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();

    let full = marker_request(producer, true, &[0, 1, 9], 2, 5);
    let denied = write_markers_as(&broker, "broker-node", 2, 1, &full).await;
    assert_eq!(
        marker_codes(&denied),
        [
            CLUSTER_AUTHORIZATION_FAILED,
            CLUSTER_AUTHORIZATION_FAILED,
            CLUSTER_AUTHORIZATION_FAILED,
        ]
    );

    metadata.set_authorization_failure_for(Some(AclResourceType::Cluster));
    let failed = write_markers_as(&broker, "broker-node", 2, 2, &full).await;
    assert_eq!(
        marker_codes(&failed),
        [
            UNKNOWN_SERVER_ERROR,
            UNKNOWN_SERVER_ERROR,
            UNKNOWN_SERVER_ERROR,
        ]
    );
    metadata.set_authorization_failure_for(None);
    metadata
        .create_acl(cluster_rule(
            "User:broker-node",
            AclOperation::ClusterAction,
        ))
        .await
        .unwrap();

    let same_epoch = marker_request(producer, true, &[0, 1], 2, 5);
    let response = write_markers_as(&broker, "broker-node", 2, 3, &same_epoch).await;
    assert_eq!(
        marker_codes(&response),
        [INVALID_PRODUCER_EPOCH, INVALID_PRODUCER_EPOCH]
    );

    let marker_epoch_one = ProducerSession {
        producer_id: producer.producer_id,
        producer_epoch: 1,
    };
    let partial = marker_request(marker_epoch_one, true, &[0], 2, 5);
    let response = write_markers_as(&broker, "broker-node", 2, 3, &partial).await;
    assert_eq!(marker_codes(&response), [INVALID_TXN_STATE]);
    assert_eq!(
        metadata
            .describe_transactions(&[transactional_id.to_owned()])
            .await
            .unwrap()[transactional_id]
            .state,
        TransactionState::Ongoing
    );
    assert!(
        metadata
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .is_empty()
    );

    let full = marker_request(marker_epoch_one, true, &[0, 1, 9], 2, 5);
    let response = write_markers_as(&broker, "broker-node", 2, 4, &full).await;
    assert_eq!(
        marker_codes(&response),
        [NO_ERROR, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION]
    );
    assert_eq!(
        metadata
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .len(),
        1
    );

    let retry = marker_request(marker_epoch_one, true, &[0, 1], 0, 5);
    let response = write_markers_as(&broker, "broker-node", 1, 5, &retry).await;
    assert_eq!(marker_codes(&response), [NO_ERROR, NO_ERROR]);
    let opposite = marker_request(marker_epoch_one, false, &[0, 1], 2, 5);
    let response = write_markers_as(&broker, "broker-node", 2, 6, &opposite).await;
    assert_eq!(
        marker_codes(&response),
        [INVALID_TXN_STATE, INVALID_TXN_STATE]
    );

    metadata
        .add_partitions_to_transaction(
            transactional_id,
            marker_epoch_one,
            &[first.clone(), second.clone()],
            false,
        )
        .await
        .unwrap();
    let late = marker_request(marker_epoch_one, false, &[0, 1], 2, 6);
    let response = write_markers_as(&broker, "broker-node", 2, 7, &late).await;
    assert_eq!(
        marker_codes(&response),
        [INVALID_PRODUCER_EPOCH, INVALID_PRODUCER_EPOCH]
    );

    let marker_epoch_two = ProducerSession {
        producer_id: producer.producer_id,
        producer_epoch: 2,
    };
    let stale_coordinator = marker_request(marker_epoch_two, false, &[0, 1], 2, 4);
    let response = write_markers_as(&broker, "broker-node", 2, 8, &stale_coordinator).await;
    assert_eq!(
        marker_codes(&response),
        [
            TRANSACTION_COORDINATOR_FENCED,
            TRANSACTION_COORDINATOR_FENCED,
        ]
    );
    assert_eq!(
        metadata
            .describe_transactions(&[transactional_id.to_owned()])
            .await
            .unwrap()[transactional_id]
            .state,
        TransactionState::Ongoing
    );

    let abort = marker_request(marker_epoch_two, false, &[0, 1], 2, 6);
    let response = write_markers_as(&broker, "broker-node", 2, 9, &abort).await;
    assert_eq!(marker_codes(&response), [NO_ERROR, NO_ERROR]);
    let response = write_markers_as(&broker, "broker-node", 2, 10, &abort).await;
    assert_eq!(marker_codes(&response), [NO_ERROR, NO_ERROR]);
}

fn marker_request(
    producer: ProducerSession,
    committed: bool,
    partitions: &[i32],
    transaction_version: i8,
    coordinator_epoch: i32,
) -> WriteTxnMarkersRequest {
    WriteTxnMarkersRequest::default().with_markers(vec![
        WritableTxnMarker::default()
            .with_producer_id(producer.producer_id.into())
            .with_producer_epoch(producer.producer_epoch)
            .with_transaction_result(committed)
            .with_topics(vec![
                WritableTxnMarkerTopic::default()
                    .with_name(topic_name("marker-wire"))
                    .with_partition_indexes(partitions.to_vec()),
            ])
            .with_coordinator_epoch(coordinator_epoch)
            .with_transaction_version(transaction_version),
    ])
}

async fn write_markers_as(
    broker: &Broker,
    username: &str,
    version: i16,
    correlation_id: i32,
    request: &WriteTxnMarkersRequest,
) -> WriteTxnMarkersResponse {
    let response = handle_as(
        broker,
        username,
        ApiKey::WriteTxnMarkers,
        version,
        correlation_id,
        request,
    )
    .await;
    decode_response(ApiKey::WriteTxnMarkers, version, response)
}

fn marker_codes(response: &WriteTxnMarkersResponse) -> Vec<i16> {
    response
        .markers
        .iter()
        .flat_map(|marker| &marker.topics)
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.error_code)
        .collect()
}

fn cluster_rule(principal: &str, operation: AclOperation) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Cluster,
        resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}
