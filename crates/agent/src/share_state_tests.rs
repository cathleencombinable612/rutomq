use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, FENCED_LEADER_EPOCH, FENCED_STATE_EPOCH, INVALID_REQUEST,
    NO_ERROR, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use kafka_protocol::messages::delete_share_group_state_request::{
    DeleteStateData, PartitionData as DeletePartition,
};
use kafka_protocol::messages::initialize_share_group_state_request::{
    InitializeStateData, PartitionData as InitializePartition,
};
use kafka_protocol::messages::read_share_group_state_request::{
    PartitionData as ReadPartition, ReadStateData,
};
use kafka_protocol::messages::read_share_group_state_summary_request::{
    PartitionData as SummaryPartition, ReadStateSummaryData,
};
use kafka_protocol::messages::write_share_group_state_request::{
    PartitionData as WritePartition, StateBatch, WriteStateData,
};
use kafka_protocol::messages::{
    DeleteShareGroupStateRequest, DeleteShareGroupStateResponse, InitializeShareGroupStateRequest,
    InitializeShareGroupStateResponse, ReadShareGroupStateRequest, ReadShareGroupStateResponse,
    ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse,
    WriteShareGroupStateRequest, WriteShareGroupStateResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclPatternType, AclPermission, AclRule, MemoryMetadataStore, ShareStateKey};

const USERNAME: &str = "share-state-broker";
const GROUP_ID: &str = "share-state-group";

#[tokio::test]
async fn share_state_wire_authorization_failures_do_not_initialize_state() {
    let (broker, metadata) = acl_broker();
    let topic = metadata.create_topic("share-state-auth", 1).await.unwrap();
    let request = initialize(topic.id, 0, 5, 10);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::InitializeShareGroupState,
        0,
        8701,
        &request,
    )
    .await;
    let response: InitializeShareGroupStateResponse =
        decode_response(ApiKey::InitializeShareGroupState, 0, response);
    assert_eq!(
        response.results[0].partitions[0].error_code,
        CLUSTER_AUTHORIZATION_FAILED
    );
    assert_state_absent(metadata.as_ref(), topic.id).await;

    metadata.set_authorization_failure_for(Some(AclResourceType::Cluster));
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::InitializeShareGroupState,
        0,
        8702,
        &request,
    )
    .await;
    let response: InitializeShareGroupStateResponse =
        decode_response(ApiKey::InitializeShareGroupState, 0, response);
    assert_eq!(
        response.results[0].partitions[0].error_code,
        UNKNOWN_SERVER_ERROR
    );
    metadata.set_authorization_failure_for(None);
    assert_state_absent(metadata.as_ref(), topic.id).await;
}

#[tokio::test]
async fn share_state_generated_wire_covers_lifecycle_versions_and_fencing() {
    let (broker, metadata) = acl_broker();
    let topic = metadata.create_topic("share-state-wire", 1).await.unwrap();
    metadata.create_acl(cluster_action_rule()).await.unwrap();

    let uninitialized = summary(topic.id, 0, -1);
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupStateSummary,
        1,
        8710,
        &uninitialized,
    )
    .await;
    let response: ReadShareGroupStateSummaryResponse =
        decode_response(ApiKey::ReadShareGroupStateSummary, 1, response);
    let partition = &response.results[0].partitions[0];
    assert_eq!(
        (
            partition.error_code,
            partition.state_epoch,
            partition.leader_epoch,
            partition.start_offset,
            partition.delivery_complete_count,
        ),
        (NO_ERROR, 0, 0, -1, -1)
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::InitializeShareGroupState,
        0,
        8711,
        &initialize(topic.id, 0, 5, 10),
    )
    .await;
    let response: InitializeShareGroupStateResponse =
        decode_response(ApiKey::InitializeShareGroupState, 0, response);
    assert_eq!(response.results[0].partitions[0].error_code, NO_ERROR);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupState,
        0,
        8712,
        &read(topic.id, 0, 3),
    )
    .await;
    let response: ReadShareGroupStateResponse =
        decode_response(ApiKey::ReadShareGroupState, 0, response);
    let partition = &response.results[0].partitions[0];
    assert_eq!(
        (
            partition.error_code,
            partition.state_epoch,
            partition.start_offset,
        ),
        (NO_ERROR, 5, 10)
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::WriteShareGroupState,
        0,
        8713,
        &write(topic.id, 0, 5, 3, 10, -1, vec![state_batch(10, 12, 0, 1)]),
    )
    .await;
    let response: WriteShareGroupStateResponse =
        decode_response(ApiKey::WriteShareGroupState, 0, response);
    assert_eq!(response.results[0].partitions[0].error_code, NO_ERROR);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::WriteShareGroupState,
        1,
        8714,
        &write(topic.id, 0, 5, 3, 10, 1, vec![state_batch(11, 11, 2, 2)]),
    )
    .await;
    let response: WriteShareGroupStateResponse =
        decode_response(ApiKey::WriteShareGroupState, 1, response);
    assert_eq!(response.results[0].partitions[0].error_code, NO_ERROR);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupState,
        0,
        8715,
        &read(topic.id, 0, 3),
    )
    .await;
    let response: ReadShareGroupStateResponse =
        decode_response(ApiKey::ReadShareGroupState, 0, response);
    let partition = &response.results[0].partitions[0];
    assert_eq!(partition.state_batches.len(), 3);
    assert_eq!(
        partition
            .state_batches
            .iter()
            .map(|batch| (
                batch.first_offset,
                batch.last_offset,
                batch.delivery_state,
                batch.delivery_count,
            ))
            .collect::<Vec<_>>(),
        [(10, 10, 0, 1), (11, 11, 2, 2), (12, 12, 0, 1)]
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupStateSummary,
        1,
        8716,
        &summary(topic.id, 0, 999),
    )
    .await;
    let response: ReadShareGroupStateSummaryResponse =
        decode_response(ApiKey::ReadShareGroupStateSummary, 1, response);
    let partition = &response.results[0].partitions[0];
    assert_eq!(
        (
            partition.error_code,
            partition.state_epoch,
            partition.leader_epoch,
            partition.start_offset,
            partition.delivery_complete_count,
        ),
        (NO_ERROR, 5, 3, 10, 1)
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupStateSummary,
        0,
        8717,
        &summary(topic.id, 0, -1),
    )
    .await;
    let response: ReadShareGroupStateSummaryResponse =
        decode_response(ApiKey::ReadShareGroupStateSummary, 0, response);
    assert_eq!(
        response.results[0].partitions[0].delivery_complete_count,
        -1
    );

    assert_eq!(
        write_error(&broker, 8718, write(topic.id, 0, 5, 2, -1, 1, Vec::new()),).await,
        FENCED_LEADER_EPOCH
    );
    assert_eq!(
        write_error(&broker, 8719, write(topic.id, 0, 4, 3, -1, 1, Vec::new()),).await,
        FENCED_STATE_EPOCH
    );
    assert_eq!(
        write_error(
            &broker,
            8720,
            write(topic.id, 0, 5, 3, -1, 1, vec![state_batch(10, 10, 1, 1)],),
        )
        .await,
        INVALID_REQUEST
    );

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupState,
        0,
        8721,
        &read(topic.id, 0, 3),
    )
    .await;
    let response: ReadShareGroupStateResponse =
        decode_response(ApiKey::ReadShareGroupState, 0, response);
    assert_eq!(response.results[0].partitions[0].state_batches.len(), 3);

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupState,
        0,
        8722,
        &read(topic.id, 9, -1),
    )
    .await;
    let response: ReadShareGroupStateResponse =
        decode_response(ApiKey::ReadShareGroupState, 0, response);
    assert_eq!(
        response.results[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );

    let delete = delete(topic.id, 0);
    for correlation_id in [8723, 8724] {
        let response = handle_as(
            &broker,
            USERNAME,
            ApiKey::DeleteShareGroupState,
            0,
            correlation_id,
            &delete,
        )
        .await;
        let response: DeleteShareGroupStateResponse =
            decode_response(ApiKey::DeleteShareGroupState, 0, response);
        assert_eq!(response.results[0].partitions[0].error_code, NO_ERROR);
    }

    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::ReadShareGroupStateSummary,
        1,
        8725,
        &summary(topic.id, 0, -1),
    )
    .await;
    let response: ReadShareGroupStateSummaryResponse =
        decode_response(ApiKey::ReadShareGroupStateSummary, 1, response);
    assert_eq!(response.results[0].partitions[0].start_offset, -1);

    let empty = InitializeShareGroupStateRequest::default();
    let response = handle_as(
        &broker,
        USERNAME,
        ApiKey::InitializeShareGroupState,
        0,
        8726,
        &empty,
    )
    .await;
    let response: InitializeShareGroupStateResponse =
        decode_response(ApiKey::InitializeShareGroupState, 0, response);
    assert!(response.results.is_empty());
}

async fn assert_state_absent(metadata: &MemoryMetadataStore, topic_id: Uuid) {
    let summary = metadata
        .summarize_share_group_state(&ShareStateKey {
            group_id: GROUP_ID.to_owned(),
            topic_id,
            partition: 0,
        })
        .await
        .unwrap();
    assert!(summary.is_none());
}

async fn write_error(
    broker: &Broker,
    correlation_id: i32,
    request: WriteShareGroupStateRequest,
) -> i16 {
    let response = handle_as(
        broker,
        USERNAME,
        ApiKey::WriteShareGroupState,
        1,
        correlation_id,
        &request,
    )
    .await;
    let response: WriteShareGroupStateResponse =
        decode_response(ApiKey::WriteShareGroupState, 1, response);
    response.results[0].partitions[0].error_code
}

fn initialize(
    topic_id: Uuid,
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
) -> InitializeShareGroupStateRequest {
    InitializeShareGroupStateRequest::default()
        .with_group_id(string(GROUP_ID))
        .with_topics(vec![
            InitializeStateData::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    InitializePartition::default()
                        .with_partition(partition)
                        .with_state_epoch(state_epoch)
                        .with_start_offset(start_offset),
                ]),
        ])
}

fn read(topic_id: Uuid, partition: i32, leader_epoch: i32) -> ReadShareGroupStateRequest {
    ReadShareGroupStateRequest::default()
        .with_group_id(string(GROUP_ID))
        .with_topics(vec![
            ReadStateData::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    ReadPartition::default()
                        .with_partition(partition)
                        .with_leader_epoch(leader_epoch),
                ]),
        ])
}

#[allow(clippy::too_many_arguments)]
fn write(
    topic_id: Uuid,
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    state_batches: Vec<StateBatch>,
) -> WriteShareGroupStateRequest {
    WriteShareGroupStateRequest::default()
        .with_group_id(string(GROUP_ID))
        .with_topics(vec![
            WriteStateData::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    WritePartition::default()
                        .with_partition(partition)
                        .with_state_epoch(state_epoch)
                        .with_leader_epoch(leader_epoch)
                        .with_start_offset(start_offset)
                        .with_delivery_complete_count(delivery_complete_count)
                        .with_state_batches(state_batches),
                ]),
        ])
}

fn state_batch(
    first_offset: i64,
    last_offset: i64,
    delivery_state: i8,
    delivery_count: i16,
) -> StateBatch {
    StateBatch::default()
        .with_first_offset(first_offset)
        .with_last_offset(last_offset)
        .with_delivery_state(delivery_state)
        .with_delivery_count(delivery_count)
}

fn delete(topic_id: Uuid, partition: i32) -> DeleteShareGroupStateRequest {
    DeleteShareGroupStateRequest::default()
        .with_group_id(string(GROUP_ID))
        .with_topics(vec![
            DeleteStateData::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![DeletePartition::default().with_partition(partition)]),
        ])
}

fn summary(topic_id: Uuid, partition: i32, leader_epoch: i32) -> ReadShareGroupStateSummaryRequest {
    ReadShareGroupStateSummaryRequest::default()
        .with_group_id(string(GROUP_ID))
        .with_topics(vec![
            ReadStateSummaryData::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    SummaryPartition::default()
                        .with_partition(partition)
                        .with_leader_epoch(leader_epoch),
                ]),
        ])
}

fn cluster_action_rule() -> AclRule {
    AclRule {
        resource_type: AclResourceType::Cluster,
        resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: format!("User:{USERNAME}"),
        host: "*".to_owned(),
        operation: AclOperation::ClusterAction,
        permission: AclPermission::Allow,
    }
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
