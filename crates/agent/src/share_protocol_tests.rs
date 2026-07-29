use super::tests::{broker, decode_response, request_frame, sample_records_count};
use super::*;
use crate::kafka_error::{INVALID_RECORD_STATE, INVALID_REQUEST};
use kafka_protocol::messages::describe_share_group_offsets_request::{
    DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::share_acknowledge_request::{
    AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeBatch,
};
use kafka_protocol::messages::share_fetch_request::{
    AcknowledgementBatch as FetchAcknowledgeBatch, FetchPartition as ShareFetchPartition,
    FetchTopic as ShareFetchTopic,
};
use kafka_protocol::messages::{
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse, GroupId, ProduceResponse,
    ShareAcknowledgeRequest, ShareAcknowledgeResponse, ShareFetchRequest, ShareFetchResponse,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse,
};
use kafka_protocol::records::RecordBatchDecoder;
use std::collections::BTreeMap;

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn share_fetch(
    group: &str,
    member: &str,
    epoch: i32,
    max_records: i32,
    mode: i8,
) -> ShareFetchRequest {
    ShareFetchRequest::default()
        .with_group_id(Some(group_id(group)))
        .with_member_id(Some(StrBytes::from_string(member.to_owned())))
        .with_share_session_epoch(epoch)
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_max_records(max_records)
        .with_batch_size(10)
        .with_share_acquire_mode(mode)
}

async fn ready_broker(
    group: &str,
    member: &str,
    topic_name_value: &str,
    record_count: usize,
) -> (Broker, Uuid) {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic(topic_name_value, 1)
        .await
        .unwrap();
    let heartbeat = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id(group))
        .with_member_id(StrBytes::from_string(member.to_owned()))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name(topic_name_value)]));
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareGroupHeartbeat,
            1,
            700,
            &heartbeat,
        ))
        .await
        .unwrap();
    let heartbeat: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(heartbeat.error_code, NO_ERROR);

    let open = share_fetch(group, member, 0, 10, 0).with_topics(vec![
        ShareFetchTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(vec![ShareFetchPartition::default().with_partition_index(0)]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 701, &open))
        .await
        .unwrap();
    let opened: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(opened.error_code, NO_ERROR);

    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name(topic_name_value))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records_count(record_count))),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 702, &produce))
        .await
        .unwrap();
    let produced: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(produced.responses[0].partition_responses[0].base_offset, 0);
    (broker, topic.id)
}

fn acknowledgement(
    group: &str,
    member: &str,
    topic_id: Uuid,
    epoch: i32,
    offset: i64,
    acknowledgement_type: i8,
    renew: bool,
) -> ShareAcknowledgeRequest {
    ShareAcknowledgeRequest::default()
        .with_group_id(Some(group_id(group)))
        .with_member_id(Some(StrBytes::from_string(member.to_owned())))
        .with_share_session_epoch(epoch)
        .with_is_renew_ack(renew)
        .with_topics(vec![
            AcknowledgeTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    AcknowledgePartition::default()
                        .with_partition_index(0)
                        .with_acknowledgement_batches(vec![
                            AcknowledgeBatch::default()
                                .with_first_offset(offset)
                                .with_last_offset(offset)
                                .with_acknowledge_types(vec![acknowledgement_type]),
                        ]),
                ]),
        ])
}

fn record_count(mut records: Bytes) -> usize {
    RecordBatchDecoder::decode_all(&mut records)
        .unwrap()
        .iter()
        .map(|batch| batch.records.len())
        .sum()
}

#[tokio::test]
async fn share_fetch_v2_honors_record_limit_and_batch_boundaries() {
    let mut encoded_batch = sample_records_count(5);
    assert_eq!(
        RecordBatchDecoder::decode_all(&mut encoded_batch)
            .unwrap()
            .len(),
        1
    );

    let (batch_broker, batch_topic) =
        ready_broker("batch-group", "batch-member", "batch-topic", 5).await;
    let request = share_fetch("batch-group", "batch-member", 1, 2, 0);
    let response = batch_broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 703, &request))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(record_count(partition.records.clone().unwrap()), 5);
    assert_eq!(
        (
            partition.acquired_records[0].first_offset,
            partition.acquired_records[0].last_offset,
        ),
        (0, 4)
    );
    assert_eq!(response.responses[0].topic_id, batch_topic);

    let (strict_broker, strict_topic) =
        ready_broker("strict-group", "strict-member", "strict-topic", 5).await;
    let request = share_fetch("strict-group", "strict-member", 1, 2, 1);
    let response = strict_broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 704, &request))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(record_count(partition.records.clone().unwrap()), 2);
    assert_eq!(
        (
            partition.acquired_records[0].first_offset,
            partition.acquired_records[0].last_offset,
        ),
        (0, 1)
    );
    assert_eq!(response.responses[0].topic_id, strict_topic);
}

#[tokio::test]
async fn share_partition_record_lock_limit_drains_before_new_acquisition() {
    let (broker, topic_id) =
        ready_broker("lock-limit-group", "lock-member", "lock-limit-topic", 101).await;
    broker
        .metadata
        .alter_group_config(
            "lock-limit-group",
            BTreeMap::from([(
                "share.partition.max.record.locks".to_owned(),
                Some("100".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();

    let first = share_fetch("lock-limit-group", "lock-member", 1, 101, 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 705, &first))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(record_count(partition.records.clone().unwrap()), 100);
    assert_eq!(partition.acquired_records[0].first_offset, 0);
    assert_eq!(partition.acquired_records[0].last_offset, 99);

    let blocked = share_fetch("lock-limit-group", "lock-member", 2, 101, 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 706, &blocked))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(record_count(partition.records.clone().unwrap()), 0);
    assert!(partition.acquired_records.is_empty());

    let release = acknowledgement("lock-limit-group", "lock-member", topic_id, 3, 0, 2, false);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareAcknowledge, 2, 707, &release))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);

    let one_slot = share_fetch("lock-limit-group", "lock-member", 4, 101, 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 708, &one_slot))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(
        record_count(response.responses[0].partitions[0].records.clone().unwrap()),
        1
    );
}

#[tokio::test]
async fn share_v2_enforces_and_executes_renew_acknowledgements() {
    let (broker, topic_id) = ready_broker("renew-group", "renew-member", "renew-topic", 2).await;
    broker
        .metadata
        .alter_group_config(
            "renew-group",
            BTreeMap::from([(
                "share.record.lock.duration.ms".to_owned(),
                Some("60000".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();

    let fetch = share_fetch("renew-group", "renew-member", 1, 1, 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 710, &fetch))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(
        record_count(response.responses[0].partitions[0].records.clone().unwrap()),
        1
    );

    let unsupported = acknowledgement("renew-group", "renew-member", topic_id, 2, 0, 4, false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareAcknowledge,
            1,
            711,
            &unsupported,
        ))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 1, response);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        INVALID_REQUEST
    );

    let missing_flag = acknowledgement("renew-group", "renew-member", topic_id, 3, 0, 4, false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareAcknowledge,
            2,
            712,
            &missing_flag,
        ))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        INVALID_REQUEST
    );

    let renew = acknowledgement("renew-group", "renew-member", topic_id, 4, 0, 4, true);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareAcknowledge, 2, 713, &renew))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(response.acquisition_lock_timeout_ms, 60_000);

    let invalid_fetch = share_fetch("renew-group", "renew-member", 5, 0, 1)
        .with_is_renew_ack(true)
        .with_max_wait_ms(1)
        .with_min_bytes(0)
        .with_max_bytes(0);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 714, &invalid_fetch))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, INVALID_REQUEST);

    let renew_fetch = share_fetch("renew-group", "renew-member", 5, 0, 1)
        .with_is_renew_ack(true)
        .with_min_bytes(0)
        .with_max_bytes(0)
        .with_topics(vec![
            ShareFetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    ShareFetchPartition::default()
                        .with_partition_index(0)
                        .with_acknowledgement_batches(vec![
                            FetchAcknowledgeBatch::default()
                                .with_first_offset(0)
                                .with_last_offset(0)
                                .with_acknowledge_types(vec![4]),
                        ]),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 715, &renew_fetch))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(partition.acknowledge_error_code, NO_ERROR);
    assert!(partition.records.as_ref().is_some_and(Bytes::is_empty));

    broker
        .metadata
        .alter_group_config(
            "renew-group",
            BTreeMap::from([(
                "share.renew.acknowledge.enable".to_owned(),
                Some("false".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    let disabled = acknowledgement("renew-group", "renew-member", topic_id, 6, 0, 4, true);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareAcknowledge, 2, 716, &disabled))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(partition.error_code, INVALID_RECORD_STATE);
    assert!(
        partition
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("not enabled"))
    );

    let disabled_fetch = share_fetch("renew-group", "renew-member", 7, 0, 1)
        .with_is_renew_ack(true)
        .with_min_bytes(0)
        .with_max_bytes(0)
        .with_topics(vec![
            ShareFetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    ShareFetchPartition::default()
                        .with_partition_index(0)
                        .with_acknowledgement_batches(vec![
                            FetchAcknowledgeBatch::default()
                                .with_first_offset(0)
                                .with_last_offset(0)
                                .with_acknowledge_types(vec![4]),
                        ]),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 717, &disabled_fetch))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    let partition = &response.responses[0].partitions[0];
    assert_eq!(partition.acknowledge_error_code, INVALID_RECORD_STATE);
    assert!(
        partition
            .acknowledge_error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("not enabled"))
    );

    let unknown_mode = share_fetch("renew-group", "renew-member", 6, 1, 9);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 718, &unknown_mode))
        .await
        .unwrap();
    let response: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 2, response);
    assert_eq!(response.error_code, INVALID_REQUEST);
}

#[tokio::test]
async fn describe_share_offsets_v1_reports_processed_record_lag() {
    let (broker, topic_id) = ready_broker("lag-group", "lag-member", "lag-topic", 3).await;
    let fetch = share_fetch("lag-group", "lag-member", 1, 3, 1);
    broker
        .handle_request(request_frame(ApiKey::ShareFetch, 2, 720, &fetch))
        .await
        .unwrap();
    let accept_middle = acknowledgement("lag-group", "lag-member", topic_id, 2, 1, 1, false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareAcknowledge,
            2,
            721,
            &accept_middle,
        ))
        .await
        .unwrap();
    let response: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 2, response);
    assert_eq!(response.responses[0].partitions[0].error_code, NO_ERROR);

    let describe = DescribeShareGroupOffsetsRequest::default().with_groups(vec![
        DescribeShareGroupOffsetsRequestGroup::default()
            .with_group_id(group_id("lag-group"))
            .with_topics(Some(vec![
                DescribeShareGroupOffsetsRequestTopic::default()
                    .with_topic_name(topic_name("lag-topic"))
                    .with_partitions(vec![0]),
            ])),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            1,
            722,
            &describe,
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 1, response);
    let partition = &response.groups[0].topics[0].partitions[0];
    assert_eq!((partition.start_offset, partition.lag), (0, 2));

    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeShareGroupOffsets,
            0,
            723,
            &describe,
        ))
        .await
        .unwrap();
    let response: DescribeShareGroupOffsetsResponse =
        decode_response(ApiKey::DescribeShareGroupOffsets, 0, response);
    assert_eq!(response.groups[0].topics[0].partitions[0].lag, -1);
}
