use super::tests::{broker, decode_response, request_frame, sample_records};
use super::*;
use crate::kafka_error::GROUP_MAX_SIZE_REACHED;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::share_acknowledge_request::{
    AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeBatch,
};
use kafka_protocol::messages::share_fetch_request::{
    FetchPartition as ShareFetchPartition, FetchTopic as ShareFetchTopic,
};
use kafka_protocol::messages::{
    GroupId, ProduceResponse, ShareAcknowledgeRequest, ShareAcknowledgeResponse, ShareFetchRequest,
    ShareFetchResponse, ShareGroupDescribeRequest, ShareGroupDescribeResponse,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse,
};
use kafka_protocol::records::RecordBatchDecoder;
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;
use std::collections::BTreeMap;

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn limited_share_broker() -> Broker {
    let config = AgentConfig {
        share_group_assignment_interval_ms: 0,
        share_assignor_offload_enable: false,
        share_group_max_size: 1,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn share_fetch(group: &str, member: &str, epoch: i32) -> ShareFetchRequest {
    ShareFetchRequest::default()
        .with_group_id(Some(group_id(group)))
        .with_member_id(Some(StrBytes::from_string(member.to_owned())))
        .with_share_session_epoch(epoch)
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_max_records(10)
        .with_batch_size(10)
}

#[tokio::test]
async fn share_group_heartbeat_uses_bounded_group_timeout_overrides() {
    let broker = broker();
    broker
        .metadata
        .alter_group_config(
            "bounded-share-workers",
            BTreeMap::from([
                (
                    "share.heartbeat.interval.ms".to_owned(),
                    Some("15000".to_owned()),
                ),
                (
                    "share.session.timeout.ms".to_owned(),
                    Some("60000".to_owned()),
                ),
            ]),
            false,
        )
        .await
        .unwrap();
    broker
        .metadata
        .create_topic("bounded-share-orders", 1)
        .await
        .unwrap();
    let request = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("bounded-share-workers"))
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name("bounded-share-orders")]));
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 499, &request))
        .await
        .unwrap();
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.heartbeat_interval_ms, 15_000);

    let runtime = broker
        .group_runtime_config("bounded-share-workers")
        .await
        .unwrap();
    assert_eq!(runtime.share_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.share_session_timeout_ms, 60_000);
}

#[tokio::test]
async fn share_group_max_size_rejects_only_new_members_at_capacity() {
    let broker = limited_share_broker();
    broker
        .metadata
        .create_topic("limited-share-orders", 1)
        .await
        .unwrap();
    let first = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("limited-share-workers"))
        .with_member_id(StrBytes::from_static_str("member-a"))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name("limited-share-orders")]));
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 490, &first))
        .await
        .unwrap();
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let second = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("limited-share-workers"))
        .with_member_id(StrBytes::from_static_str("member-b"))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name("limited-share-orders")]));
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 491, &second))
        .await
        .unwrap();
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, GROUP_MAX_SIZE_REACHED);

    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupHeartbeat, 1, 492, &first))
        .await
        .unwrap();
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, NO_ERROR);

    let groups = broker
        .metadata
        .describe_share_groups(&["limited-share-workers".to_owned()])
        .await
        .unwrap();
    assert_eq!(groups["limited-share-workers"].members.len(), 1);
    assert_eq!(
        groups["limited-share-workers"].members[0].member_id,
        "member-a"
    );
}

#[tokio::test]
async fn share_group_fetch_release_redelivery_and_accept_round_trip() {
    let broker = broker();
    let topic = broker
        .metadata
        .create_topic("share-orders", 1)
        .await
        .unwrap();
    let heartbeat = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("share-workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_subscribed_topic_names(Some(vec![topic_name("share-orders")]));
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareGroupHeartbeat,
            1,
            500,
            &heartbeat,
        ))
        .await
        .unwrap();
    let heartbeat: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(heartbeat.error_code, NO_ERROR);
    assert_eq!(heartbeat.member_epoch, 2);
    assert_eq!(
        heartbeat.assignment.unwrap().topic_partitions[0].partitions,
        [0]
    );

    let describe = ShareGroupDescribeRequest::default()
        .with_group_ids(vec![group_id("share-workers")])
        .with_include_authorized_operations(true);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupDescribe, 1, 501, &describe))
        .await
        .unwrap();
    let described: ShareGroupDescribeResponse =
        decode_response(ApiKey::ShareGroupDescribe, 1, response);
    assert_eq!(described.groups[0].group_state.as_str(), "Reconciling");
    assert_eq!(
        described.groups[0].members[0].member_id.as_str(),
        "member-a"
    );

    let heartbeat = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("share-workers"))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(heartbeat.member_epoch);
    let response = broker
        .handle_request(request_frame(
            ApiKey::ShareGroupHeartbeat,
            1,
            502,
            &heartbeat,
        ))
        .await
        .unwrap();
    let heartbeat: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(heartbeat.error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(ApiKey::ShareGroupDescribe, 1, 503, &describe))
        .await
        .unwrap();
    let described: ShareGroupDescribeResponse =
        decode_response(ApiKey::ShareGroupDescribe, 1, response);
    assert_eq!(described.groups[0].group_state.as_str(), "Stable");

    let initial = share_fetch("share-workers", "member-a", 0).with_topics(vec![
        ShareFetchTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(vec![ShareFetchPartition::default().with_partition_index(0)]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 1, 502, &initial))
        .await
        .unwrap();
    let initial: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 1, response);
    assert_eq!(initial.error_code, NO_ERROR);

    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("share-orders"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records())),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 503, &produce))
        .await
        .unwrap();
    let produced: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(produced.responses[0].partition_responses[0].base_offset, 0);

    let first = share_fetch("share-workers", "member-a", 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 1, 504, &first))
        .await
        .unwrap();
    let first: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 1, response);
    let partition = &first.responses[0].partitions[0];
    assert_eq!(partition.acquired_records[0].delivery_count, 1);
    assert_eq!(decoded_offset(partition.records.clone().unwrap()), 0);

    let release = acknowledge(topic.id, 2, 2);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareAcknowledge, 1, 505, &release))
        .await
        .unwrap();
    let released: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 1, response);
    assert_eq!(released.error_code, NO_ERROR);
    assert_eq!(released.responses[0].partitions[0].error_code, NO_ERROR);

    let redelivered = share_fetch("share-workers", "member-a", 3);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 1, 506, &redelivered))
        .await
        .unwrap();
    let redelivered: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 1, response);
    let partition = &redelivered.responses[0].partitions[0];
    assert_eq!(partition.acquired_records[0].delivery_count, 2);
    assert_eq!(decoded_offset(partition.records.clone().unwrap()), 0);

    let accept = acknowledge(topic.id, 4, 1);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareAcknowledge, 1, 507, &accept))
        .await
        .unwrap();
    let accepted: ShareAcknowledgeResponse = decode_response(ApiKey::ShareAcknowledge, 1, response);
    assert_eq!(accepted.error_code, NO_ERROR);
    assert_eq!(accepted.responses[0].partitions[0].error_code, NO_ERROR);

    let empty = share_fetch("share-workers", "member-a", 5);
    let response = broker
        .handle_request(request_frame(ApiKey::ShareFetch, 1, 508, &empty))
        .await
        .unwrap();
    let empty: ShareFetchResponse = decode_response(ApiKey::ShareFetch, 1, response);
    assert!(
        empty.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(Bytes::is_empty)
    );
}

fn acknowledge(
    topic_id: uuid::Uuid,
    epoch: i32,
    acknowledgement_type: i8,
) -> ShareAcknowledgeRequest {
    ShareAcknowledgeRequest::default()
        .with_group_id(Some(group_id("share-workers")))
        .with_member_id(Some(StrBytes::from_string("member-a".to_owned())))
        .with_share_session_epoch(epoch)
        .with_topics(vec![
            AcknowledgeTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    AcknowledgePartition::default()
                        .with_partition_index(0)
                        .with_acknowledgement_batches(vec![
                            AcknowledgeBatch::default()
                                .with_first_offset(0)
                                .with_last_offset(0)
                                .with_acknowledge_types(vec![acknowledgement_type]),
                        ]),
                ]),
        ])
}

fn decoded_offset(mut records: Bytes) -> i64 {
    RecordBatchDecoder::decode_all(&mut records).unwrap()[0].records[0].offset
}
