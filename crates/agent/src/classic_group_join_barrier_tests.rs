use super::*;
use crate::kafka_error::{
    COORDINATOR_NOT_AVAILABLE, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED, INVALID_SESSION_TIMEOUT,
};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::{
    ApiKey, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rutomq_control::{MemoryMetadataStore, MetadataStore};
use rutomq_storage::OpenDalObjectStore;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn join_group_waits_for_members_across_agents_and_returns_one_generation() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let first_agent = broker(metadata.clone(), 120);
    let second_agent = broker(metadata, 120);
    let first_id = allocate(&first_agent, "classic-barrier-wire").await;
    let second_id = allocate(&second_agent, "classic-barrier-wire").await;

    let first = tokio::spawn({
        let broker = first_agent.clone();
        async move {
            broker
                .handle_request(frame(
                    ApiKey::JoinGroup,
                    4,
                    &join_request("classic-barrier-wire", first_id.as_str()),
                ))
                .await
                .unwrap()
        }
    });
    sleep(Duration::from_millis(20)).await;
    assert!(
        !first.is_finished(),
        "first JoinGroup returned before the barrier"
    );

    let second = second_agent
        .handle_request(frame(
            ApiKey::JoinGroup,
            4,
            &join_request("classic-barrier-wire", second_id.as_str()),
        ))
        .await
        .unwrap();
    let first = first.await.unwrap();
    let first: JoinGroupResponse = response(ApiKey::JoinGroup, 4, first);
    let second: JoinGroupResponse = response(ApiKey::JoinGroup, 4, second);

    assert_eq!(first.error_code, NO_ERROR);
    assert_eq!(second.error_code, NO_ERROR);
    assert_eq!(first.generation_id, 1);
    assert_eq!(second.generation_id, 1);
    assert_eq!(first.leader, second.leader);
    let leader = if first.member_id == first.leader {
        &first
    } else {
        &second
    };
    assert_eq!(leader.members.len(), 2);
}

#[tokio::test]
async fn join_group_rejects_session_timeouts_outside_broker_bounds() {
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    );

    for (group_id, session_timeout_ms) in [
        ("classic-session-too-short", 5_999),
        ("classic-session-too-long", 1_800_001),
    ] {
        let response: JoinGroupResponse = response(
            ApiKey::JoinGroup,
            4,
            broker
                .handle_request(frame(
                    ApiKey::JoinGroup,
                    4,
                    &join_request(group_id, "")
                        .with_session_timeout_ms(session_timeout_ms)
                        .with_rebalance_timeout_ms(session_timeout_ms),
                ))
                .await
                .unwrap(),
        );
        assert_eq!(response.error_code, INVALID_SESSION_TIMEOUT);
        assert_eq!(response.member_id.as_str(), "");
    }
}

#[tokio::test]
async fn join_group_max_size_rejects_new_members_but_allows_static_replacement() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let broker = broker_with_max_size(metadata.clone(), 0, 1);
    let first_request = join_request("classic-limit-wire", "")
        .with_group_instance_id(Some(StrBytes::from_string("instance-a".to_owned())));
    let first: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        9,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 9, &first_request))
            .await
            .unwrap(),
    );
    assert_eq!(first.error_code, NO_ERROR);

    let second_request = join_request("classic-limit-wire", "")
        .with_group_instance_id(Some(StrBytes::from_string("instance-b".to_owned())));
    let second: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        9,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 9, &second_request))
            .await
            .unwrap(),
    );
    assert_eq!(second.error_code, GROUP_MAX_SIZE_REACHED);

    let dynamic: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        4,
        broker
            .handle_request(frame(
                ApiKey::JoinGroup,
                4,
                &join_request("classic-limit-wire", ""),
            ))
            .await
            .unwrap(),
    );
    assert_eq!(dynamic.error_code, GROUP_MAX_SIZE_REACHED);

    let replacement: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        9,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 9, &first_request))
            .await
            .unwrap(),
    );
    assert_eq!(replacement.error_code, NO_ERROR);
    assert_ne!(replacement.member_id, first.member_id);

    let descriptions = metadata
        .describe_classic_groups(&["classic-limit-wire".to_owned()])
        .await
        .unwrap();
    assert_eq!(descriptions["classic-limit-wire"].members.len(), 1);
    assert_eq!(
        descriptions["classic-limit-wire"].members[0]
            .group_instance_id
            .as_deref(),
        Some("instance-a")
    );
}

#[tokio::test]
async fn empty_classic_and_consumer_groups_convert_bidirectionally_on_the_wire() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let broker = broker(metadata.clone(), 0);
    metadata
        .create_topic("group-conversion-topic", 1)
        .await
        .unwrap();
    let group_id = "group-conversion-wire";
    let classic: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        3,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 3, &join_request(group_id, "")))
            .await
            .unwrap(),
    );
    assert_eq!(classic.error_code, NO_ERROR);

    let consumer_join = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group_id.to_owned()),
        ))
        .with_member_id(StrBytes::from_static_str("consumer-member"))
        .with_member_epoch(0)
        .with_rebalance_timeout_ms(300_000)
        .with_subscribed_topic_names(Some(vec![topic_name("group-conversion-topic")]))
        .with_topic_partitions(Some(Vec::new()));
    let active_classic: ConsumerGroupHeartbeatResponse = response(
        ApiKey::ConsumerGroupHeartbeat,
        1,
        broker
            .handle_request(frame(ApiKey::ConsumerGroupHeartbeat, 1, &consumer_join))
            .await
            .unwrap(),
    );
    assert_eq!(active_classic.error_code, GROUP_ID_NOT_FOUND);

    let classic_leave = LeaveGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group_id.to_owned()),
        ))
        .with_members(vec![
            MemberIdentity::default().with_member_id(classic.member_id.clone()),
        ]);
    let classic_leave: LeaveGroupResponse = response(
        ApiKey::LeaveGroup,
        5,
        broker
            .handle_request(frame(ApiKey::LeaveGroup, 5, &classic_leave))
            .await
            .unwrap(),
    );
    assert_eq!(classic_leave.members[0].error_code, NO_ERROR);

    let consumer: ConsumerGroupHeartbeatResponse = response(
        ApiKey::ConsumerGroupHeartbeat,
        1,
        broker
            .handle_request(frame(ApiKey::ConsumerGroupHeartbeat, 1, &consumer_join))
            .await
            .unwrap(),
    );
    assert_eq!(consumer.error_code, NO_ERROR);

    let active_consumer: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        3,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 3, &join_request(group_id, "")))
            .await
            .unwrap(),
    );
    assert_eq!(active_consumer.error_code, COORDINATOR_NOT_AVAILABLE);

    let consumer_leave = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group_id.to_owned()),
        ))
        .with_member_id(consumer.member_id.unwrap())
        .with_member_epoch(-1);
    let consumer_leave: ConsumerGroupHeartbeatResponse = response(
        ApiKey::ConsumerGroupHeartbeat,
        1,
        broker
            .handle_request(frame(ApiKey::ConsumerGroupHeartbeat, 1, &consumer_leave))
            .await
            .unwrap(),
    );
    assert_eq!(consumer_leave.error_code, NO_ERROR);

    let converted: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        3,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 3, &join_request(group_id, "")))
            .await
            .unwrap(),
    );
    assert_eq!(converted.error_code, NO_ERROR);
}

fn broker(metadata: Arc<MemoryMetadataStore>, initial_delay_ms: i32) -> Broker {
    broker_with_max_size(metadata, initial_delay_ms, i32::MAX)
}

fn broker_with_max_size(
    metadata: Arc<MemoryMetadataStore>,
    initial_delay_ms: i32,
    classic_group_max_size: i32,
) -> Broker {
    let config = AgentConfig {
        classic_group_initial_rebalance_delay_ms: initial_delay_ms,
        classic_group_min_session_timeout_ms: 1,
        classic_group_max_size,
        ..AgentConfig::default()
    };
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

async fn allocate(broker: &Broker, group_id: &str) -> StrBytes {
    let response: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        4,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 4, &join_request(group_id, "")))
            .await
            .unwrap(),
    );
    assert_eq!(response.error_code, MEMBER_ID_REQUIRED);
    response.member_id
}

fn join_request(group_id: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string(group_id.to_owned()),
        ))
        .with_session_timeout_ms(1_000)
        .with_rebalance_timeout_ms(1_000)
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
        .with_protocol_type(StrBytes::from_string("consumer".to_owned()))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_string("range".to_owned()))
                .with_metadata(Bytes::from_static(b"subscription")),
        ])
}

fn frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_string("classic-barrier".to_owned())))
        .encode(&mut payload, api_key.request_header_version(version))
        .unwrap();
    body.encode(&mut payload, version).unwrap();
    payload.freeze()
}

fn response<T: Decodable>(api_key: ApiKey, version: i16, mut frame: Bytes) -> T {
    let size = frame.get_i32() as usize;
    assert_eq!(size, frame.remaining());
    ResponseHeader::decode(&mut frame, api_key.response_header_version(version)).unwrap();
    T::decode(&mut frame, version).unwrap()
}
