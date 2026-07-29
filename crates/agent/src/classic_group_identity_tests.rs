use super::*;
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, RequestHeader, ResponseHeader, SyncGroupRequest,
    SyncGroupResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn broker() -> Broker {
    let config = AgentConfig {
        classic_group_initial_rebalance_delay_ms: 0,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_string("classic-wire".to_owned())))
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

fn join_request(member_id: &str, instance_id: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("classic-wire-group".to_owned()),
        ))
        .with_session_timeout_ms(45_000)
        .with_rebalance_timeout_ms(45_000)
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
        .with_group_instance_id(instance_id.map(|value| StrBytes::from_string(value.to_owned())))
        .with_protocol_type(StrBytes::from_string("consumer".to_owned()))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_string("range".to_owned()))
                .with_metadata(Bytes::from_static(b"subscription")),
        ])
}

#[tokio::test]
async fn join_group_v4_requires_the_allocated_dynamic_member_id() {
    let broker = broker();
    let first: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        4,
        broker
            .handle_request(frame(ApiKey::JoinGroup, 4, &join_request("", None)))
            .await
            .unwrap(),
    );
    assert_eq!(first.error_code, MEMBER_ID_REQUIRED);

    let unknown: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        4,
        broker
            .handle_request(frame(
                ApiKey::JoinGroup,
                4,
                &join_request("not-allocated", None),
            ))
            .await
            .unwrap(),
    );
    assert_eq!(unknown.error_code, UNKNOWN_MEMBER_ID);

    let joined: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        4,
        broker
            .handle_request(frame(
                ApiKey::JoinGroup,
                4,
                &join_request(first.member_id.as_str(), None),
            ))
            .await
            .unwrap(),
    );
    assert_eq!(joined.error_code, NO_ERROR);
    assert_eq!(joined.member_id, first.member_id);
}

#[tokio::test]
async fn static_replacement_and_leave_group_identity_are_fenced_on_the_wire() {
    let broker = broker();
    let original: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        9,
        broker
            .handle_request(frame(
                ApiKey::JoinGroup,
                9,
                &join_request("", Some("instance-a")),
            ))
            .await
            .unwrap(),
    );
    assert_eq!(original.error_code, NO_ERROR);
    let sync = SyncGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("classic-wire-group".to_owned()),
        ))
        .with_generation_id(original.generation_id)
        .with_member_id(original.member_id.clone())
        .with_group_instance_id(Some(StrBytes::from_string("instance-a".to_owned())))
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(original.member_id.clone())
                .with_assignment(Bytes::from_static(b"assignment")),
        ]);
    let synced: SyncGroupResponse = response(
        ApiKey::SyncGroup,
        5,
        broker
            .handle_request(frame(ApiKey::SyncGroup, 5, &sync))
            .await
            .unwrap(),
    );
    assert_eq!(synced.error_code, NO_ERROR);

    let replacement: JoinGroupResponse = response(
        ApiKey::JoinGroup,
        9,
        broker
            .handle_request(frame(
                ApiKey::JoinGroup,
                9,
                &join_request("", Some("instance-a")),
            ))
            .await
            .unwrap(),
    );
    assert_eq!(replacement.error_code, NO_ERROR);
    assert_ne!(replacement.member_id, original.member_id);
    assert_eq!(replacement.generation_id, original.generation_id);
    assert!(replacement.skip_assignment);

    let heartbeat = HeartbeatRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("classic-wire-group".to_owned()),
        ))
        .with_generation_id(replacement.generation_id)
        .with_member_id(original.member_id.clone())
        .with_group_instance_id(Some(StrBytes::from_string("instance-a".to_owned())));
    let heartbeat: HeartbeatResponse = response(
        ApiKey::Heartbeat,
        4,
        broker
            .handle_request(frame(ApiKey::Heartbeat, 4, &heartbeat))
            .await
            .unwrap(),
    );
    assert_eq!(heartbeat.error_code, FENCED_INSTANCE_ID);

    let leave = LeaveGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("classic-wire-group".to_owned()),
        ))
        .with_members(vec![
            MemberIdentity::default()
                .with_member_id(original.member_id)
                .with_group_instance_id(Some(StrBytes::from_string("instance-a".to_owned()))),
            MemberIdentity::default()
                .with_group_instance_id(Some(StrBytes::from_string("unknown-instance".to_owned()))),
        ]);
    let leave: LeaveGroupResponse = response(
        ApiKey::LeaveGroup,
        5,
        broker
            .handle_request(frame(ApiKey::LeaveGroup, 5, &leave))
            .await
            .unwrap(),
    );
    assert_eq!(leave.error_code, NO_ERROR);
    assert_eq!(leave.members[0].error_code, FENCED_INSTANCE_ID);
    assert_eq!(leave.members[1].error_code, UNKNOWN_MEMBER_ID);

    let admin_leave = LeaveGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("classic-wire-group".to_owned()),
        ))
        .with_members(vec![MemberIdentity::default().with_group_instance_id(
            Some(StrBytes::from_string("instance-a".to_owned())),
        )]);
    let admin_leave: LeaveGroupResponse = response(
        ApiKey::LeaveGroup,
        5,
        broker
            .handle_request(frame(ApiKey::LeaveGroup, 5, &admin_leave))
            .await
            .unwrap(),
    );
    assert_eq!(admin_leave.members[0].error_code, NO_ERROR);
}
