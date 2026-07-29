use super::acl_tests::{acl_broker, decode_response, handle_as};
use super::*;
use crate::kafka_error::{GROUP_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR};
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::{
    ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, GroupId, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, StreamsGroupHeartbeatRequest,
    StreamsGroupHeartbeatResponse, SyncGroupRequest, SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;

#[tokio::test]
async fn membership_authorization_backend_failures_are_server_errors() {
    let (broker, metadata) = acl_broker();
    metadata.set_authorization_failure(true);

    assert_membership_errors(&broker, UNKNOWN_SERVER_ERROR).await;
}

#[tokio::test]
async fn explicit_membership_authorization_denials_remain_group_errors() {
    let (broker, _) = acl_broker();

    assert_membership_errors(&broker, GROUP_AUTHORIZATION_FAILED).await;
}

async fn assert_membership_errors(broker: &Broker, expected: i16) {
    let join = JoinGroupRequest::default()
        .with_group_id(group_id("join-auth"))
        .with_member_id(string("join-member"));
    let response = handle_as(broker, "reader", ApiKey::JoinGroup, 9, 7901, &join).await;
    let response: JoinGroupResponse = decode_response(ApiKey::JoinGroup, 9, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(response.member_id.as_str(), "join-member");

    let sync = SyncGroupRequest::default()
        .with_group_id(group_id("sync-auth"))
        .with_member_id(string("sync-member"))
        .with_protocol_type(Some(string("consumer")))
        .with_protocol_name(Some(string("range")));
    let response = handle_as(broker, "reader", ApiKey::SyncGroup, 5, 7902, &sync).await;
    let response: SyncGroupResponse = decode_response(ApiKey::SyncGroup, 5, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(
        response.protocol_type.as_ref().unwrap().as_str(),
        "consumer"
    );
    assert_eq!(response.protocol_name.as_ref().unwrap().as_str(), "range");

    let heartbeat = HeartbeatRequest::default()
        .with_group_id(group_id("heartbeat-auth"))
        .with_member_id(string("heartbeat-member"));
    let response = handle_as(broker, "reader", ApiKey::Heartbeat, 4, 7903, &heartbeat).await;
    let response: HeartbeatResponse = decode_response(ApiKey::Heartbeat, 4, response);
    assert_eq!(response.error_code, expected);

    let leave = LeaveGroupRequest::default()
        .with_group_id(group_id("leave-auth"))
        .with_members(vec![
            MemberIdentity::default()
                .with_member_id(string("leave-member"))
                .with_group_instance_id(Some(string("leave-instance"))),
        ]);
    let response = handle_as(broker, "reader", ApiKey::LeaveGroup, 5, 7904, &leave).await;
    let response: LeaveGroupResponse = decode_response(ApiKey::LeaveGroup, 5, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(response.members.len(), 1);
    assert_eq!(response.members[0].member_id.as_str(), "leave-member");
    assert_eq!(response.members[0].error_code, expected);

    assert_modern_heartbeat_errors(broker, expected).await;
}

async fn assert_modern_heartbeat_errors(broker: &Broker, expected: i16) {
    let consumer = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group_id("consumer-auth"))
        .with_member_id(string("consumer-member"));
    let response = handle_as(
        broker,
        "reader",
        ApiKey::ConsumerGroupHeartbeat,
        1,
        7905,
        &consumer,
    )
    .await;
    let response: ConsumerGroupHeartbeatResponse =
        decode_response(ApiKey::ConsumerGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(
        response.member_id.as_ref().unwrap().as_str(),
        "consumer-member"
    );

    let streams = StreamsGroupHeartbeatRequest::default()
        .with_group_id(group_id("streams-auth"))
        .with_member_id(string("streams-member"));
    let response = handle_as(
        broker,
        "reader",
        ApiKey::StreamsGroupHeartbeat,
        0,
        7906,
        &streams,
    )
    .await;
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(response.member_id.as_str(), "streams-member");

    let share = ShareGroupHeartbeatRequest::default()
        .with_group_id(group_id("share-auth"))
        .with_member_id(string("share-member"));
    let response = handle_as(
        broker,
        "reader",
        ApiKey::ShareGroupHeartbeat,
        1,
        7907,
        &share,
    )
    .await;
    let response: ShareGroupHeartbeatResponse =
        decode_response(ApiKey::ShareGroupHeartbeat, 1, response);
    assert_eq!(response.error_code, expected);
    assert_eq!(
        response.member_id.as_ref().unwrap().as_str(),
        "share-member"
    );
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(string(value))
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
