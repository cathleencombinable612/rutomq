use super::authorization::AuthorizationContext;
use super::tests::broker;
use bytes::Bytes;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{GroupId, SyncGroupRequest};
use kafka_protocol::protocol::StrBytes;
use std::net::Ipv4Addr;
use std::time::Duration;

#[tokio::test]
async fn follower_sync_waits_for_the_leader_assignment() {
    let broker = broker();
    let leader = broker
        .metadata
        .join_group(
            "streams-app",
            "",
            None,
            "stream",
            &[("stream".to_owned(), vec![1])],
            ("leader-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    let follower = broker
        .metadata
        .join_group(
            "streams-app",
            "",
            None,
            "stream",
            &[("stream".to_owned(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    let leader = broker
        .metadata
        .join_group(
            "streams-app",
            &leader.member_id,
            None,
            "stream",
            &[("stream".to_owned(), vec![1])],
            ("leader-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(leader.generation_id, follower.generation_id);

    let follower_request = SyncGroupRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("streams-app")))
        .with_generation_id(follower.generation_id)
        .with_member_id(StrBytes::from_string(follower.member_id.clone()))
        .with_protocol_type(Some(StrBytes::from_static_str("stream")))
        .with_protocol_name(Some(StrBytes::from_static_str("stream")));
    let follower_broker = broker.clone();
    let follower_sync = tokio::spawn(async move {
        follower_broker
            .handle_sync_group(
                follower_request,
                &AuthorizationContext::anonymous(Ipv4Addr::LOCALHOST.into()),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let expected = vec![9, 8, 7];
    let leader_request = SyncGroupRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("streams-app")))
        .with_generation_id(leader.generation_id)
        .with_member_id(StrBytes::from_string(leader.member_id.clone()))
        .with_protocol_type(Some(StrBytes::from_static_str("stream")))
        .with_protocol_name(Some(StrBytes::from_static_str("stream")))
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(StrBytes::from_string(leader.member_id))
                .with_assignment(Bytes::from_static(b"leader")),
            SyncGroupRequestAssignment::default()
                .with_member_id(StrBytes::from_string(follower.member_id))
                .with_assignment(Bytes::from(expected.clone())),
        ]);
    let leader_response = broker
        .handle_sync_group(
            leader_request,
            &AuthorizationContext::anonymous(Ipv4Addr::LOCALHOST.into()),
        )
        .await;
    assert_eq!(leader_response.error_code, 0);

    let follower_response = follower_sync.await.unwrap();
    assert_eq!(follower_response.error_code, 0);
    assert_eq!(follower_response.assignment.as_ref(), expected);
}
