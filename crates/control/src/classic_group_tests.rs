use crate::{
    ControlError, GroupAssignment, GroupMemberIdentity, LeaveGroupMemberError, MemoryMetadataStore,
    MetadataStore,
};

const CLIENT: (&str, &str, &[String], i32) = ("classic-test", "127.0.0.1", &[], 45_000);

#[tokio::test]
async fn dynamic_members_complete_the_version_four_member_id_handshake() {
    let store = MemoryMetadataStore::new();
    let allocated = match store
        .join_group(
            "dynamic-v4",
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            CLIENT,
            4,
        )
        .await
    {
        Err(ControlError::MemberIdRequired { member_id }) => member_id,
        result => panic!("expected member id allocation, got {result:?}"),
    };

    assert!(matches!(
        store
            .join_group(
                "dynamic-v4",
                "not-allocated",
                None,
                "consumer",
                &[("range".to_owned(), vec![1])],
                CLIENT,
                4,
            )
            .await,
        Err(ControlError::GroupMemberNotFound { .. })
    ));
    let joined = store
        .join_group(
            "dynamic-v4",
            &allocated,
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            CLIENT,
            4,
        )
        .await
        .unwrap();
    assert_eq!(joined.member_id, allocated);
    assert_eq!(joined.generation_id, 1);
}

#[tokio::test]
async fn classic_group_protocol_is_supported_by_every_member() {
    let store = MemoryMetadataStore::new();
    let first = store
        .join_group(
            "protocol-vote",
            "",
            None,
            "consumer",
            &[
                ("range".to_owned(), vec![1]),
                ("roundrobin".to_owned(), vec![2]),
            ],
            CLIENT,
            3,
        )
        .await
        .unwrap();
    let second = store
        .join_group(
            "protocol-vote",
            "",
            None,
            "consumer",
            &[("roundrobin".to_owned(), vec![3])],
            CLIENT,
            3,
        )
        .await
        .unwrap();
    assert_eq!(second.protocol_name, "roundrobin");
    assert_eq!(second.members.len(), 2);
    assert_eq!(
        second
            .members
            .iter()
            .find(|member| member.member_id == first.member_id)
            .unwrap()
            .metadata,
        [2]
    );
    assert!(matches!(
        store
            .join_group(
                "protocol-vote",
                "",
                None,
                "consumer",
                &[("cooperative-sticky".to_owned(), vec![4])],
                CLIENT,
                3,
            )
            .await,
        Err(ControlError::InconsistentGroupProtocol(_))
    ));
}

#[tokio::test]
async fn protocol_metadata_refresh_does_not_start_another_generation() {
    let store = MemoryMetadataStore::new();
    let joined = store
        .join_group(
            "metadata-refresh",
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            CLIENT,
            3,
        )
        .await
        .unwrap();
    let refreshed = store
        .join_group(
            "metadata-refresh",
            &joined.member_id,
            None,
            "consumer",
            &[("range".to_owned(), vec![2])],
            CLIENT,
            3,
        )
        .await
        .unwrap();

    assert_eq!(refreshed.generation_id, joined.generation_id);
    assert_eq!(refreshed.members.len(), 1);
    assert_eq!(refreshed.members[0].metadata, [2]);
}

#[tokio::test]
async fn static_member_replacement_preserves_state_and_fences_the_old_identity() {
    let store = MemoryMetadataStore::new();
    let joined = store
        .join_group(
            "static-v9",
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![1])],
            CLIENT,
            9,
        )
        .await
        .unwrap();
    store
        .sync_group(
            "static-v9",
            joined.generation_id,
            &joined.member_id,
            Some("instance-a"),
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![9],
            }],
        )
        .await
        .unwrap();

    let replacement = store
        .join_group(
            "static-v9",
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![1])],
            CLIENT,
            9,
        )
        .await
        .unwrap();
    assert_ne!(replacement.member_id, joined.member_id);
    assert_eq!(replacement.generation_id, joined.generation_id);
    assert!(replacement.skip_assignment);
    assert_eq!(
        store
            .sync_group(
                "static-v9",
                replacement.generation_id,
                &replacement.member_id,
                Some("instance-a"),
                Vec::new(),
            )
            .await
            .unwrap(),
        [9]
    );
    assert!(matches!(
        store
            .heartbeat_group(
                "static-v9",
                replacement.generation_id,
                &joined.member_id,
                Some("instance-a"),
            )
            .await,
        Err(ControlError::FencedInstanceId { .. })
    ));

    let fenced = store
        .leave_group(
            "static-v9",
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: Some("instance-a".to_owned()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        fenced[0].error,
        Some(LeaveGroupMemberError::FencedInstanceId)
    );
    let removed = store
        .leave_group(
            "static-v9",
            &[GroupMemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-a".to_owned()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(removed[0].error, None);
}
