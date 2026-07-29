use rutomq_control::{
    ControlError, GroupAssignment, GroupMemberIdentity, LeaveGroupMemberError, MetadataStore,
    PostgresMetadataStore,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_persists_classic_identity_protocols_and_static_replacement() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let dynamic_group = format!("classic-membership-{suffix}");
    let static_group = format!("classic-static-{suffix}");
    let client = ("postgres-classic-test", "127.0.0.1", &[][..], 45_000);

    let allocated = match store
        .join_group(
            &dynamic_group,
            "",
            None,
            "consumer",
            &[
                ("range".to_owned(), vec![1]),
                ("roundrobin".to_owned(), vec![2]),
            ],
            client,
            4,
        )
        .await
    {
        Err(ControlError::MemberIdRequired { member_id }) => member_id,
        result => panic!("expected member id allocation, got {result:?}"),
    };
    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let first = reconnected
        .join_group(
            &dynamic_group,
            &allocated,
            None,
            "consumer",
            &[
                ("range".to_owned(), vec![1]),
                ("roundrobin".to_owned(), vec![2]),
            ],
            client,
            4,
        )
        .await
        .unwrap();
    assert_eq!(first.member_id, allocated);
    let refreshed = reconnected
        .join_group(
            &dynamic_group,
            &allocated,
            None,
            "consumer",
            &[
                ("range".to_owned(), vec![9]),
                ("roundrobin".to_owned(), vec![10]),
            ],
            client,
            4,
        )
        .await
        .unwrap();
    assert_eq!(refreshed.generation_id, first.generation_id);
    assert_eq!(refreshed.members[0].metadata, [9]);

    let second_allocated = match reconnected
        .join_group(
            &dynamic_group,
            "",
            None,
            "consumer",
            &[("roundrobin".to_owned(), vec![3])],
            client,
            4,
        )
        .await
    {
        Err(ControlError::MemberIdRequired { member_id }) => member_id,
        result => panic!("expected second member id allocation, got {result:?}"),
    };
    let second = reconnected
        .join_group(
            &dynamic_group,
            &second_allocated,
            None,
            "consumer",
            &[("roundrobin".to_owned(), vec![3])],
            client,
            4,
        )
        .await
        .unwrap();
    assert_eq!(second.protocol_name, "roundrobin");
    assert_eq!(second.members.len(), 2);

    let original = reconnected
        .join_group(
            &static_group,
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![7])],
            client,
            9,
        )
        .await
        .unwrap();
    reconnected
        .sync_group(
            &static_group,
            original.generation_id,
            &original.member_id,
            Some("instance-a"),
            vec![GroupAssignment {
                member_id: original.member_id.clone(),
                assignment: vec![8],
            }],
        )
        .await
        .unwrap();
    let replacement = reconnected
        .join_group(
            &static_group,
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![7])],
            client,
            9,
        )
        .await
        .unwrap();
    assert_eq!(replacement.generation_id, original.generation_id);
    assert!(replacement.skip_assignment);
    assert_eq!(
        reconnected
            .sync_group(
                &static_group,
                replacement.generation_id,
                &replacement.member_id,
                Some("instance-a"),
                Vec::new(),
            )
            .await
            .unwrap(),
        [8]
    );
    assert!(matches!(
        reconnected
            .heartbeat_group(
                &static_group,
                replacement.generation_id,
                &original.member_id,
                Some("instance-a"),
            )
            .await,
        Err(ControlError::FencedInstanceId { .. })
    ));
    let fenced = reconnected
        .leave_group(
            &static_group,
            &[GroupMemberIdentity {
                member_id: original.member_id,
                group_instance_id: Some("instance-a".to_owned()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        fenced[0].error,
        Some(LeaveGroupMemberError::FencedInstanceId)
    );
    let removed = reconnected
        .leave_group(
            &static_group,
            &[GroupMemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-a".to_owned()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(removed[0].error, None);
}
