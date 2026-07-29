use rutomq_control::{
    BatchDraft, ControlError, MetadataStore, ObjectRef, PartitionKey, PostgresMetadataStore,
    ShareGroupHeartbeat, ShareOffsetUpdate,
};
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn postgres_share_offset_admin_resets_lists_and_deletes_state() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("share-offset-topic-{suffix}");
    let group_id = format!("share-offset-group-{suffix}");
    store.create_topic(&topic_name, 2).await.unwrap();
    let object = ObjectRef {
        key: format!("share-offset-object-{suffix}"),
        size: 3,
    };
    store.stage_object(object.clone()).await.unwrap();
    store
        .commit_object(
            object,
            vec![BatchDraft {
                partition: PartitionKey::new(&topic_name, 0),
                byte_start: 0,
                byte_end: 3,
                record_count: 3,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();

    let results = store
        .alter_share_group_offsets(
            &group_id,
            &[
                ShareOffsetUpdate {
                    partition: PartitionKey::new(&topic_name, 0),
                    start_offset: 1,
                },
                ShareOffsetUpdate {
                    partition: PartitionKey::new(&topic_name, 8),
                    start_offset: 0,
                },
                ShareOffsetUpdate {
                    partition: PartitionKey::new("missing", 0),
                    start_offset: 0,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.updated)
            .collect::<Vec<_>>(),
        [true, false, false]
    );
    let explicit = store
        .describe_share_group_offsets(
            &group_id,
            Some(&[
                PartitionKey::new(&topic_name, 0),
                PartitionKey::new(&topic_name, 1),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(
        explicit
            .iter()
            .map(|offset| (offset.start_offset, offset.high_watermark, offset.lag()))
            .collect::<Vec<_>>(),
        [(1, 3, 2), (-1, 0, -1)]
    );
    assert_eq!(
        store
            .describe_share_group_offsets(&group_id, None)
            .await
            .unwrap()
            .len(),
        1
    );

    let joined = store
        .share_group_heartbeat(heartbeat(&group_id, 0, Some(&topic_name)))
        .await
        .unwrap();
    assert!(matches!(
        store
            .alter_share_group_offsets(
                &group_id,
                &[ShareOffsetUpdate {
                    partition: PartitionKey::new(&topic_name, 0),
                    start_offset: 2,
                }],
            )
            .await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    let pool = PgPool::connect(&database_url).await.unwrap();
    sqlx::query(
        "UPDATE share_group_members
         SET last_heartbeat = now() - interval '1 minute'
         WHERE group_id = $1",
    )
    .bind(&group_id)
    .execute(&pool)
    .await
    .unwrap();
    let expired_reset = store
        .alter_share_group_offsets(
            &group_id,
            &[ShareOffsetUpdate {
                partition: PartitionKey::new(&topic_name, 0),
                start_offset: 2,
            }],
        )
        .await
        .unwrap();
    assert!(expired_reset[0].updated);
    assert!(
        store
            .describe_share_groups(std::slice::from_ref(&group_id))
            .await
            .unwrap()[&group_id]
            .members
            .is_empty()
    );
    store
        .share_group_heartbeat(heartbeat(&group_id, -1, None))
        .await
        .unwrap();
    assert!(joined.member_epoch > 0);

    let deleted = store
        .delete_share_group_offsets(&group_id, std::slice::from_ref(&topic_name))
        .await
        .unwrap();
    assert!(deleted[0].deleted);
    assert!(
        store
            .describe_share_group_offsets(&group_id, None)
            .await
            .unwrap()
            .is_empty()
    );
    store.delete_group(&group_id).await.unwrap();
}

fn heartbeat(group_id: &str, epoch: i32, subscription: Option<&str>) -> ShareGroupHeartbeat {
    ShareGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: "member-a".to_owned(),
        member_epoch: epoch,
        rack_id: None,
        subscribed_topic_names: subscription.map(|topic| vec![topic.to_owned()]),
        client_id: "postgres-share-offset-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        assignment_interval_ms: 0,
        max_size: 200,
    }
}
