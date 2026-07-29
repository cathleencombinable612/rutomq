use rutomq_control::{
    AclFilter, AclOperation, AclPatternFilter, AclPatternType, AclPermission, AclResourceType,
    AclRule, MetadataStore, PostgresMetadataStore,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_acl_crud_and_authorization_are_consistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let prefix = format!("orders-{suffix}-");
    let principal = format!("User:alice-{suffix}");
    let denied_principal = format!("User:bob-{suffix}");
    let allow = AclRule {
        resource_type: AclResourceType::Topic,
        resource_name: prefix.clone(),
        pattern_type: AclPatternType::Prefixed,
        principal: principal.clone(),
        host: "*".to_owned(),
        operation: AclOperation::Write,
        permission: AclPermission::Allow,
    };
    store.create_acl(allow.clone()).await.unwrap();
    store.create_acl(allow.clone()).await.unwrap();
    assert!(
        store
            .authorize_by_resource_type(
                &principal,
                "127.0.0.1",
                AclResourceType::Topic,
                AclOperation::Write,
                false,
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .authorize_by_resource_type(
                &denied_principal,
                "127.0.0.1",
                AclResourceType::Topic,
                AclOperation::Write,
                false,
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .authorize(
                &principal,
                "127.0.0.1",
                AclResourceType::Topic,
                &format!("{prefix}eu"),
                AclOperation::Describe,
                false,
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .authorize(
                &denied_principal,
                "127.0.0.1",
                AclResourceType::Topic,
                &format!("{prefix}eu"),
                AclOperation::Write,
                true,
            )
            .await
            .unwrap()
    );

    let two_phase = AclRule {
        resource_type: AclResourceType::TransactionalId,
        resource_name: format!("orders-tx-{suffix}"),
        pattern_type: AclPatternType::Literal,
        principal: "User:alice".to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::TwoPhaseCommit,
        permission: AclPermission::Allow,
    };
    store.create_acl(two_phase.clone()).await.unwrap();
    assert!(
        store
            .authorize(
                "User:alice",
                "127.0.0.1",
                AclResourceType::TransactionalId,
                &two_phase.resource_name,
                AclOperation::TwoPhaseCommit,
                false,
            )
            .await
            .unwrap()
    );
    let two_phase_filter = AclFilter {
        resource_type: Some(AclResourceType::TransactionalId),
        resource_name: Some(two_phase.resource_name.clone()),
        pattern_type: AclPatternFilter::Literal,
        principal: Some("User:alice".to_owned()),
        host: None,
        operation: Some(AclOperation::TwoPhaseCommit),
        permission: Some(AclPermission::Allow),
    };
    assert_eq!(
        store.describe_acls(&two_phase_filter).await.unwrap(),
        [two_phase.clone()]
    );

    let filter = AclFilter {
        resource_type: Some(AclResourceType::Topic),
        resource_name: Some(format!("{prefix}eu")),
        pattern_type: AclPatternFilter::Match,
        principal: Some(principal.clone()),
        host: None,
        operation: None,
        permission: None,
    };
    assert_eq!(store.describe_acls(&filter).await.unwrap(), [allow.clone()]);
    assert_eq!(
        store
            .delete_acls(std::slice::from_ref(&filter))
            .await
            .unwrap(),
        [vec![allow]]
    );
    assert!(store.describe_acls(&filter).await.unwrap().is_empty());
    assert!(
        !store
            .authorize_by_resource_type(
                &principal,
                "127.0.0.1",
                AclResourceType::Topic,
                AclOperation::Write,
                false,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .delete_acls(std::slice::from_ref(&two_phase_filter))
            .await
            .unwrap(),
        [vec![two_phase]]
    );
}
