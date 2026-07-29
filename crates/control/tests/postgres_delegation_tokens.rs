use rutomq_control::{ControlError, DelegationToken, MetadataStore, PostgresMetadataStore};
use uuid::Uuid;

#[tokio::test]
async fn postgres_delegation_tokens_are_atomic_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let id = format!("token-{}", Uuid::new_v4().simple());
    let token = DelegationToken {
        token_id: id.clone(),
        owner_principal: "User:alice".to_owned(),
        requester_principal: "User:admin".to_owned(),
        renewers: vec![
            "User:".to_owned(),
            "User:bob".to_owned(),
            "User:bob".to_owned(),
        ],
        issue_timestamp_ms: 10_000,
        expiry_timestamp_ms: 20_000,
        max_timestamp_ms: 50_000,
        hmac: Uuid::new_v4().as_bytes().repeat(4),
    };
    store.create_delegation_token(token.clone()).await.unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected
            .delegation_token_by_id(&id, 15_000)
            .await
            .unwrap(),
        Some(token.clone())
    );
    assert!(matches!(
        reconnected
            .renew_delegation_token(&token.hmac, "User:mallory", 15_000, 1_000, 2_000)
            .await,
        Err(ControlError::DelegationTokenOwnerMismatch)
    ));
    assert_eq!(
        reconnected
            .renew_delegation_token(&token.hmac, "User:admin", 15_000, 1_000, 2_000)
            .await
            .unwrap(),
        16_000
    );
    assert_eq!(
        reconnected
            .renew_delegation_token(&token.hmac, "User:bob", 15_000, 10_000, 2_000)
            .await
            .unwrap(),
        17_000
    );
    assert_eq!(
        reconnected
            .expire_delegation_token(&token.hmac, "User:admin", 15_500, -1)
            .await
            .unwrap(),
        15_500
    );
    assert!(
        reconnected
            .delegation_token_by_id(&id, 15_500)
            .await
            .unwrap()
            .is_none()
    );
}
