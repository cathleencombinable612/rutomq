use rutomq_control::{ControlError, DelegationToken, MemoryMetadataStore, MetadataStore};

fn token(id: &str, hmac: u8) -> DelegationToken {
    DelegationToken {
        token_id: id.to_owned(),
        owner_principal: "User:alice".to_owned(),
        requester_principal: "User:admin".to_owned(),
        renewers: vec![
            "User:".to_owned(),
            "User:bob".to_owned(),
            "User:bob".to_owned(),
        ],
        issue_timestamp_ms: 1_000,
        expiry_timestamp_ms: 2_000,
        max_timestamp_ms: 5_000,
        hmac: vec![hmac; 64],
    }
}

#[tokio::test]
async fn memory_delegation_token_lifecycle_matches_kafka_rules() {
    let store = MemoryMetadataStore::new();
    let token = token("token-a", 1);
    store.create_delegation_token(token.clone()).await.unwrap();

    assert_eq!(
        store
            .delegation_token_by_id("token-a", 1_500)
            .await
            .unwrap(),
        Some(token.clone())
    );
    assert!(format!("{token:?}").contains("<redacted>"));
    assert!(!format!("{token:?}").contains("[1, 1"));
    assert!(token.owner_or_renewer("User:admin"));
    assert!(token.owner_or_renewer("User:"));

    assert!(matches!(
        store
            .renew_delegation_token(&token.hmac, "User:mallory", 1_500, 1_000, 2_000)
            .await,
        Err(ControlError::DelegationTokenOwnerMismatch)
    ));
    assert_eq!(
        store
            .renew_delegation_token(&token.hmac, "User:admin", 1_500, 1_000, 2_000)
            .await
            .unwrap(),
        2_500
    );
    assert_eq!(
        store
            .renew_delegation_token(&token.hmac, "User:bob", 1_500, 10_000, 2_000)
            .await
            .unwrap(),
        3_500
    );
    assert_eq!(
        store
            .expire_delegation_token(&token.hmac, "User:admin", 2_000, 500)
            .await
            .unwrap(),
        2_500
    );
    assert_eq!(
        store
            .expire_delegation_token(&token.hmac, "User:bob", 2_100, -1)
            .await
            .unwrap(),
        2_100
    );
    assert!(
        store
            .delegation_token_by_id("token-a", 2_100)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn memory_delegation_token_rejects_expired_and_sweeps() {
    let store = MemoryMetadataStore::new();
    let expired = token("expired", 2);
    store
        .create_delegation_token(expired.clone())
        .await
        .unwrap();

    assert!(matches!(
        store
            .renew_delegation_token(&expired.hmac, "User:alice", 2_001, -1, 1_000)
            .await,
        Err(ControlError::DelegationTokenExpired)
    ));
    assert!(store.delegation_tokens(2_001).await.unwrap().is_empty());
    assert_eq!(
        store
            .delete_expired_delegation_tokens(2_001, 100)
            .await
            .unwrap(),
        1
    );
}
