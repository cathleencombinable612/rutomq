use rutomq_control::{
    MetadataStore, PostgresMetadataStore, ScramCredential, ScramCredentialAlteration,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_scram_credentials_are_atomic_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let user = format!("scram-{}", Uuid::new_v4().simple());
    let credential = ScramCredential {
        user: user.clone(),
        mechanism: 1,
        iterations: 4096,
        salt: vec![1, 2, 3],
        stored_key: vec![4; 32],
        server_key: vec![5; 32],
    };
    assert!(
        store
            .alter_scram_credentials(vec![ScramCredentialAlteration::Upsert(credential.clone())])
            .await
            .unwrap()
            .is_empty()
    );

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(
        reconnected
            .scram_credentials(Some(std::slice::from_ref(&user)))
            .await
            .unwrap(),
        [credential]
    );
    assert!(
        reconnected
            .alter_scram_credentials(vec![ScramCredentialAlteration::Delete {
                user: user.clone(),
                mechanism: 1,
            }])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reconnected
            .alter_scram_credentials(vec![ScramCredentialAlteration::Delete {
                user: user.clone(),
                mechanism: 1,
            }])
            .await
            .unwrap(),
        [user].into()
    );
}
