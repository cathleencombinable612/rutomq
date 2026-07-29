use rutomq_control::{
    ClientQuotaAlteration, ClientQuotaEntity, MetadataStore, PostgresMetadataStore,
};
use std::collections::BTreeMap;
use uuid::Uuid;

#[tokio::test]
async fn postgres_client_quotas_are_atomic_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let user = format!("quota-{}", Uuid::new_v4().simple());
    let entity = ClientQuotaEntity {
        user: Some(Some(user)),
        client_id: Some(None),
        ip: None,
    };
    store
        .alter_client_quotas(vec![ClientQuotaAlteration {
            entity: entity.clone(),
            ops: BTreeMap::from([
                ("producer_byte_rate".to_owned(), Some(1_024.0)),
                ("consumer_byte_rate".to_owned(), Some(2_048.0)),
            ]),
        }])
        .await
        .unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let quota = reconnected
        .client_quotas()
        .await
        .unwrap()
        .into_iter()
        .find(|quota| quota.entity == entity)
        .unwrap();
    assert_eq!(quota.values["producer_byte_rate"], 1_024.0);
    assert_eq!(quota.values["consumer_byte_rate"], 2_048.0);

    reconnected
        .alter_client_quotas(vec![ClientQuotaAlteration {
            entity: entity.clone(),
            ops: BTreeMap::from([
                ("producer_byte_rate".to_owned(), Some(4_096.0)),
                ("consumer_byte_rate".to_owned(), None),
            ]),
        }])
        .await
        .unwrap();
    let quota = reconnected
        .client_quotas()
        .await
        .unwrap()
        .into_iter()
        .find(|quota| quota.entity == entity)
        .unwrap();
    assert_eq!(
        quota.values,
        BTreeMap::from([("producer_byte_rate".to_owned(), 4_096.0)])
    );
}
