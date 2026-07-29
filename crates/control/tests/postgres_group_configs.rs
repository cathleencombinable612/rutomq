use rutomq_control::{MetadataStore, PostgresMetadataStore};
use std::collections::BTreeMap;
use uuid::Uuid;

#[tokio::test]
async fn postgres_group_configs_are_atomic_and_survive_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let group_id = format!("group-config-{}", Uuid::new_v4().simple());
    let changes = BTreeMap::from([
        (
            "streams.num.standby.replicas".to_owned(),
            Some("1".to_owned()),
        ),
        (
            "streams.heartbeat.interval.ms".to_owned(),
            Some("1000".to_owned()),
        ),
    ]);

    store
        .alter_group_config(&group_id, changes.clone(), true)
        .await
        .unwrap();
    assert!(store.group_config(&group_id).await.unwrap().is_empty());
    store
        .alter_group_config(&group_id, changes, false)
        .await
        .unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let config = reconnected.group_config(&group_id).await.unwrap();
    assert_eq!(config["streams.num.standby.replicas"], "1");
    assert!(
        reconnected
            .group_config_ids()
            .await
            .unwrap()
            .contains(&group_id)
    );
    reconnected
        .alter_group_config(
            &group_id,
            BTreeMap::from([
                ("streams.num.standby.replicas".to_owned(), None),
                ("streams.heartbeat.interval.ms".to_owned(), None),
            ]),
            false,
        )
        .await
        .unwrap();
    assert!(
        reconnected
            .group_config(&group_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn postgres_broker_configs_are_atomic_and_survive_reconnect() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let key = format!("test.assignment.interval.{}", Uuid::new_v4().simple());
    let change = BTreeMap::from([(key.clone(), Some("123".to_owned()))]);

    store
        .alter_broker_config(change.clone(), true)
        .await
        .unwrap();
    assert!(!store.broker_config().await.unwrap().contains_key(&key));
    store.alter_broker_config(change, false).await.unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(reconnected.broker_config().await.unwrap()[&key], "123");
    reconnected
        .alter_broker_config(BTreeMap::from([(key.clone(), None)]), false)
        .await
        .unwrap();
    assert!(
        !reconnected
            .broker_config()
            .await
            .unwrap()
            .contains_key(&key)
    );
}
