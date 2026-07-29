use rutomq_control::{
    CLIENT_METRICS_INTERVAL_MS, CLIENT_METRICS_MATCH, CLIENT_METRICS_METRICS,
    ClientMetricConfigAlteration, MetadataStore, PostgresMetadataStore,
};
use std::collections::BTreeMap;

fn alteration(
    name: &str,
    ops: impl IntoIterator<Item = (&'static str, Option<&'static str>)>,
) -> ClientMetricConfigAlteration {
    ClientMetricConfigAlteration {
        name: name.to_owned(),
        ops: ops
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.map(str::to_owned)))
            .collect::<BTreeMap<_, _>>(),
    }
}

#[tokio::test]
async fn postgres_client_metric_configs_are_validated_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let name = format!("telemetry-{}", uuid::Uuid::new_v4());

    store
        .alter_client_metric_subscription(
            alteration(
                &name,
                [
                    (CLIENT_METRICS_METRICS, Some("*")),
                    (CLIENT_METRICS_INTERVAL_MS, Some("100")),
                    (CLIENT_METRICS_MATCH, Some("client_id=flink-.*")),
                ],
            ),
            true,
        )
        .await
        .unwrap();
    assert!(
        store
            .client_metric_subscription(&name)
            .await
            .unwrap()
            .is_none()
    );

    store
        .alter_client_metric_subscription(
            alteration(
                &name,
                [
                    (CLIENT_METRICS_METRICS, Some("*")),
                    (CLIENT_METRICS_INTERVAL_MS, Some("100")),
                    (CLIENT_METRICS_MATCH, Some("client_id=flink-.*")),
                ],
            ),
            false,
        )
        .await
        .unwrap();
    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let stored = reconnected
        .client_metric_subscription(&name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.push_interval_ms(), 100);
    assert_eq!(stored.metrics(), vec!["*"]);

    let error = reconnected
        .alter_client_metric_subscription(
            alteration(&name, [(CLIENT_METRICS_INTERVAL_MS, Some("99"))]),
            false,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("between 100 and 3600000"));
    assert_eq!(
        reconnected
            .client_metric_subscription(&name)
            .await
            .unwrap()
            .unwrap(),
        stored
    );

    reconnected
        .alter_client_metric_subscription(
            alteration(
                &name,
                [
                    (CLIENT_METRICS_METRICS, None),
                    (CLIENT_METRICS_INTERVAL_MS, None),
                    (CLIENT_METRICS_MATCH, None),
                ],
            ),
            false,
        )
        .await
        .unwrap();
    assert!(
        reconnected
            .client_metric_subscription(&name)
            .await
            .unwrap()
            .is_none()
    );
}
