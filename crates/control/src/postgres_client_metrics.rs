use crate::client_metrics::apply_alteration;
use crate::{ClientMetricConfigAlteration, ClientMetricSubscription, ControlError};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn list(pool: &PgPool) -> Result<Vec<ClientMetricSubscription>, ControlError> {
    let rows = sqlx::query(
        "SELECT subscription_name, config_key, config_value
         FROM client_metric_configs
         ORDER BY subscription_name, config_key",
    )
    .fetch_all(pool)
    .await?;
    Ok(group_rows(rows))
}

pub(crate) async fn get(
    pool: &PgPool,
    name: &str,
) -> Result<Option<ClientMetricSubscription>, ControlError> {
    let rows = sqlx::query(
        "SELECT subscription_name, config_key, config_value
         FROM client_metric_configs
         WHERE subscription_name = $1
         ORDER BY config_key",
    )
    .bind(name)
    .fetch_all(pool)
    .await?;
    Ok(group_rows(rows).pop())
}

pub(crate) async fn alter(
    pool: &PgPool,
    alteration: ClientMetricConfigAlteration,
    validate_only: bool,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&alteration.name)
        .execute(&mut *transaction)
        .await?;
    let rows = sqlx::query(
        "SELECT subscription_name, config_key, config_value
         FROM client_metric_configs
         WHERE subscription_name = $1
         ORDER BY config_key",
    )
    .bind(&alteration.name)
    .fetch_all(&mut *transaction)
    .await?;
    let proposed = apply_alteration(group_rows(rows).pop(), &alteration)?;
    if validate_only {
        transaction.rollback().await?;
        return Ok(());
    }

    sqlx::query("DELETE FROM client_metric_configs WHERE subscription_name = $1")
        .bind(&alteration.name)
        .execute(&mut *transaction)
        .await?;
    if let Some(subscription) = proposed {
        for (key, value) in subscription.configs {
            sqlx::query(
                "INSERT INTO client_metric_configs (
                     subscription_name, config_key, config_value
                 ) VALUES ($1, $2, $3)",
            )
            .bind(&subscription.name)
            .bind(key)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

fn group_rows(rows: Vec<sqlx::postgres::PgRow>) -> Vec<ClientMetricSubscription> {
    let mut subscriptions = BTreeMap::<String, BTreeMap<String, String>>::new();
    for row in rows {
        subscriptions
            .entry(row.get("subscription_name"))
            .or_default()
            .insert(row.get("config_key"), row.get("config_value"));
    }
    subscriptions
        .into_iter()
        .map(|(name, configs)| ClientMetricSubscription { name, configs })
        .collect()
}
