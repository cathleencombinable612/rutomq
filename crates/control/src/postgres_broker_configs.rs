use crate::ControlError;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn get(pool: &PgPool) -> Result<BTreeMap<String, String>, ControlError> {
    Ok(sqlx::query(
        "SELECT config_key, config_value
         FROM broker_configs ORDER BY config_key",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.get("config_key"), row.get("config_value")))
    .collect())
}

pub(crate) async fn alter(
    pool: &PgPool,
    changes: BTreeMap<String, Option<String>>,
    validate_only: bool,
) -> Result<(), ControlError> {
    if validate_only {
        return Ok(());
    }
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('broker-configs', 3))")
        .execute(&mut *transaction)
        .await?;
    for (key, value) in changes {
        if let Some(value) = value {
            sqlx::query(
                "INSERT INTO broker_configs (config_key, config_value)
                 VALUES ($1, $2)
                 ON CONFLICT (config_key) DO UPDATE
                 SET config_value = EXCLUDED.config_value,
                     updated_at = now()",
            )
            .bind(key)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query("DELETE FROM broker_configs WHERE config_key = $1")
                .bind(key)
                .execute(&mut *transaction)
                .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}
