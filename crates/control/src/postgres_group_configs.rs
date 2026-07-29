use crate::ControlError;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn get(
    pool: &PgPool,
    group_id: &str,
) -> Result<BTreeMap<String, String>, ControlError> {
    Ok(sqlx::query(
        "SELECT config_key, config_value
         FROM group_configs WHERE group_id = $1 ORDER BY config_key",
    )
    .bind(group_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.get("config_key"), row.get("config_value")))
    .collect())
}

pub(crate) async fn ids(pool: &PgPool) -> Result<Vec<String>, ControlError> {
    Ok(
        sqlx::query("SELECT DISTINCT group_id FROM group_configs ORDER BY group_id")
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.get("group_id"))
            .collect(),
    )
}

pub(crate) async fn alter(
    pool: &PgPool,
    group_id: &str,
    changes: BTreeMap<String, Option<String>>,
    validate_only: bool,
) -> Result<(), ControlError> {
    if validate_only {
        return Ok(());
    }
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 2))")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    for (key, value) in changes {
        if let Some(value) = value {
            sqlx::query(
                "INSERT INTO group_configs
                     (group_id, config_key, config_value)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (group_id, config_key) DO UPDATE
                 SET config_value = EXCLUDED.config_value,
                     updated_at = now()",
            )
            .bind(group_id)
            .bind(key)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "DELETE FROM group_configs
                 WHERE group_id = $1 AND config_key = $2",
            )
            .bind(group_id)
            .bind(key)
            .execute(&mut *transaction)
            .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}
