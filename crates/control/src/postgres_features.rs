use crate::features::apply_updates;
use crate::{ControlError, FeatureLevelUpdate, FeatureMetadata};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn describe(pool: &PgPool) -> Result<FeatureMetadata, ControlError> {
    let rows = sqlx::query(
        "SELECT state.epoch, feature.feature_name, feature.version_level
         FROM cluster_feature_state state
         LEFT JOIN cluster_features feature ON TRUE
         WHERE state.singleton = TRUE
         ORDER BY feature.feature_name",
    )
    .fetch_all(pool)
    .await?;
    metadata_from_rows(&rows)
}

pub(crate) async fn update(
    pool: &PgPool,
    updates: Vec<FeatureLevelUpdate>,
    validate_only: bool,
) -> Result<FeatureMetadata, ControlError> {
    let mut transaction = pool.begin().await?;
    let epoch: i64 = sqlx::query_scalar(
        "SELECT epoch
         FROM cluster_feature_state
         WHERE singleton = TRUE
         FOR UPDATE",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let rows = sqlx::query(
        "SELECT feature_name, version_level
         FROM cluster_features
         ORDER BY feature_name",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let current = rows
        .into_iter()
        .map(|row| (row.get("feature_name"), row.get("version_level")))
        .collect::<BTreeMap<String, i16>>();
    let proposed = apply_updates(&current, &updates)?;
    let changed = proposed != current;

    if !validate_only && changed {
        for update in updates {
            if update.max_version_level == 0 {
                sqlx::query("DELETE FROM cluster_features WHERE feature_name = $1")
                    .bind(update.name)
                    .execute(&mut *transaction)
                    .await?;
            } else {
                sqlx::query(
                    "INSERT INTO cluster_features (feature_name, version_level)
                     VALUES ($1, $2)
                     ON CONFLICT (feature_name) DO UPDATE
                     SET version_level = EXCLUDED.version_level",
                )
                .bind(update.name)
                .bind(update.max_version_level)
                .execute(&mut *transaction)
                .await?;
            }
        }
        sqlx::query(
            "UPDATE cluster_feature_state
             SET epoch = epoch + 1
             WHERE singleton = TRUE",
        )
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;

    Ok(FeatureMetadata {
        epoch: epoch + i64::from(!validate_only && changed),
        finalized: if validate_only { current } else { proposed },
    })
}

fn metadata_from_rows(rows: &[sqlx::postgres::PgRow]) -> Result<FeatureMetadata, ControlError> {
    let epoch = rows
        .first()
        .ok_or_else(|| ControlError::InvalidRequest("feature state is missing".to_owned()))?
        .get("epoch");
    let finalized = rows
        .iter()
        .filter_map(|row| {
            let name = row.get::<Option<String>, _>("feature_name")?;
            Some((name, row.get("version_level")))
        })
        .collect();
    Ok(FeatureMetadata { epoch, finalized })
}
