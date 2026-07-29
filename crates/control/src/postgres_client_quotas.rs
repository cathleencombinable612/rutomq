use crate::{ClientQuota, ClientQuotaAlteration, ClientQuotaEntity, ControlError};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) async fn describe(pool: &PgPool) -> Result<Vec<ClientQuota>, ControlError> {
    let rows = sqlx::query(
        "SELECT has_user, user_name, has_client_id, client_id, has_ip, ip, quota_key, quota_value
         FROM client_quotas
         ORDER BY entity_key, quota_key",
    )
    .fetch_all(pool)
    .await?;

    let mut quotas = BTreeMap::<ClientQuotaEntity, BTreeMap<String, f64>>::new();
    for row in rows {
        let entity = ClientQuotaEntity {
            user: dimension(row.get("has_user"), row.get("user_name")),
            client_id: dimension(row.get("has_client_id"), row.get("client_id")),
            ip: dimension(row.get("has_ip"), row.get("ip")),
        };
        quotas
            .entry(entity)
            .or_default()
            .insert(row.get("quota_key"), row.get("quota_value"));
    }
    Ok(quotas
        .into_iter()
        .map(|(entity, values)| ClientQuota { entity, values })
        .collect())
}

pub(crate) async fn alter(
    pool: &PgPool,
    alterations: Vec<ClientQuotaAlteration>,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    for alteration in alterations {
        let entity_key = alteration.entity.storage_key();
        for (quota_key, value) in alteration.ops {
            if let Some(value) = value {
                sqlx::query(
                    "INSERT INTO client_quotas (
                         entity_key, has_user, user_name, has_client_id, client_id,
                         has_ip, ip, quota_key, quota_value
                     )
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (entity_key, quota_key) DO UPDATE
                     SET quota_value = EXCLUDED.quota_value",
                )
                .bind(&entity_key)
                .bind(alteration.entity.user.is_some())
                .bind(alteration.entity.user.as_ref().and_then(Clone::clone))
                .bind(alteration.entity.client_id.is_some())
                .bind(alteration.entity.client_id.as_ref().and_then(Clone::clone))
                .bind(alteration.entity.ip.is_some())
                .bind(alteration.entity.ip.as_ref().and_then(Clone::clone))
                .bind(quota_key)
                .bind(value)
                .execute(&mut *transaction)
                .await?;
            } else {
                sqlx::query(
                    "DELETE FROM client_quotas
                     WHERE entity_key = $1 AND quota_key = $2",
                )
                .bind(&entity_key)
                .bind(quota_key)
                .execute(&mut *transaction)
                .await?;
            }
        }
    }
    transaction.commit().await?;
    Ok(())
}

fn dimension(present: bool, value: Option<String>) -> Option<Option<String>> {
    present.then_some(value)
}
