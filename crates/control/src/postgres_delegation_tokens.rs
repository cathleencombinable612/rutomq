use crate::{ControlError, DelegationToken};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

pub(crate) async fn create(pool: &PgPool, token: DelegationToken) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO delegation_tokens (
             token_id, owner_principal, requester_principal, renewers,
             issue_timestamp_ms, expiry_timestamp_ms, max_timestamp_ms, hmac
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(token.token_id)
    .bind(token.owner_principal)
    .bind(token.requester_principal)
    .bind(token.renewers)
    .bind(token.issue_timestamp_ms)
    .bind(token.expiry_timestamp_ms)
    .bind(token.max_timestamp_ms)
    .bind(token.hmac)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn by_id(
    pool: &PgPool,
    token_id: &str,
    now_ms: i64,
) -> Result<Option<DelegationToken>, ControlError> {
    sqlx::query(
        "SELECT token_id, owner_principal, requester_principal, renewers,
                issue_timestamp_ms, expiry_timestamp_ms, max_timestamp_ms, hmac
         FROM delegation_tokens
         WHERE token_id = $1
           AND expiry_timestamp_ms >= $2
           AND max_timestamp_ms >= $2",
    )
    .bind(token_id)
    .bind(now_ms)
    .fetch_optional(pool)
    .await
    .map(|row| row.map(token_from_row))
    .map_err(Into::into)
}

pub(crate) async fn list(pool: &PgPool, now_ms: i64) -> Result<Vec<DelegationToken>, ControlError> {
    let rows = sqlx::query(
        "SELECT token_id, owner_principal, requester_principal, renewers,
                issue_timestamp_ms, expiry_timestamp_ms, max_timestamp_ms, hmac
         FROM delegation_tokens
         WHERE expiry_timestamp_ms >= $1
           AND max_timestamp_ms >= $1
         ORDER BY token_id",
    )
    .bind(now_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(token_from_row).collect())
}

pub(crate) async fn renew(
    pool: &PgPool,
    hmac: &[u8],
    principal: &str,
    now_ms: i64,
    requested_period_ms: i64,
    default_period_ms: i64,
) -> Result<i64, ControlError> {
    let mut transaction = pool.begin().await?;
    let row = locked_by_hmac(&mut transaction, hmac).await?;
    let token = row
        .map(token_from_row)
        .ok_or(ControlError::DelegationTokenNotFound)?;
    if token.is_expired(now_ms) {
        return Err(ControlError::DelegationTokenExpired);
    }
    if !token.owner_or_renewer(principal) {
        return Err(ControlError::DelegationTokenOwnerMismatch);
    }
    let period_ms = if requested_period_ms > 0 {
        requested_period_ms.min(default_period_ms)
    } else {
        default_period_ms
    };
    let expiry_timestamp_ms = token.max_timestamp_ms.min(now_ms.saturating_add(period_ms));
    sqlx::query(
        "UPDATE delegation_tokens
         SET expiry_timestamp_ms = $1
         WHERE token_id = $2",
    )
    .bind(expiry_timestamp_ms)
    .bind(token.token_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(expiry_timestamp_ms)
}

pub(crate) async fn expire(
    pool: &PgPool,
    hmac: &[u8],
    principal: &str,
    now_ms: i64,
    expiry_period_ms: i64,
) -> Result<i64, ControlError> {
    let mut transaction = pool.begin().await?;
    let row = locked_by_hmac(&mut transaction, hmac).await?;
    let token = row
        .map(token_from_row)
        .ok_or(ControlError::DelegationTokenNotFound)?;
    if !token.owner_or_renewer(principal) {
        return Err(ControlError::DelegationTokenOwnerMismatch);
    }
    if expiry_period_ms < 0 {
        sqlx::query("DELETE FROM delegation_tokens WHERE token_id = $1")
            .bind(token.token_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        return Ok(now_ms);
    }
    if token.is_expired(now_ms) {
        return Err(ControlError::DelegationTokenExpired);
    }
    let expiry_timestamp_ms = token
        .max_timestamp_ms
        .min(now_ms.saturating_add(expiry_period_ms));
    sqlx::query(
        "UPDATE delegation_tokens
         SET expiry_timestamp_ms = $1
         WHERE token_id = $2",
    )
    .bind(expiry_timestamp_ms)
    .bind(token.token_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(expiry_timestamp_ms)
}

pub(crate) async fn delete_expired(
    pool: &PgPool,
    now_ms: i64,
    limit: usize,
) -> Result<u64, ControlError> {
    let result = sqlx::query(
        "DELETE FROM delegation_tokens
         WHERE token_id IN (
             SELECT token_id
             FROM delegation_tokens
             WHERE expiry_timestamp_ms < $1 OR max_timestamp_ms < $1
             ORDER BY expiry_timestamp_ms
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )",
    )
    .bind(now_ms)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

async fn locked_by_hmac(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    hmac: &[u8],
) -> Result<Option<PgRow>, ControlError> {
    Ok(sqlx::query(
        "SELECT token_id, owner_principal, requester_principal, renewers,
                issue_timestamp_ms, expiry_timestamp_ms, max_timestamp_ms, hmac
         FROM delegation_tokens
         WHERE hmac = $1
         FOR UPDATE",
    )
    .bind(hmac)
    .fetch_optional(&mut **transaction)
    .await?)
}

fn token_from_row(row: PgRow) -> DelegationToken {
    DelegationToken {
        token_id: row.get("token_id"),
        owner_principal: row.get("owner_principal"),
        requester_principal: row.get("requester_principal"),
        renewers: row.get("renewers"),
        issue_timestamp_ms: row.get("issue_timestamp_ms"),
        expiry_timestamp_ms: row.get("expiry_timestamp_ms"),
        max_timestamp_ms: row.get("max_timestamp_ms"),
        hmac: row.get("hmac"),
    }
}
