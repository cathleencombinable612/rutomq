use crate::{ControlError, ObjectRef};
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub async fn stage(pool: &PgPool, object: &ObjectRef) -> Result<(), ControlError> {
    let size = object_size(object)?;
    let inserted = sqlx::query(
        "INSERT INTO objects (object_key, size_bytes, committed)
         VALUES ($1, $2, FALSE)
         ON CONFLICT (object_key) DO NOTHING",
    )
    .bind(&object.key)
    .bind(size)
    .execute(pool)
    .await?;
    if inserted.rows_affected() != 1 {
        return Err(ControlError::InvalidRequest(format!(
            "object {} is already staged or committed",
            object.key
        )));
    }
    Ok(())
}

pub async fn lock_staged(
    transaction: &mut Transaction<'_, Postgres>,
    object: &ObjectRef,
) -> Result<(), ControlError> {
    let row = sqlx::query(
        "SELECT size_bytes, committed, orphan_gc_claimed_at
         FROM objects
         WHERE object_key = $1
         FOR UPDATE",
    )
    .bind(&object.key)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| ControlError::InvalidRequest(format!("object {} was not staged", object.key)))?;
    if row.get::<bool, _>("committed") {
        return Err(ControlError::InvalidRequest(format!(
            "object {} is already committed",
            object.key
        )));
    }
    if row
        .get::<Option<DateTime<Utc>>, _>("orphan_gc_claimed_at")
        .is_some()
    {
        return Err(ControlError::InvalidRequest(format!(
            "object {} was claimed for orphan deletion",
            object.key
        )));
    }
    if row.get::<i64, _>("size_bytes") != object_size(object)? {
        return Err(ControlError::InvalidRequest(format!(
            "staged object {} changed size",
            object.key
        )));
    }
    Ok(())
}

pub async fn mark_committed(
    transaction: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<(), ControlError> {
    let updated = sqlx::query(
        "UPDATE objects
         SET committed = TRUE, unreferenced_at = NULL, delete_after = NULL
         WHERE object_key = $1
           AND committed = FALSE
           AND orphan_gc_claimed_at IS NULL",
    )
    .bind(key)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ControlError::InvalidRequest(format!(
            "object {key} lost its upload intent"
        )));
    }
    Ok(())
}

pub async fn defer_delete(
    transaction: &mut Transaction<'_, Postgres>,
    object_keys: &[String],
    now_ms: i64,
    delay_ms: i64,
) -> Result<(), ControlError> {
    if object_keys.is_empty() {
        return Ok(());
    }
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms).ok_or_else(|| {
        ControlError::InvalidRequest(format!("invalid object deletion timestamp {now_ms}"))
    })?;
    let delete_after = now
        .checked_add_signed(Duration::milliseconds(delay_ms.max(0)))
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    sqlx::query(
        "UPDATE objects o
         SET delete_after = GREATEST(COALESCE(o.delete_after, $1), $1),
             unreferenced_at = CASE
                 WHEN NOT EXISTS (
                     SELECT 1 FROM object_spans s
                     WHERE s.object_key = o.object_key
                 )
                 THEN COALESCE(o.unreferenced_at, $2)
                 ELSE o.unreferenced_at
             END
         WHERE o.object_key = ANY($3)
           AND o.committed = TRUE",
    )
    .bind(delete_after)
    .bind(now)
    .bind(object_keys)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub async fn claim_stale(
    pool: &PgPool,
    before_ms: i64,
    limit: i64,
) -> Result<Vec<String>, ControlError> {
    let before = DateTime::<Utc>::from_timestamp_millis(before_ms).ok_or_else(|| {
        ControlError::InvalidRequest(format!("invalid object intent cutoff {before_ms}"))
    })?;
    let rows = sqlx::query(
        "WITH stale AS (
             SELECT object_key
             FROM objects
             WHERE committed = FALSE
               AND (created_at <= $1 OR orphan_gc_claimed_at IS NOT NULL)
             ORDER BY COALESCE(orphan_gc_claimed_at, created_at),
                      object_key
             FOR UPDATE SKIP LOCKED
             LIMIT $2
         )
         UPDATE objects o
         SET orphan_gc_claimed_at = now()
         FROM stale
         WHERE o.object_key = stale.object_key
         RETURNING o.object_key",
    )
    .bind(before)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("object_key")).collect())
}

pub async fn complete_stale_deletion(pool: &PgPool, key: &str) -> Result<bool, ControlError> {
    let deleted = sqlx::query(
        "DELETE FROM objects o
         WHERE o.object_key = $1
           AND o.committed = FALSE
           AND o.orphan_gc_claimed_at IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM object_spans s
               WHERE s.object_key = o.object_key
           )",
    )
    .bind(key)
    .execute(pool)
    .await?;
    Ok(deleted.rows_affected() == 1)
}

pub async fn staged(pool: &PgPool, key: &str) -> Result<bool, ControlError> {
    Ok(sqlx::query(
        "SELECT 1 FROM objects
         WHERE object_key = $1 AND committed = FALSE",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?
    .is_some())
}

fn object_size(object: &ObjectRef) -> Result<i64, ControlError> {
    i64::try_from(object.size)
        .map_err(|_| ControlError::InvalidRequest("object size exceeds BIGINT".to_owned()))
}
