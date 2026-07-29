use crate::share_state::{
    merge_state_batches_and_completion_count, normalize_state_batches,
    validate_delivery_complete_count, validate_epoch, validate_key, validate_start_offset,
};
use crate::{
    ControlError, ShareStateBatch, ShareStateInitialization, ShareStateKey, ShareStateRead,
    ShareStateSnapshot, ShareStateSummary, ShareStateWrite,
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub(crate) async fn initialize(
    pool: &PgPool,
    initialization: ShareStateInitialization,
) -> Result<(), ControlError> {
    validate_key(&initialization.key)?;
    validate_epoch(initialization.state_epoch, "epoch")?;
    validate_start_offset(initialization.start_offset)?;

    let mut transaction = pool.begin().await?;
    validate_partition(&mut transaction, &initialization.key).await?;
    super::postgres_share_records::lock_partition(
        &mut transaction,
        &initialization.key.group_id,
        initialization.key.topic_id,
        initialization.key.partition,
    )
    .await?;
    let current = sqlx::query_scalar::<_, i32>(
        "SELECT state_epoch
         FROM share_partition_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
         FOR UPDATE",
    )
    .bind(&initialization.key.group_id)
    .bind(initialization.key.topic_id)
    .bind(initialization.key.partition)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(current) = current
        && initialization.state_epoch != -1
        && current > initialization.state_epoch
    {
        return Err(ControlError::FencedShareStateEpoch {
            current,
            requested: initialization.state_epoch,
        });
    }
    sqlx::query(
        "INSERT INTO share_partition_states (
             group_id, topic_id, partition_index, start_offset, state_epoch,
             leader_epoch, delivery_complete_count, state_batches
         ) VALUES ($1, $2, $3, $4, $5, -1, $6, '[]'::jsonb)
         ON CONFLICT (group_id, topic_id, partition_index) DO UPDATE
         SET start_offset = EXCLUDED.start_offset,
             state_epoch = EXCLUDED.state_epoch,
             leader_epoch = -1,
             delivery_complete_count = EXCLUDED.delivery_complete_count,
             state_batches = '[]'::jsonb",
    )
    .bind(&initialization.key.group_id)
    .bind(initialization.key.topic_id)
    .bind(initialization.key.partition)
    .bind(initialization.start_offset)
    .bind(initialization.state_epoch)
    .bind(if initialization.start_offset == -1 {
        -1
    } else {
        0
    })
    .execute(&mut *transaction)
    .await?;
    delete_record_overrides(&mut transaction, &initialization.key).await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn read(
    pool: &PgPool,
    read: ShareStateRead,
) -> Result<ShareStateSnapshot, ControlError> {
    validate_key(&read.key)?;
    validate_epoch(read.leader_epoch, "leader epoch")?;

    let mut transaction = pool.begin().await?;
    validate_partition(&mut transaction, &read.key).await?;
    super::postgres_share_records::lock_partition(
        &mut transaction,
        &read.key.group_id,
        read.key.topic_id,
        read.key.partition,
    )
    .await?;
    let mut state = load_state(&mut transaction, &read.key)
        .await?
        .ok_or_else(|| {
            ControlError::InvalidRequest(
                "read operation on uninitialized share partition is not allowed".to_owned(),
            )
        })?;
    if read.leader_epoch != -1 && state.leader_epoch > read.leader_epoch {
        return Err(ControlError::FencedShareLeaderEpoch {
            current: state.leader_epoch,
            requested: read.leader_epoch,
        });
    }
    if read.leader_epoch > state.leader_epoch {
        state.leader_epoch = read.leader_epoch;
        sqlx::query(
            "UPDATE share_partition_states
             SET leader_epoch = $4
             WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
        )
        .bind(&read.key.group_id)
        .bind(read.key.topic_id)
        .bind(read.key.partition)
        .bind(read.leader_epoch)
        .execute(&mut *transaction)
        .await?;
    }
    apply_record_overrides(&mut transaction, &read.key, &mut state).await?;
    transaction.commit().await?;
    Ok(state)
}

pub(crate) async fn write(pool: &PgPool, write: ShareStateWrite) -> Result<(), ControlError> {
    validate_key(&write.key)?;
    validate_epoch(write.state_epoch, "epoch")?;
    validate_epoch(write.leader_epoch, "leader epoch")?;
    validate_start_offset(write.start_offset)?;
    validate_delivery_complete_count(write.delivery_complete_count)?;
    normalize_state_batches(&[], &write.state_batches, write.start_offset)?;

    let mut transaction = pool.begin().await?;
    validate_partition(&mut transaction, &write.key).await?;
    super::postgres_share_records::lock_partition(
        &mut transaction,
        &write.key.group_id,
        write.key.topic_id,
        write.key.partition,
    )
    .await?;
    let mut state = load_state(&mut transaction, &write.key)
        .await?
        .ok_or_else(|| {
            ControlError::InvalidRequest(
                "write operation on uninitialized share partition is not allowed".to_owned(),
            )
        })?;
    if write.leader_epoch != -1 && state.leader_epoch > write.leader_epoch {
        return Err(ControlError::FencedShareLeaderEpoch {
            current: state.leader_epoch,
            requested: write.leader_epoch,
        });
    }
    if write.state_epoch != -1 && state.state_epoch > write.state_epoch {
        return Err(ControlError::FencedShareStateEpoch {
            current: state.state_epoch,
            requested: write.state_epoch,
        });
    }
    apply_record_overrides(&mut transaction, &write.key, &mut state).await?;
    let start_offset = if write.start_offset == -1 {
        state.start_offset
    } else {
        write.start_offset
    };
    let state_batches =
        normalize_state_batches(&state.state_batches, &write.state_batches, start_offset)?;
    let state_batches_json = serde_json::to_value(&state_batches)
        .map_err(|error| ControlError::Database(sqlx::Error::Encode(Box::new(error))))?;
    sqlx::query(
        "UPDATE share_partition_states
         SET start_offset = $4, delivery_complete_count = $5, state_batches = $6
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(&write.key.group_id)
    .bind(write.key.topic_id)
    .bind(write.key.partition)
    .bind(start_offset)
    .bind(write.delivery_complete_count)
    .bind(state_batches_json)
    .execute(&mut *transaction)
    .await?;
    delete_record_overrides(&mut transaction, &write.key).await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn delete(pool: &PgPool, key: &ShareStateKey) -> Result<(), ControlError> {
    validate_key(key)?;
    let mut transaction = pool.begin().await?;
    validate_partition(&mut transaction, key).await?;
    super::postgres_share_records::lock_partition(
        &mut transaction,
        &key.group_id,
        key.topic_id,
        key.partition,
    )
    .await?;
    sqlx::query(
        "DELETE FROM share_partition_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(&key.group_id)
    .bind(key.topic_id)
    .bind(key.partition)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn summarize(
    pool: &PgPool,
    key: &ShareStateKey,
) -> Result<Option<ShareStateSummary>, ControlError> {
    validate_key(key)?;
    let mut transaction = pool.begin().await?;
    validate_partition(&mut transaction, key).await?;
    super::postgres_share_records::lock_partition(
        &mut transaction,
        &key.group_id,
        key.topic_id,
        key.partition,
    )
    .await?;
    let Some(mut state) = load_state(&mut transaction, key).await? else {
        transaction.commit().await?;
        return Ok(None);
    };
    apply_record_overrides(&mut transaction, key, &mut state).await?;
    transaction.commit().await?;
    Ok(Some(ShareStateSummary {
        state_epoch: state.state_epoch,
        leader_epoch: state.leader_epoch,
        start_offset: state.start_offset,
        delivery_complete_count: state.delivery_complete_count,
    }))
}

async fn validate_partition(
    transaction: &mut Transaction<'_, Postgres>,
    key: &ShareStateKey,
) -> Result<(), ControlError> {
    if sqlx::query(
        "SELECT 1 FROM partitions
         WHERE topic_id = $1 AND partition_index = $2",
    )
    .bind(key.topic_id)
    .bind(key.partition)
    .fetch_optional(&mut **transaction)
    .await?
    .is_none()
    {
        return Err(ControlError::PartitionNotFound {
            topic: key.topic_id.to_string(),
            partition: key.partition,
        });
    }
    Ok(())
}

async fn load_state(
    transaction: &mut Transaction<'_, Postgres>,
    key: &ShareStateKey,
) -> Result<Option<ShareStateSnapshot>, ControlError> {
    let row = sqlx::query(
        "SELECT state_epoch, leader_epoch, start_offset,
                delivery_complete_count, state_batches
         FROM share_partition_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
         FOR UPDATE",
    )
    .bind(&key.group_id)
    .bind(key.topic_id)
    .bind(key.partition)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        let state_batches = serde_json::from_value(row.get("state_batches"))
            .map_err(|error| ControlError::Database(sqlx::Error::Decode(Box::new(error))))?;
        Ok(ShareStateSnapshot {
            state_epoch: row.get("state_epoch"),
            leader_epoch: row.get("leader_epoch"),
            start_offset: row.get("start_offset"),
            delivery_complete_count: row.get("delivery_complete_count"),
            state_batches,
        })
    })
    .transpose()
}

async fn apply_record_overrides(
    transaction: &mut Transaction<'_, Postgres>,
    key: &ShareStateKey,
    state: &mut ShareStateSnapshot,
) -> Result<(), ControlError> {
    let records = sqlx::query(
        "SELECT record_offset, delivery_state, delivery_count
         FROM share_record_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3
         ORDER BY record_offset",
    )
    .bind(&key.group_id)
    .bind(key.topic_id)
    .bind(key.partition)
    .fetch_all(&mut **transaction)
    .await?;
    if records.is_empty() {
        return Ok(());
    }
    let record_batches = records
        .into_iter()
        .map(|row| {
            let offset = row.get("record_offset");
            ShareStateBatch {
                first_offset: offset,
                last_offset: offset,
                delivery_state: row.get::<i16, _>("delivery_state") as i8,
                delivery_count: row.get("delivery_count"),
            }
        })
        .collect::<Vec<_>>();
    (state.state_batches, state.delivery_complete_count) =
        merge_state_batches_and_completion_count(
            &state.state_batches,
            &record_batches,
            state.start_offset,
            state.delivery_complete_count,
        )?;
    Ok(())
}

async fn delete_record_overrides(
    transaction: &mut Transaction<'_, Postgres>,
    key: &ShareStateKey,
) -> Result<(), ControlError> {
    sqlx::query(
        "DELETE FROM share_record_states
         WHERE group_id = $1 AND topic_id = $2 AND partition_index = $3",
    )
    .bind(&key.group_id)
    .bind(key.topic_id)
    .bind(key.partition)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
