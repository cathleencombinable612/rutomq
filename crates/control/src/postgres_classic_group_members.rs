use crate::postgres_classic_group_store::{
    load_members, lock_group, reap_and_rebalance, rebalance_after_removal, validate_identity,
};
use crate::{
    ControlError, GroupAssignment, GroupMemberIdentity, LeaveGroupMemberError,
    LeaveGroupMemberResult,
};
use sqlx::{PgPool, Row};

pub(crate) async fn sync(
    pool: &PgPool,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    assignments: Vec<GroupAssignment>,
) -> Result<Vec<u8>, ControlError> {
    let mut transaction = pool.begin().await?;
    let group = lock_group(&mut transaction, group_id).await?;
    if group.get::<bool, _>("classic_rebalance_pending") {
        transaction.commit().await?;
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    let current_generation = group.get("generation_id");
    let current_leader: Option<String> = group.get("leader_id");
    let expected = reap_and_rebalance(
        &mut transaction,
        group_id,
        current_generation,
        current_leader.as_deref(),
    )
    .await?;
    if expected != generation_id {
        transaction.commit().await?;
        return Err(ControlError::IllegalGeneration {
            group: group_id.to_owned(),
            expected,
            actual: generation_id,
        });
    }
    validate_identity(&mut transaction, group_id, member_id, group_instance_id).await?;
    for assignment in assignments {
        let result = sqlx::query(
            "UPDATE consumer_group_members SET assignment = $3
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(group_id)
        .bind(&assignment.member_id)
        .bind(assignment.assignment)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(ControlError::GroupMemberNotFound {
                group: group_id.to_owned(),
                member: assignment.member_id,
            });
        }
    }
    let assignment = sqlx::query(
        "SELECT assignment FROM consumer_group_members
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(group_id)
    .bind(member_id)
    .fetch_one(&mut *transaction)
    .await?
    .get::<Option<Vec<u8>>, _>("assignment")
    .unwrap_or_default();
    transaction.commit().await?;
    Ok(assignment)
}

pub(crate) async fn heartbeat(
    pool: &PgPool,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    let group = lock_group(&mut transaction, group_id).await?;
    if group.get::<bool, _>("classic_rebalance_pending") {
        transaction.commit().await?;
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    let current_generation = group.get("generation_id");
    let current_leader: Option<String> = group.get("leader_id");
    let expected = reap_and_rebalance(
        &mut transaction,
        group_id,
        current_generation,
        current_leader.as_deref(),
    )
    .await?;
    if expected != generation_id {
        transaction.commit().await?;
        return Err(ControlError::IllegalGeneration {
            group: group_id.to_owned(),
            expected,
            actual: generation_id,
        });
    }
    validate_identity(&mut transaction, group_id, member_id, group_instance_id).await?;
    sqlx::query(
        "UPDATE consumer_group_members SET last_heartbeat = now()
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(group_id)
    .bind(member_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn leave(
    pool: &PgPool,
    group_id: &str,
    members: &[GroupMemberIdentity],
) -> Result<Vec<LeaveGroupMemberResult>, ControlError> {
    let mut transaction = pool.begin().await?;
    let group = lock_group(&mut transaction, group_id).await?;
    let current_generation: i32 = group.get("generation_id");
    let current_leader: Option<String> = group.get("leader_id");
    let rebalance_pending: bool = group.get("classic_rebalance_pending");
    let stored_members = load_members(&mut transaction, group_id).await?;
    let mut removed = Vec::<String>::new();
    let mut results = Vec::with_capacity(members.len());

    for identity in members {
        let resolved = match identity.group_instance_id.as_deref() {
            Some(instance_id) => stored_members
                .iter()
                .find(|member| member.group_instance_id.as_deref() == Some(instance_id))
                .ok_or(LeaveGroupMemberError::UnknownMemberId)
                .and_then(|member| {
                    if !identity.member_id.is_empty() && identity.member_id != member.member_id {
                        Err(LeaveGroupMemberError::FencedInstanceId)
                    } else {
                        Ok(member.member_id.clone())
                    }
                }),
            None if identity.member_id.is_empty() => Err(LeaveGroupMemberError::UnknownMemberId),
            None => stored_members
                .iter()
                .find(|member| member.member_id == identity.member_id)
                .map(|member| member.member_id.clone())
                .ok_or(LeaveGroupMemberError::UnknownMemberId),
        };
        let error = match resolved {
            Ok(member_id) => {
                if !removed.contains(&member_id) {
                    removed.push(member_id);
                }
                None
            }
            Err(error) => Some(error),
        };
        results.push(LeaveGroupMemberResult {
            identity: identity.clone(),
            error,
        });
    }

    for member_id in &removed {
        sqlx::query(
            "DELETE FROM consumer_group_members
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(group_id)
        .bind(member_id)
        .execute(&mut *transaction)
        .await?;
    }
    if removed.is_empty() {
        transaction.commit().await?;
        return Ok(results);
    }

    let remaining = load_members(&mut transaction, group_id).await?;
    if remaining.is_empty() {
        sqlx::query(
            "UPDATE consumer_groups
             SET generation_id = generation_id + 1,
                 protocol_name = NULL,
                 leader_id = NULL,
                 classic_rebalance_id = NULL,
                 classic_rebalance_pending = FALSE,
                 classic_rebalance_started_at = NULL,
                 classic_rebalance_deadline = NULL,
                 classic_initial_rebalance_deadline = NULL,
                 empty_since_ms =
                     FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT,
                 updated_at = now()
             WHERE group_id = $1",
        )
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    } else if rebalance_pending {
        crate::postgres_classic_join_barrier::finish_after_membership_change(
            &mut transaction,
            group_id,
        )
        .await?;
    } else {
        rebalance_after_removal(
            &mut transaction,
            group_id,
            current_generation,
            current_leader.as_deref(),
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(results)
}

pub(crate) async fn validate_member(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_id: i32,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    let group = lock_group(&mut transaction, group_id).await?;
    if group.get::<bool, _>("classic_rebalance_pending") {
        transaction.commit().await?;
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    let current_generation = group.get("generation_id");
    let current_leader: Option<String> = group.get("leader_id");
    let expected = reap_and_rebalance(
        &mut transaction,
        group_id,
        current_generation,
        current_leader.as_deref(),
    )
    .await?;
    if expected != generation_id {
        transaction.commit().await?;
        return Err(ControlError::IllegalGeneration {
            group: group_id.to_owned(),
            expected,
            actual: generation_id,
        });
    }
    validate_identity(&mut transaction, group_id, member_id, group_instance_id).await?;
    transaction.commit().await?;
    Ok(())
}
