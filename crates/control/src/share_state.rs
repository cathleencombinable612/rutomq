use crate::ControlError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub const AVAILABLE_DELIVERY_STATE: i8 = 0;
pub const ACKNOWLEDGED_DELIVERY_STATE: i8 = 2;
pub const ARCHIVED_DELIVERY_STATE: i8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShareStateKey {
    pub group_id: String,
    pub topic_id: Uuid,
    pub partition: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareStateBatch {
    pub first_offset: i64,
    pub last_offset: i64,
    pub delivery_state: i8,
    pub delivery_count: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateInitialization {
    pub key: ShareStateKey,
    pub state_epoch: i32,
    pub start_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateRead {
    pub key: ShareStateKey,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateWrite {
    pub key: ShareStateKey,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<ShareStateBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateSnapshot {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<ShareStateBatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareStateSummary {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub delivery_complete_count: i32,
}

pub(crate) fn validate_key(key: &ShareStateKey) -> Result<(), ControlError> {
    if key.group_id.is_empty() || key.partition < 0 {
        return Err(ControlError::InvalidRequest(
            "share state group ID must be non-empty and partition must be nonnegative".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_epoch(epoch: i32, name: &str) -> Result<(), ControlError> {
    if epoch < -1 {
        return Err(ControlError::InvalidRequest(format!(
            "share state {name} must be -1 or nonnegative"
        )));
    }
    Ok(())
}

pub(crate) fn validate_start_offset(start_offset: i64) -> Result<(), ControlError> {
    if start_offset < -1 {
        return Err(ControlError::InvalidRequest(
            "share state start offset must be -1 or nonnegative".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_delivery_complete_count(value: i32) -> Result<(), ControlError> {
    if value < -1 {
        return Err(ControlError::InvalidRequest(
            "share state delivery-complete count must be -1 or nonnegative".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn normalize_state_batches(
    existing: &[ShareStateBatch],
    incoming: &[ShareStateBatch],
    start_offset: i64,
) -> Result<Vec<ShareStateBatch>, ControlError> {
    validate_start_offset(start_offset)?;
    for batch in existing.iter().chain(incoming) {
        validate_batch(batch)?;
    }

    let batches = existing
        .iter()
        .chain(incoming)
        .filter_map(|batch| {
            if start_offset != -1 && batch.last_offset < start_offset {
                return None;
            }
            Some(ShareStateBatch {
                first_offset: if start_offset == -1 {
                    batch.first_offset
                } else {
                    batch.first_offset.max(start_offset)
                },
                ..*batch
            })
        })
        .collect::<Vec<_>>();
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut events = BTreeMap::<i64, (Vec<usize>, Vec<usize>)>::new();
    for (index, batch) in batches.iter().enumerate() {
        events.entry(batch.first_offset).or_default().1.push(index);
        if let Some(after) = batch.last_offset.checked_add(1) {
            events.entry(after).or_default().0.push(index);
        }
    }
    let boundaries = events.keys().copied().collect::<Vec<_>>();

    let mut normalized = Vec::<ShareStateBatch>::new();
    let mut active = BTreeSet::<(i16, i8, usize)>::new();
    for (index, first_offset) in boundaries.iter().copied().enumerate() {
        let (removed, added) = events
            .get(&first_offset)
            .expect("boundary was collected from events");
        for batch_index in removed {
            let batch = batches[*batch_index];
            active.remove(&(batch.delivery_count, batch.delivery_state, *batch_index));
        }
        for batch_index in added {
            let batch = batches[*batch_index];
            active.insert((batch.delivery_count, batch.delivery_state, *batch_index));
        }
        let Some((_, _, selected_index)) = active.last() else {
            continue;
        };
        let selected = batches[*selected_index];
        let last_offset = boundaries
            .get(index + 1)
            .map_or(i64::MAX, |next| next - 1)
            .min(selected.last_offset);
        if let Some(previous) = normalized.last_mut()
            && previous.last_offset.checked_add(1) == Some(first_offset)
            && previous.delivery_state == selected.delivery_state
            && previous.delivery_count == selected.delivery_count
        {
            previous.last_offset = last_offset;
        } else {
            normalized.push(ShareStateBatch {
                first_offset,
                last_offset,
                delivery_state: selected.delivery_state,
                delivery_count: selected.delivery_count,
            });
        }
    }
    Ok(normalized)
}

pub(crate) fn merge_state_batches_and_completion_count(
    persisted: &[ShareStateBatch],
    overrides: &[ShareStateBatch],
    start_offset: i64,
    persisted_completion_count: i32,
) -> Result<(Vec<ShareStateBatch>, i32), ControlError> {
    validate_delivery_complete_count(persisted_completion_count)?;
    let state_batches = normalize_state_batches(persisted, overrides, start_offset)?;
    if overrides.is_empty() {
        return Ok((state_batches, persisted_completion_count));
    }
    let completed = state_batches
        .iter()
        .filter(|batch| {
            matches!(
                batch.delivery_state,
                ACKNOWLEDGED_DELIVERY_STATE | ARCHIVED_DELIVERY_STATE
            )
        })
        .fold(0i64, |count, batch| {
            count.saturating_add(
                batch
                    .last_offset
                    .saturating_sub(batch.first_offset)
                    .saturating_add(1),
            )
        })
        .min(i64::from(i32::MAX)) as i32;
    Ok((state_batches, persisted_completion_count.max(completed)))
}

fn validate_batch(batch: &ShareStateBatch) -> Result<(), ControlError> {
    if batch.first_offset < 0
        || batch.last_offset < batch.first_offset
        || batch.delivery_count < 0
        || !matches!(
            batch.delivery_state,
            AVAILABLE_DELIVERY_STATE | ACKNOWLEDGED_DELIVERY_STATE | ARCHIVED_DELIVERY_STATE
        )
    {
        return Err(ControlError::InvalidRequest(
            "share state batch range, delivery state, or delivery count is invalid".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_overlaps_by_delivery_priority_without_expanding_ranges() {
        let normalized = normalize_state_batches(
            &[
                ShareStateBatch {
                    first_offset: 0,
                    last_offset: 1_000_000_000,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                },
                ShareStateBatch {
                    first_offset: 20,
                    last_offset: 30,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 2,
                },
            ],
            &[
                ShareStateBatch {
                    first_offset: 10,
                    last_offset: 25,
                    delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                    delivery_count: 2,
                },
                ShareStateBatch {
                    first_offset: 31,
                    last_offset: 40,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 2,
                },
            ],
            15,
        )
        .unwrap();

        assert_eq!(
            normalized,
            [
                ShareStateBatch {
                    first_offset: 15,
                    last_offset: 19,
                    delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                    delivery_count: 2,
                },
                ShareStateBatch {
                    first_offset: 20,
                    last_offset: 40,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 2,
                },
                ShareStateBatch {
                    first_offset: 41,
                    last_offset: 1_000_000_000,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                },
            ]
        );
    }

    #[test]
    fn rejects_invalid_batches_and_supports_the_maximum_offset() {
        assert!(
            normalize_state_batches(
                &[],
                &[ShareStateBatch {
                    first_offset: i64::MAX,
                    last_offset: i64::MAX,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 1,
                }],
                -1,
            )
            .is_ok()
        );
        assert!(
            normalize_state_batches(
                &[],
                &[ShareStateBatch {
                    first_offset: 2,
                    last_offset: 1,
                    delivery_state: 1,
                    delivery_count: -1,
                }],
                -1,
            )
            .is_err()
        );
    }
}
