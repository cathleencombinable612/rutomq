use crate::share_state::{
    ACKNOWLEDGED_DELIVERY_STATE, ARCHIVED_DELIVERY_STATE, AVAILABLE_DELIVERY_STATE,
    normalize_state_batches, validate_delivery_complete_count, validate_epoch, validate_key,
    validate_start_offset,
};
use crate::{
    ControlError, PartitionKey, ShareStateBatch, ShareStateInitialization, ShareStateKey,
    ShareStateRead, ShareStateSnapshot, ShareStateSummary, ShareStateWrite,
};
use chrono::{DateTime, Duration, Utc};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAutoOffsetReset {
    Earliest,
    Latest,
    Exact(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShareSessionPartition {
    pub topic_id: Uuid,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub struct ShareFetchSessionUpdate {
    pub group_id: String,
    pub member_id: String,
    pub session_epoch: i32,
    pub added: Vec<ShareSessionPartition>,
    pub forgotten: Vec<ShareSessionPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchSession {
    pub next_epoch: i32,
    pub partitions: Vec<ShareSessionPartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharePartitionState {
    pub start_offset: i64,
    pub state_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharePartitionOffset {
    pub partition: PartitionKey,
    pub topic_id: Uuid,
    pub start_offset: i64,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub delivery_complete_count: i64,
}

impl SharePartitionOffset {
    pub fn lag(&self) -> i64 {
        if self.start_offset < 0 || self.delivery_complete_count < 0 {
            return -1;
        }
        self.high_watermark
            .saturating_sub(self.start_offset)
            .saturating_sub(self.delivery_complete_count)
            .max(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareOffsetUpdate {
    pub partition: PartitionKey,
    pub start_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareOffsetUpdateResult {
    pub partition: PartitionKey,
    pub topic_id: Option<Uuid>,
    pub updated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareOffsetDeleteResult {
    pub topic: String,
    pub topic_id: Option<Uuid>,
    pub deleted: bool,
}

pub(crate) fn validate_offset_updates(
    group_id: &str,
    updates: &[ShareOffsetUpdate],
) -> Result<(), ControlError> {
    if group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "share group ID cannot be empty".to_owned(),
        ));
    }
    let mut partitions = HashSet::new();
    for update in updates {
        if update.start_offset < 0 {
            return Err(ControlError::InvalidRequest(format!(
                "share start offset {} cannot be negative",
                update.start_offset
            )));
        }
        if !partitions.insert(&update.partition) {
            return Err(ControlError::InvalidRequest(format!(
                "share partition {:?} appears more than once",
                update.partition
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_offset_topics(
    group_id: &str,
    topics: &[String],
) -> Result<(), ControlError> {
    if group_id.is_empty() || topics.iter().any(String::is_empty) {
        return Err(ControlError::InvalidRequest(
            "share group and topic names cannot be empty".to_owned(),
        ));
    }
    let mut names = HashSet::new();
    if topics.iter().any(|topic| !names.insert(topic)) {
        return Err(ControlError::InvalidRequest(
            "share offset deletion contains duplicate topics".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ShareAcquireRequest {
    pub group_id: String,
    pub member_id: String,
    pub topic_id: Uuid,
    pub partition: i32,
    pub candidate_offsets: Vec<i64>,
    pub max_records: usize,
    pub max_record_locks: usize,
    pub lock_duration_ms: i32,
    pub delivery_count_limit: i16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareAcquiredRecord {
    pub offset: i64,
    pub delivery_count: i16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ShareAcknowledgementType {
    Gap = 0,
    Accept = 1,
    Release = 2,
    Reject = 3,
    Renew = 4,
}

impl TryFrom<i8> for ShareAcknowledgementType {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Gap),
            1 => Ok(Self::Accept),
            2 => Ok(Self::Release),
            3 => Ok(Self::Reject),
            4 => Ok(Self::Renew),
            _ => Err(ControlError::InvalidRequest(format!(
                "unknown share acknowledgement type {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShareAcknowledgementBatch {
    pub first_offset: i64,
    pub last_offset: i64,
    pub types: Vec<i8>,
}

#[derive(Debug, Clone)]
pub struct ShareAcknowledgeRecords {
    pub group_id: String,
    pub member_id: String,
    pub topic_id: Uuid,
    pub partition: i32,
    pub batches: Vec<ShareAcknowledgementBatch>,
    pub lock_duration_ms: i32,
    pub delivery_count_limit: i16,
}

#[derive(Clone, Default)]
pub(crate) struct MemoryShareStore {
    sessions: HashMap<(String, String), MemorySession>,
    partitions: HashMap<(String, Uuid, i32), MemoryPartition>,
}

#[derive(Clone)]
struct MemorySession {
    next_epoch: i32,
    partitions: HashSet<ShareSessionPartition>,
}

#[derive(Clone)]
struct MemoryPartition {
    start_offset: i64,
    state_epoch: i32,
    leader_epoch: i32,
    delivery_complete_count: i32,
    state_batches: Vec<ShareStateBatch>,
    records: BTreeMap<i64, MemoryRecord>,
}

#[derive(Clone)]
struct MemoryRecord {
    state: RecordState,
    delivery_state: i8,
    member_id: Option<String>,
    delivery_count: i16,
    lock_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecordState {
    Available,
    Acquired,
    Archived,
}

impl MemoryShareStore {
    pub fn update_session(
        &mut self,
        update: ShareFetchSessionUpdate,
        assigned: &HashSet<ShareSessionPartition>,
    ) -> Result<ShareFetchSession, ControlError> {
        validate_session_update(&update, assigned)?;
        let key = (update.group_id.clone(), update.member_id.clone());
        if update.session_epoch == -1 {
            self.sessions.remove(&key);
            return Ok(ShareFetchSession {
                next_epoch: -1,
                partitions: Vec::new(),
            });
        }
        let session = if update.session_epoch == 0 {
            self.sessions.insert(
                key.clone(),
                MemorySession {
                    next_epoch: 1,
                    partitions: HashSet::new(),
                },
            );
            self.sessions.get_mut(&key).expect("share session inserted")
        } else {
            let session =
                self.sessions
                    .get_mut(&key)
                    .ok_or_else(|| ControlError::ShareSessionNotFound {
                        group: update.group_id.clone(),
                        member: update.member_id.clone(),
                    })?;
            if session.next_epoch != update.session_epoch {
                return Err(ControlError::InvalidShareSessionEpoch {
                    group: update.group_id,
                    member: update.member_id,
                    expected: session.next_epoch,
                    actual: update.session_epoch,
                });
            }
            session.next_epoch = next_epoch(update.session_epoch);
            session
        };
        for partition in update.forgotten {
            session.partitions.remove(&partition);
        }
        session.partitions.extend(update.added);
        let mut partitions = session.partitions.iter().cloned().collect::<Vec<_>>();
        sort_partitions(&mut partitions);
        Ok(ShareFetchSession {
            next_epoch: session.next_epoch,
            partitions,
        })
    }

    pub fn partition_state(
        &mut self,
        group_id: &str,
        topic_id: Uuid,
        partition: i32,
        log_start_offset: i64,
        high_watermark: i64,
        reset: ShareAutoOffsetReset,
    ) -> SharePartitionState {
        let state = self
            .partitions
            .entry((group_id.to_owned(), topic_id, partition))
            .or_insert_with(|| MemoryPartition {
                start_offset: match reset {
                    ShareAutoOffsetReset::Earliest => log_start_offset,
                    ShareAutoOffsetReset::Latest => high_watermark,
                    ShareAutoOffsetReset::Exact(offset) => offset.max(log_start_offset),
                },
                state_epoch: 0,
                leader_epoch: -1,
                delivery_complete_count: 0,
                state_batches: Vec::new(),
                records: BTreeMap::new(),
            });
        state.start_offset = state.start_offset.max(log_start_offset);
        SharePartitionState {
            start_offset: state.start_offset,
            state_epoch: state.state_epoch,
        }
    }

    pub fn existing_partition_state(
        &mut self,
        group_id: &str,
        topic_id: Uuid,
        partition: i32,
        log_start_offset: i64,
    ) -> Option<SharePartitionState> {
        let state = self
            .partitions
            .get_mut(&(group_id.to_owned(), topic_id, partition))?;
        state.start_offset = state.start_offset.max(log_start_offset);
        Some(SharePartitionState {
            start_offset: state.start_offset,
            state_epoch: state.state_epoch,
        })
    }

    pub fn acquire(
        &mut self,
        request: ShareAcquireRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<ShareAcquiredRecord>, ControlError> {
        validate_acquire(&request)?;
        let partition = self
            .partitions
            .get_mut(&(request.group_id, request.topic_id, request.partition))
            .ok_or_else(|| {
                ControlError::InvalidRequest(
                    "share partition state must be initialized before acquisition".to_owned(),
                )
            })?;
        release_expired(partition, now, request.delivery_count_limit);
        let expires_at = now + Duration::milliseconds(i64::from(request.lock_duration_ms));
        let mut offsets = request.candidate_offsets;
        offsets.sort_unstable();
        offsets.dedup();
        let locked = partition
            .records
            .values()
            .filter(|record| record.state == RecordState::Acquired)
            .count();
        let acquisition_limit = request
            .max_records
            .min(request.max_record_locks.saturating_sub(locked));
        let persisted_batches = partition.state_batches.clone();
        let mut acquired = Vec::new();
        for offset in offsets {
            if acquired.len() >= acquisition_limit || offset < partition.start_offset {
                continue;
            }
            let record = match partition.records.entry(offset) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let persisted = state_batch_at(&persisted_batches, offset);
                    if persisted.is_some_and(|batch| {
                        matches!(
                            batch.delivery_state,
                            ACKNOWLEDGED_DELIVERY_STATE | ARCHIVED_DELIVERY_STATE
                        )
                    }) {
                        continue;
                    }
                    entry.insert(MemoryRecord {
                        state: RecordState::Available,
                        delivery_state: AVAILABLE_DELIVERY_STATE,
                        member_id: None,
                        delivery_count: persisted.map_or(0, |batch| batch.delivery_count),
                        lock_expires_at: None,
                    })
                }
            };
            if record.state != RecordState::Available {
                continue;
            }
            if record.delivery_count >= request.delivery_count_limit {
                record.state = RecordState::Archived;
                record.delivery_state = ARCHIVED_DELIVERY_STATE;
                continue;
            }
            record.state = RecordState::Acquired;
            record.member_id = Some(request.member_id.clone());
            record.delivery_count += 1;
            record.lock_expires_at = Some(expires_at);
            acquired.push(ShareAcquiredRecord {
                offset,
                delivery_count: record.delivery_count,
            });
        }
        if !acquired.is_empty() {
            partition.state_epoch = partition.state_epoch.wrapping_add(1);
        }
        advance_start_offset(partition)?;
        refresh_delivery_complete_count(partition)?;
        Ok(acquired)
    }

    pub fn acknowledge(
        &mut self,
        request: ShareAcknowledgeRecords,
        now: DateTime<Utc>,
    ) -> Result<(), ControlError> {
        if request.lock_duration_ms <= 0 || request.delivery_count_limit <= 0 {
            return Err(ControlError::InvalidRequest(
                "share acknowledgement limits must be positive".to_owned(),
            ));
        }
        let actions = expand_acknowledgements(&request.batches)?;
        let partition = self
            .partitions
            .get_mut(&(request.group_id, request.topic_id, request.partition))
            .ok_or_else(|| {
                ControlError::InvalidRequest(
                    "share partition state must be initialized before acknowledgement".to_owned(),
                )
            })?;
        let mut staged = partition.clone();
        for (offset, action) in actions {
            apply_acknowledgement(
                &mut staged,
                offset,
                action,
                &request.member_id,
                now,
                request.lock_duration_ms,
                request.delivery_count_limit,
            )?;
        }
        staged.state_epoch = staged.state_epoch.wrapping_add(1);
        advance_start_offset(&mut staged)?;
        refresh_delivery_complete_count(&mut staged)?;
        *partition = staged;
        Ok(())
    }

    pub fn offset(
        &self,
        group_id: &str,
        topic_id: Uuid,
        partition: i32,
    ) -> Option<SharePartitionState> {
        self.partitions
            .get(&(group_id.to_owned(), topic_id, partition))
            .map(|state| SharePartitionState {
                start_offset: state.start_offset,
                state_epoch: state.state_epoch,
            })
    }

    pub fn offsets(&self, group_id: &str) -> Vec<(Uuid, i32, SharePartitionState)> {
        let mut offsets = self
            .partitions
            .iter()
            .filter_map(|((state_group, topic_id, partition), state)| {
                (state_group == group_id).then_some((
                    *topic_id,
                    *partition,
                    SharePartitionState {
                        start_offset: state.start_offset,
                        state_epoch: state.state_epoch,
                    },
                ))
            })
            .collect::<Vec<_>>();
        offsets.sort_by_key(|(topic_id, partition, _)| (*topic_id, *partition));
        offsets
    }

    pub fn delivery_complete_count(
        &self,
        group_id: &str,
        topic_id: Uuid,
        partition: i32,
        start_offset: i64,
        high_watermark: i64,
    ) -> i64 {
        self.partitions
            .get(&(group_id.to_owned(), topic_id, partition))
            .map(|partition| {
                let completed = effective_delivery_complete_count(partition).unwrap_or(0);
                if completed < 0 {
                    -1
                } else {
                    i64::from(completed).min(high_watermark.saturating_sub(start_offset).max(0))
                }
            })
            .unwrap_or(0)
    }

    pub fn reset_offset(
        &mut self,
        group_id: &str,
        topic_id: Uuid,
        partition: i32,
        start_offset: i64,
    ) {
        let state = self
            .partitions
            .entry((group_id.to_owned(), topic_id, partition))
            .or_insert_with(|| MemoryPartition {
                start_offset,
                state_epoch: 0,
                leader_epoch: -1,
                delivery_complete_count: 0,
                state_batches: Vec::new(),
                records: BTreeMap::new(),
            });
        state.start_offset = start_offset;
        state.state_epoch = state.state_epoch.wrapping_add(1);
        state.delivery_complete_count = 0;
        state.state_batches.clear();
        state.records.clear();
    }

    pub fn initialize_state(
        &mut self,
        initialization: ShareStateInitialization,
    ) -> Result<(), ControlError> {
        validate_key(&initialization.key)?;
        validate_epoch(initialization.state_epoch, "epoch")?;
        validate_start_offset(initialization.start_offset)?;
        let storage_key = (
            initialization.key.group_id,
            initialization.key.topic_id,
            initialization.key.partition,
        );
        if let Some(current) = self.partitions.get(&storage_key)
            && initialization.state_epoch != -1
            && current.state_epoch > initialization.state_epoch
        {
            return Err(ControlError::FencedShareStateEpoch {
                current: current.state_epoch,
                requested: initialization.state_epoch,
            });
        }
        self.partitions.insert(
            storage_key,
            MemoryPartition {
                start_offset: initialization.start_offset,
                state_epoch: initialization.state_epoch,
                leader_epoch: -1,
                delivery_complete_count: if initialization.start_offset == -1 {
                    -1
                } else {
                    0
                },
                state_batches: Vec::new(),
                records: BTreeMap::new(),
            },
        );
        Ok(())
    }

    pub fn read_state(&mut self, read: ShareStateRead) -> Result<ShareStateSnapshot, ControlError> {
        validate_key(&read.key)?;
        validate_epoch(read.leader_epoch, "leader epoch")?;
        let state = self
            .partitions
            .get_mut(&(read.key.group_id, read.key.topic_id, read.key.partition))
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
        }
        snapshot(state)
    }

    pub fn write_state(&mut self, write: ShareStateWrite) -> Result<(), ControlError> {
        validate_key(&write.key)?;
        validate_epoch(write.state_epoch, "epoch")?;
        validate_epoch(write.leader_epoch, "leader epoch")?;
        validate_start_offset(write.start_offset)?;
        validate_delivery_complete_count(write.delivery_complete_count)?;
        normalize_state_batches(&[], &write.state_batches, write.start_offset)?;
        let state = self
            .partitions
            .get_mut(&(write.key.group_id, write.key.topic_id, write.key.partition))
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
        let start_offset = if write.start_offset == -1 {
            state.start_offset
        } else {
            write.start_offset
        };
        let existing = effective_state_batches(state)?;
        state.state_batches =
            normalize_state_batches(&existing, &write.state_batches, start_offset)?;
        state.start_offset = start_offset;
        state.delivery_complete_count = write.delivery_complete_count;
        state.records.clear();
        Ok(())
    }

    pub fn delete_state(&mut self, key: &ShareStateKey) -> Result<(), ControlError> {
        validate_key(key)?;
        self.partitions
            .remove(&(key.group_id.clone(), key.topic_id, key.partition));
        Ok(())
    }

    pub fn summarize_state(
        &self,
        key: &ShareStateKey,
    ) -> Result<Option<ShareStateSummary>, ControlError> {
        validate_key(key)?;
        self.partitions
            .get(&(key.group_id.clone(), key.topic_id, key.partition))
            .map(|state| {
                Ok(ShareStateSummary {
                    state_epoch: state.state_epoch,
                    leader_epoch: state.leader_epoch,
                    start_offset: state.start_offset,
                    delivery_complete_count: effective_delivery_complete_count(state)?,
                })
            })
            .transpose()
    }

    pub fn delete_topic_offsets(&mut self, group_id: &str, topic_id: Uuid) -> bool {
        let before = self.partitions.len();
        self.partitions.retain(|(state_group, state_topic, _), _| {
            state_group != group_id || *state_topic != topic_id
        });
        before != self.partitions.len()
    }

    pub fn delete_group(&mut self, group_id: &str) {
        self.sessions
            .retain(|(session_group, _), _| session_group != group_id);
        self.partitions
            .retain(|(state_group, _, _), _| state_group != group_id);
    }
}

pub(crate) fn expand_acknowledgements(
    batches: &[ShareAcknowledgementBatch],
) -> Result<BTreeMap<i64, ShareAcknowledgementType>, ControlError> {
    let mut actions = BTreeMap::new();
    for batch in batches {
        if batch.first_offset < 0 || batch.last_offset < batch.first_offset {
            return Err(ControlError::InvalidRequest(
                "share acknowledgement offset range is invalid".to_owned(),
            ));
        }
        let count = usize::try_from(batch.last_offset - batch.first_offset + 1).map_err(|_| {
            ControlError::InvalidRequest("acknowledgement range is too large".into())
        })?;
        if count > 100_000 || (batch.types.len() != 1 && batch.types.len() != count) {
            return Err(ControlError::InvalidRequest(
                "share acknowledgement types must contain one value or one value per offset"
                    .to_owned(),
            ));
        }
        for index in 0..count {
            let offset = batch.first_offset + index as i64;
            let action = ShareAcknowledgementType::try_from(
                batch.types[if batch.types.len() == 1 { 0 } else { index }],
            )?;
            if actions.insert(offset, action).is_some() {
                return Err(ControlError::InvalidRequest(format!(
                    "share acknowledgement offset {offset} appears more than once"
                )));
            }
        }
    }
    Ok(actions)
}

fn validate_session_update(
    update: &ShareFetchSessionUpdate,
    assigned: &HashSet<ShareSessionPartition>,
) -> Result<(), ControlError> {
    if update.group_id.is_empty() || update.member_id.is_empty() || update.session_epoch < -1 {
        return Err(ControlError::InvalidRequest(
            "share session group, member, or epoch is invalid".to_owned(),
        ));
    }
    if update
        .added
        .iter()
        .any(|partition| !assigned.contains(partition))
    {
        return Err(ControlError::InvalidRequest(
            "share session contains a partition not assigned to the member".to_owned(),
        ));
    }
    Ok(())
}

fn validate_acquire(request: &ShareAcquireRequest) -> Result<(), ControlError> {
    if request.max_records == 0
        || request.max_record_locks == 0
        || request.lock_duration_ms <= 0
        || request.delivery_count_limit <= 0
    {
        return Err(ControlError::InvalidRequest(
            "share acquisition limits must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn apply_acknowledgement(
    partition: &mut MemoryPartition,
    offset: i64,
    action: ShareAcknowledgementType,
    member_id: &str,
    now: DateTime<Utc>,
    lock_duration_ms: i32,
    delivery_count_limit: i16,
) -> Result<(), ControlError> {
    if action == ShareAcknowledgementType::Gap {
        partition.records.insert(
            offset,
            MemoryRecord {
                state: RecordState::Archived,
                delivery_state: ARCHIVED_DELIVERY_STATE,
                member_id: None,
                delivery_count: 0,
                lock_expires_at: None,
            },
        );
        return Ok(());
    }
    let record = partition
        .records
        .get_mut(&offset)
        .ok_or(ControlError::InvalidShareRecordState(offset))?;
    let owned = record.state == RecordState::Acquired
        && record.member_id.as_deref() == Some(member_id)
        && record.lock_expires_at.is_some_and(|expiry| expiry > now);
    match action {
        ShareAcknowledgementType::Accept | ShareAcknowledgementType::Reject
            if owned || record.state == RecordState::Archived =>
        {
            record.state = RecordState::Archived;
            record.delivery_state = if action == ShareAcknowledgementType::Accept {
                ACKNOWLEDGED_DELIVERY_STATE
            } else {
                ARCHIVED_DELIVERY_STATE
            };
            record.member_id = None;
            record.lock_expires_at = None;
        }
        ShareAcknowledgementType::Release if owned || record.state == RecordState::Available => {
            record.state = if record.delivery_count >= delivery_count_limit {
                RecordState::Archived
            } else {
                RecordState::Available
            };
            record.delivery_state = if record.state == RecordState::Archived {
                ARCHIVED_DELIVERY_STATE
            } else {
                AVAILABLE_DELIVERY_STATE
            };
            record.member_id = None;
            record.lock_expires_at = None;
        }
        ShareAcknowledgementType::Renew if owned => {
            record.lock_expires_at =
                Some(now + Duration::milliseconds(i64::from(lock_duration_ms)));
        }
        _ => return Err(ControlError::InvalidShareRecordState(offset)),
    }
    Ok(())
}

fn release_expired(partition: &mut MemoryPartition, now: DateTime<Utc>, limit: i16) {
    for record in partition.records.values_mut() {
        if record.state == RecordState::Acquired
            && record.lock_expires_at.is_some_and(|expiry| expiry <= now)
        {
            record.state = if record.delivery_count >= limit {
                RecordState::Archived
            } else {
                RecordState::Available
            };
            record.delivery_state = if record.state == RecordState::Archived {
                ARCHIVED_DELIVERY_STATE
            } else {
                AVAILABLE_DELIVERY_STATE
            };
            record.member_id = None;
            record.lock_expires_at = None;
        }
    }
}

fn advance_start_offset(partition: &mut MemoryPartition) -> Result<(), ControlError> {
    let current = partition.start_offset;
    for batch in effective_state_batches(partition)? {
        if batch.first_offset > partition.start_offset {
            break;
        }
        if batch.last_offset < partition.start_offset {
            continue;
        }
        if !matches!(
            batch.delivery_state,
            ACKNOWLEDGED_DELIVERY_STATE | ARCHIVED_DELIVERY_STATE
        ) {
            break;
        }
        let next = batch.last_offset.saturating_add(1);
        if next == partition.start_offset {
            break;
        }
        partition.start_offset = next;
    }
    if partition.start_offset != current {
        partition.delivery_complete_count = 0;
    }
    Ok(())
}

fn snapshot(partition: &MemoryPartition) -> Result<ShareStateSnapshot, ControlError> {
    Ok(ShareStateSnapshot {
        state_epoch: partition.state_epoch,
        leader_epoch: partition.leader_epoch,
        start_offset: partition.start_offset,
        delivery_complete_count: effective_delivery_complete_count(partition)?,
        state_batches: effective_state_batches(partition)?,
    })
}

fn effective_state_batches(
    partition: &MemoryPartition,
) -> Result<Vec<ShareStateBatch>, ControlError> {
    let records = partition
        .records
        .iter()
        .map(|(offset, record)| ShareStateBatch {
            first_offset: *offset,
            last_offset: *offset,
            delivery_state: record.delivery_state,
            delivery_count: record.delivery_count,
        })
        .collect::<Vec<_>>();
    normalize_state_batches(&partition.state_batches, &records, partition.start_offset)
}

fn state_batch_at(batches: &[ShareStateBatch], offset: i64) -> Option<&ShareStateBatch> {
    let index = batches.partition_point(|batch| batch.first_offset <= offset);
    index
        .checked_sub(1)
        .and_then(|index| batches.get(index))
        .filter(|batch| offset <= batch.last_offset)
}

fn effective_delivery_complete_count(partition: &MemoryPartition) -> Result<i32, ControlError> {
    if partition.records.is_empty() {
        return Ok(partition.delivery_complete_count);
    }
    let complete = effective_state_batches(partition)?
        .into_iter()
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
    Ok(partition.delivery_complete_count.max(complete))
}

fn refresh_delivery_complete_count(partition: &mut MemoryPartition) -> Result<(), ControlError> {
    partition.delivery_complete_count = effective_delivery_complete_count(partition)?;
    Ok(())
}

fn next_epoch(epoch: i32) -> i32 {
    if epoch == i32::MAX { 1 } else { epoch + 1 }
}

fn sort_partitions(partitions: &mut [ShareSessionPartition]) {
    partitions.sort_by_key(|partition| (partition.topic_id, partition.partition));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(topic_id: Uuid) -> ShareSessionPartition {
        ShareSessionPartition {
            topic_id,
            partition: 0,
        }
    }

    #[test]
    fn sessions_enforce_epochs_and_assignments() {
        let topic_id = Uuid::new_v4();
        let assigned = HashSet::from([partition(topic_id)]);
        let mut store = MemoryShareStore::default();
        let opened = store
            .update_session(
                ShareFetchSessionUpdate {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    session_epoch: 0,
                    added: vec![partition(topic_id)],
                    forgotten: Vec::new(),
                },
                &assigned,
            )
            .unwrap();
        assert_eq!(opened.next_epoch, 1);
        let continued = store
            .update_session(
                ShareFetchSessionUpdate {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    session_epoch: 1,
                    added: Vec::new(),
                    forgotten: Vec::new(),
                },
                &assigned,
            )
            .unwrap();
        assert_eq!(continued.next_epoch, 2);
        assert!(matches!(
            store.update_session(
                ShareFetchSessionUpdate {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    session_epoch: 1,
                    added: Vec::new(),
                    forgotten: Vec::new(),
                },
                &assigned,
            ),
            Err(ControlError::InvalidShareSessionEpoch { .. })
        ));
    }

    #[test]
    fn records_are_locked_released_and_archived() {
        let topic_id = Uuid::new_v4();
        let now = Utc::now();
        let mut store = MemoryShareStore::default();
        store.partition_state("g", topic_id, 0, 0, 0, ShareAutoOffsetReset::Earliest);
        let request = ShareAcquireRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            topic_id,
            partition: 0,
            candidate_offsets: vec![0, 1],
            max_records: 2,
            max_record_locks: 2,
            lock_duration_ms: 100,
            delivery_count_limit: 3,
        };
        assert_eq!(store.acquire(request.clone(), now).unwrap().len(), 2);
        assert!(store.acquire(request.clone(), now).unwrap().is_empty());
        let mut constrained = request.clone();
        constrained.candidate_offsets = vec![2];
        constrained.max_record_locks = 1;
        assert!(store.acquire(constrained, now).unwrap().is_empty());
        let mut retry = request;
        retry.member_id = "m2".into();
        let acquired = store
            .acquire(retry, now + Duration::milliseconds(101))
            .unwrap();
        assert_eq!(acquired[0].delivery_count, 2);
        store
            .acknowledge(
                ShareAcknowledgeRecords {
                    group_id: "g".into(),
                    member_id: "m2".into(),
                    topic_id,
                    partition: 0,
                    batches: vec![ShareAcknowledgementBatch {
                        first_offset: 0,
                        last_offset: 1,
                        types: vec![2],
                    }],
                    lock_duration_ms: 100,
                    delivery_count_limit: 2,
                },
                now + Duration::milliseconds(102),
            )
            .unwrap();
        assert_eq!(
            store
                .partition_state("g", topic_id, 0, 0, 2, ShareAutoOffsetReset::Earliest)
                .start_offset,
            2
        );
    }

    #[test]
    fn persisted_share_state_controls_acquisition_without_expanding_batches() {
        let topic_id = Uuid::new_v4();
        let now = Utc::now();
        let mut store = MemoryShareStore::default();
        store
            .initialize_state(ShareStateInitialization {
                key: ShareStateKey {
                    group_id: "g".into(),
                    topic_id,
                    partition: 0,
                },
                state_epoch: 4,
                start_offset: 0,
            })
            .unwrap();
        store
            .write_state(ShareStateWrite {
                key: ShareStateKey {
                    group_id: "g".into(),
                    topic_id,
                    partition: 0,
                },
                state_epoch: 4,
                leader_epoch: -1,
                start_offset: -1,
                delivery_complete_count: 2,
                state_batches: vec![
                    ShareStateBatch {
                        first_offset: 0,
                        last_offset: 1,
                        delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                        delivery_count: 1,
                    },
                    ShareStateBatch {
                        first_offset: 2,
                        last_offset: 3,
                        delivery_state: AVAILABLE_DELIVERY_STATE,
                        delivery_count: 2,
                    },
                ],
            })
            .unwrap();

        let acquired = store
            .acquire(
                ShareAcquireRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    topic_id,
                    partition: 0,
                    candidate_offsets: vec![0, 1, 2, 3, 4],
                    max_records: 3,
                    max_record_locks: 3,
                    lock_duration_ms: 30_000,
                    delivery_count_limit: 5,
                },
                now,
            )
            .unwrap();
        assert_eq!(
            acquired
                .iter()
                .map(|record| (record.offset, record.delivery_count))
                .collect::<Vec<_>>(),
            [(2, 3), (3, 3), (4, 1)]
        );

        let snapshot = store
            .read_state(ShareStateRead {
                key: ShareStateKey {
                    group_id: "g".into(),
                    topic_id,
                    partition: 0,
                },
                leader_epoch: -1,
            })
            .unwrap();
        assert_eq!(snapshot.start_offset, 2);
        assert_eq!(snapshot.delivery_complete_count, 0);
        assert_eq!(
            snapshot
                .state_batches
                .iter()
                .map(|batch| (
                    batch.first_offset,
                    batch.last_offset,
                    batch.delivery_state,
                    batch.delivery_count,
                ))
                .collect::<Vec<_>>(),
            [(2, 3, AVAILABLE_DELIVERY_STATE, 3), (4, 4, 0, 1)]
        );
    }

    #[test]
    fn persisted_completion_count_drives_lag_without_terminal_ranges() {
        let topic_id = Uuid::new_v4();
        let mut store = MemoryShareStore::default();
        let key = ShareStateKey {
            group_id: "g".into(),
            topic_id,
            partition: 0,
        };
        store
            .initialize_state(ShareStateInitialization {
                key: key.clone(),
                state_epoch: 1,
                start_offset: 0,
            })
            .unwrap();
        store
            .write_state(ShareStateWrite {
                key: key.clone(),
                state_epoch: 1,
                leader_epoch: -1,
                start_offset: -1,
                delivery_complete_count: 2,
                state_batches: vec![ShareStateBatch {
                    first_offset: 0,
                    last_offset: 4,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                }],
            })
            .unwrap();
        let completed = store.delivery_complete_count("g", topic_id, 0, 0, 10);
        assert_eq!(completed, 2);
        assert_eq!(
            SharePartitionOffset {
                partition: PartitionKey::new("topic", 0),
                topic_id,
                start_offset: 0,
                leader_epoch: 0,
                high_watermark: 10,
                delivery_complete_count: completed,
            }
            .lag(),
            8
        );

        store
            .write_state(ShareStateWrite {
                key,
                state_epoch: 1,
                leader_epoch: -1,
                start_offset: -1,
                delivery_complete_count: -1,
                state_batches: Vec::new(),
            })
            .unwrap();
        let unavailable = store.delivery_complete_count("g", topic_id, 0, 0, 10);
        assert_eq!(unavailable, -1);
        assert_eq!(
            SharePartitionOffset {
                partition: PartitionKey::new("topic", 0),
                topic_id,
                start_offset: 0,
                leader_epoch: 0,
                high_watermark: 10,
                delivery_complete_count: unavailable,
            }
            .lag(),
            -1
        );
    }

    #[test]
    fn resetting_offsets_clears_record_locks_and_deleting_removes_state() {
        let topic_id = Uuid::new_v4();
        let now = Utc::now();
        let mut store = MemoryShareStore::default();
        store.partition_state("g", topic_id, 0, 0, 10, ShareAutoOffsetReset::Earliest);
        store
            .acquire(
                ShareAcquireRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    topic_id,
                    partition: 0,
                    candidate_offsets: vec![0],
                    max_records: 1,
                    max_record_locks: 1,
                    lock_duration_ms: 30_000,
                    delivery_count_limit: 5,
                },
                now,
            )
            .unwrap();

        store.reset_offset("g", topic_id, 0, 4);
        assert_eq!(store.offset("g", topic_id, 0).unwrap().start_offset, 4);
        assert_eq!(
            store
                .acquire(
                    ShareAcquireRequest {
                        group_id: "g".into(),
                        member_id: "other".into(),
                        topic_id,
                        partition: 0,
                        candidate_offsets: vec![4],
                        max_records: 1,
                        max_record_locks: 1,
                        lock_duration_ms: 30_000,
                        delivery_count_limit: 5,
                    },
                    now,
                )
                .unwrap()[0]
                .delivery_count,
            1
        );
        assert!(store.delete_topic_offsets("g", topic_id));
        assert!(store.offsets("g").is_empty());
    }
}
