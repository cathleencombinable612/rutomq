use crate::PartitionKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchIsolation {
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSession {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerInitialization {
    pub producer: ProducerSession,
    pub ongoing_transaction: Option<ProducerSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerBatch {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
    pub last_sequence: i32,
}

pub fn increment_producer_sequence(sequence: i32, increment: i32) -> i32 {
    const MODULUS: i64 = i32::MAX as i64 + 1;
    (i64::from(sequence) + i64::from(increment)).rem_euclid(MODULUS) as i32
}

impl ProducerBatch {
    pub fn expected_next_sequence(self) -> i32 {
        increment_producer_sequence(self.last_sequence, 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    Ongoing,
    Committed,
    Aborted,
}

impl TransactionStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ongoing => "ongoing",
            Self::Committed => "committed",
            Self::Aborted => "aborted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Empty,
    Ongoing,
    CompleteCommit,
    CompleteAbort,
}

impl TransactionState {
    pub fn kafka_name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Ongoing => "Ongoing",
            Self::CompleteCommit => "CompleteCommit",
            Self::CompleteAbort => "CompleteAbort",
        }
    }
}

impl From<TransactionStatus> for TransactionState {
    fn from(status: TransactionStatus) -> Self {
        match status {
            TransactionStatus::Ongoing => Self::Ongoing,
            TransactionStatus::Committed => Self::CompleteCommit,
            TransactionStatus::Aborted => Self::CompleteAbort,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionDescription {
    pub transactional_id: String,
    pub producer: ProducerSession,
    pub state: TransactionState,
    pub timeout_ms: i32,
    pub start_time_ms: i64,
    pub partitions: Vec<PartitionKey>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionFilter {
    pub state_filters: Vec<String>,
    pub producer_id_filters: Vec<i64>,
    pub min_duration_ms: Option<i64>,
    pub transactional_id_pattern: Option<String>,
}
