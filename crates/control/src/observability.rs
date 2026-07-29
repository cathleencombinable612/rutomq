use crate::{PartitionKey, TransactionState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerLag {
    pub group_id: String,
    pub partition: PartitionKey,
    pub committed_offset: i64,
    pub high_watermark: i64,
    pub lag: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRetentionSize {
    pub partition: PartitionKey,
    pub size_bytes: i64,
    pub retention_bytes: i64,
}

impl PartitionRetentionSize {
    pub fn percent(&self) -> i64 {
        if self.size_bytes <= 0 || self.retention_bytes <= 0 {
            return 0;
        }
        let percent = (self.size_bytes as u128 * 100) / self.retention_bytes as u128;
        i64::try_from(percent).unwrap_or(i64::MAX)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionStateCounts {
    pub empty: i64,
    pub ongoing: i64,
    pub complete_commit: i64,
    pub complete_abort: i64,
}

impl TransactionStateCounts {
    pub(crate) fn record(&mut self, state: TransactionState) {
        match state {
            TransactionState::Empty => self.empty += 1,
            TransactionState::Ongoing => self.ongoing += 1,
            TransactionState::CompleteCommit => self.complete_commit += 1,
            TransactionState::CompleteAbort => self.complete_abort += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_percentage_is_integer_unbounded_and_overflow_safe() {
        let observation = |size_bytes, retention_bytes| PartitionRetentionSize {
            partition: PartitionKey::new("events", 0),
            size_bytes,
            retention_bytes,
        };

        assert_eq!(observation(2, 3).percent(), 66);
        assert_eq!(observation(3, 2).percent(), 150);
        assert_eq!(observation(10, -1).percent(), 0);
        assert_eq!(observation(10, 0).percent(), 0);
        assert_eq!(observation(i64::MAX, 1).percent(), i64::MAX);
    }
}
