use rutomq_protocol::records::Record;

pub(super) const RENEW_DISABLED_MESSAGE: &str =
    "Renewing acquisition locks is not enabled for the group.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ShareAcquireMode {
    BatchOptimized,
    RecordLimit,
}

impl ShareAcquireMode {
    pub(super) fn parse(version: i16, value: i8) -> Result<Self, &'static str> {
        match (version, value) {
            (_, 0) => Ok(Self::BatchOptimized),
            (2.., 1) => Ok(Self::RecordLimit),
            (..=1, _) => Err("ShareFetch v1 only supports batch-optimized acquisition"),
            _ => Err("unknown share acquire mode"),
        }
    }
}

pub(super) fn validate_renew_fetch(
    is_renew_ack: bool,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
) -> Result<(), &'static str> {
    if is_renew_ack && (max_wait_ms != 0 || min_bytes != 0 || max_bytes != 0 || max_records != 0) {
        return Err(
            "renew ShareFetch requires maxWaitMs, minBytes, maxBytes, and maxRecords to be zero",
        );
    }
    Ok(())
}

pub(super) fn validate_acknowledgement_types<'a>(
    version: i16,
    is_renew_ack: bool,
    batches: impl IntoIterator<Item = &'a [i8]>,
) -> Result<(), &'static str> {
    let max_type = if version >= 2 { 4 } else { 3 };
    for types in batches {
        if types
            .iter()
            .any(|acknowledgement| *acknowledgement < 0 || *acknowledgement > max_type)
        {
            return Err("share acknowledgement type is not supported by this API version");
        }
        if !is_renew_ack && types.contains(&4) {
            return Err("renew acknowledgement requires isRenewAck");
        }
    }
    Ok(())
}

pub(super) fn has_renew_acknowledgement<'a>(batches: impl IntoIterator<Item = &'a [i8]>) -> bool {
    batches.into_iter().any(|types| types.contains(&4))
}

pub(super) fn acquisition_candidates(
    batches: &[Vec<Record>],
    mode: ShareAcquireMode,
    max_records: usize,
) -> (Vec<i64>, usize) {
    match mode {
        ShareAcquireMode::RecordLimit => (
            batches
                .iter()
                .flatten()
                .map(|record| record.offset)
                .collect(),
            max_records,
        ),
        ShareAcquireMode::BatchOptimized => {
            let mut offsets = Vec::new();
            for batch in batches {
                offsets.extend(batch.iter().map(|record| record.offset));
                if offsets.len() >= max_records {
                    break;
                }
            }
            let limit = offsets.len();
            (offsets, limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rutomq_protocol::records::TimestampType;

    fn record(offset: i64) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 0,
            key: None,
            value: Some(Bytes::new()),
            headers: Vec::new(),
        }
    }

    #[test]
    fn acquire_modes_apply_strict_or_batch_aligned_limits() {
        let batches = vec![
            (0..5).map(record).collect::<Vec<_>>(),
            (5..10).map(record).collect::<Vec<_>>(),
        ];
        let (strict, strict_limit) =
            acquisition_candidates(&batches, ShareAcquireMode::RecordLimit, 6);
        assert_eq!(strict, (0..10).collect::<Vec<_>>());
        assert_eq!(strict_limit, 6);

        let (aligned, aligned_limit) =
            acquisition_candidates(&batches, ShareAcquireMode::BatchOptimized, 6);
        assert_eq!(aligned, (0..10).collect::<Vec<_>>());
        assert_eq!(aligned_limit, 10);
    }

    #[test]
    fn renew_rules_are_versioned() {
        assert!(validate_acknowledgement_types(1, false, [&[4][..]]).is_err());
        assert!(validate_acknowledgement_types(2, false, [&[4][..]]).is_err());
        assert!(validate_acknowledgement_types(2, true, [&[4][..]]).is_ok());
        assert!(has_renew_acknowledgement([&[1, 4][..]]));
        assert!(!has_renew_acknowledgement([&[1, 2][..]]));
        assert!(validate_renew_fetch(true, 0, 0, 0, 0).is_ok());
        assert!(validate_renew_fetch(true, 1, 0, 0, 0).is_err());
    }
}
