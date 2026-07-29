use crate::kafka_error::{
    INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT, INVALID_REPLICATION_FACTOR, INVALID_REQUEST,
    POLICY_VIOLATION,
};
use kafka_protocol::messages::BrokerId;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use std::collections::HashSet;

const VIRTUAL_REPLICATION_FACTOR: i16 = 1;
const MAX_PARTITIONS_PER_BATCH: u64 = 10_000;

pub(super) struct ValidatedCreation {
    pub partitions: i32,
    pub replication_factor: i16,
}

pub(super) fn validate_batch(
    topics: &[CreatableTopic],
    default_partitions: i32,
) -> Result<(), (i16, String)> {
    let mut total = 0u64;
    for topic in topics {
        let partitions = if topic.assignments.is_empty() {
            match topic.num_partitions {
                -1 => u64::try_from(default_partitions).unwrap_or(u64::MAX),
                count if count > 0 => u64::from(count as u32),
                _ => 0,
            }
        } else {
            u64::try_from(topic.assignments.len()).unwrap_or(u64::MAX)
        };
        total = total.saturating_add(partitions);
        if total > MAX_PARTITIONS_PER_BATCH {
            return error(
                POLICY_VIOLATION,
                "Excessively large number of partitions per request.",
            );
        }
    }
    Ok(())
}

pub(super) fn validate(
    topic: &CreatableTopic,
    default_partitions: i32,
    default_replication_factor: i16,
) -> Result<ValidatedCreation, (i16, String)> {
    if topic.assignments.is_empty() {
        validate_automatic(topic, default_partitions, default_replication_factor)
    } else {
        validate_manual(topic)
    }
}

fn validate_automatic(
    topic: &CreatableTopic,
    default_partitions: i32,
    default_replication_factor: i16,
) -> Result<ValidatedCreation, (i16, String)> {
    let replication_factor =
        resolve_replication_factor(topic.replication_factor, default_replication_factor)?;
    if topic.num_partitions < -1 || topic.num_partitions == 0 {
        return error(
            INVALID_PARTITIONS,
            "number of partitions must be positive, or -1 to use the default",
        );
    }

    Ok(ValidatedCreation {
        partitions: if topic.num_partitions == -1 {
            default_partitions
        } else {
            topic.num_partitions
        },
        replication_factor,
    })
}

pub(super) fn resolve_replication_factor(
    requested: i16,
    default_replication_factor: i16,
) -> Result<i16, (i16, String)> {
    if requested < -1 || requested == 0 {
        return error(
            INVALID_REPLICATION_FACTOR,
            "replication factor must be positive, or -1 to use the default",
        );
    }
    let replication_factor = if requested == -1 {
        default_replication_factor
    } else {
        requested
    };
    if replication_factor != VIRTUAL_REPLICATION_FACTOR {
        return error(
            INVALID_REPLICATION_FACTOR,
            "replication factor exceeds the one-broker virtual topology",
        );
    }
    Ok(replication_factor)
}

fn validate_manual(topic: &CreatableTopic) -> Result<ValidatedCreation, (i16, String)> {
    if topic.replication_factor != -1 {
        return error(
            INVALID_REQUEST,
            "manual assignments require replication factor -1",
        );
    }
    if topic.num_partitions != -1 {
        return error(
            INVALID_REQUEST,
            "manual assignments require partition count -1",
        );
    }

    let mut indexes = HashSet::with_capacity(topic.assignments.len());
    for assignment in &topic.assignments {
        if !indexes.insert(assignment.partition_index) {
            return error(
                INVALID_REPLICA_ASSIGNMENT,
                "manual assignments contain a duplicate partition index",
            );
        }
        if assignment.broker_ids != [BrokerId::from(0)] {
            return error(
                INVALID_REPLICA_ASSIGNMENT,
                "every partition must be assigned exactly once to virtual broker 0",
            );
        }
    }
    for partition in 0..topic.assignments.len() {
        if !indexes.contains(&(partition as i32)) {
            return error(
                INVALID_REPLICA_ASSIGNMENT,
                "partition indexes must be a consecutive zero-based sequence",
            );
        }
    }
    let partitions = i32::try_from(topic.assignments.len()).map_err(|_| {
        (
            INVALID_PARTITIONS,
            "manual assignment contains too many partitions".to_owned(),
        )
    })?;
    Ok(ValidatedCreation {
        partitions,
        replication_factor: VIRTUAL_REPLICATION_FACTOR,
    })
}

fn error<T>(code: i16, message: &str) -> Result<T, (i16, String)> {
    Err((code, message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::create_topics_request::CreatableReplicaAssignment;

    #[test]
    fn batch_limit_counts_explicit_defaults_and_manual_assignments() {
        assert!(validate_batch(&[automatic(10_000)], 1).is_ok());
        assert_eq!(
            validate_batch(&[automatic(9_999), automatic(-1), automatic(-1)], 1)
                .unwrap_err()
                .0,
            POLICY_VIOLATION
        );
        assert_eq!(
            validate_batch(&[automatic(-1), automatic(-1)], 5_001)
                .unwrap_err()
                .0,
            POLICY_VIOLATION
        );

        let assignments = (0..10_001)
            .map(|partition| {
                CreatableReplicaAssignment::default()
                    .with_partition_index(partition)
                    .with_broker_ids(vec![BrokerId::from(0)])
            })
            .collect();
        let manual = CreatableTopic::default()
            .with_num_partitions(-1)
            .with_replication_factor(-1)
            .with_assignments(assignments);
        assert_eq!(
            validate_batch(&[manual], 1).unwrap_err().0,
            POLICY_VIOLATION
        );
    }

    fn automatic(partitions: i32) -> CreatableTopic {
        CreatableTopic::default()
            .with_num_partitions(partitions)
            .with_replication_factor(1)
    }
}
