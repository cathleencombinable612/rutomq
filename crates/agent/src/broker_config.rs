use super::Broker;
use super::config_synonyms;
use crate::config::AgentConfig;
use kafka_protocol::messages::alter_configs_request::AlterableConfig as LegacyConfig;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
use kafka_protocol::messages::incremental_alter_configs_request::AlterableConfig;
use kafka_protocol::protocol::StrBytes;
use rutomq_control::ControlError;
use std::collections::{BTreeMap, HashSet};

pub(super) const BROKER_RESOURCE: i8 = 4;
pub(super) const BROKER_LOGGER_RESOURCE: i8 = 8;

const STATIC_BROKER_CONFIG: i8 = 4;
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;
const DYNAMIC_BROKER_LOGGER_CONFIG: i8 = 6;
const BOOLEAN_CONFIG: i8 = 1;
const STRING_CONFIG: i8 = 2;
const INT_CONFIG: i8 = 3;
const LONG_CONFIG: i8 = 5;
const LIST_CONFIG: i8 = 7;
const SET: i8 = 0;
const DELETE: i8 = 1;

pub(super) const CONSUMER_ASSIGNMENT_INTERVAL: &str = "group.consumer.assignment.interval.ms";
pub(super) const SHARE_ASSIGNMENT_INTERVAL: &str = "group.share.assignment.interval.ms";
pub(super) const STREAMS_ASSIGNMENT_INTERVAL: &str = "group.streams.assignment.interval.ms";
pub(super) const CONSUMER_ASSIGNOR_OFFLOAD: &str = "group.consumer.assignor.offload.enable";
pub(super) const SHARE_ASSIGNOR_OFFLOAD: &str = "group.share.assignor.offload.enable";
pub(super) const STREAMS_ASSIGNOR_OFFLOAD: &str = "group.streams.assignor.offload.enable";
pub(super) const GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES: &str =
    "group.coordinator.cached.buffer.max.bytes";
pub(super) const GROUP_COORDINATOR_REBALANCE_PROTOCOLS: &str =
    "group.coordinator.rebalance.protocols";
pub(super) const GROUP_MIN_SESSION_TIMEOUT_MS: &str = "group.min.session.timeout.ms";
pub(super) const GROUP_MAX_SESSION_TIMEOUT_MS: &str = "group.max.session.timeout.ms";
pub(super) const GROUP_MAX_SIZE: &str = "group.max.size";
pub(super) const GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS: &str =
    "group.consumer.min.heartbeat.interval.ms";
pub(super) const GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS: &str =
    "group.consumer.max.heartbeat.interval.ms";
pub(super) const GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS: &str =
    "group.consumer.min.session.timeout.ms";
pub(super) const GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS: &str =
    "group.consumer.max.session.timeout.ms";
pub(super) const GROUP_CONSUMER_MAX_SIZE: &str = "group.consumer.max.size";
pub(super) const GROUP_CONSUMER_ASSIGNORS: &str = "group.consumer.assignors";
pub(super) const GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS: &str =
    "group.consumer.regex.refresh.interval.ms";
pub(super) const GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS: &str =
    "group.streams.min.heartbeat.interval.ms";
pub(super) const GROUP_STREAMS_MAX_HEARTBEAT_INTERVAL_MS: &str =
    "group.streams.max.heartbeat.interval.ms";
pub(super) const GROUP_STREAMS_MIN_SESSION_TIMEOUT_MS: &str =
    "group.streams.min.session.timeout.ms";
pub(super) const GROUP_STREAMS_MAX_SESSION_TIMEOUT_MS: &str =
    "group.streams.max.session.timeout.ms";
pub(super) const GROUP_STREAMS_MAX_SIZE: &str = "group.streams.max.size";
pub(super) const GROUP_STREAMS_MAX_STANDBY_REPLICAS: &str = "group.streams.max.standby.replicas";
pub(super) const GROUP_STREAMS_INITIAL_REBALANCE_DELAY_MS: &str =
    "group.streams.initial.rebalance.delay.ms";
pub(super) const GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS: &str =
    "group.share.min.heartbeat.interval.ms";
pub(super) const GROUP_SHARE_MAX_HEARTBEAT_INTERVAL_MS: &str =
    "group.share.max.heartbeat.interval.ms";
pub(super) const GROUP_SHARE_MIN_SESSION_TIMEOUT_MS: &str = "group.share.min.session.timeout.ms";
pub(super) const GROUP_SHARE_MAX_SESSION_TIMEOUT_MS: &str = "group.share.max.session.timeout.ms";
pub(super) const GROUP_SHARE_MAX_SIZE: &str = "group.share.max.size";
pub(super) const GROUP_SHARE_ASSIGNORS: &str = "group.share.assignors";
pub(super) const GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS: &str =
    "group.share.min.record.lock.duration.ms";
pub(super) const GROUP_SHARE_MAX_RECORD_LOCK_DURATION_MS: &str =
    "group.share.max.record.lock.duration.ms";
pub(super) const TRANSACTION_TWO_PHASE_COMMIT_ENABLE: &str = "transaction.two.phase.commit.enable";
pub(super) const TRANSACTION_MAX_TIMEOUT_MS: &str = "transaction.max.timeout.ms";
pub(super) const CONNECTIONS_MAX_REAUTH_MS: &str = "connections.max.reauth.ms";
pub(super) const OFFSET_METADATA_MAX_BYTES: &str = "offset.metadata.max.bytes";
pub(super) const OFFSETS_RETENTION_MINUTES: &str = "offsets.retention.minutes";
pub(super) const OFFSETS_RETENTION_CHECK_INTERVAL_MS: &str = "offsets.retention.check.interval.ms";
pub(super) const TRANSACTIONAL_ID_EXPIRATION_MS: &str = "transactional.id.expiration.ms";
pub(super) const TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS: &str =
    "transaction.remove.expired.transaction.cleanup.interval.ms";
pub(super) const TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS: &str =
    "transaction.abort.timed.out.transaction.cleanup.interval.ms";
pub(super) const ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS: &str =
    "add.partitions.to.txn.retry.backoff.ms";
pub(super) const ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS: &str =
    "add.partitions.to.txn.retry.backoff.max.ms";
pub(super) const TRANSACTION_PARTITION_VERIFICATION_ENABLE: &str =
    "transaction.partition.verification.enable";
pub(super) const SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES: &str =
    "share.coordinator.cached.buffer.max.bytes";
const DYNAMIC_BROKER_KEYS: [&str; 9] = [
    CONSUMER_ASSIGNMENT_INTERVAL,
    SHARE_ASSIGNMENT_INTERVAL,
    STREAMS_ASSIGNMENT_INTERVAL,
    CONSUMER_ASSIGNOR_OFFLOAD,
    SHARE_ASSIGNOR_OFFLOAD,
    STREAMS_ASSIGNOR_OFFLOAD,
    GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
    SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
    TRANSACTION_PARTITION_VERIFICATION_ENABLE,
];

struct ConfigEntry {
    name: &'static str,
    value: String,
    source: i8,
    config_type: i8,
    documentation: &'static str,
    read_only: bool,
    cluster_wide: bool,
    static_value: Option<String>,
}

pub(super) fn describe_broker(
    config: &AgentConfig,
    dynamic: &BTreeMap<String, String>,
    resource_name: &str,
    requested_keys: Option<&[StrBytes]>,
    version: i16,
    include_synonyms: bool,
    include_documentation: bool,
) -> Result<Vec<DescribeConfigsResourceResult>, ControlError> {
    validate_broker_id(resource_name, true)?;
    let mut entries = broker_entries(config, dynamic);
    if resource_name.is_empty() {
        entries.retain(|entry| entry.cluster_wide);
    }
    Ok(describe_entries(
        entries,
        requested_keys,
        version,
        include_synonyms,
        include_documentation,
    ))
}

pub(super) fn describe_broker_logger(
    config: &AgentConfig,
    resource_name: &str,
    requested_keys: Option<&[StrBytes]>,
    version: i16,
    include_synonyms: bool,
    include_documentation: bool,
) -> Result<Vec<DescribeConfigsResourceResult>, ControlError> {
    validate_broker_id(resource_name, false)?;
    Ok(describe_entries(
        vec![ConfigEntry {
            name: "rutomq.tracing.filter",
            value: config.log_filter.clone(),
            source: DYNAMIC_BROKER_LOGGER_CONFIG,
            config_type: STRING_CONFIG,
            documentation: "Read-only tracing EnvFilter used by this stateless Agent.",
            read_only: true,
            cluster_wide: false,
            static_value: None,
        }],
        requested_keys,
        version,
        include_synonyms,
        include_documentation,
    ))
}

impl Broker {
    pub(super) async fn effective_group_defaults(&self) -> Result<AgentConfig, ControlError> {
        let dynamic = self.metadata.broker_config().await?;
        effective_assignment_defaults(&self.config, &dynamic)
    }

    pub(super) async fn transaction_partition_verification_enabled(
        &self,
    ) -> Result<bool, ControlError> {
        let dynamic = self.metadata.broker_config().await?;
        effective_boolean_value(
            self.config.transaction_partition_verification_enable,
            &dynamic,
            TRANSACTION_PARTITION_VERIFICATION_ENABLE,
        )
    }

    pub(super) async fn alter_broker_config(
        &self,
        resource_name: &str,
        changes: &[AlterableConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        validate_dynamic_resource_name(resource_name)?;
        let mut names = HashSet::new();
        let mut proposed = self.metadata.broker_config().await?;
        let mut updates = BTreeMap::new();
        for change in changes {
            let key = change.name.as_str();
            if !names.insert(key.to_owned()) {
                return Err(ControlError::InvalidRequest(format!(
                    "duplicate broker configuration {key}"
                )));
            }
            ensure_dynamic_supported(key)?;
            match change.config_operation {
                SET => {
                    let value = change.value.as_ref().ok_or_else(|| {
                        ControlError::InvalidRequest(format!(
                            "broker configuration {key} requires a value"
                        ))
                    })?;
                    proposed.insert(key.to_owned(), value.as_str().to_owned());
                    updates.insert(key.to_owned(), Some(value.as_str().to_owned()));
                }
                DELETE => {
                    proposed.remove(key);
                    updates.insert(key.to_owned(), None);
                }
                operation => {
                    return Err(ControlError::InvalidRequest(format!(
                        "broker configuration {key} does not support operation {operation}"
                    )));
                }
            }
        }
        validate_dynamic_broker_defaults(&self.config, &proposed)?;
        self.metadata
            .alter_broker_config(updates, validate_only)
            .await
    }

    pub(super) async fn replace_broker_config(
        &self,
        resource_name: &str,
        configs: &[LegacyConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        validate_dynamic_resource_name(resource_name)?;
        let mut names = HashSet::new();
        let mut replacement = BTreeMap::new();
        for config in configs {
            let key = config.name.as_str();
            if !names.insert(key.to_owned()) {
                return Err(ControlError::InvalidRequest(format!(
                    "duplicate broker configuration {key}"
                )));
            }
            ensure_dynamic_supported(key)?;
            let value = config.value.as_ref().ok_or_else(|| {
                ControlError::InvalidRequest(format!("broker configuration {key} requires a value"))
            })?;
            replacement.insert(key.to_owned(), value.as_str().to_owned());
        }
        validate_dynamic_broker_defaults(&self.config, &replacement)?;
        let mut updates = DYNAMIC_BROKER_KEYS
            .into_iter()
            .map(|key| (key.to_owned(), None))
            .collect::<BTreeMap<_, _>>();
        for (key, value) in replacement {
            updates.insert(key, Some(value));
        }
        self.metadata
            .alter_broker_config(updates, validate_only)
            .await
    }
}

fn effective_assignment_defaults(
    config: &AgentConfig,
    dynamic: &BTreeMap<String, String>,
) -> Result<AgentConfig, ControlError> {
    let mut effective = config.clone();
    effective.group_assignment_interval_ms =
        effective_assignment_value(config, dynamic, CONSUMER_ASSIGNMENT_INTERVAL)?;
    effective.share_group_assignment_interval_ms =
        effective_assignment_value(config, dynamic, SHARE_ASSIGNMENT_INTERVAL)?;
    effective.streams_group_assignment_interval_ms =
        effective_assignment_value(config, dynamic, STREAMS_ASSIGNMENT_INTERVAL)?;
    effective.consumer_assignor_offload_enable = effective_boolean_value(
        config.consumer_assignor_offload_enable,
        dynamic,
        CONSUMER_ASSIGNOR_OFFLOAD,
    )?;
    effective.share_assignor_offload_enable = effective_boolean_value(
        config.share_assignor_offload_enable,
        dynamic,
        SHARE_ASSIGNOR_OFFLOAD,
    )?;
    effective.streams_assignor_offload_enable = effective_boolean_value(
        config.streams_assignor_offload_enable,
        dynamic,
        STREAMS_ASSIGNOR_OFFLOAD,
    )?;
    Ok(effective)
}

fn effective_boolean_value(
    default: bool,
    dynamic: &BTreeMap<String, String>,
    key: &str,
) -> Result<bool, ControlError> {
    dynamic.get(key).map_or(Ok(default), |value| {
        value.parse::<bool>().map_err(|_| {
            ControlError::InvalidRequest(format!(
                "broker configuration {key} must be true or false"
            ))
        })
    })
}

fn effective_assignment_value(
    config: &AgentConfig,
    dynamic: &BTreeMap<String, String>,
    key: &str,
) -> Result<i32, ControlError> {
    let (default, minimum, maximum) = assignment_bounds(config, key);
    let Some(value) = dynamic.get(key) else {
        return Ok(default);
    };
    let value = value.parse::<i32>().map_err(|_| {
        ControlError::InvalidRequest(format!("broker configuration {key} must be an integer"))
    })?;
    Ok(value.clamp(minimum, maximum))
}

fn validate_dynamic_broker_defaults(
    config: &AgentConfig,
    values: &BTreeMap<String, String>,
) -> Result<(), ControlError> {
    for key in [
        CONSUMER_ASSIGNMENT_INTERVAL,
        SHARE_ASSIGNMENT_INTERVAL,
        STREAMS_ASSIGNMENT_INTERVAL,
    ] {
        let Some(value) = values.get(key) else {
            continue;
        };
        let value = value.parse::<i32>().map_err(|_| {
            ControlError::InvalidRequest(format!("broker configuration {key} must be an integer"))
        })?;
        let (_, minimum, maximum) = assignment_bounds(config, key);
        if !(minimum..=maximum).contains(&value) {
            return Err(ControlError::InvalidRequest(format!(
                "broker configuration {key} must be between {minimum} and {maximum}"
            )));
        }
    }
    for key in [
        CONSUMER_ASSIGNOR_OFFLOAD,
        SHARE_ASSIGNOR_OFFLOAD,
        STREAMS_ASSIGNOR_OFFLOAD,
        TRANSACTION_PARTITION_VERIFICATION_ENABLE,
    ] {
        if let Some(value) = values.get(key) {
            value.parse::<bool>().map_err(|_| {
                ControlError::InvalidRequest(format!(
                    "broker configuration {key} must be true or false"
                ))
            })?;
        }
    }
    for key in [
        GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
        SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
    ] {
        let Some(value) = values.get(key) else {
            continue;
        };
        let value = value.parse::<i32>().map_err(|_| {
            ControlError::InvalidRequest(format!("broker configuration {key} must be an integer"))
        })?;
        if value < 524_288 {
            return Err(ControlError::InvalidRequest(format!(
                "broker configuration {key} must be at least 524288"
            )));
        }
    }
    Ok(())
}

fn assignment_bounds(config: &AgentConfig, key: &str) -> (i32, i32, i32) {
    match key {
        CONSUMER_ASSIGNMENT_INTERVAL => (
            config.group_assignment_interval_ms,
            config.group_min_assignment_interval_ms,
            config.group_max_assignment_interval_ms,
        ),
        SHARE_ASSIGNMENT_INTERVAL => (
            config.share_group_assignment_interval_ms,
            config.share_group_min_assignment_interval_ms,
            config.share_group_max_assignment_interval_ms,
        ),
        STREAMS_ASSIGNMENT_INTERVAL => (
            config.streams_group_assignment_interval_ms,
            config.streams_group_min_assignment_interval_ms,
            config.streams_group_max_assignment_interval_ms,
        ),
        _ => unreachable!("assignment key was validated"),
    }
}

fn validate_dynamic_resource_name(resource_name: &str) -> Result<(), ControlError> {
    if resource_name.is_empty() {
        Ok(())
    } else {
        Err(ControlError::InvalidRequest(
            "cluster-wide broker configurations require an empty resource name".to_owned(),
        ))
    }
}

fn ensure_dynamic_supported(key: &str) -> Result<(), ControlError> {
    if DYNAMIC_BROKER_KEYS.contains(&key) {
        Ok(())
    } else {
        Err(ControlError::InvalidRequest(format!(
            "broker configuration {key} is read-only or unknown"
        )))
    }
}

fn validate_broker_id(resource_name: &str, allow_empty: bool) -> Result<(), ControlError> {
    if resource_name == "0" || (allow_empty && resource_name.is_empty()) {
        return Ok(());
    }
    let expected = if allow_empty { "0 or empty" } else { "0" };
    Err(ControlError::InvalidRequest(format!(
        "unexpected broker id, expected {expected}, but received {resource_name}"
    )))
}

fn broker_entries(config: &AgentConfig, dynamic: &BTreeMap<String, String>) -> Vec<ConfigEntry> {
    let protocol = security_protocol(config);
    vec![
        entry("node.id", "0", INT_CONFIG, "Virtual Kafka broker node ID."),
        entry(
            "broker.id",
            "0",
            INT_CONFIG,
            "Legacy alias for the virtual Kafka broker node ID.",
        ),
        entry(
            "listeners",
            &format!("{protocol}://{}", config.kafka_addr),
            LIST_CONFIG,
            "Kafka protocol listener owned by this Agent.",
        ),
        entry(
            "advertised.listeners",
            &format!(
                "{protocol}://{}:{}",
                advertised_host(&config.advertise_host),
                config.advertise_port
            ),
            LIST_CONFIG,
            "Stable Kafka service endpoint advertised to clients.",
        ),
        entry(
            "socket.request.max.bytes",
            &config.max_frame_size.to_string(),
            INT_CONFIG,
            "Maximum accepted Kafka request frame size.",
        ),
        entry(
            CONNECTIONS_MAX_REAUTH_MS,
            &config.security.sasl_max_reauth_ms.to_string(),
            LONG_CONFIG,
            "Maximum lifetime of an authenticated SCRAM connection before re-authentication; zero disables the bound.",
        ),
        entry(
            "max.request.partition.size.limit",
            &config.max_request_partition_size_limit.to_string(),
            INT_CONFIG,
            "Maximum partition metadata entries returned in one request.",
        ),
        entry(
            "num.partitions",
            &config.num_partitions.to_string(),
            INT_CONFIG,
            "Virtual-controller partition default for Metadata, Streams, and Admin topic creation.",
        ),
        entry(
            "default.replication.factor",
            &config.default_replication_factor.to_string(),
            INT_CONFIG,
            "Replication factor of the one-broker virtual topology.",
        ),
        entry(
            "auto.create.topics.enable",
            &config.auto_create_topics_enable.to_string(),
            BOOLEAN_CONFIG,
            "Whether Metadata requests may automatically create missing topics.",
        ),
        entry(
            "min.insync.replicas",
            "1",
            INT_CONFIG,
            "Default minimum in-sync replica count of the virtual topology.",
        ),
        entry(
            "producer.id.expiration.ms",
            &config.producer_id_expiration_ms.to_string(),
            INT_CONFIG,
            "Maximum idle time before partition-local producer state expires.",
        ),
        entry(
            OFFSET_METADATA_MAX_BYTES,
            &config.offset_metadata_max_bytes.to_string(),
            INT_CONFIG,
            "Maximum Kafka string length of metadata associated with one offset commit.",
        ),
        entry(
            OFFSETS_RETENTION_MINUTES,
            &config.offsets_retention_minutes.to_string(),
            INT_CONFIG,
            "Retention period for committed consumer offsets.",
        ),
        entry(
            OFFSETS_RETENTION_CHECK_INTERVAL_MS,
            &config
                .offsets_retention_check_interval
                .as_millis()
                .to_string(),
            LONG_CONFIG,
            "Interval between consumer offset expiration sweeps.",
        ),
        entry(
            TRANSACTIONAL_ID_EXPIRATION_MS,
            &config.transactional_id_expiration_ms.to_string(),
            INT_CONFIG,
            "Maximum idle time before a completed or empty transactional ID expires.",
        ),
        entry(
            TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS,
            &config
                .transactional_id_expiration_check_interval
                .as_millis()
                .to_string(),
            INT_CONFIG,
            "Interval between transactional ID expiration sweeps.",
        ),
        entry(
            TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS,
            &config
                .transaction_abort_timed_out_cleanup_interval
                .as_millis()
                .to_string(),
            INT_CONFIG,
            "Interval between timed-out transaction abort sweeps.",
        ),
        entry(
            ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS,
            "20",
            INT_CONFIG,
            "Initial server-side retry backoff for adding a partition to a transaction.",
        ),
        entry(
            ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS,
            "100",
            INT_CONFIG,
            "Maximum server-side retry backoff for adding a partition to a transaction.",
        ),
        dynamic_boolean_entry(
            TRANSACTION_PARTITION_VERIFICATION_ENABLE,
            config.transaction_partition_verification_enable,
            dynamic,
            "Whether legacy transactional Produce verifies that each partition was added to the transaction.",
        ),
        entry(
            TRANSACTION_MAX_TIMEOUT_MS,
            &config.transaction_max_timeout_ms.to_string(),
            INT_CONFIG,
            "Maximum timeout accepted for an ordinary transaction.",
        ),
        entry(
            TRANSACTION_TWO_PHASE_COMMIT_ENABLE,
            &config.transaction_two_phase_commit_enable.to_string(),
            BOOLEAN_CONFIG,
            "Whether KIP-939 two-phase transaction participation is enabled.",
        ),
        entry(
            "group.initial.rebalance.delay.ms",
            &config.classic_group_initial_rebalance_delay_ms.to_string(),
            INT_CONFIG,
            "Initial delay used to gather classic JoinGroup members.",
        ),
        entry(
            GROUP_MIN_SESSION_TIMEOUT_MS,
            &config.classic_group_min_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Minimum session timeout accepted from classic group members.",
        ),
        entry(
            GROUP_MAX_SESSION_TIMEOUT_MS,
            &config.classic_group_max_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Maximum session timeout accepted from classic group members.",
        ),
        entry(
            GROUP_MAX_SIZE,
            &config.classic_group_max_size.to_string(),
            INT_CONFIG,
            "Maximum number of members in a classic group.",
        ),
        entry(
            "group.coordinator.background.threads",
            &config.group_coordinator_background_threads.to_string(),
            INT_CONFIG,
            "Dedicated background workers used for group assignment computation.",
        ),
        entry(
            GROUP_COORDINATOR_REBALANCE_PROTOCOLS,
            "classic,consumer,streams",
            LIST_CONFIG,
            "Deprecated in Kafka 4.3; group protocol availability is controlled by feature versions.",
        ),
        dynamic_int_entry(
            GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
            config.group_coordinator_cached_buffer_max_bytes,
            dynamic,
            "Maximum reusable GroupCoordinator append buffer size; rutomq retains no such buffers.",
        ),
        dynamic_int_entry(
            SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES,
            config.share_coordinator_cached_buffer_max_bytes,
            dynamic,
            "Maximum reusable ShareCoordinator append buffer size; rutomq retains no such buffers.",
        ),
        dynamic_boolean_entry(
            CONSUMER_ASSIGNOR_OFFLOAD,
            config.consumer_assignor_offload_enable,
            dynamic,
            "Whether consumer group assignment computation is offloaded.",
        ),
        dynamic_int_entry(
            CONSUMER_ASSIGNMENT_INTERVAL,
            config.group_assignment_interval_ms,
            dynamic,
            "Interval between assignment updates for consumer groups.",
        ),
        entry(
            "group.consumer.min.assignment.interval.ms",
            &config.group_min_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level consumer assignment interval.",
        ),
        entry(
            "group.consumer.max.assignment.interval.ms",
            &config.group_max_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level consumer assignment interval.",
        ),
        entry(
            "group.consumer.heartbeat.interval.ms",
            &config.group_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Broker-driven consumer group heartbeat interval.",
        ),
        entry(
            GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS,
            &config.consumer_group_min_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level consumer heartbeat interval.",
        ),
        entry(
            GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS,
            &config.consumer_group_max_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level consumer heartbeat interval.",
        ),
        entry(
            "group.consumer.session.timeout.ms",
            &config.group_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Broker-driven consumer group session timeout.",
        ),
        entry(
            GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS,
            &config.consumer_group_min_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level consumer session timeout.",
        ),
        entry(
            GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS,
            &config.consumer_group_max_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level consumer session timeout.",
        ),
        entry(
            GROUP_CONSUMER_MAX_SIZE,
            &config.consumer_group_max_size.to_string(),
            INT_CONFIG,
            "Maximum number of members in a consumer-protocol group.",
        ),
        entry(
            GROUP_CONSUMER_ASSIGNORS,
            &config.consumer_group_assignors.join(","),
            LIST_CONFIG,
            "Ordered built-in assignors available to consumer-protocol groups.",
        ),
        entry(
            GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS,
            &config.consumer_group_regex_refresh_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum interval between consumer group regex resolution refreshes.",
        ),
        dynamic_int_entry(
            STREAMS_ASSIGNMENT_INTERVAL,
            config.streams_group_assignment_interval_ms,
            dynamic,
            "Interval between assignment updates for streams groups.",
        ),
        entry(
            "group.streams.min.assignment.interval.ms",
            &config.streams_group_min_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level streams assignment interval.",
        ),
        entry(
            "group.streams.max.assignment.interval.ms",
            &config.streams_group_max_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level streams assignment interval.",
        ),
        entry(
            "group.streams.heartbeat.interval.ms",
            &config.streams_group_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Broker-driven Kafka Streams group heartbeat interval.",
        ),
        entry(
            GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS,
            &config.streams_group_min_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level streams heartbeat interval.",
        ),
        entry(
            GROUP_STREAMS_MAX_HEARTBEAT_INTERVAL_MS,
            &config.streams_group_max_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level streams heartbeat interval.",
        ),
        entry(
            "group.streams.session.timeout.ms",
            &config.streams_group_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Broker-driven Kafka Streams group session timeout.",
        ),
        entry(
            GROUP_STREAMS_MIN_SESSION_TIMEOUT_MS,
            &config.streams_group_min_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level streams session timeout.",
        ),
        entry(
            GROUP_STREAMS_MAX_SESSION_TIMEOUT_MS,
            &config.streams_group_max_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level streams session timeout.",
        ),
        entry(
            GROUP_STREAMS_MAX_SIZE,
            &config.streams_group_max_size.to_string(),
            INT_CONFIG,
            "Maximum number of clients in a Streams group.",
        ),
        dynamic_boolean_entry(
            STREAMS_ASSIGNOR_OFFLOAD,
            config.streams_assignor_offload_enable,
            dynamic,
            "Whether streams group assignment computation is offloaded.",
        ),
        entry(
            "group.streams.num.standby.replicas",
            &config.streams_group_num_standby_replicas.to_string(),
            INT_CONFIG,
            "Default standby task count for the Kafka Streams group protocol.",
        ),
        entry(
            GROUP_STREAMS_MAX_STANDBY_REPLICAS,
            &config.streams_group_max_standby_replicas.to_string(),
            INT_CONFIG,
            "Maximum group-level standby task count for the Kafka Streams group protocol.",
        ),
        entry(
            GROUP_STREAMS_INITIAL_REBALANCE_DELAY_MS,
            &config.streams_group_initial_rebalance_delay_ms.to_string(),
            INT_CONFIG,
            "Initial delay used to gather members of a new Kafka Streams group.",
        ),
        dynamic_int_entry(
            SHARE_ASSIGNMENT_INTERVAL,
            config.share_group_assignment_interval_ms,
            dynamic,
            "Interval between assignment updates for share groups.",
        ),
        entry(
            "group.share.min.assignment.interval.ms",
            &config.share_group_min_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level share assignment interval.",
        ),
        entry(
            "group.share.max.assignment.interval.ms",
            &config.share_group_max_assignment_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level share assignment interval.",
        ),
        entry(
            "group.share.heartbeat.interval.ms",
            &config.share_group_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Share group heartbeat interval.",
        ),
        entry(
            GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS,
            &config.share_group_min_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level share heartbeat interval.",
        ),
        entry(
            GROUP_SHARE_MAX_HEARTBEAT_INTERVAL_MS,
            &config.share_group_max_heartbeat_interval_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level share heartbeat interval.",
        ),
        entry(
            "group.share.session.timeout.ms",
            &config.share_group_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Share group session timeout.",
        ),
        entry(
            GROUP_SHARE_MIN_SESSION_TIMEOUT_MS,
            &config.share_group_min_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level share session timeout.",
        ),
        entry(
            GROUP_SHARE_MAX_SESSION_TIMEOUT_MS,
            &config.share_group_max_session_timeout_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level share session timeout.",
        ),
        entry(
            GROUP_SHARE_MAX_SIZE,
            &config.share_group_max_size.to_string(),
            INT_CONFIG,
            "Maximum number of members in a share group.",
        ),
        entry(
            GROUP_SHARE_ASSIGNORS,
            &config.share_group_assignors.join(","),
            LIST_CONFIG,
            "The single server-side partition assignor used by all share groups.",
        ),
        dynamic_boolean_entry(
            SHARE_ASSIGNOR_OFFLOAD,
            config.share_assignor_offload_enable,
            dynamic,
            "Whether share group assignment computation is offloaded.",
        ),
        entry(
            "group.share.record.lock.duration.ms",
            &config.share_record_lock_duration_ms.to_string(),
            INT_CONFIG,
            "Share record acquisition lock duration.",
        ),
        entry(
            GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS,
            &config.share_min_record_lock_duration_ms.to_string(),
            INT_CONFIG,
            "Minimum group-level share record acquisition lock duration.",
        ),
        entry(
            GROUP_SHARE_MAX_RECORD_LOCK_DURATION_MS,
            &config.share_max_record_lock_duration_ms.to_string(),
            INT_CONFIG,
            "Maximum group-level share record acquisition lock duration.",
        ),
        entry(
            "group.share.delivery.count.limit",
            &config.share_record_delivery_count_limit.to_string(),
            INT_CONFIG,
            "Maximum delivery attempts for a share record.",
        ),
        entry(
            "group.share.max.delivery.count.limit",
            &config.share_max_delivery_count_limit.to_string(),
            INT_CONFIG,
            "Maximum group-level delivery-count limit for share records.",
        ),
        entry(
            "group.share.min.delivery.count.limit",
            &config.share_min_delivery_count_limit.to_string(),
            INT_CONFIG,
            "Minimum group-level delivery-count limit for share records.",
        ),
        entry(
            "group.share.partition.max.record.locks",
            &config.share_partition_max_record_locks.to_string(),
            INT_CONFIG,
            "Default record-lock limit per share partition.",
        ),
        entry(
            "group.share.max.partition.max.record.locks",
            &config.share_max_partition_max_record_locks.to_string(),
            INT_CONFIG,
            "Maximum group-level record-lock limit per share partition.",
        ),
        entry(
            "group.share.min.partition.max.record.locks",
            &config.share_min_partition_max_record_locks.to_string(),
            INT_CONFIG,
            "Minimum group-level record-lock limit per share partition.",
        ),
        entry(
            "rutomq.object.flush.interval.ms",
            &config.flush_interval.as_millis().to_string(),
            LONG_CONFIG,
            "Maximum Agent memory-batch interval before immutable object commit.",
        ),
        entry(
            "rutomq.object.flush.max.bytes",
            &config.max_batch_bytes.to_string(),
            LONG_CONFIG,
            "Maximum Agent memory-batch size before immutable object commit.",
        ),
        entry(
            "rutomq.fetch.cache.bytes",
            &config.fetch_cache_bytes.to_string(),
            LONG_CONFIG,
            "Bound on the Agent-local immutable object range cache.",
        ),
        entry(
            "rutomq.local.wal.enabled",
            "false",
            BOOLEAN_CONFIG,
            "Always false: Agents persist acknowledged data directly to object storage.",
        ),
    ]
}

fn entry(
    name: &'static str,
    value: &str,
    config_type: i8,
    documentation: &'static str,
) -> ConfigEntry {
    ConfigEntry {
        name,
        value: value.to_owned(),
        source: STATIC_BROKER_CONFIG,
        config_type,
        documentation,
        read_only: true,
        cluster_wide: false,
        static_value: None,
    }
}

fn dynamic_int_entry(
    name: &'static str,
    static_value: i32,
    dynamic: &BTreeMap<String, String>,
    documentation: &'static str,
) -> ConfigEntry {
    let static_value = static_value.to_string();
    let value = dynamic
        .get(name)
        .cloned()
        .unwrap_or_else(|| static_value.clone());
    ConfigEntry {
        name,
        value,
        source: if dynamic.contains_key(name) {
            DYNAMIC_DEFAULT_BROKER_CONFIG
        } else {
            STATIC_BROKER_CONFIG
        },
        config_type: INT_CONFIG,
        documentation,
        read_only: false,
        cluster_wide: true,
        static_value: Some(static_value),
    }
}

fn dynamic_boolean_entry(
    name: &'static str,
    static_value: bool,
    dynamic: &BTreeMap<String, String>,
    documentation: &'static str,
) -> ConfigEntry {
    let static_value = static_value.to_string();
    let value = dynamic
        .get(name)
        .cloned()
        .unwrap_or_else(|| static_value.clone());
    ConfigEntry {
        name,
        value,
        source: if dynamic.contains_key(name) {
            DYNAMIC_DEFAULT_BROKER_CONFIG
        } else {
            STATIC_BROKER_CONFIG
        },
        config_type: BOOLEAN_CONFIG,
        documentation,
        read_only: false,
        cluster_wide: true,
        static_value: Some(static_value),
    }
}

fn describe_entries(
    entries: Vec<ConfigEntry>,
    requested_keys: Option<&[StrBytes]>,
    version: i16,
    include_synonyms: bool,
    include_documentation: bool,
) -> Vec<DescribeConfigsResourceResult> {
    let requested = requested_keys.map(|keys| {
        keys.iter()
            .map(|key| key.as_str().to_owned())
            .collect::<HashSet<_>>()
    });
    entries
        .into_iter()
        .filter(|entry| {
            requested
                .as_ref()
                .is_none_or(|keys| keys.contains(entry.name))
        })
        .map(|entry| {
            let synonyms = if include_synonyms && entry.source == DYNAMIC_DEFAULT_BROKER_CONFIG {
                config_synonyms::dynamic_default_broker(
                    entry.name,
                    &entry.value,
                    entry
                        .static_value
                        .as_deref()
                        .expect("dynamic broker config has a static fallback"),
                )
            } else if include_synonyms {
                config_synonyms::same_name(entry.name, &entry.value, entry.source)
            } else {
                Vec::new()
            };
            let result = DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_static_str(entry.name))
                .with_value(Some(StrBytes::from_string(entry.value)))
                .with_read_only(entry.read_only)
                .with_config_source(entry.source)
                .with_is_sensitive(false)
                .with_synonyms(synonyms);
            if version >= 3 {
                result
                    .with_config_type(entry.config_type)
                    .with_documentation(
                        include_documentation
                            .then(|| StrBytes::from_static_str(entry.documentation)),
                    )
            } else {
                result
            }
        })
        .collect()
}

fn security_protocol(config: &AgentConfig) -> &'static str {
    match (
        config.security.tls_enabled(),
        config.security.sasl_enabled(),
    ) {
        (true, true) => "SASL_SSL",
        (true, false) => "SSL",
        (false, true) => "SASL_PLAINTEXT",
        (false, false) => "PLAINTEXT",
    }
}

fn advertised_host(host: &str) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}
