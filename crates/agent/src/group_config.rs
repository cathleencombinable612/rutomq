use super::group_offset_reset::{ShareOffsetReset, parse_share_offset_reset};
use super::{Broker, config_synonyms};
use crate::config::AgentConfig;
use kafka_protocol::messages::alter_configs_request::AlterableConfig as LegacyConfig;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
use kafka_protocol::messages::incremental_alter_configs_request::AlterableConfig;
use kafka_protocol::protocol::StrBytes;
use rutomq_control::ControlError;
use std::collections::{BTreeMap, HashSet};

pub(super) const GROUP_RESOURCE: i8 = 32;
const SET: i8 = 0;
const DELETE: i8 = 1;
const BOOLEAN_CONFIG: i8 = 1;
const INT_CONFIG: i8 = 3;
const STRING_CONFIG: i8 = 2;
const DEFAULT_CONFIG: i8 = 5;
const DYNAMIC_GROUP_CONFIG: i8 = 8;

const CONSUMER_ASSIGNMENT: &str = "consumer.assignment.interval.ms";
const CONSUMER_ASSIGNOR_OFFLOAD: &str = "consumer.assignor.offload.enable";
const CONSUMER_HEARTBEAT: &str = "consumer.heartbeat.interval.ms";
const CONSUMER_SESSION: &str = "consumer.session.timeout.ms";
const SHARE_ASSIGNMENT: &str = "share.assignment.interval.ms";
const SHARE_ASSIGNOR_OFFLOAD: &str = "share.assignor.offload.enable";
const SHARE_AUTO_OFFSET_RESET: &str = "share.auto.offset.reset";
const SHARE_DELIVERY_COUNT_LIMIT: &str = "share.delivery.count.limit";
const SHARE_HEARTBEAT: &str = "share.heartbeat.interval.ms";
const SHARE_ISOLATION: &str = "share.isolation.level";
const SHARE_PARTITION_MAX_RECORD_LOCKS: &str = "share.partition.max.record.locks";
const SHARE_LOCK_DURATION: &str = "share.record.lock.duration.ms";
const SHARE_RENEW_ACKNOWLEDGE_ENABLE: &str = "share.renew.acknowledge.enable";
const SHARE_SESSION: &str = "share.session.timeout.ms";
const STREAMS_ASSIGNMENT: &str = "streams.assignment.interval.ms";
const STREAMS_ASSIGNOR_OFFLOAD: &str = "streams.assignor.offload.enable";
const STREAMS_HEARTBEAT: &str = "streams.heartbeat.interval.ms";
const STREAMS_INITIAL_DELAY: &str = "streams.initial.rebalance.delay.ms";
const STREAMS_STANDBY: &str = "streams.num.standby.replicas";
const STREAMS_SESSION: &str = "streams.session.timeout.ms";
const CONFIG_KEYS: [&str; 20] = [
    CONSUMER_ASSIGNMENT,
    CONSUMER_ASSIGNOR_OFFLOAD,
    CONSUMER_HEARTBEAT,
    CONSUMER_SESSION,
    SHARE_ASSIGNMENT,
    SHARE_ASSIGNOR_OFFLOAD,
    SHARE_AUTO_OFFSET_RESET,
    SHARE_DELIVERY_COUNT_LIMIT,
    SHARE_HEARTBEAT,
    SHARE_ISOLATION,
    SHARE_PARTITION_MAX_RECORD_LOCKS,
    SHARE_LOCK_DURATION,
    SHARE_RENEW_ACKNOWLEDGE_ENABLE,
    SHARE_SESSION,
    STREAMS_ASSIGNMENT,
    STREAMS_ASSIGNOR_OFFLOAD,
    STREAMS_HEARTBEAT,
    STREAMS_INITIAL_DELAY,
    STREAMS_STANDBY,
    STREAMS_SESSION,
];

#[derive(Clone)]
pub(super) struct GroupRuntimeConfig {
    pub consumer_assignment_interval_ms: i32,
    pub consumer_assignor_offload_enable: bool,
    pub consumer_heartbeat_interval_ms: i32,
    pub consumer_session_timeout_ms: i32,
    pub share_assignment_interval_ms: i32,
    pub share_assignor_offload_enable: bool,
    pub share_auto_offset_reset: ShareOffsetReset,
    pub share_delivery_count_limit: i16,
    pub share_heartbeat_interval_ms: i32,
    pub share_isolation_level: String,
    pub share_partition_max_record_locks: i32,
    pub share_record_lock_duration_ms: i32,
    pub share_renew_acknowledge_enable: bool,
    pub share_session_timeout_ms: i32,
    pub streams_assignment_interval_ms: i32,
    pub streams_assignor_offload_enable: bool,
    pub streams_heartbeat_interval_ms: i32,
    pub streams_initial_rebalance_delay_ms: i32,
    pub streams_num_standby_replicas: i32,
    pub streams_session_timeout_ms: i32,
}

impl Broker {
    pub(super) async fn group_runtime_config(
        &self,
        group_id: &str,
    ) -> Result<GroupRuntimeConfig, ControlError> {
        let values = self.metadata.group_config(group_id).await?;
        let defaults = self.effective_group_defaults().await?;
        runtime_config(&defaults, &values)
    }

    pub(super) async fn alter_group_config(
        &self,
        group_id: &str,
        changes: &[AlterableConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        if group_id.is_empty() {
            return Err(ControlError::InvalidRequest(
                "group configuration resource name must not be empty".to_owned(),
            ));
        }
        let mut names = HashSet::new();
        let mut proposed = self.metadata.group_config(group_id).await?;
        let mut updates = BTreeMap::new();
        for change in changes {
            let key = change.name.as_str();
            if !names.insert(key.to_owned()) {
                return Err(ControlError::InvalidRequest(format!(
                    "duplicate group configuration {key}"
                )));
            }
            ensure_supported(key)?;
            match change.config_operation {
                SET => {
                    let value = change.value.as_ref().ok_or_else(|| {
                        ControlError::InvalidRequest(format!(
                            "group configuration {key} requires a value"
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
                        "group configuration {key} does not support operation {operation}"
                    )));
                }
            }
        }
        let defaults = self.effective_group_defaults().await?;
        validate_update_bounds(&defaults, &proposed)?;
        runtime_config(&defaults, &proposed)?;
        self.metadata
            .alter_group_config(group_id, updates, validate_only)
            .await
    }

    pub(super) async fn replace_group_config(
        &self,
        group_id: &str,
        configs: &[LegacyConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        if group_id.is_empty() {
            return Err(ControlError::InvalidRequest(
                "group configuration resource name must not be empty".to_owned(),
            ));
        }
        let mut names = HashSet::new();
        let mut replacement = BTreeMap::new();
        for config in configs {
            let key = config.name.as_str();
            if !names.insert(key.to_owned()) {
                return Err(ControlError::InvalidRequest(format!(
                    "duplicate group configuration {key}"
                )));
            }
            ensure_supported(key)?;
            let value = config.value.as_ref().ok_or_else(|| {
                ControlError::InvalidRequest(format!("group configuration {key} requires a value"))
            })?;
            replacement.insert(key.to_owned(), value.as_str().to_owned());
        }
        let defaults = self.effective_group_defaults().await?;
        validate_update_bounds(&defaults, &replacement)?;
        runtime_config(&defaults, &replacement)?;

        let mut updates = CONFIG_KEYS
            .into_iter()
            .map(|key| (key.to_owned(), None))
            .collect::<BTreeMap<_, _>>();
        for (key, value) in replacement {
            updates.insert(key, Some(value));
        }
        self.metadata
            .alter_group_config(group_id, updates, validate_only)
            .await
    }

    pub(super) async fn describe_group_config(
        &self,
        group_id: &str,
        requested_keys: Option<&[StrBytes]>,
        version: i16,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<Vec<DescribeConfigsResourceResult>, ControlError> {
        let values = self.metadata.group_config(group_id).await?;
        let defaults = self.effective_group_defaults().await?;
        let runtime = runtime_config(&defaults, &values)?;
        let requested = requested_keys.map(|keys| {
            keys.iter()
                .map(|key| key.as_str().to_owned())
                .collect::<HashSet<_>>()
        });
        Ok(entries(&runtime)
            .into_iter()
            .filter(|entry| {
                requested
                    .as_ref()
                    .is_none_or(|requested| requested.contains(entry.name))
            })
            .map(|entry| {
                let source = if values.contains_key(entry.name) {
                    DYNAMIC_GROUP_CONFIG
                } else {
                    DEFAULT_CONFIG
                };
                let synonyms = if include_synonyms {
                    config_synonyms::same_name(entry.name, &entry.value, source)
                } else {
                    Vec::new()
                };
                let result = DescribeConfigsResourceResult::default()
                    .with_name(StrBytes::from_string(entry.name.to_owned()))
                    .with_value(Some(StrBytes::from_string(entry.value)))
                    .with_read_only(false)
                    .with_config_source(source)
                    .with_is_sensitive(false)
                    .with_synonyms(synonyms);
                if version >= 3 {
                    result
                        .with_config_type(entry.config_type)
                        .with_documentation(
                            include_documentation
                                .then(|| StrBytes::from_string(entry.documentation.to_owned())),
                        )
                } else {
                    result
                }
            })
            .collect())
    }
}

struct ConfigEntry {
    name: &'static str,
    value: String,
    config_type: i8,
    documentation: &'static str,
}

fn entries(config: &GroupRuntimeConfig) -> Vec<ConfigEntry> {
    vec![
        integer_entry(
            CONSUMER_ASSIGNMENT,
            config.consumer_assignment_interval_ms,
            "Interval between assignment updates for consumer groups.",
        ),
        boolean_entry(
            CONSUMER_ASSIGNOR_OFFLOAD,
            config.consumer_assignor_offload_enable,
            "Whether assignment computation is offloaded for this consumer group.",
        ),
        integer_entry(
            CONSUMER_HEARTBEAT,
            config.consumer_heartbeat_interval_ms,
            "Heartbeat interval for the consumer group protocol.",
        ),
        integer_entry(
            CONSUMER_SESSION,
            config.consumer_session_timeout_ms,
            "Session timeout for the consumer group protocol.",
        ),
        integer_entry(
            SHARE_ASSIGNMENT,
            config.share_assignment_interval_ms,
            "Interval between assignment updates for share groups.",
        ),
        boolean_entry(
            SHARE_ASSIGNOR_OFFLOAD,
            config.share_assignor_offload_enable,
            "Whether assignment computation is offloaded for this share group.",
        ),
        string_entry(
            SHARE_AUTO_OFFSET_RESET,
            config.share_auto_offset_reset.configured_value(),
            "Initial offset strategy for share groups.",
        ),
        integer_entry(
            SHARE_DELIVERY_COUNT_LIMIT,
            i32::from(config.share_delivery_count_limit),
            "Maximum delivery attempts for a record delivered to the share group.",
        ),
        integer_entry(
            SHARE_HEARTBEAT,
            config.share_heartbeat_interval_ms,
            "Heartbeat interval for share groups.",
        ),
        string_entry(
            SHARE_ISOLATION,
            &config.share_isolation_level,
            "Transactional isolation level for share groups.",
        ),
        integer_entry(
            SHARE_PARTITION_MAX_RECORD_LOCKS,
            config.share_partition_max_record_locks,
            "Record-lock limit per share partition.",
        ),
        integer_entry(
            SHARE_LOCK_DURATION,
            config.share_record_lock_duration_ms,
            "Record acquisition lock duration for share groups.",
        ),
        boolean_entry(
            SHARE_RENEW_ACKNOWLEDGE_ENABLE,
            config.share_renew_acknowledge_enable,
            "Whether renew acknowledgements are enabled for the share group.",
        ),
        integer_entry(
            SHARE_SESSION,
            config.share_session_timeout_ms,
            "Session timeout for share groups.",
        ),
        integer_entry(
            STREAMS_ASSIGNMENT,
            config.streams_assignment_interval_ms,
            "Interval between assignment updates for streams groups.",
        ),
        boolean_entry(
            STREAMS_ASSIGNOR_OFFLOAD,
            config.streams_assignor_offload_enable,
            "Whether assignment computation is offloaded for this streams group.",
        ),
        integer_entry(
            STREAMS_HEARTBEAT,
            config.streams_heartbeat_interval_ms,
            "Heartbeat interval for streams groups.",
        ),
        integer_entry(
            STREAMS_INITIAL_DELAY,
            config.streams_initial_rebalance_delay_ms,
            "Delay before the first streams group rebalance.",
        ),
        integer_entry(
            STREAMS_STANDBY,
            config.streams_num_standby_replicas,
            "Number of standby replicas for each stateful streams task.",
        ),
        integer_entry(
            STREAMS_SESSION,
            config.streams_session_timeout_ms,
            "Session timeout for streams groups.",
        ),
    ]
}

fn integer_entry(name: &'static str, value: i32, documentation: &'static str) -> ConfigEntry {
    ConfigEntry {
        name,
        value: value.to_string(),
        config_type: INT_CONFIG,
        documentation,
    }
}

fn string_entry(name: &'static str, value: &str, documentation: &'static str) -> ConfigEntry {
    ConfigEntry {
        name,
        value: value.to_owned(),
        config_type: STRING_CONFIG,
        documentation,
    }
}

fn boolean_entry(name: &'static str, value: bool, documentation: &'static str) -> ConfigEntry {
    ConfigEntry {
        name,
        value: value.to_string(),
        config_type: BOOLEAN_CONFIG,
        documentation,
    }
}

fn runtime_config(
    defaults: &AgentConfig,
    values: &BTreeMap<String, String>,
) -> Result<GroupRuntimeConfig, ControlError> {
    let share_auto_offset_reset = parse_share_offset_reset(
        values
            .get(SHARE_AUTO_OFFSET_RESET)
            .map(String::as_str)
            .unwrap_or("latest"),
    )?;
    let config = GroupRuntimeConfig {
        consumer_assignment_interval_ms: bounded_integer(
            values,
            CONSUMER_ASSIGNMENT,
            defaults.group_assignment_interval_ms,
            defaults.group_min_assignment_interval_ms,
            defaults.group_max_assignment_interval_ms,
        )?,
        consumer_assignor_offload_enable: boolean(
            values,
            CONSUMER_ASSIGNOR_OFFLOAD,
            defaults.consumer_assignor_offload_enable,
        )?,
        consumer_heartbeat_interval_ms: bounded_integer(
            values,
            CONSUMER_HEARTBEAT,
            defaults.group_heartbeat_interval_ms,
            defaults.consumer_group_min_heartbeat_interval_ms,
            defaults.consumer_group_max_heartbeat_interval_ms,
        )?,
        consumer_session_timeout_ms: bounded_integer(
            values,
            CONSUMER_SESSION,
            defaults.group_session_timeout_ms,
            defaults.consumer_group_min_session_timeout_ms,
            defaults.consumer_group_max_session_timeout_ms,
        )?,
        share_assignment_interval_ms: bounded_integer(
            values,
            SHARE_ASSIGNMENT,
            defaults.share_group_assignment_interval_ms,
            defaults.share_group_min_assignment_interval_ms,
            defaults.share_group_max_assignment_interval_ms,
        )?,
        share_assignor_offload_enable: boolean(
            values,
            SHARE_ASSIGNOR_OFFLOAD,
            defaults.share_assignor_offload_enable,
        )?,
        share_auto_offset_reset,
        share_delivery_count_limit: i16::try_from(bounded_integer(
            values,
            SHARE_DELIVERY_COUNT_LIMIT,
            i32::from(defaults.share_record_delivery_count_limit),
            i32::from(defaults.share_min_delivery_count_limit),
            i32::from(defaults.share_max_delivery_count_limit),
        )?)
        .map_err(|_| {
            ControlError::InvalidRequest(
                "effective share delivery count limit exceeds protocol storage".to_owned(),
            )
        })?,
        share_heartbeat_interval_ms: bounded_integer(
            values,
            SHARE_HEARTBEAT,
            defaults.share_group_heartbeat_interval_ms,
            defaults.share_group_min_heartbeat_interval_ms,
            defaults.share_group_max_heartbeat_interval_ms,
        )?,
        share_isolation_level: choice(
            values,
            SHARE_ISOLATION,
            "read_uncommitted",
            &["read_uncommitted", "read_committed"],
        )?,
        share_partition_max_record_locks: bounded_integer(
            values,
            SHARE_PARTITION_MAX_RECORD_LOCKS,
            defaults.share_partition_max_record_locks,
            defaults.share_min_partition_max_record_locks,
            defaults.share_max_partition_max_record_locks,
        )?,
        share_record_lock_duration_ms: bounded_integer(
            values,
            SHARE_LOCK_DURATION,
            defaults.share_record_lock_duration_ms,
            defaults.share_min_record_lock_duration_ms,
            defaults.share_max_record_lock_duration_ms,
        )?,
        share_renew_acknowledge_enable: boolean(values, SHARE_RENEW_ACKNOWLEDGE_ENABLE, true)?,
        share_session_timeout_ms: bounded_integer(
            values,
            SHARE_SESSION,
            defaults.share_group_session_timeout_ms,
            defaults.share_group_min_session_timeout_ms,
            defaults.share_group_max_session_timeout_ms,
        )?,
        streams_assignment_interval_ms: bounded_integer(
            values,
            STREAMS_ASSIGNMENT,
            defaults.streams_group_assignment_interval_ms,
            defaults.streams_group_min_assignment_interval_ms,
            defaults.streams_group_max_assignment_interval_ms,
        )?,
        streams_assignor_offload_enable: boolean(
            values,
            STREAMS_ASSIGNOR_OFFLOAD,
            defaults.streams_assignor_offload_enable,
        )?,
        streams_heartbeat_interval_ms: bounded_integer(
            values,
            STREAMS_HEARTBEAT,
            defaults.streams_group_heartbeat_interval_ms,
            defaults.streams_group_min_heartbeat_interval_ms,
            defaults.streams_group_max_heartbeat_interval_ms,
        )?,
        streams_initial_rebalance_delay_ms: non_negative_integer(
            values,
            STREAMS_INITIAL_DELAY,
            defaults.streams_group_initial_rebalance_delay_ms,
        )?,
        streams_num_standby_replicas: bounded_integer(
            values,
            STREAMS_STANDBY,
            defaults.streams_group_num_standby_replicas,
            0,
            defaults.streams_group_max_standby_replicas,
        )?,
        streams_session_timeout_ms: bounded_integer(
            values,
            STREAMS_SESSION,
            defaults.streams_group_session_timeout_ms,
            defaults.streams_group_min_session_timeout_ms,
            defaults.streams_group_max_session_timeout_ms,
        )?,
    };
    for (heartbeat, session, protocol) in [
        (
            config.consumer_heartbeat_interval_ms,
            config.consumer_session_timeout_ms,
            "consumer",
        ),
        (
            config.share_heartbeat_interval_ms,
            config.share_session_timeout_ms,
            "share",
        ),
        (
            config.streams_heartbeat_interval_ms,
            config.streams_session_timeout_ms,
            "streams",
        ),
    ] {
        if heartbeat >= session {
            return Err(ControlError::InvalidRequest(format!(
                "{protocol} heartbeat interval must be lower than its session timeout"
            )));
        }
    }
    Ok(config)
}

fn validate_update_bounds(
    defaults: &AgentConfig,
    values: &BTreeMap<String, String>,
) -> Result<(), ControlError> {
    validate_integer_bounds(
        values,
        CONSUMER_HEARTBEAT,
        defaults.consumer_group_min_heartbeat_interval_ms,
        defaults.consumer_group_max_heartbeat_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        CONSUMER_SESSION,
        defaults.consumer_group_min_session_timeout_ms,
        defaults.consumer_group_max_session_timeout_ms,
    )?;
    validate_integer_bounds(
        values,
        SHARE_HEARTBEAT,
        defaults.share_group_min_heartbeat_interval_ms,
        defaults.share_group_max_heartbeat_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        SHARE_SESSION,
        defaults.share_group_min_session_timeout_ms,
        defaults.share_group_max_session_timeout_ms,
    )?;
    validate_integer_bounds(
        values,
        STREAMS_HEARTBEAT,
        defaults.streams_group_min_heartbeat_interval_ms,
        defaults.streams_group_max_heartbeat_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        STREAMS_SESSION,
        defaults.streams_group_min_session_timeout_ms,
        defaults.streams_group_max_session_timeout_ms,
    )?;
    validate_integer_bounds(
        values,
        STREAMS_STANDBY,
        0,
        defaults.streams_group_max_standby_replicas,
    )?;
    validate_integer_bounds(
        values,
        SHARE_LOCK_DURATION,
        defaults.share_min_record_lock_duration_ms,
        defaults.share_max_record_lock_duration_ms,
    )?;
    validate_integer_bounds(
        values,
        CONSUMER_ASSIGNMENT,
        defaults.group_min_assignment_interval_ms,
        defaults.group_max_assignment_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        SHARE_ASSIGNMENT,
        defaults.share_group_min_assignment_interval_ms,
        defaults.share_group_max_assignment_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        STREAMS_ASSIGNMENT,
        defaults.streams_group_min_assignment_interval_ms,
        defaults.streams_group_max_assignment_interval_ms,
    )?;
    validate_integer_bounds(
        values,
        SHARE_DELIVERY_COUNT_LIMIT,
        i32::from(defaults.share_min_delivery_count_limit),
        i32::from(defaults.share_max_delivery_count_limit),
    )?;
    validate_integer_bounds(
        values,
        SHARE_PARTITION_MAX_RECORD_LOCKS,
        defaults.share_min_partition_max_record_locks,
        defaults.share_max_partition_max_record_locks,
    )
}

fn ensure_supported(key: &str) -> Result<(), ControlError> {
    if CONFIG_KEYS.contains(&key) {
        Ok(())
    } else {
        Err(ControlError::InvalidRequest(format!(
            "unknown group configuration {key}"
        )))
    }
}

fn non_negative_integer(
    values: &BTreeMap<String, String>,
    key: &str,
    default: i32,
) -> Result<i32, ControlError> {
    minimum_integer(values, key, default, 0)
}

fn minimum_integer(
    values: &BTreeMap<String, String>,
    key: &str,
    default: i32,
    minimum: i32,
) -> Result<i32, ControlError> {
    let value = values.get(key).map_or_else(
        || Ok(default),
        |value| {
            value.parse::<i32>().map_err(|_| {
                ControlError::InvalidRequest(format!(
                    "group configuration {key} must be an integer"
                ))
            })
        },
    )?;
    if value < minimum {
        return Err(ControlError::InvalidRequest(format!(
            "group configuration {key} must be at least {minimum}"
        )));
    }
    Ok(value)
}

fn bounded_integer(
    values: &BTreeMap<String, String>,
    key: &str,
    default: i32,
    minimum: i32,
    maximum: i32,
) -> Result<i32, ControlError> {
    let value = values.get(key).map_or_else(
        || Ok(default),
        |value| {
            value.parse::<i32>().map_err(|_| {
                ControlError::InvalidRequest(format!(
                    "group configuration {key} must be an integer"
                ))
            })
        },
    )?;
    Ok(value.clamp(minimum, maximum))
}

fn validate_integer_bounds(
    values: &BTreeMap<String, String>,
    key: &str,
    minimum: i32,
    maximum: i32,
) -> Result<(), ControlError> {
    let Some(value) = values.get(key) else {
        return Ok(());
    };
    let value = value.parse::<i32>().map_err(|_| {
        ControlError::InvalidRequest(format!("group configuration {key} must be an integer"))
    })?;
    if !(minimum..=maximum).contains(&value) {
        return Err(ControlError::InvalidRequest(format!(
            "group configuration {key} must be between {minimum} and {maximum}"
        )));
    }
    Ok(())
}

fn boolean(
    values: &BTreeMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool, ControlError> {
    let Some(value) = values.get(key) else {
        return Ok(default);
    };
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(ControlError::InvalidRequest(format!(
            "group configuration {key} must be true or false"
        )))
    }
}

fn choice(
    values: &BTreeMap<String, String>,
    key: &str,
    default: &str,
    choices: &[&str],
) -> Result<String, ControlError> {
    let value = values.get(key).map(String::as_str).unwrap_or(default);
    if choices.contains(&value) {
        Ok(value.to_owned())
    } else {
        Err(ControlError::InvalidRequest(format!(
            "group configuration {key} must be one of {}",
            choices.join(",")
        )))
    }
}
