use super::config_synonyms;
use super::*;
use kafka_protocol::messages::alter_configs_request::AlterableConfig as LegacyAlterableConfig;
use kafka_protocol::messages::create_topics_request::CreatableTopicConfig;
use kafka_protocol::messages::create_topics_response::CreatableTopicConfigs;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
use kafka_protocol::messages::incremental_alter_configs_request::AlterableConfig;
use rutomq_control::ControlError;
use std::collections::HashSet;

pub(super) const TOPIC_RESOURCE: i8 = 2;
const DYNAMIC_TOPIC_CONFIG: i8 = 1;
const DEFAULT_CONFIG: i8 = 5;
const STRING_CONFIG: i8 = 2;
const INT_CONFIG: i8 = 3;
const LONG_CONFIG: i8 = 5;
const DOUBLE_CONFIG: i8 = 6;
const LIST_CONFIG: i8 = 7;

pub(super) fn create_topic_config(
    changes: &[CreatableTopicConfig],
) -> Result<TopicConfig, ControlError> {
    let entries = changes
        .iter()
        .map(|change| {
            let value = change.value.as_ref().ok_or_else(|| {
                ControlError::InvalidConfiguration(format!(
                    "configuration {} requires a value",
                    change.name.as_str()
                ))
            })?;
            Ok((change.name.as_str(), value.as_str()))
        })
        .collect::<Result<Vec<_>, ControlError>>()?;
    create_topic_config_entries(entries)
}

pub(super) fn create_topic_config_entries<'a>(
    changes: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<TopicConfig, ControlError> {
    let changes = changes.into_iter().collect::<Vec<_>>();
    ensure_unique_config_names(changes.iter().map(|(name, _)| *name))?;
    let mut config = TopicConfig::default();
    for (name, value) in changes {
        apply_config_set(&mut config, name, value)?;
    }
    config.validate()?;
    Ok(config)
}

impl Broker {
    pub(super) async fn replace_topic_config(
        &self,
        resource_type: i8,
        resource_name: &str,
        changes: &[LegacyAlterableConfig],
        validate_only: bool,
    ) -> std::result::Result<(), ControlError> {
        if resource_type != TOPIC_RESOURCE {
            return Err(ControlError::InvalidRequest(format!(
                "resource type {resource_type} is not supported"
            )));
        }
        ensure_unique_config_names(changes.iter().map(|change| change.name.as_str()))?;
        self.metadata.topic_config(resource_name).await?;
        let mut config = TopicConfig::default();
        for change in changes {
            let value = change.value.as_ref().ok_or_else(|| {
                ControlError::InvalidConfiguration(format!(
                    "configuration {} requires a value",
                    change.name.as_str()
                ))
            })?;
            apply_config_set(&mut config, change.name.as_str(), value.as_str())?;
        }
        config.validate()?;
        if !validate_only {
            self.metadata
                .set_topic_config(resource_name, config)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn alter_topic_config(
        &self,
        resource_type: i8,
        resource_name: &str,
        changes: &[AlterableConfig],
        validate_only: bool,
    ) -> std::result::Result<(), ControlError> {
        if resource_type != TOPIC_RESOURCE {
            return Err(ControlError::InvalidRequest(format!(
                "resource type {resource_type} is not supported"
            )));
        }
        ensure_unique_config_names(changes.iter().map(|change| change.name.as_str()))?;
        let mut config = self.metadata.topic_config(resource_name).await?;
        for change in changes {
            apply_config_change(&mut config, change)?;
        }
        config.validate()?;
        if !validate_only {
            self.metadata
                .set_topic_config(resource_name, config)
                .await?;
        }
        Ok(())
    }
}

pub(super) fn describe_topic_config(
    config: &TopicConfig,
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
    topic_config_entries(config)
        .into_iter()
        .zip(topic_config_entries(&TopicConfig::default()))
        .filter(|((name, _, _, _), _)| {
            requested
                .as_ref()
                .is_none_or(|requested| requested.contains(*name))
        })
        .map(
            |((name, value, config_type, documentation), (_, default_value, _, _))| {
                let source = if config.is_dynamic(name) {
                    DYNAMIC_TOPIC_CONFIG
                } else {
                    DEFAULT_CONFIG
                };
                let synonyms = if include_synonyms {
                    config_synonyms::topic(name, &value, &default_value, config.is_dynamic(name))
                } else {
                    Vec::new()
                };
                let result = DescribeConfigsResourceResult::default()
                    .with_name(StrBytes::from_string(name.to_owned()))
                    .with_value(Some(StrBytes::from_string(value)))
                    .with_read_only(false)
                    .with_config_source(source)
                    .with_is_sensitive(false)
                    .with_synonyms(synonyms);
                if version >= 3 {
                    result.with_config_type(config_type).with_documentation(
                        include_documentation
                            .then(|| StrBytes::from_string(documentation.to_owned())),
                    )
                } else {
                    result
                }
            },
        )
        .collect()
}

pub(super) fn create_topic_response_configs(config: &TopicConfig) -> Vec<CreatableTopicConfigs> {
    let mut entries = topic_config_entries(config);
    entries.sort_by_key(|(name, _, _, _)| *name);
    entries
        .into_iter()
        .map(|(name, value, _, _)| {
            CreatableTopicConfigs::default()
                .with_name(StrBytes::from_string(name.to_owned()))
                .with_value(Some(StrBytes::from_string(value)))
                .with_read_only(false)
                .with_config_source(if config.is_dynamic(name) {
                    DYNAMIC_TOPIC_CONFIG
                } else {
                    DEFAULT_CONFIG
                })
                .with_is_sensitive(false)
        })
        .collect()
}

fn topic_config_entries(config: &TopicConfig) -> [(&'static str, String, i8, &'static str); 19] {
    [
        (
            "cleanup.policy",
            config.cleanup_policy.clone(),
            LIST_CONFIG,
            "Retention cleanup policy.",
        ),
        (
            "retention.ms",
            config.retention_ms.to_string(),
            LONG_CONFIG,
            "Maximum record retention time in milliseconds; -1 disables time retention.",
        ),
        (
            "retention.bytes",
            config.retention_bytes.to_string(),
            LONG_CONFIG,
            "Maximum retained bytes per partition; -1 disables size retention.",
        ),
        (
            "file.delete.delay.ms",
            config.file_delete_delay_ms.to_string(),
            LONG_CONFIG,
            "Delay before an unreferenced immutable object may be deleted.",
        ),
        (
            "flush.messages",
            config.flush_messages.to_string(),
            LONG_CONFIG,
            "Maximum admitted records per partition before an object commit.",
        ),
        (
            "flush.ms",
            config.flush_ms.to_string(),
            LONG_CONFIG,
            "Maximum in-memory interval before an object commit.",
        ),
        (
            "delete.retention.ms",
            config.delete_retention_ms.to_string(),
            LONG_CONFIG,
            "Tombstone retention time used by log compaction.",
        ),
        (
            "min.compaction.lag.ms",
            config.min_compaction_lag_ms.to_string(),
            LONG_CONFIG,
            "Minimum age before a record is eligible for compaction.",
        ),
        (
            "max.compaction.lag.ms",
            config.max_compaction_lag_ms.to_string(),
            LONG_CONFIG,
            "Maximum delay before dirty records are forced through compaction.",
        ),
        (
            "min.cleanable.dirty.ratio",
            format_ratio(config.min_cleanable_dirty_ratio),
            DOUBLE_CONFIG,
            "Minimum dirty-to-cleanable byte ratio before compaction.",
        ),
        (
            "min.insync.replicas",
            config.min_insync_replicas.to_string(),
            INT_CONFIG,
            "Minimum virtual in-sync replicas required for acks=all Produce.",
        ),
        (
            "max.message.bytes",
            config.max_message_bytes.to_string(),
            INT_CONFIG,
            "Largest accepted record batch after topic compression.",
        ),
        (
            "compression.type",
            config.compression_type.clone(),
            STRING_CONFIG,
            "Final compression codec, or producer to preserve the producer codec.",
        ),
        (
            "compression.gzip.level",
            config.compression_gzip_level.to_string(),
            INT_CONFIG,
            "Compression level used for gzip record-batch rewrites.",
        ),
        (
            "compression.lz4.level",
            config.compression_lz4_level.to_string(),
            INT_CONFIG,
            "Compression level used for LZ4 record-batch rewrites.",
        ),
        (
            "compression.zstd.level",
            config.compression_zstd_level.to_string(),
            INT_CONFIG,
            "Compression level used for Zstandard record-batch rewrites.",
        ),
        (
            "message.timestamp.type",
            config.message_timestamp_type.clone(),
            STRING_CONFIG,
            "Use producer CreateTime or broker LogAppendTime timestamps.",
        ),
        (
            "message.timestamp.before.max.ms",
            config.message_timestamp_before_max_ms.to_string(),
            LONG_CONFIG,
            "Maximum accepted age of a CreateTime record.",
        ),
        (
            "message.timestamp.after.max.ms",
            config.message_timestamp_after_max_ms.to_string(),
            LONG_CONFIG,
            "Maximum accepted future offset of a CreateTime record.",
        ),
    ]
}

fn apply_config_change(
    config: &mut TopicConfig,
    change: &AlterableConfig,
) -> Result<(), ControlError> {
    let name = change.name.as_str();
    if change.config_operation == 1 {
        let defaults = TopicConfig::default();
        match name {
            "retention.ms" => config.retention_ms = defaults.retention_ms,
            "retention.bytes" => config.retention_bytes = defaults.retention_bytes,
            "cleanup.policy" => config.cleanup_policy = defaults.cleanup_policy,
            "file.delete.delay.ms" => {
                config.file_delete_delay_ms = defaults.file_delete_delay_ms;
            }
            "flush.messages" => config.flush_messages = defaults.flush_messages,
            "flush.ms" => config.flush_ms = defaults.flush_ms,
            "delete.retention.ms" => config.delete_retention_ms = defaults.delete_retention_ms,
            "min.compaction.lag.ms" => {
                config.min_compaction_lag_ms = defaults.min_compaction_lag_ms;
            }
            "max.compaction.lag.ms" => {
                config.max_compaction_lag_ms = defaults.max_compaction_lag_ms;
            }
            "min.cleanable.dirty.ratio" => {
                config.min_cleanable_dirty_ratio = defaults.min_cleanable_dirty_ratio;
            }
            "min.insync.replicas" => {
                config.min_insync_replicas = defaults.min_insync_replicas;
            }
            "max.message.bytes" => config.max_message_bytes = defaults.max_message_bytes,
            "compression.type" => config.compression_type = defaults.compression_type,
            "compression.gzip.level" => {
                config.compression_gzip_level = defaults.compression_gzip_level;
            }
            "compression.lz4.level" => {
                config.compression_lz4_level = defaults.compression_lz4_level;
            }
            "compression.zstd.level" => {
                config.compression_zstd_level = defaults.compression_zstd_level;
            }
            "message.timestamp.type" => {
                config.message_timestamp_type = defaults.message_timestamp_type;
            }
            "message.timestamp.before.max.ms" => {
                config.message_timestamp_before_max_ms = defaults.message_timestamp_before_max_ms;
            }
            "message.timestamp.after.max.ms" => {
                config.message_timestamp_after_max_ms = defaults.message_timestamp_after_max_ms;
            }
            _ => return Err(unknown_config(name)),
        }
        config.reset_dynamic(name);
        return Ok(());
    }
    let value = change.value.as_ref().ok_or_else(|| {
        ControlError::InvalidConfiguration(format!("configuration {name} requires a value"))
    })?;
    if change.config_operation == 0 {
        return apply_config_set(config, name, value.as_str());
    }
    match name {
        "retention.ms" => {
            require_set(name, change.config_operation)?;
            config.retention_ms = parse_integer(name, value.as_str())?;
        }
        "retention.bytes" => {
            require_set(name, change.config_operation)?;
            config.retention_bytes = parse_integer(name, value.as_str())?;
        }
        "file.delete.delay.ms" => {
            require_set(name, change.config_operation)?;
            config.file_delete_delay_ms = parse_integer(name, value.as_str())?;
        }
        "flush.messages" | "flush.ms" => {
            require_set(name, change.config_operation)?;
        }
        "delete.retention.ms" => {
            require_set(name, change.config_operation)?;
            config.delete_retention_ms = parse_integer(name, value.as_str())?;
        }
        "min.compaction.lag.ms" => {
            require_set(name, change.config_operation)?;
            config.min_compaction_lag_ms = parse_integer(name, value.as_str())?;
        }
        "max.compaction.lag.ms" | "min.cleanable.dirty.ratio" => {
            require_set(name, change.config_operation)?;
        }
        "min.insync.replicas"
        | "max.message.bytes"
        | "compression.type"
        | "compression.gzip.level"
        | "compression.lz4.level"
        | "compression.zstd.level"
        | "message.timestamp.type"
        | "message.timestamp.before.max.ms"
        | "message.timestamp.after.max.ms" => {
            require_set(name, change.config_operation)?;
        }
        "cleanup.policy" => {
            config.cleanup_policy = alter_cleanup_policy(
                &config.cleanup_policy,
                change.config_operation,
                value.as_str(),
            )?;
        }
        _ => return Err(unknown_config(name)),
    }
    config.mark_dynamic(name);
    Ok(())
}

fn apply_config_set(config: &mut TopicConfig, name: &str, value: &str) -> Result<(), ControlError> {
    match name {
        "retention.ms" => config.retention_ms = parse_integer(name, value)?,
        "retention.bytes" => config.retention_bytes = parse_integer(name, value)?,
        "file.delete.delay.ms" => config.file_delete_delay_ms = parse_integer(name, value)?,
        "flush.messages" => config.flush_messages = parse_integer(name, value)?,
        "flush.ms" => config.flush_ms = parse_integer(name, value)?,
        "delete.retention.ms" => config.delete_retention_ms = parse_integer(name, value)?,
        "min.compaction.lag.ms" => {
            config.min_compaction_lag_ms = parse_integer(name, value)?;
        }
        "max.compaction.lag.ms" => {
            config.max_compaction_lag_ms = parse_integer(name, value)?;
        }
        "min.cleanable.dirty.ratio" => {
            config.min_cleanable_dirty_ratio = parse_double(name, value)?;
        }
        "min.insync.replicas" => {
            config.min_insync_replicas = parse_int(name, value)?;
        }
        "cleanup.policy" => config.cleanup_policy = value.to_owned(),
        "max.message.bytes" => config.max_message_bytes = parse_int(name, value)?,
        "compression.type" => config.compression_type = value.to_owned(),
        "compression.gzip.level" => config.compression_gzip_level = parse_int(name, value)?,
        "compression.lz4.level" => config.compression_lz4_level = parse_int(name, value)?,
        "compression.zstd.level" => config.compression_zstd_level = parse_int(name, value)?,
        "message.timestamp.type" => config.message_timestamp_type = value.to_owned(),
        "message.timestamp.before.max.ms" => {
            config.message_timestamp_before_max_ms = parse_integer(name, value)?;
        }
        "message.timestamp.after.max.ms" => {
            config.message_timestamp_after_max_ms = parse_integer(name, value)?;
        }
        _ => return Err(unknown_config(name)),
    }
    config.mark_dynamic(name);
    Ok(())
}

fn ensure_unique_config_names<'a>(
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ControlError> {
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(ControlError::InvalidRequest(
                "configuration keys must be unique".to_owned(),
            ));
        }
    }
    Ok(())
}

fn require_set(name: &str, operation: i8) -> Result<(), ControlError> {
    if operation != 0 {
        return Err(ControlError::InvalidConfiguration(format!(
            "configuration {name} only supports SET or DELETE"
        )));
    }
    Ok(())
}

fn parse_integer(name: &str, value: &str) -> Result<i64, ControlError> {
    value.parse().map_err(|_| {
        ControlError::InvalidConfiguration(format!("configuration {name} must be an integer"))
    })
}

fn parse_int(name: &str, value: &str) -> Result<i32, ControlError> {
    value.parse().map_err(|_| {
        ControlError::InvalidConfiguration(format!("configuration {name} must be a 32-bit integer"))
    })
}

fn parse_double(name: &str, value: &str) -> Result<f64, ControlError> {
    value.parse().map_err(|_| {
        ControlError::InvalidConfiguration(format!("configuration {name} must be a number"))
    })
}

fn format_ratio(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn alter_cleanup_policy(current: &str, operation: i8, value: &str) -> Result<String, ControlError> {
    if operation == 0 {
        return Ok(value.to_owned());
    }
    if !matches!(operation, 2 | 3) {
        return Err(ControlError::InvalidConfiguration(format!(
            "unsupported cleanup.policy operation {operation}"
        )));
    }
    let mut policies = current.split(',').map(str::to_owned).collect::<Vec<_>>();
    for policy in value.split(',') {
        if operation == 2 && !policies.iter().any(|existing| existing == policy) {
            policies.push(policy.to_owned());
        } else if operation == 3 {
            policies.retain(|existing| existing != policy);
        }
    }
    Ok(policies.join(","))
}

fn unknown_config(name: &str) -> ControlError {
    ControlError::InvalidConfiguration(format!(
        "topic configuration {name} is unsupported by the stateless object-storage data path"
    ))
}
