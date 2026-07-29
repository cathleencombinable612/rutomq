use super::{Broker, config_synonyms};
use kafka_protocol::messages::alter_configs_request::AlterableConfig as LegacyConfig;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
use kafka_protocol::messages::incremental_alter_configs_request::AlterableConfig;
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    CLIENT_METRICS_INTERVAL_MS, CLIENT_METRICS_MATCH, CLIENT_METRICS_METRICS,
    ClientMetricConfigAlteration, ClientMetricSubscription, ControlError,
};
use std::collections::{BTreeMap, HashSet};

pub(super) const CLIENT_METRICS_RESOURCE: i8 = 16;
const DYNAMIC_CLIENT_METRICS_CONFIG: i8 = 7;
const INT_CONFIG: i8 = 3;
const LIST_CONFIG: i8 = 7;
const CONFIG_KEYS: [&str; 3] = [
    CLIENT_METRICS_METRICS,
    CLIENT_METRICS_INTERVAL_MS,
    CLIENT_METRICS_MATCH,
];

impl Broker {
    pub(super) async fn alter_client_metric_config(
        &self,
        resource_name: &str,
        changes: &[AlterableConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        let alteration = alteration(resource_name, changes)?;
        self.metadata
            .alter_client_metric_subscription(alteration, validate_only)
            .await
    }

    pub(super) async fn replace_client_metric_config(
        &self,
        resource_name: &str,
        configs: &[LegacyConfig],
        validate_only: bool,
    ) -> Result<(), ControlError> {
        let mut seen = HashSet::new();
        let mut ops = CONFIG_KEYS
            .into_iter()
            .map(|key| (key.to_owned(), None))
            .collect::<BTreeMap<_, _>>();
        for config in configs {
            let name = config.name.as_str();
            if !seen.insert(name) {
                return Err(ControlError::InvalidRequest(
                    "configuration keys must be unique".to_owned(),
                ));
            }
            let value = config.value.as_ref().ok_or_else(|| {
                ControlError::InvalidRequest(format!("configuration {name} requires a value"))
            })?;
            ops.insert(name.to_owned(), Some(value.as_str().to_owned()));
        }
        self.metadata
            .alter_client_metric_subscription(
                ClientMetricConfigAlteration {
                    name: resource_name.to_owned(),
                    ops,
                },
                validate_only,
            )
            .await
    }
}

pub(super) fn describe_client_metric_config(
    subscription: &ClientMetricSubscription,
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
    subscription
        .configs
        .iter()
        .filter(|(name, _)| {
            requested
                .as_ref()
                .is_none_or(|requested| requested.contains(name.as_str()))
        })
        .map(|(name, value)| {
            let (config_type, documentation) = config_metadata(name);
            let synonyms = if include_synonyms {
                config_synonyms::same_name(name, value, DYNAMIC_CLIENT_METRICS_CONFIG)
            } else {
                Vec::new()
            };
            let result = DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_string(name.clone()))
                .with_value(Some(StrBytes::from_string(value.clone())))
                .with_read_only(false)
                .with_config_source(DYNAMIC_CLIENT_METRICS_CONFIG)
                .with_is_sensitive(false)
                .with_synonyms(synonyms);
            if version >= 3 {
                result.with_config_type(config_type).with_documentation(
                    include_documentation.then(|| StrBytes::from_static_str(documentation)),
                )
            } else {
                result
            }
        })
        .collect()
}

fn alteration(
    resource_name: &str,
    changes: &[AlterableConfig],
) -> Result<ClientMetricConfigAlteration, ControlError> {
    let mut seen = HashSet::new();
    let mut ops = BTreeMap::new();
    for change in changes {
        let name = change.name.as_str();
        if !seen.insert(name) {
            return Err(ControlError::InvalidRequest(
                "configuration keys must be unique".to_owned(),
            ));
        }
        let value = match change.config_operation {
            0 => Some(
                change
                    .value
                    .as_ref()
                    .ok_or_else(|| {
                        ControlError::InvalidRequest(format!(
                            "configuration {name} requires a value"
                        ))
                    })?
                    .as_str()
                    .to_owned(),
            ),
            1 => None,
            operation => {
                return Err(ControlError::InvalidRequest(format!(
                    "client metrics configuration {name} does not support operation {operation}"
                )));
            }
        };
        ops.insert(name.to_owned(), value);
    }
    Ok(ClientMetricConfigAlteration {
        name: resource_name.to_owned(),
        ops,
    })
}

fn config_metadata(name: &str) -> (i8, &'static str) {
    match name {
        CLIENT_METRICS_INTERVAL_MS => (
            INT_CONFIG,
            "How often matching clients push telemetry, in milliseconds.",
        ),
        CLIENT_METRICS_MATCH => (
            LIST_CONFIG,
            "Comma-separated client attribute regular-expression matches.",
        ),
        CLIENT_METRICS_METRICS => (
            LIST_CONFIG,
            "Comma-separated metric prefixes; * subscribes all metrics.",
        ),
        _ => (LIST_CONFIG, "Client telemetry subscription configuration."),
    }
}
