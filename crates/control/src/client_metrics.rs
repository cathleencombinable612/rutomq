use crate::ControlError;
use regex::Regex;
use std::collections::BTreeMap;

pub const CLIENT_METRICS_METRICS: &str = "metrics";
pub const CLIENT_METRICS_INTERVAL_MS: &str = "interval.ms";
pub const CLIENT_METRICS_MATCH: &str = "match";
pub const CLIENT_METRICS_DEFAULT_INTERVAL_MS: i32 = 300_000;
pub const CLIENT_METRICS_MIN_INTERVAL_MS: i32 = 100;
pub const CLIENT_METRICS_MAX_INTERVAL_MS: i32 = 3_600_000;

pub const CLIENT_INSTANCE_ID: &str = "client_instance_id";
pub const CLIENT_ID: &str = "client_id";
pub const CLIENT_SOFTWARE_NAME: &str = "client_software_name";
pub const CLIENT_SOFTWARE_VERSION: &str = "client_software_version";
pub const CLIENT_SOURCE_ADDRESS: &str = "client_source_address";
pub const CLIENT_SOURCE_PORT: &str = "client_source_port";

const CONFIG_KEYS: [&str; 3] = [
    CLIENT_METRICS_METRICS,
    CLIENT_METRICS_INTERVAL_MS,
    CLIENT_METRICS_MATCH,
];
const MATCH_KEYS: [&str; 6] = [
    CLIENT_INSTANCE_ID,
    CLIENT_ID,
    CLIENT_SOFTWARE_NAME,
    CLIENT_SOFTWARE_VERSION,
    CLIENT_SOURCE_ADDRESS,
    CLIENT_SOURCE_PORT,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientMetricSubscription {
    pub name: String,
    pub configs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientMetricConfigAlteration {
    pub name: String,
    /// `None` removes a configuration; `Some(value)` sets it.
    pub ops: BTreeMap<String, Option<String>>,
}

impl ClientMetricSubscription {
    pub fn validate(&self) -> Result<(), ControlError> {
        validate_name(&self.name)?;
        for key in self.configs.keys() {
            validate_key(key)?;
        }
        if let Some(value) = self.configs.get(CLIENT_METRICS_INTERVAL_MS) {
            let interval = value.parse::<i32>().map_err(|_| {
                invalid(format!(
                    "client metrics configuration {CLIENT_METRICS_INTERVAL_MS} must be an integer"
                ))
            })?;
            if !(CLIENT_METRICS_MIN_INTERVAL_MS..=CLIENT_METRICS_MAX_INTERVAL_MS)
                .contains(&interval)
            {
                return Err(invalid(format!(
                    "client metrics configuration {CLIENT_METRICS_INTERVAL_MS} must be between \
                     {CLIENT_METRICS_MIN_INTERVAL_MS} and {CLIENT_METRICS_MAX_INTERVAL_MS}"
                )));
            }
        }
        if let Some(value) = self.configs.get(CLIENT_METRICS_MATCH) {
            matching_patterns(value)?;
        }
        Ok(())
    }

    pub fn metrics(&self) -> Vec<String> {
        self.configs
            .get(CLIENT_METRICS_METRICS)
            .map_or_else(Vec::new, |value| split_list(value))
    }

    pub fn push_interval_ms(&self) -> i32 {
        self.configs
            .get(CLIENT_METRICS_INTERVAL_MS)
            .and_then(|value| value.parse().ok())
            .unwrap_or(CLIENT_METRICS_DEFAULT_INTERVAL_MS)
    }

    pub fn matches(&self, attributes: &BTreeMap<String, String>) -> bool {
        let Some(value) = self.configs.get(CLIENT_METRICS_MATCH) else {
            return true;
        };
        matching_patterns(value).is_ok_and(|patterns| {
            patterns.into_iter().all(|(name, pattern)| {
                attributes
                    .get(&name)
                    .is_some_and(|attribute| pattern.is_match(attribute))
            })
        })
    }
}

pub(crate) fn apply_alteration(
    current: Option<ClientMetricSubscription>,
    alteration: &ClientMetricConfigAlteration,
) -> Result<Option<ClientMetricSubscription>, ControlError> {
    validate_name(&alteration.name)?;
    for key in alteration.ops.keys() {
        validate_key(key)?;
    }
    let mut configs = current.map_or_else(BTreeMap::new, |current| current.configs);
    for (key, value) in &alteration.ops {
        match value {
            Some(value) => {
                configs.insert(key.clone(), value.clone());
            }
            None => {
                configs.remove(key);
            }
        }
    }
    if configs.is_empty() {
        return Ok(None);
    }
    let subscription = ClientMetricSubscription {
        name: alteration.name.clone(),
        configs,
    };
    subscription.validate()?;
    Ok(Some(subscription))
}

fn validate_name(name: &str) -> Result<(), ControlError> {
    if name.is_empty() {
        Err(invalid("client metrics subscription name cannot be empty"))
    } else {
        Ok(())
    }
}

fn validate_key(key: &str) -> Result<(), ControlError> {
    if CONFIG_KEYS.contains(&key) {
        Ok(())
    } else {
        Err(invalid(format!(
            "unknown client metrics configuration {key}"
        )))
    }
}

fn matching_patterns(value: &str) -> Result<Vec<(String, Regex)>, ControlError> {
    split_list(value)
        .into_iter()
        .map(|entry| {
            let (name, expression) = entry
                .split_once('=')
                .filter(|(_, expression)| !expression.contains('='))
                .ok_or_else(|| invalid(format!("illegal client matching pattern {entry}")))?;
            let name = name.trim();
            if !MATCH_KEYS.contains(&name) {
                return Err(invalid(format!("illegal client matching pattern {entry}")));
            }
            let pattern = Regex::new(expression.trim())
                .map_err(|_| invalid(format!("illegal client matching pattern {entry}")))?;
            Ok((name.to_owned(), pattern))
        })
        .collect()
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

fn invalid(message: impl Into<String>) -> ControlError {
    ControlError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alteration(
        ops: impl IntoIterator<Item = (&'static str, Option<&'static str>)>,
    ) -> ClientMetricConfigAlteration {
        ClientMetricConfigAlteration {
            name: "java-clients".to_owned(),
            ops: ops
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.map(str::to_owned)))
                .collect(),
        }
    }

    #[test]
    fn validates_and_matches_client_subscriptions() {
        let subscription = apply_alteration(
            None,
            &alteration([
                (CLIENT_METRICS_METRICS, Some("producer., consumer.")),
                (CLIENT_METRICS_INTERVAL_MS, Some("100")),
                (
                    CLIENT_METRICS_MATCH,
                    Some("client_id=flink-.*,client_software_name=apache-kafka-java"),
                ),
            ]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(subscription.metrics(), vec!["producer.", "consumer."]);
        assert_eq!(subscription.push_interval_ms(), 100);
        assert!(subscription.matches(&BTreeMap::from([
            (CLIENT_ID.to_owned(), "flink-producer".to_owned()),
            (
                CLIENT_SOFTWARE_NAME.to_owned(),
                "apache-kafka-java".to_owned()
            ),
        ])));
    }

    #[test]
    fn rejects_invalid_keys_intervals_and_patterns_atomically() {
        for ops in [
            vec![("unknown", Some("value"))],
            vec![(CLIENT_METRICS_INTERVAL_MS, Some("99"))],
            vec![(CLIENT_METRICS_MATCH, Some("client_id=["))],
            vec![(CLIENT_METRICS_MATCH, Some("unknown=.*"))],
        ] {
            assert!(apply_alteration(None, &alteration(ops)).is_err());
        }
    }

    #[test]
    fn removing_the_last_key_removes_the_resource() {
        let current =
            apply_alteration(None, &alteration([(CLIENT_METRICS_METRICS, Some("*"))])).unwrap();
        let removed =
            apply_alteration(current, &alteration([(CLIENT_METRICS_METRICS, None)])).unwrap();
        assert!(removed.is_none());
    }
}
