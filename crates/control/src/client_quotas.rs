use std::collections::BTreeMap;

pub const USER_ENTITY: &str = "user";
pub const CLIENT_ID_ENTITY: &str = "client-id";
pub const IP_ENTITY: &str = "ip";

pub const PRODUCER_BYTE_RATE: &str = "producer_byte_rate";
pub const CONSUMER_BYTE_RATE: &str = "consumer_byte_rate";
pub const REQUEST_PERCENTAGE: &str = "request_percentage";
pub const CONTROLLER_MUTATION_RATE: &str = "controller_mutation_rate";
pub const CONNECTION_CREATION_RATE: &str = "connection_creation_rate";

/// An outer `None` means that the dimension is absent. `Some(None)` is Kafka's
/// default entity and `Some(Some(name))` is a named entity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientQuotaEntity {
    pub user: Option<Option<String>>,
    pub client_id: Option<Option<String>>,
    pub ip: Option<Option<String>>,
}

impl ClientQuotaEntity {
    pub fn dimension_count(&self) -> usize {
        usize::from(self.user.is_some())
            + usize::from(self.client_id.is_some())
            + usize::from(self.ip.is_some())
    }

    pub fn dimensions(&self) -> Vec<(&'static str, Option<&str>)> {
        let mut dimensions = Vec::with_capacity(self.dimension_count());
        if let Some(user) = &self.user {
            dimensions.push((USER_ENTITY, user.as_deref()));
        }
        if let Some(client_id) = &self.client_id {
            dimensions.push((CLIENT_ID_ENTITY, client_id.as_deref()));
        }
        if let Some(ip) = &self.ip {
            dimensions.push((IP_ENTITY, ip.as_deref()));
        }
        dimensions
    }

    pub(crate) fn storage_key(&self) -> String {
        fn encode(marker: char, value: &Option<Option<String>>, output: &mut String) {
            output.push(marker);
            match value {
                None => output.push('-'),
                Some(None) => output.push('d'),
                Some(Some(value)) => {
                    output.push_str(&value.len().to_string());
                    output.push(':');
                    output.push_str(value);
                }
            }
            output.push('|');
        }

        let mut key = String::new();
        encode('u', &self.user, &mut key);
        encode('c', &self.client_id, &mut key);
        encode('i', &self.ip, &mut key);
        key
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuota {
    pub entity: ClientQuotaEntity,
    pub values: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuotaAlteration {
    pub entity: ClientQuotaEntity,
    /// `None` removes the key; `Some(value)` sets it.
    pub ops: BTreeMap<String, Option<f64>>,
}
