use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationToken {
    pub token_id: String,
    pub owner_principal: String,
    pub requester_principal: String,
    pub renewers: Vec<String>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub hmac: Vec<u8>,
}

impl fmt::Debug for DelegationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DelegationToken")
            .field("token_id", &self.token_id)
            .field("owner_principal", &self.owner_principal)
            .field("requester_principal", &self.requester_principal)
            .field("renewers", &self.renewers)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("hmac", &"<redacted>")
            .finish()
    }
}

impl DelegationToken {
    pub fn owner_or_renewer(&self, principal: &str) -> bool {
        self.owner_principal == principal
            || self.requester_principal == principal
            || self.renewers.iter().any(|renewer| renewer == principal)
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expiry_timestamp_ms < now_ms || self.max_timestamp_ms < now_ms
    }
}
