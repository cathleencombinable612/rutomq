use crate::ControlError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub enum AclResourceType {
    Topic = 2,
    Group = 3,
    Cluster = 4,
    TransactionalId = 5,
    DelegationToken = 6,
    User = 7,
}

impl TryFrom<i8> for AclResourceType {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::Topic),
            3 => Ok(Self::Group),
            4 => Ok(Self::Cluster),
            5 => Ok(Self::TransactionalId),
            6 => Ok(Self::DelegationToken),
            7 => Ok(Self::User),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid ACL resource type {value}"
            ))),
        }
    }
}

impl AclResourceType {
    pub fn code(self) -> i8 {
        self as i8
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Topic => "TOPIC",
            Self::Group => "GROUP",
            Self::Cluster => "CLUSTER",
            Self::TransactionalId => "TRANSACTIONAL_ID",
            Self::DelegationToken => "DELEGATION_TOKEN",
            Self::User => "USER",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self, ControlError> {
        match value {
            "TOPIC" => Ok(Self::Topic),
            "GROUP" => Ok(Self::Group),
            "CLUSTER" => Ok(Self::Cluster),
            "TRANSACTIONAL_ID" => Ok(Self::TransactionalId),
            "DELEGATION_TOKEN" => Ok(Self::DelegationToken),
            "USER" => Ok(Self::User),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid stored ACL resource type {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub enum AclPatternType {
    Literal = 3,
    Prefixed = 4,
}

impl TryFrom<i8> for AclPatternType {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::Literal),
            4 => Ok(Self::Prefixed),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid ACL pattern type {value}"
            ))),
        }
    }
}

impl AclPatternType {
    pub fn code(self) -> i8 {
        self as i8
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Literal => "LITERAL",
            Self::Prefixed => "PREFIXED",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self, ControlError> {
        match value {
            "LITERAL" => Ok(Self::Literal),
            "PREFIXED" => Ok(Self::Prefixed),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid stored ACL pattern type {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclPatternFilter {
    Any,
    Match,
    Literal,
    Prefixed,
}

impl TryFrom<i8> for AclPatternFilter {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Any),
            2 => Ok(Self::Match),
            3 => Ok(Self::Literal),
            4 => Ok(Self::Prefixed),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid ACL pattern filter {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub enum AclOperation {
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = 14,
    TwoPhaseCommit = 15,
}

impl TryFrom<i8> for AclOperation {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::All),
            3 => Ok(Self::Read),
            4 => Ok(Self::Write),
            5 => Ok(Self::Create),
            6 => Ok(Self::Delete),
            7 => Ok(Self::Alter),
            8 => Ok(Self::Describe),
            9 => Ok(Self::ClusterAction),
            10 => Ok(Self::DescribeConfigs),
            11 => Ok(Self::AlterConfigs),
            12 => Ok(Self::IdempotentWrite),
            13 => Ok(Self::CreateTokens),
            14 => Ok(Self::DescribeTokens),
            15 => Ok(Self::TwoPhaseCommit),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid ACL operation {value}"
            ))),
        }
    }
}

impl AclOperation {
    pub fn code(self) -> i8 {
        self as i8
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::All => "ALL",
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Create => "CREATE",
            Self::Delete => "DELETE",
            Self::Alter => "ALTER",
            Self::Describe => "DESCRIBE",
            Self::ClusterAction => "CLUSTER_ACTION",
            Self::DescribeConfigs => "DESCRIBE_CONFIGS",
            Self::AlterConfigs => "ALTER_CONFIGS",
            Self::IdempotentWrite => "IDEMPOTENT_WRITE",
            Self::CreateTokens => "CREATE_TOKENS",
            Self::DescribeTokens => "DESCRIBE_TOKENS",
            Self::TwoPhaseCommit => "TWO_PHASE_COMMIT",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self, ControlError> {
        match value {
            "ALL" => Ok(Self::All),
            "READ" => Ok(Self::Read),
            "WRITE" => Ok(Self::Write),
            "CREATE" => Ok(Self::Create),
            "DELETE" => Ok(Self::Delete),
            "ALTER" => Ok(Self::Alter),
            "DESCRIBE" => Ok(Self::Describe),
            "CLUSTER_ACTION" => Ok(Self::ClusterAction),
            "DESCRIBE_CONFIGS" => Ok(Self::DescribeConfigs),
            "ALTER_CONFIGS" => Ok(Self::AlterConfigs),
            "IDEMPOTENT_WRITE" => Ok(Self::IdempotentWrite),
            "CREATE_TOKENS" => Ok(Self::CreateTokens),
            "DESCRIBE_TOKENS" => Ok(Self::DescribeTokens),
            "TWO_PHASE_COMMIT" => Ok(Self::TwoPhaseCommit),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid stored ACL operation {value}"
            ))),
        }
    }

    fn covers(self, requested: Self) -> bool {
        self == Self::All
            || self == requested
            || matches!(
                (self, requested),
                (
                    Self::Read | Self::Write | Self::Delete | Self::Alter,
                    Self::Describe
                ) | (Self::AlterConfigs, Self::DescribeConfigs)
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub enum AclPermission {
    Deny = 2,
    Allow = 3,
}

impl TryFrom<i8> for AclPermission {
    type Error = ControlError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::Deny),
            3 => Ok(Self::Allow),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid ACL permission type {value}"
            ))),
        }
    }
}

impl AclPermission {
    pub fn code(self) -> i8 {
        self as i8
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Deny => "DENY",
            Self::Allow => "ALLOW",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self, ControlError> {
        match value {
            "DENY" => Ok(Self::Deny),
            "ALLOW" => Ok(Self::Allow),
            _ => Err(ControlError::InvalidRequest(format!(
                "invalid stored ACL permission {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AclRule {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl AclRule {
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.resource_name.is_empty() || self.principal.is_empty() || self.host.is_empty() {
            return Err(ControlError::InvalidRequest(
                "ACL resource name, principal, and host must not be empty".to_owned(),
            ));
        }
        if !self.principal.contains(':') {
            return Err(ControlError::InvalidRequest(
                "ACL principal must use PrincipalType:name form".to_owned(),
            ));
        }
        Ok(())
    }

    fn applies_to(&self, resource_name: &str) -> bool {
        match self.pattern_type {
            AclPatternType::Literal => {
                self.resource_name == "*" || self.resource_name == resource_name
            }
            AclPatternType::Prefixed => resource_name.starts_with(&self.resource_name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclFilter {
    pub resource_type: Option<AclResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: AclPatternFilter,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission: Option<AclPermission>,
}

impl AclFilter {
    pub fn matches(&self, rule: &AclRule) -> bool {
        self.resource_type
            .is_none_or(|resource_type| resource_type == rule.resource_type)
            && self.pattern_matches(rule)
            && self
                .principal
                .as_ref()
                .is_none_or(|principal| principal == &rule.principal)
            && self.host.as_ref().is_none_or(|host| host == &rule.host)
            && self
                .operation
                .is_none_or(|operation| operation == rule.operation)
            && self
                .permission
                .is_none_or(|permission| permission == rule.permission)
    }

    fn pattern_matches(&self, rule: &AclRule) -> bool {
        let Some(resource_name) = self.resource_name.as_deref() else {
            return matches!(self.pattern_type, AclPatternFilter::Any)
                || matches!(
                    (self.pattern_type, rule.pattern_type),
                    (AclPatternFilter::Literal, AclPatternType::Literal)
                        | (AclPatternFilter::Prefixed, AclPatternType::Prefixed)
                );
        };
        match self.pattern_type {
            AclPatternFilter::Any => rule.resource_name == resource_name,
            AclPatternFilter::Match => rule.applies_to(resource_name),
            AclPatternFilter::Literal => {
                rule.pattern_type == AclPatternType::Literal && rule.resource_name == resource_name
            }
            AclPatternFilter::Prefixed => {
                rule.pattern_type == AclPatternType::Prefixed && rule.resource_name == resource_name
            }
        }
    }
}

pub(crate) fn authorize_rules(
    rules: &[AclRule],
    principal: &str,
    host: &str,
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
    allow_if_no_acl: bool,
) -> bool {
    let resource_rules = rules
        .iter()
        .filter(|rule| rule.resource_type == resource_type && rule.applies_to(resource_name))
        .collect::<Vec<_>>();
    if resource_rules.is_empty() {
        return allow_if_no_acl;
    }
    let applicable = resource_rules.into_iter().filter(|rule| {
        (rule.principal == principal || rule.principal == "User:*")
            && (rule.host == "*" || rule.host == host)
            && rule.operation.covers(operation)
    });
    let mut allowed = false;
    for rule in applicable {
        match rule.permission {
            AclPermission::Deny => return false,
            AclPermission::Allow => allowed = true,
        }
    }
    allowed
}

pub(crate) fn authorize_by_resource_type_rules(
    rules: &[AclRule],
    principal: &str,
    host: &str,
    resource_type: AclResourceType,
    operation: AclOperation,
    allow_if_no_acl: bool,
) -> bool {
    if authorize_rules(
        rules,
        principal,
        host,
        resource_type,
        "hardcode",
        operation,
        allow_if_no_acl,
    ) {
        return true;
    }

    let mut deny_literals = HashSet::new();
    let mut deny_prefixes = HashSet::new();
    let mut allow_literals = HashSet::new();
    let mut allow_prefixes = HashSet::new();
    let applicable = rules.iter().filter(|rule| {
        rule.resource_type == resource_type
            && (rule.principal == principal || rule.principal == "User:*")
            && (rule.host == "*" || rule.host == host)
            && (rule.operation == operation || rule.operation == AclOperation::All)
    });
    for rule in applicable {
        let target = match (rule.permission, rule.pattern_type) {
            (AclPermission::Deny, AclPatternType::Literal) => &mut deny_literals,
            (AclPermission::Deny, AclPatternType::Prefixed) => &mut deny_prefixes,
            (AclPermission::Allow, AclPatternType::Literal) => &mut allow_literals,
            (AclPermission::Allow, AclPatternType::Prefixed) => &mut allow_prefixes,
        };
        target.insert(rule.resource_name.as_str());
    }

    if deny_literals.contains("*") {
        return false;
    }
    if allow_literals.contains("*") {
        return true;
    }
    allow_literals
        .iter()
        .filter(|name| !deny_literals.contains(*name))
        .chain(allow_prefixes.iter())
        .any(|name| !deny_prefixes.iter().any(|deny| name.starts_with(deny)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(pattern_type: AclPatternType, permission: AclPermission) -> AclRule {
        AclRule {
            resource_type: AclResourceType::Topic,
            resource_name: "orders".to_owned(),
            pattern_type,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission,
        }
    }

    #[test]
    fn match_filter_applies_literal_prefixed_and_wildcard_rules() {
        let filter = AclFilter {
            resource_type: Some(AclResourceType::Topic),
            resource_name: Some("orders-v2".to_owned()),
            pattern_type: AclPatternFilter::Match,
            principal: None,
            host: None,
            operation: None,
            permission: None,
        };
        assert!(filter.matches(&rule(AclPatternType::Prefixed, AclPermission::Allow)));
        assert!(!filter.matches(&rule(AclPatternType::Literal, AclPermission::Allow)));
        let wildcard = AclRule {
            resource_name: "*".to_owned(),
            pattern_type: AclPatternType::Literal,
            ..rule(AclPatternType::Literal, AclPermission::Allow)
        };
        assert!(filter.matches(&wildcard));
    }

    #[test]
    fn resource_type_authorization_honors_dominant_denies() {
        let make_rule =
            |name: &str, pattern_type: AclPatternType, permission: AclPermission| AclRule {
                resource_type: AclResourceType::Topic,
                resource_name: name.to_owned(),
                pattern_type,
                principal: "User:alice".to_owned(),
                host: "*".to_owned(),
                operation: AclOperation::Write,
                permission,
            };
        let authorized = |rules: &[AclRule]| {
            authorize_by_resource_type_rules(
                rules,
                "User:alice",
                "127.0.0.1",
                AclResourceType::Topic,
                AclOperation::Write,
                false,
            )
        };

        assert!(authorized(&[make_rule(
            "orders",
            AclPatternType::Literal,
            AclPermission::Allow,
        )]));
        assert!(!authorized(&[
            make_rule("orders", AclPatternType::Literal, AclPermission::Allow,),
            make_rule("orders", AclPatternType::Literal, AclPermission::Deny,),
        ]));
        assert!(!authorized(&[
            make_rule(
                "private-orders",
                AclPatternType::Literal,
                AclPermission::Allow,
            ),
            make_rule("private-", AclPatternType::Prefixed, AclPermission::Deny,),
        ]));
        assert!(authorized(&[
            make_rule("orders-", AclPatternType::Prefixed, AclPermission::Allow,),
            make_rule(
                "orders-secret",
                AclPatternType::Literal,
                AclPermission::Deny,
            ),
        ]));
        assert!(!authorized(&[
            make_rule("*", AclPatternType::Literal, AclPermission::Allow),
            make_rule("*", AclPatternType::Literal, AclPermission::Deny),
        ]));
    }

    #[test]
    fn deny_wins_and_operation_implications_match_kafka() {
        let allow = AclRule {
            operation: AclOperation::Write,
            ..rule(AclPatternType::Literal, AclPermission::Allow)
        };
        assert!(authorize_rules(
            std::slice::from_ref(&allow),
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Describe,
            false,
        ));
        let deny = AclRule {
            principal: "User:*".to_owned(),
            operation: AclOperation::All,
            permission: AclPermission::Deny,
            ..allow
        };
        assert!(!authorize_rules(
            &[rule(AclPatternType::Literal, AclPermission::Allow), deny],
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Read,
            true,
        ));
    }

    #[test]
    fn two_phase_commit_operation_round_trips_without_write_implication() {
        assert_eq!(
            AclOperation::try_from(15).unwrap(),
            AclOperation::TwoPhaseCommit
        );
        assert_eq!(AclOperation::TwoPhaseCommit.name(), "TWO_PHASE_COMMIT");
        assert_eq!(
            AclOperation::from_name("TWO_PHASE_COMMIT").unwrap(),
            AclOperation::TwoPhaseCommit
        );
        let write = AclRule {
            resource_type: AclResourceType::TransactionalId,
            resource_name: "orders-tx".to_owned(),
            operation: AclOperation::Write,
            ..rule(AclPatternType::Literal, AclPermission::Allow)
        };
        assert!(!authorize_rules(
            std::slice::from_ref(&write),
            "User:alice",
            "127.0.0.1",
            AclResourceType::TransactionalId,
            "orders-tx",
            AclOperation::TwoPhaseCommit,
            false,
        ));
        let all = AclRule {
            operation: AclOperation::All,
            ..write
        };
        assert!(authorize_rules(
            std::slice::from_ref(&all),
            "User:alice",
            "127.0.0.1",
            AclResourceType::TransactionalId,
            "orders-tx",
            AclOperation::TwoPhaseCommit,
            false,
        ));
    }
}
