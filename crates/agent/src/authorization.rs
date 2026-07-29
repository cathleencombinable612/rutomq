use super::Broker;
use crate::kafka_error::UNKNOWN_SERVER_ERROR;
use anyhow::Result;
use rutomq_control::{AclOperation, AclResourceType};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

pub(crate) const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";

#[derive(Debug, Clone)]
pub(crate) struct AuthorizationContext {
    pub principal: String,
    pub host: String,
    pub token_authenticated: bool,
    pub source_port: Option<u16>,
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl AuthorizationContext {
    pub fn anonymous(host: IpAddr) -> Self {
        Self {
            principal: "User:ANONYMOUS".to_owned(),
            host: host.to_string(),
            token_authenticated: false,
            source_port: None,
            client_software_name: None,
            client_software_version: None,
        }
    }

    pub fn authenticated(username: &str, host: IpAddr) -> Self {
        Self {
            principal: format!("User:{username}"),
            host: host.to_string(),
            token_authenticated: false,
            source_port: None,
            client_software_name: None,
            client_software_version: None,
        }
    }

    pub fn authenticated_token(username: &str, host: IpAddr) -> Self {
        Self {
            principal: format!("User:{username}"),
            host: host.to_string(),
            token_authenticated: true,
            source_port: None,
            client_software_name: None,
            client_software_version: None,
        }
    }

    pub fn with_client_connection(
        mut self,
        source_port: u16,
        software_name: Option<&str>,
        software_version: Option<&str>,
    ) -> Self {
        self.source_port = Some(source_port);
        self.client_software_name = software_name.map(str::to_owned);
        self.client_software_version = software_version.map(str::to_owned);
        self
    }
}

impl Broker {
    pub(super) async fn authorized(
        &self,
        context: &AuthorizationContext,
        resource_type: AclResourceType,
        resource_name: &str,
        operation: AclOperation,
    ) -> Result<bool> {
        let security = &self.config.security;
        if !security.acl_enabled || security.super_users.contains(&context.principal) {
            return Ok(true);
        }
        Ok(self
            .metadata
            .authorize(
                &context.principal,
                &context.host,
                resource_type,
                resource_name,
                operation,
                security.allow_everyone_if_no_acl_found,
            )
            .await?)
    }

    pub(super) async fn topic_names_describable(
        &self,
        context: &AuthorizationContext,
        topic_names: &[&str],
    ) -> Result<bool> {
        Ok(self
            .topic_authorizations(context, topic_names, AclOperation::Describe)
            .await?
            .into_values()
            .all(|authorized| authorized))
    }

    pub(super) async fn authorized_by_resource_type(
        &self,
        context: &AuthorizationContext,
        resource_type: AclResourceType,
        operation: AclOperation,
    ) -> Result<bool> {
        let security = &self.config.security;
        if !security.acl_enabled || security.super_users.contains(&context.principal) {
            return Ok(true);
        }
        Ok(self
            .metadata
            .authorize_by_resource_type(
                &context.principal,
                &context.host,
                resource_type,
                operation,
                security.allow_everyone_if_no_acl_found,
            )
            .await?)
    }

    pub(super) async fn topic_authorizations(
        &self,
        context: &AuthorizationContext,
        topic_names: &[&str],
        operation: AclOperation,
    ) -> Result<HashMap<String, bool>> {
        let mut checked = HashSet::new();
        let mut authorizations = HashMap::new();
        for &topic_name in topic_names {
            if checked.insert(topic_name) {
                authorizations.insert(
                    topic_name.to_owned(),
                    self.authorized(context, AclResourceType::Topic, topic_name, operation)
                        .await?,
                );
            }
        }
        Ok(authorizations)
    }

    pub(super) fn acl_enabled(&self) -> bool {
        self.config.security.acl_enabled
    }
}

pub(super) fn authorization_failure(
    result: Result<bool>,
    denied_error_code: i16,
) -> Option<(i16, Option<String>)> {
    match result {
        Ok(true) => None,
        Ok(false) => Some((denied_error_code, None)),
        Err(error) => Some((UNKNOWN_SERVER_ERROR, Some(error.to_string()))),
    }
}
