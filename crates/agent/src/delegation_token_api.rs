use super::Broker;
use super::authorization::AuthorizationContext;
use crate::kafka_error::{
    DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_AUTHORIZATION_FAILED,
    DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_NOT_FOUND, DELEGATION_TOKEN_OWNER_MISMATCH,
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, INVALID_PRINCIPAL_TYPE, NO_ERROR, UNKNOWN_SERVER_ERROR,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use chrono::Utc;
use hmac::{Hmac, Mac};
use kafka_protocol::messages::describe_delegation_token_response::{
    DescribedDelegationToken, DescribedDelegationTokenRenewer,
};
use kafka_protocol::messages::{
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, ControlError, DelegationToken};
use sha2::Sha512;
use std::collections::HashSet;
use uuid::Uuid;

impl Broker {
    pub(super) async fn handle_create_delegation_token(
        &self,
        request: CreateDelegationTokenRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> CreateDelegationTokenResponse {
        let Some((requester_type, requester_name)) = principal_parts(&context.principal) else {
            return create_unresolved_error(INVALID_PRINCIPAL_TYPE, -1);
        };
        let owner = match requested_owner(&request, version, &context.principal) {
            Some(owner) => owner,
            None => return create_unresolved_error(INVALID_PRINCIPAL_TYPE, -1),
        };
        let Some((owner_type, owner_name)) = principal_parts(&owner) else {
            return create_unresolved_error(INVALID_PRINCIPAL_TYPE, -1);
        };
        let owner_type = owner_type.to_owned();
        let owner_name = owner_name.to_owned();
        if !token_request_allowed(context) {
            return create_error(
                DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                version,
                &owner_type,
                &owner_name,
                requester_type,
                requester_name,
                -1,
            );
        }
        let Some(secret) = self.config.security.delegation_token_secret.as_deref() else {
            return create_error(
                DELEGATION_TOKEN_AUTH_DISABLED,
                version,
                &owner_type,
                &owner_name,
                requester_type,
                requester_name,
                0,
            );
        };
        if owner != context.principal
            && !self
                .authorized(
                    context,
                    AclResourceType::User,
                    &owner,
                    AclOperation::CreateTokens,
                )
                .await
                .unwrap_or(false)
        {
            return create_error(
                DELEGATION_TOKEN_AUTHORIZATION_FAILED,
                version,
                &owner_type,
                &owner_name,
                requester_type,
                requester_name,
                -1,
            );
        }

        let mut renewers = Vec::with_capacity(request.renewers.len());
        for renewer in request.renewers {
            if renewer.principal_type.as_str() != "User" {
                return create_error(
                    INVALID_PRINCIPAL_TYPE,
                    version,
                    &owner_type,
                    &owner_name,
                    requester_type,
                    requester_name,
                    -1,
                );
            }
            let principal = format!(
                "{}:{}",
                renewer.principal_type.as_str(),
                renewer.principal_name.as_str()
            );
            renewers.push(principal);
        }

        let now_ms = Utc::now().timestamp_millis();
        let max_lifetime_ms = if request.max_lifetime_ms > 0 {
            request
                .max_lifetime_ms
                .min(self.config.security.delegation_token_max_lifetime_ms)
        } else {
            self.config.security.delegation_token_max_lifetime_ms
        };
        let max_timestamp_ms = now_ms.saturating_add(max_lifetime_ms);
        let expiry_timestamp_ms = max_timestamp_ms
            .min(now_ms.saturating_add(self.config.security.delegation_token_expiry_ms));
        let token_id = URL_SAFE_NO_PAD.encode(Uuid::new_v4().as_bytes());
        let hmac = token_hmac(secret, &token_id);
        let token = DelegationToken {
            token_id,
            owner_principal: owner,
            requester_principal: context.principal.clone(),
            renewers,
            issue_timestamp_ms: now_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            hmac,
        };
        if self
            .metadata
            .create_delegation_token(token.clone())
            .await
            .is_err()
        {
            return create_error(
                UNKNOWN_SERVER_ERROR,
                version,
                &owner_type,
                &owner_name,
                requester_type,
                requester_name,
                0,
            );
        }

        let mut response = CreateDelegationTokenResponse::default()
            .with_error_code(NO_ERROR)
            .with_principal_type(text(&owner_type))
            .with_principal_name(text(&owner_name))
            .with_issue_timestamp_ms(token.issue_timestamp_ms)
            .with_expiry_timestamp_ms(token.expiry_timestamp_ms)
            .with_max_timestamp_ms(token.max_timestamp_ms)
            .with_token_id(text(&token.token_id))
            .with_hmac(Bytes::from(token.hmac));
        if version >= 3 {
            response = response
                .with_token_requester_principal_type(text(requester_type))
                .with_token_requester_principal_name(text(requester_name));
        }
        response
    }

    pub(super) async fn handle_renew_delegation_token(
        &self,
        request: RenewDelegationTokenRequest,
        context: &AuthorizationContext,
    ) -> RenewDelegationTokenResponse {
        if !token_request_allowed(context) {
            return renew_error(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, -1);
        }
        if self.config.security.delegation_token_secret.is_none() {
            return renew_error(DELEGATION_TOKEN_AUTH_DISABLED, 0);
        }
        match self
            .metadata
            .renew_delegation_token(
                &request.hmac,
                &context.principal,
                Utc::now().timestamp_millis(),
                request.renew_period_ms,
                self.config.security.delegation_token_expiry_ms,
            )
            .await
        {
            Ok(expiry_timestamp_ms) => RenewDelegationTokenResponse::default()
                .with_error_code(NO_ERROR)
                .with_expiry_timestamp_ms(expiry_timestamp_ms),
            Err(error) => renew_error(token_error_code(&error), 0),
        }
    }

    pub(super) async fn handle_expire_delegation_token(
        &self,
        request: ExpireDelegationTokenRequest,
        context: &AuthorizationContext,
    ) -> ExpireDelegationTokenResponse {
        if !token_request_allowed(context) {
            return expire_error(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, -1);
        }
        if self.config.security.delegation_token_secret.is_none() {
            return expire_error(DELEGATION_TOKEN_AUTH_DISABLED, 0);
        }
        match self
            .metadata
            .expire_delegation_token(
                &request.hmac,
                &context.principal,
                Utc::now().timestamp_millis(),
                request.expiry_time_period_ms,
            )
            .await
        {
            Ok(expiry_timestamp_ms) => ExpireDelegationTokenResponse::default()
                .with_error_code(NO_ERROR)
                .with_expiry_timestamp_ms(expiry_timestamp_ms),
            Err(error) => expire_error(token_error_code(&error), 0),
        }
    }

    pub(super) async fn handle_describe_delegation_token(
        &self,
        request: DescribeDelegationTokenRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> DescribeDelegationTokenResponse {
        if !token_request_allowed(context) {
            return DescribeDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
        }
        if self.config.security.delegation_token_secret.is_none() {
            return DescribeDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED);
        }
        let owners = request.owners.map(|owners| {
            owners
                .into_iter()
                .map(|owner| {
                    format!(
                        "{}:{}",
                        owner.principal_type.as_str(),
                        owner.principal_name.as_str()
                    )
                })
                .collect::<HashSet<_>>()
        });
        if owners.as_ref().is_some_and(HashSet::is_empty) {
            return DescribeDelegationTokenResponse::default().with_error_code(NO_ERROR);
        }
        let tokens = match self
            .metadata
            .delegation_tokens(Utc::now().timestamp_millis())
            .await
        {
            Ok(tokens) => tokens,
            Err(_) => {
                return DescribeDelegationTokenResponse::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR);
            }
        };
        let mut described = Vec::new();
        for token in tokens {
            if owners
                .as_ref()
                .is_some_and(|owners| !owners.contains(&token.owner_principal))
            {
                continue;
            }
            if !token.owner_or_renewer(&context.principal)
                && !self.can_describe_token(context, &token).await
            {
                continue;
            }
            described.push(described_token(token, version));
        }
        DescribeDelegationTokenResponse::default()
            .with_error_code(NO_ERROR)
            .with_tokens(described)
    }

    async fn can_describe_token(
        &self,
        context: &AuthorizationContext,
        token: &DelegationToken,
    ) -> bool {
        if self
            .authorized(
                context,
                AclResourceType::DelegationToken,
                &token.token_id,
                AclOperation::Describe,
            )
            .await
            .unwrap_or(false)
        {
            return true;
        }
        self.authorized(
            context,
            AclResourceType::User,
            &token.owner_principal,
            AclOperation::DescribeTokens,
        )
        .await
        .unwrap_or(false)
    }
}

fn token_request_allowed(context: &AuthorizationContext) -> bool {
    context.principal != "User:ANONYMOUS" && !context.token_authenticated
}

fn requested_owner(
    request: &CreateDelegationTokenRequest,
    version: i16,
    requester: &str,
) -> Option<String> {
    if version < 3 {
        return Some(requester.to_owned());
    }
    let Some(principal_name) = request
        .owner_principal_name
        .as_ref()
        .filter(|value| !value.is_empty())
    else {
        return Some(requester.to_owned());
    };
    let principal_type = request
        .owner_principal_type
        .as_ref()
        .filter(|value| !value.is_empty());
    principal_type
        .map(|principal_type| format!("{}:{}", principal_type.as_str(), principal_name.as_str()))
}

fn described_token(token: DelegationToken, version: i16) -> DescribedDelegationToken {
    let (owner_type, owner_name) = principal_parts(&token.owner_principal).unwrap_or(("", ""));
    let (requester_type, requester_name) =
        principal_parts(&token.requester_principal).unwrap_or(("", ""));
    let renewers = token
        .renewers
        .iter()
        .filter_map(|renewer| renewer.split_once(':'))
        .map(|(principal_type, principal_name)| {
            DescribedDelegationTokenRenewer::default()
                .with_principal_type(text(principal_type))
                .with_principal_name(text(principal_name))
        })
        .collect();
    let mut described = DescribedDelegationToken::default()
        .with_principal_type(text(owner_type))
        .with_principal_name(text(owner_name))
        .with_issue_timestamp(token.issue_timestamp_ms)
        .with_expiry_timestamp(token.expiry_timestamp_ms)
        .with_max_timestamp(token.max_timestamp_ms)
        .with_token_id(text(&token.token_id))
        .with_hmac(Bytes::from(token.hmac))
        .with_renewers(renewers);
    if version >= 3 {
        described = described
            .with_token_requester_principal_type(text(requester_type))
            .with_token_requester_principal_name(text(requester_name));
    }
    described
}

fn token_hmac(secret: &str, token_id: &str) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(token_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn token_error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::DelegationTokenNotFound => DELEGATION_TOKEN_NOT_FOUND,
        ControlError::DelegationTokenOwnerMismatch => DELEGATION_TOKEN_OWNER_MISMATCH,
        ControlError::DelegationTokenExpired => DELEGATION_TOKEN_EXPIRED,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn principal_parts(principal: &str) -> Option<(&str, &str)> {
    principal
        .split_once(':')
        .filter(|(principal_type, principal_name)| {
            !principal_type.is_empty() && !principal_name.is_empty()
        })
}

fn create_error(
    error_code: i16,
    version: i16,
    owner_type: &str,
    owner_name: &str,
    requester_type: &str,
    requester_name: &str,
    error_timestamp_ms: i64,
) -> CreateDelegationTokenResponse {
    let mut response = CreateDelegationTokenResponse::default()
        .with_error_code(error_code)
        .with_principal_type(text(owner_type))
        .with_principal_name(text(owner_name))
        .with_issue_timestamp_ms(error_timestamp_ms)
        .with_expiry_timestamp_ms(error_timestamp_ms)
        .with_max_timestamp_ms(error_timestamp_ms);
    if version >= 3 {
        response = response
            .with_token_requester_principal_type(text(requester_type))
            .with_token_requester_principal_name(text(requester_name));
    }
    response
}

fn create_unresolved_error(
    error_code: i16,
    error_timestamp_ms: i64,
) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse::default()
        .with_error_code(error_code)
        .with_issue_timestamp_ms(error_timestamp_ms)
        .with_expiry_timestamp_ms(error_timestamp_ms)
        .with_max_timestamp_ms(error_timestamp_ms)
}

fn renew_error(error_code: i16, expiry_timestamp_ms: i64) -> RenewDelegationTokenResponse {
    RenewDelegationTokenResponse::default()
        .with_error_code(error_code)
        .with_expiry_timestamp_ms(expiry_timestamp_ms)
}

fn expire_error(error_code: i16, expiry_timestamp_ms: i64) -> ExpireDelegationTokenResponse {
    ExpireDelegationTokenResponse::default()
        .with_error_code(error_code)
        .with_expiry_timestamp_ms(expiry_timestamp_ms)
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
