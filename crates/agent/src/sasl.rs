use crate::config::SecurityConfig;
use crate::kafka_error::{
    ILLEGAL_SASL_STATE, SASL_AUTHENTICATION_FAILED, UNSUPPORTED_SASL_MECHANISM,
};
use crate::scram::{ScramMechanism, ScramSession, credential_from_password};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use chrono::Utc;
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::MetadataStore;
use std::collections::HashMap;
use std::sync::Arc;

const MAX_AUTH_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct SaslAuthenticator {
    users: Arc<HashMap<String, String>>,
    metadata: Arc<dyn MetadataStore>,
    iterations: u32,
    max_reauth_ms: i64,
    required: bool,
    delegation_tokens_enabled: bool,
}

impl SaslAuthenticator {
    pub fn new(config: &SecurityConfig, metadata: Arc<dyn MetadataStore>) -> Self {
        Self {
            users: Arc::new(config.scram_users.clone()),
            metadata,
            iterations: config.scram_iterations,
            max_reauth_ms: config.sasl_max_reauth_ms,
            required: config.sasl_enabled,
            delegation_tokens_enabled: config.delegation_token_secret.is_some(),
        }
    }

    pub fn connection(&self) -> SaslConnection {
        SaslConnection {
            users: self.users.clone(),
            metadata: self.metadata.clone(),
            iterations: self.iterations,
            max_reauth_ms: self.max_reauth_ms,
            required: self.required,
            delegation_tokens_enabled: self.delegation_tokens_enabled,
            state: SaslState::Initial,
        }
    }

    pub fn enabled(&self) -> bool {
        self.required
    }
}

pub struct SaslConnection {
    users: Arc<HashMap<String, String>>,
    metadata: Arc<dyn MetadataStore>,
    iterations: u32,
    max_reauth_ms: i64,
    required: bool,
    delegation_tokens_enabled: bool,
    state: SaslState,
}

enum SaslState {
    Initial,
    Selected {
        mechanism: ScramMechanism,
        framing: SaslFraming,
        reauthentication: Option<ReauthenticationIdentity>,
    },
    Exchange {
        session: ScramSession,
        token: Option<TokenIdentity>,
        framing: SaslFraming,
        reauthentication: Option<ReauthenticationIdentity>,
    },
    Authenticated {
        principal: String,
        token_authenticated: bool,
        expiry_timestamp_ms: Option<i64>,
        mechanism: ScramMechanism,
    },
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaslFraming {
    Kafka,
    Opaque,
}

struct TokenIdentity {
    owner: String,
    expiry_timestamp_ms: i64,
}

struct ReauthenticationIdentity {
    principal: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationStatus {
    Continue,
    Complete,
    Failed,
}

pub struct AuthenticateResult {
    pub response: SaslAuthenticateResponse,
    pub status: AuthenticationStatus,
}

impl SaslConnection {
    pub fn is_authenticated(&self) -> bool {
        !self.required
            || matches!(
                self.state,
                SaslState::Authenticated {
                    expiry_timestamp_ms: None,
                    ..
                }
            )
            || matches!(
                self.state,
                SaslState::Authenticated {
                    expiry_timestamp_ms: Some(expiry_timestamp_ms),
                    ..
                } if expiry_timestamp_ms >= Utc::now().timestamp_millis()
            )
    }

    pub fn principal(&self) -> Option<&str> {
        match &self.state {
            SaslState::Authenticated { principal, .. } => Some(principal),
            _ => None,
        }
    }

    pub fn has_authenticated_session(&self) -> bool {
        matches!(self.state, SaslState::Authenticated { .. })
    }

    pub fn is_reauthenticating(&self) -> bool {
        matches!(
            self.state,
            SaslState::Selected {
                reauthentication: Some(_),
                ..
            } | SaslState::Exchange {
                reauthentication: Some(_),
                ..
            }
        )
    }

    pub fn token_authenticated(&self) -> bool {
        matches!(
            self.state,
            SaslState::Authenticated {
                token_authenticated: true,
                ..
            }
        )
    }

    pub fn expects_opaque_token(&self) -> bool {
        matches!(
            self.state,
            SaslState::Selected {
                framing: SaslFraming::Opaque,
                ..
            } | SaslState::Exchange {
                framing: SaslFraming::Opaque,
                ..
            }
        )
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.state, SaslState::Failed)
    }

    pub fn handshake(
        &mut self,
        request: SaslHandshakeRequest,
        version: i16,
    ) -> SaslHandshakeResponse {
        let mechanisms = self.mechanisms();
        let reauthentication = match &self.state {
            SaslState::Initial => None,
            SaslState::Authenticated {
                principal,
                mechanism: previous_mechanism,
                ..
            } if version > 0 => Some((principal.clone(), *previous_mechanism)),
            _ => {
                self.state = SaslState::Failed;
                return SaslHandshakeResponse::default()
                    .with_error_code(ILLEGAL_SASL_STATE)
                    .with_mechanisms(mechanisms);
            }
        };
        let Some(mechanism) = ScramMechanism::parse(request.mechanism.as_str()) else {
            self.state = SaslState::Failed;
            return SaslHandshakeResponse::default()
                .with_error_code(UNSUPPORTED_SASL_MECHANISM)
                .with_mechanisms(mechanisms);
        };
        if !self.required {
            self.state = SaslState::Failed;
            return SaslHandshakeResponse::default()
                .with_error_code(UNSUPPORTED_SASL_MECHANISM)
                .with_mechanisms(Vec::new());
        }
        if reauthentication
            .as_ref()
            .is_some_and(|(_, previous_mechanism)| *previous_mechanism != mechanism)
        {
            self.state = SaslState::Failed;
            return SaslHandshakeResponse::default().with_mechanisms(mechanisms);
        }
        self.state = SaslState::Selected {
            mechanism,
            framing: if version == 0 {
                SaslFraming::Opaque
            } else {
                SaslFraming::Kafka
            },
            reauthentication: reauthentication
                .map(|(principal, _)| ReauthenticationIdentity { principal }),
        };
        SaslHandshakeResponse::default().with_mechanisms(mechanisms)
    }

    pub async fn authenticate(&mut self, request: SaslAuthenticateRequest) -> AuthenticateResult {
        if self.expects_opaque_token() {
            self.state = SaslState::Failed;
            return illegal_state("SASL v0 requires opaque authentication packets");
        }
        self.authenticate_token(request.auth_bytes).await
    }

    pub async fn authenticate_opaque(&mut self, auth_bytes: Bytes) -> AuthenticateResult {
        if !self.expects_opaque_token() {
            self.state = SaslState::Failed;
            return illegal_state("SASL opaque authentication is not active");
        }
        self.authenticate_token(auth_bytes).await
    }

    async fn authenticate_token(&mut self, auth_bytes: Bytes) -> AuthenticateResult {
        if auth_bytes.len() > MAX_AUTH_BYTES {
            self.state = SaslState::Failed;
            return failed("SASL authentication message is too large");
        }
        let state = std::mem::replace(&mut self.state, SaslState::Failed);
        match state {
            SaslState::Selected {
                mechanism,
                framing,
                reauthentication,
            } => {
                let (username, token_authenticated) = match ScramSession::identity(&auth_bytes) {
                    Ok(identity) => identity,
                    Err(_) => return failed("SASL authentication failed"),
                };
                let mut token_identity = None;
                let credential = if token_authenticated {
                    if !self.delegation_tokens_enabled {
                        None
                    } else {
                        match self
                            .metadata
                            .delegation_token_by_id(&username, Utc::now().timestamp_millis())
                            .await
                        {
                            Ok(Some(token)) => {
                                let Some(owner) = principal_name(&token.owner_principal) else {
                                    return failed("SASL authentication failed");
                                };
                                let password = STANDARD.encode(&token.hmac);
                                token_identity = Some(TokenIdentity {
                                    owner,
                                    expiry_timestamp_ms: token.expiry_timestamp_ms,
                                });
                                Some(credential_from_password(
                                    username.clone(),
                                    mechanism,
                                    self.iterations,
                                    password.as_bytes(),
                                ))
                            }
                            Ok(None) => None,
                            Err(_) => return failed("SASL authentication failed"),
                        }
                    }
                } else {
                    let users = [username.clone()];
                    match self.metadata.scram_credentials(Some(&users)).await {
                        Ok(credentials) => credentials
                            .into_iter()
                            .find(|credential| credential.mechanism == mechanism.code()),
                        Err(_) => return failed("SASL authentication failed"),
                    }
                };
                let empty_users = HashMap::new();
                let users = if token_authenticated {
                    &empty_users
                } else {
                    self.users.as_ref()
                };
                match ScramSession::start(
                    mechanism,
                    &auth_bytes,
                    users,
                    self.iterations,
                    credential.as_ref(),
                ) {
                    Ok((session, auth_bytes)) => {
                        self.state = SaslState::Exchange {
                            session,
                            token: token_identity,
                            framing,
                            reauthentication,
                        };
                        AuthenticateResult {
                            response: success(auth_bytes, 0),
                            status: AuthenticationStatus::Continue,
                        }
                    }
                    Err(_) => failed("SASL authentication failed"),
                }
            }
            SaslState::Exchange {
                session,
                token,
                framing: _,
                reauthentication,
            } => {
                let now_ms = Utc::now().timestamp_millis();
                if token
                    .as_ref()
                    .is_some_and(|token| token.expiry_timestamp_ms < now_ms)
                {
                    return failed("SASL authentication failed");
                }
                let mechanism = session.mechanism();
                match session.finish(&auth_bytes) {
                    Ok((username, auth_bytes)) => {
                        let token_expiry_timestamp_ms =
                            token.as_ref().map(|token| token.expiry_timestamp_ms);
                        let (principal, token_authenticated) =
                            token.map_or((username, false), |token| (token.owner, true));
                        if reauthentication
                            .as_ref()
                            .is_some_and(|identity| identity.principal != principal)
                        {
                            return failed(
                                "Cannot change principals during SASL re-authentication",
                            );
                        }
                        let (session_lifetime_ms, expiry_timestamp_ms) =
                            self.session_lifetime(now_ms, token_expiry_timestamp_ms);
                        self.state = SaslState::Authenticated {
                            principal,
                            token_authenticated,
                            expiry_timestamp_ms,
                            mechanism,
                        };
                        AuthenticateResult {
                            response: success(auth_bytes, session_lifetime_ms),
                            status: AuthenticationStatus::Complete,
                        }
                    }
                    Err(_) => failed("SASL authentication failed"),
                }
            }
            _ => illegal_state("SASL handshake is not complete"),
        }
    }

    fn mechanisms(&self) -> Vec<StrBytes> {
        ScramMechanism::NAMES
            .iter()
            .map(|mechanism| message(mechanism))
            .collect()
    }

    fn session_lifetime(
        &self,
        now_ms: i64,
        credential_expiry_timestamp_ms: Option<i64>,
    ) -> (i64, Option<i64>) {
        let credential_lifetime_ms =
            credential_expiry_timestamp_ms.map(|expiry| expiry.saturating_sub(now_ms).max(0));
        let configured_lifetime_ms = (self.max_reauth_ms > 0).then_some(self.max_reauth_ms);
        let lifetime_ms = match (credential_lifetime_ms, configured_lifetime_ms) {
            (Some(credential), Some(configured)) => credential.min(configured),
            (Some(credential), None) => credential,
            (None, Some(configured)) => configured,
            (None, None) => return (0, None),
        };
        (lifetime_ms, Some(now_ms.saturating_add(lifetime_ms)))
    }
}

fn illegal_state(error: &str) -> AuthenticateResult {
    AuthenticateResult {
        response: SaslAuthenticateResponse::default()
            .with_error_code(ILLEGAL_SASL_STATE)
            .with_error_message(Some(message(error))),
        status: AuthenticationStatus::Failed,
    }
}

fn success(auth_bytes: Bytes, session_lifetime_ms: i64) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse::default()
        .with_error_message(None)
        .with_auth_bytes(auth_bytes)
        .with_session_lifetime_ms(session_lifetime_ms)
}

fn failed(error: &str) -> AuthenticateResult {
    AuthenticateResult {
        response: SaslAuthenticateResponse::default()
            .with_error_code(SASL_AUTHENTICATION_FAILED)
            .with_error_message(Some(message(error))),
        status: AuthenticationStatus::Failed,
    }
}

fn message(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}

fn principal_name(principal: &str) -> Option<String> {
    principal
        .split_once(':')
        .filter(|(principal_type, name)| !principal_type.is_empty() && !name.is_empty())
        .map(|(_, name)| name.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_mechanisms() {
        let config = SecurityConfig {
            scram_users: HashMap::from([("alice".to_owned(), "secret".to_owned())]),
            scram_iterations: 4_096,
            sasl_enabled: true,
            ..SecurityConfig::default()
        };
        let mut connection = SaslAuthenticator::new(
            &config,
            Arc::new(rutomq_control::MemoryMetadataStore::new()),
        )
        .connection();
        let response = connection.handshake(
            SaslHandshakeRequest::default().with_mechanism(message("PLAIN")),
            1,
        );
        assert_eq!(response.error_code, UNSUPPORTED_SASL_MECHANISM);
        assert_eq!(response.mechanisms.len(), 2);
        assert!(!connection.is_authenticated());
    }

    #[test]
    fn disabled_authenticator_allows_anonymous_requests() {
        let config = SecurityConfig {
            scram_iterations: 4_096,
            ..SecurityConfig::default()
        };
        let authenticator = SaslAuthenticator::new(
            &config,
            Arc::new(rutomq_control::MemoryMetadataStore::new()),
        );
        assert!(!authenticator.enabled());
        assert!(authenticator.connection().is_authenticated());
    }

    #[test]
    fn database_only_authenticator_still_requires_sasl() {
        let config = SecurityConfig {
            scram_iterations: 4_096,
            sasl_enabled: true,
            ..SecurityConfig::default()
        };
        let authenticator = SaslAuthenticator::new(
            &config,
            Arc::new(rutomq_control::MemoryMetadataStore::new()),
        );
        assert!(authenticator.enabled());
        assert!(!authenticator.connection().is_authenticated());
    }
}
