use super::authorization::CLUSTER_RESOURCE_NAME;
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, DUPLICATE_RESOURCE, RESOURCE_NOT_FOUND, UNACCEPTABLE_CREDENTIAL,
    UNSUPPORTED_SASL_MECHANISM,
};
use crate::scram::{ScramMechanism, credential_from_salted_password};
use kafka_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;
use kafka_protocol::messages::describe_user_scram_credentials_response::{
    CredentialInfo, DescribeUserScramCredentialsResult,
};
use rutomq_control::{ScramCredential, ScramCredentialAlteration};
use std::collections::BTreeMap;

const MIN_ITERATIONS: i32 = 4_096;
const MAX_ITERATIONS: i32 = 16_384;

impl Broker {
    pub(super) async fn handle_describe_user_scram_credentials(
        &self,
        request: DescribeUserScramCredentialsRequest,
        context: &AuthorizationContext,
    ) -> DescribeUserScramCredentialsResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Describe,
            )
            .await
            .unwrap_or(false)
        {
            return DescribeUserScramCredentialsResponse::default()
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                .with_error_message(Some(message("cluster authorization failed")));
        }

        let requested = request.users.unwrap_or_default();
        let mut counts = BTreeMap::<String, usize>::new();
        for user in &requested {
            *counts.entry(user.name.as_str().to_owned()).or_default() += 1;
        }
        let names = (!requested.is_empty()).then(|| counts.keys().cloned().collect::<Vec<_>>());
        let credentials = match self.metadata.scram_credentials(names.as_deref()).await {
            Ok(credentials) => credentials,
            Err(error) => {
                return DescribeUserScramCredentialsResponse::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR)
                    .with_error_message(Some(message(&error.to_string())));
            }
        };
        let mut by_user = BTreeMap::<String, Vec<ScramCredential>>::new();
        for credential in credentials {
            by_user
                .entry(credential.user.clone())
                .or_default()
                .push(credential);
        }
        if requested.is_empty() {
            counts.extend(by_user.keys().cloned().map(|user| (user, 1)));
        }

        let results = counts
            .into_iter()
            .map(|(user, count)| {
                if count > 1 {
                    return describe_result(
                        user,
                        DUPLICATE_RESOURCE,
                        Some("cannot describe the same SCRAM user twice"),
                        Vec::new(),
                    );
                }
                let Some(credentials) = by_user.remove(&user) else {
                    return describe_result(
                        user,
                        RESOURCE_NOT_FOUND,
                        Some("SCRAM credentials were not found"),
                        Vec::new(),
                    );
                };
                let infos = credentials
                    .into_iter()
                    .map(|credential| {
                        CredentialInfo::default()
                            .with_mechanism(credential.mechanism)
                            .with_iterations(credential.iterations)
                    })
                    .collect();
                describe_result(user, NO_ERROR, None, infos)
            })
            .collect();
        DescribeUserScramCredentialsResponse::default()
            .with_error_code(NO_ERROR)
            .with_error_message(None)
            .with_results(results)
    }

    pub(super) async fn handle_alter_user_scram_credentials(
        &self,
        request: AlterUserScramCredentialsRequest,
        context: &AuthorizationContext,
    ) -> AlterUserScramCredentialsResponse {
        let mut users = BTreeMap::<String, usize>::new();
        for deletion in &request.deletions {
            *users.entry(deletion.name.as_str().to_owned()).or_default() += 1;
        }
        for upsertion in &request.upsertions {
            *users.entry(upsertion.name.as_str().to_owned()).or_default() += 1;
        }
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
            .unwrap_or(false)
        {
            return alter_response(
                users
                    .into_keys()
                    .map(|user| {
                        (
                            user,
                            CLUSTER_AUTHORIZATION_FAILED,
                            Some("cluster authorization failed".to_owned()),
                        )
                    })
                    .collect(),
            );
        }

        let mut errors = BTreeMap::<String, (i16, String)>::new();
        for (user, count) in &users {
            if *count > 1 {
                errors.insert(
                    user.clone(),
                    (
                        DUPLICATE_RESOURCE,
                        "a user credential cannot be altered twice".to_owned(),
                    ),
                );
            }
        }
        let mut alterations = Vec::new();
        for deletion in request.deletions {
            let user = deletion.name.as_str().to_owned();
            if errors.contains_key(&user) {
                continue;
            }
            match validate_user_and_mechanism(&user, deletion.mechanism) {
                Ok(_) => alterations.push(ScramCredentialAlteration::Delete {
                    user,
                    mechanism: deletion.mechanism,
                }),
                Err(error) => {
                    errors.insert(user, error);
                }
            }
        }
        for upsertion in request.upsertions {
            let user = upsertion.name.as_str().to_owned();
            if errors.contains_key(&user) {
                continue;
            }
            let mechanism = match validate_user_and_mechanism(&user, upsertion.mechanism) {
                Ok(mechanism) => mechanism,
                Err(error) => {
                    errors.insert(user, error);
                    continue;
                }
            };
            if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&upsertion.iterations) {
                errors.insert(
                    user,
                    (
                        UNACCEPTABLE_CREDENTIAL,
                        format!(
                            "SCRAM iterations must be between {MIN_ITERATIONS} and {MAX_ITERATIONS}"
                        ),
                    ),
                );
                continue;
            }
            alterations.push(ScramCredentialAlteration::Upsert(
                credential_from_salted_password(
                    user,
                    mechanism,
                    upsertion.iterations,
                    upsertion.salt.to_vec(),
                    &upsertion.salted_password,
                ),
            ));
        }

        let alteration_users = alterations
            .iter()
            .map(|alteration| alteration.user().to_owned())
            .collect::<Vec<_>>();
        match self.metadata.alter_scram_credentials(alterations).await {
            Ok(missing) => {
                for user in missing {
                    errors.insert(
                        user,
                        (
                            RESOURCE_NOT_FOUND,
                            "attempt to delete a SCRAM credential that does not exist".to_owned(),
                        ),
                    );
                }
            }
            Err(error) => {
                for user in alteration_users {
                    errors.insert(user, (UNKNOWN_SERVER_ERROR, error.to_string()));
                }
            }
        }
        alter_response(
            users
                .into_keys()
                .map(|user| match errors.remove(&user) {
                    Some((code, error)) => (user, code, Some(error)),
                    None => (user, NO_ERROR, None),
                })
                .collect(),
        )
    }
}

fn validate_user_and_mechanism(user: &str, mechanism: i8) -> Result<ScramMechanism, (i16, String)> {
    if user.is_empty() {
        return Err((
            UNACCEPTABLE_CREDENTIAL,
            "SCRAM username must not be empty".to_owned(),
        ));
    }
    ScramMechanism::from_code(mechanism).ok_or_else(|| {
        (
            UNSUPPORTED_SASL_MECHANISM,
            "unknown SCRAM mechanism".to_owned(),
        )
    })
}

fn describe_result(
    user: String,
    error_code: i16,
    error: Option<&str>,
    infos: Vec<CredentialInfo>,
) -> DescribeUserScramCredentialsResult {
    DescribeUserScramCredentialsResult::default()
        .with_user(message(&user))
        .with_error_code(error_code)
        .with_error_message(error.map(message))
        .with_credential_infos(infos)
}

fn alter_response(
    results: Vec<(String, i16, Option<String>)>,
) -> AlterUserScramCredentialsResponse {
    AlterUserScramCredentialsResponse::default().with_results(
        results
            .into_iter()
            .map(|(user, error_code, error)| {
                AlterUserScramCredentialsResult::default()
                    .with_user(message(&user))
                    .with_error_code(error_code)
                    .with_error_message(error.as_deref().map(message))
            })
            .collect(),
    )
}

fn message(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
