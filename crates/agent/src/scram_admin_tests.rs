use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    DUPLICATE_RESOURCE, RESOURCE_NOT_FOUND, UNACCEPTABLE_CREDENTIAL, UNSUPPORTED_SASL_MECHANISM,
};
use kafka_protocol::messages::alter_user_scram_credentials_request::{
    ScramCredentialDeletion, ScramCredentialUpsertion,
};
use kafka_protocol::messages::describe_user_scram_credentials_request::UserName;
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;

fn user(value: &str) -> UserName {
    UserName::default().with_name(StrBytes::from_string(value.to_owned()))
}

fn upsertion(user: &str, mechanism: i8, iterations: i32) -> ScramCredentialUpsertion {
    let salt = b"rutomq-admin-test";
    let mut salted_password = [0u8; 32];
    pbkdf2_hmac::<Sha256>(
        b"dynamic-secret",
        salt,
        u32::try_from(iterations.max(1)).unwrap(),
        &mut salted_password,
    );
    ScramCredentialUpsertion::default()
        .with_name(StrBytes::from_string(user.to_owned()))
        .with_mechanism(mechanism)
        .with_iterations(iterations)
        .with_salt(Bytes::from_static(salt))
        .with_salted_password(Bytes::copy_from_slice(&salted_password))
}

fn deletion(user: &str, mechanism: i8) -> ScramCredentialDeletion {
    ScramCredentialDeletion::default()
        .with_name(StrBytes::from_string(user.to_owned()))
        .with_mechanism(mechanism)
}

#[tokio::test]
async fn scram_credentials_round_trip_and_delete() {
    let broker = broker();
    let request = AlterUserScramCredentialsRequest::default()
        .with_upsertions(vec![upsertion("alice", 1, 4096)]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterUserScramCredentials,
            0,
            110,
            &request,
        ))
        .await
        .unwrap();
    let response: AlterUserScramCredentialsResponse =
        decode_response(ApiKey::AlterUserScramCredentials, 0, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);

    let request =
        DescribeUserScramCredentialsRequest::default().with_users(Some(vec![user("alice")]));
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeUserScramCredentials,
            0,
            111,
            &request,
        ))
        .await
        .unwrap();
    let response: DescribeUserScramCredentialsResponse =
        decode_response(ApiKey::DescribeUserScramCredentials, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(response.results[0].credential_infos[0].mechanism, 1);
    assert_eq!(response.results[0].credential_infos[0].iterations, 4096);

    let request =
        AlterUserScramCredentialsRequest::default().with_deletions(vec![deletion("alice", 1)]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterUserScramCredentials,
            0,
            112,
            &request,
        ))
        .await
        .unwrap();
    let response: AlterUserScramCredentialsResponse =
        decode_response(ApiKey::AlterUserScramCredentials, 0, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);

    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterUserScramCredentials,
            0,
            113,
            &request,
        ))
        .await
        .unwrap();
    let response: AlterUserScramCredentialsResponse =
        decode_response(ApiKey::AlterUserScramCredentials, 0, response);
    assert_eq!(response.results[0].error_code, RESOURCE_NOT_FOUND);
}

#[tokio::test]
async fn scram_admin_validates_duplicates_mechanisms_and_iterations() {
    let broker = broker();
    let request = AlterUserScramCredentialsRequest::default()
        .with_deletions(vec![deletion("duplicate", 1), deletion("missing", 1)])
        .with_upsertions(vec![
            upsertion("duplicate", 1, 4096),
            upsertion("mechanism", 0, 4096),
            upsertion("iterations", 1, 4095),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::AlterUserScramCredentials,
            0,
            120,
            &request,
        ))
        .await
        .unwrap();
    let response: AlterUserScramCredentialsResponse =
        decode_response(ApiKey::AlterUserScramCredentials, 0, response);
    let errors = response
        .results
        .iter()
        .map(|result| (result.user.as_str(), result.error_code))
        .collect::<HashMap<_, _>>();
    assert_eq!(errors["duplicate"], DUPLICATE_RESOURCE);
    assert_eq!(errors["missing"], RESOURCE_NOT_FOUND);
    assert_eq!(errors["mechanism"], UNSUPPORTED_SASL_MECHANISM);
    assert_eq!(errors["iterations"], UNACCEPTABLE_CREDENTIAL);

    let request = DescribeUserScramCredentialsRequest::default()
        .with_users(Some(vec![user("duplicate"), user("duplicate")]));
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeUserScramCredentials,
            0,
            121,
            &request,
        ))
        .await
        .unwrap();
    let response: DescribeUserScramCredentialsResponse =
        decode_response(ApiKey::DescribeUserScramCredentials, 0, response);
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].error_code, DUPLICATE_RESOURCE);
}
