use anyhow::{Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rutomq_control::ScramCredential;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use subtle::ConstantTimeEq;

const GS2_HEADER: &str = "n,,";
const CHANNEL_BINDING: &str = "biws";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    pub const NAMES: [&'static str; 2] = ["SCRAM-SHA-256", "SCRAM-SHA-512"];

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }

    pub fn code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
        }
    }

    pub fn from_code(value: i8) -> Option<Self> {
        match value {
            1 => Some(Self::Sha256),
            2 => Some(Self::Sha512),
            _ => None,
        }
    }
}

pub struct ScramSession {
    mechanism: ScramMechanism,
    username: String,
    known_user: bool,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    nonce: String,
    client_first_bare: String,
    server_first: String,
}

impl ScramSession {
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    pub fn identity(input: &[u8]) -> Result<(String, bool)> {
        parse_client_first(input)
            .map(|(username, _, _, token_authenticated)| (username, token_authenticated))
    }

    pub fn start(
        mechanism: ScramMechanism,
        input: &[u8],
        users: &HashMap<String, String>,
        iterations: u32,
        credential: Option<&ScramCredential>,
    ) -> Result<(Self, Bytes)> {
        let (username, client_nonce, client_first_bare, _) = parse_client_first(input)?;
        let (known_user, iterations, salt, stored_key, server_key) =
            if let Some(credential) = credential {
                if credential.user != username || credential.mechanism != mechanism.code() {
                    bail!("SCRAM credential does not match the exchange");
                }
                (
                    true,
                    u32::try_from(credential.iterations)
                        .map_err(|_| anyhow!("invalid SCRAM iteration count"))?,
                    credential.salt.clone(),
                    credential.stored_key.clone(),
                    credential.server_key.clone(),
                )
            } else {
                let known_user = users.contains_key(&username);
                let password = users.get(&username).cloned().unwrap_or_else(fake_password);
                let salt = rand::random::<[u8; 16]>().to_vec();
                let salted_password =
                    derive_salted_password(mechanism, password.as_bytes(), &salt, iterations);
                let (stored_key, server_key) = derive_credential_keys(mechanism, &salted_password);
                (known_user, iterations, salt, stored_key, server_key)
            };
        let server_nonce = STANDARD_NO_PAD.encode(rand::random::<[u8; 18]>());
        let nonce = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={nonce},s={},i={iterations}", STANDARD.encode(&salt));
        let response = Bytes::copy_from_slice(server_first.as_bytes());
        Ok((
            Self {
                mechanism,
                username,
                known_user,
                stored_key,
                server_key,
                nonce,
                client_first_bare,
                server_first,
            },
            response,
        ))
    }

    pub fn finish(self, input: &[u8]) -> Result<(String, Bytes)> {
        let message =
            std::str::from_utf8(input).map_err(|_| anyhow!("SCRAM message is not UTF-8"))?;
        let proof_marker = message
            .rfind(",p=")
            .ok_or_else(|| anyhow!("SCRAM client proof is missing"))?;
        let (without_proof, proof) = message.split_at(proof_marker);
        let proof = proof
            .strip_prefix(",p=")
            .ok_or_else(|| anyhow!("SCRAM client proof is malformed"))?;
        if proof.contains(',') {
            bail!("SCRAM client proof must be the final attribute");
        }
        let attributes = parse_attributes(message)?;
        if required(&attributes, "c")? != CHANNEL_BINDING
            || required(&attributes, "r")? != self.nonce
        {
            bail!("SCRAM channel binding or nonce does not match");
        }
        let proof = STANDARD
            .decode(proof)
            .map_err(|_| anyhow!("SCRAM client proof is not valid base64"))?;
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let server_signature = match self.mechanism {
            ScramMechanism::Sha256 => verify_sha256_proof(
                &self.stored_key,
                &self.server_key,
                auth_message.as_bytes(),
                &proof,
            )?,
            ScramMechanism::Sha512 => verify_sha512_proof(
                &self.stored_key,
                &self.server_key,
                auth_message.as_bytes(),
                &proof,
            )?,
        };
        if !self.known_user {
            bail!("unknown SCRAM user");
        }
        let response = Bytes::from(format!("v={}", STANDARD.encode(server_signature)));
        Ok((self.username, response))
    }
}

pub(crate) fn credential_from_salted_password(
    user: String,
    mechanism: ScramMechanism,
    iterations: i32,
    salt: Vec<u8>,
    salted_password: &[u8],
) -> ScramCredential {
    let (stored_key, server_key) = derive_credential_keys(mechanism, salted_password);
    ScramCredential {
        user,
        mechanism: mechanism.code(),
        iterations,
        salt,
        stored_key,
        server_key,
    }
}

pub(crate) fn credential_from_password(
    user: String,
    mechanism: ScramMechanism,
    iterations: u32,
    password: &[u8],
) -> ScramCredential {
    let salt = rand::random::<[u8; 16]>().to_vec();
    let salted_password = derive_salted_password(mechanism, password, &salt, iterations);
    credential_from_salted_password(
        user,
        mechanism,
        i32::try_from(iterations).expect("SCRAM iterations fit i32"),
        salt,
        &salted_password,
    )
}

fn derive_salted_password(
    mechanism: ScramMechanism,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Vec<u8> {
    match mechanism {
        ScramMechanism::Sha256 => {
            let mut salted = vec![0u8; 32];
            pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut salted);
            salted
        }
        ScramMechanism::Sha512 => {
            let mut salted = vec![0u8; 64];
            pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut salted);
            salted
        }
    }
}

fn derive_credential_keys(mechanism: ScramMechanism, salted_password: &[u8]) -> (Vec<u8>, Vec<u8>) {
    match mechanism {
        ScramMechanism::Sha256 => {
            let client_key = hmac_sha256(salted_password, b"Client Key");
            (
                Sha256::digest(client_key).to_vec(),
                hmac_sha256(salted_password, b"Server Key").to_vec(),
            )
        }
        ScramMechanism::Sha512 => {
            let client_key = hmac_sha512(salted_password, b"Client Key");
            (
                Sha512::digest(client_key).to_vec(),
                hmac_sha512(salted_password, b"Server Key").to_vec(),
            )
        }
    }
}

fn verify_sha256_proof(
    stored_key: &[u8],
    server_key: &[u8],
    auth_message: &[u8],
    proof: &[u8],
) -> Result<Vec<u8>> {
    if proof.len() != 32 || stored_key.len() != 32 || server_key.len() != 32 {
        bail!("SCRAM-SHA-256 proof has an invalid length");
    }
    let signature = hmac_sha256(stored_key, auth_message);
    let recovered = xor(proof, &signature);
    let recovered_stored = Sha256::digest(&recovered);
    if !bool::from(recovered_stored.as_slice().ct_eq(stored_key)) {
        bail!("SCRAM client proof does not match");
    }
    Ok(hmac_sha256(server_key, auth_message).to_vec())
}

fn verify_sha512_proof(
    stored_key: &[u8],
    server_key: &[u8],
    auth_message: &[u8],
    proof: &[u8],
) -> Result<Vec<u8>> {
    if proof.len() != 64 || stored_key.len() != 64 || server_key.len() != 64 {
        bail!("SCRAM-SHA-512 proof has an invalid length");
    }
    let signature = hmac_sha512(stored_key, auth_message);
    let recovered = xor(proof, &signature);
    let recovered_stored = Sha512::digest(&recovered);
    if !bool::from(recovered_stored.as_slice().ct_eq(stored_key)) {
        bail!("SCRAM client proof does not match");
    }
    Ok(hmac_sha512(server_key, auth_message).to_vec())
}

fn hmac_sha256(key: &[u8], input: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(input);
    mac.finalize().into_bytes().into()
}

fn hmac_sha512(key: &[u8], input: &[u8]) -> [u8; 64] {
    let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(input);
    mac.finalize().into_bytes().into()
}

fn xor(left: &[u8], right: &[u8]) -> Vec<u8> {
    left.iter()
        .zip(right)
        .map(|(left, right)| left ^ right)
        .collect()
}

fn parse_attributes(message: &str) -> Result<HashMap<&str, &str>> {
    let mut attributes = HashMap::new();
    for part in message.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            bail!("malformed SCRAM attribute");
        };
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            bail!("malformed SCRAM attribute");
        }
        if attributes.insert(key, value).is_some() {
            bail!("duplicate SCRAM attribute {key}");
        }
    }
    Ok(attributes)
}

fn parse_client_first(input: &[u8]) -> Result<(String, String, String, bool)> {
    let message = std::str::from_utf8(input).map_err(|_| anyhow!("SCRAM message is not UTF-8"))?;
    let client_first_bare = message
        .strip_prefix(GS2_HEADER)
        .ok_or_else(|| anyhow!("SCRAM channel binding or authorization ID is unsupported"))?;
    let attributes = parse_attributes(client_first_bare)?;
    if attributes.contains_key("m") {
        bail!("SCRAM mandatory extensions are unsupported");
    }
    let username = decode_username(required(&attributes, "n")?)?;
    let client_nonce = required(&attributes, "r")?;
    let token_authenticated = attributes
        .get("tokenauth")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    if username.is_empty() || client_nonce.len() < 8 || client_nonce.contains(',') {
        bail!("invalid SCRAM username or nonce");
    }
    Ok((
        username,
        client_nonce.to_owned(),
        client_first_bare.to_owned(),
        token_authenticated,
    ))
}

fn required<'a>(attributes: &'a HashMap<&str, &'a str>, key: &str) -> Result<&'a str> {
    attributes
        .get(key)
        .copied()
        .ok_or_else(|| anyhow!("SCRAM attribute {key} is missing"))
}

fn decode_username(username: &str) -> Result<String> {
    let mut decoded = String::with_capacity(username.len());
    let mut chars = username.chars();
    while let Some(character) = chars.next() {
        if character != '=' {
            decoded.push(character);
            continue;
        }
        match (chars.next(), chars.next()) {
            (Some('2'), Some('C')) => decoded.push(','),
            (Some('3'), Some('D')) => decoded.push('='),
            _ => bail!("invalid SCRAM username escape"),
        }
    }
    Ok(decoded)
}

fn fake_password() -> String {
    STANDARD_NO_PAD.encode(rand::random::<[u8; 32]>())
}

#[cfg(test)]
#[path = "scram_tests.rs"]
pub(crate) mod tests;
