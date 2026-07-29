use super::*;

#[test]
fn authenticates_both_scram_mechanisms() {
    for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
        let users = HashMap::from([("alice".to_owned(), "secret".to_owned())]);
        let first = b"n,,n=alice,r=client-nonce";
        let (session, server_first) =
            ScramSession::start(mechanism, first, &users, 4_096, None).unwrap();
        let final_message = client_final(
            mechanism,
            "secret",
            "n=alice,r=client-nonce",
            std::str::from_utf8(&server_first).unwrap(),
        );
        let (principal, server_final) = session.finish(final_message.as_bytes()).unwrap();
        assert_eq!(principal, "alice");
        assert!(server_final.starts_with(b"v="));
    }
}

#[test]
fn rejects_wrong_password_without_exposing_unknown_users() {
    let users = HashMap::from([("alice".to_owned(), "secret".to_owned())]);
    for username in ["alice", "missing"] {
        let bare = format!("n={username},r=client-nonce");
        let first = format!("{GS2_HEADER}{bare}");
        let (session, server_first) = ScramSession::start(
            ScramMechanism::Sha256,
            first.as_bytes(),
            &users,
            4_096,
            None,
        )
        .unwrap();
        let final_message = client_final(
            ScramMechanism::Sha256,
            "wrong",
            &bare,
            std::str::from_utf8(&server_first).unwrap(),
        );
        assert!(session.finish(final_message.as_bytes()).is_err());
    }
}

pub(crate) fn client_final(
    mechanism: ScramMechanism,
    password: &str,
    client_first_bare: &str,
    server_first: &str,
) -> String {
    let attributes = parse_attributes(server_first).unwrap();
    let nonce = required(&attributes, "r").unwrap();
    let salt = STANDARD
        .decode(required(&attributes, "s").unwrap())
        .unwrap();
    let iterations = required(&attributes, "i").unwrap().parse::<u32>().unwrap();
    let without_proof = format!("c={CHANNEL_BINDING},r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let proof = match mechanism {
        ScramMechanism::Sha256 => {
            let mut salted = [0u8; 32];
            pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);
            let client_key = hmac_sha256(&salted, b"Client Key");
            let stored_key = Sha256::digest(client_key);
            let signature = hmac_sha256(stored_key.as_slice(), auth_message.as_bytes());
            xor(&client_key, &signature)
        }
        ScramMechanism::Sha512 => {
            let mut salted = [0u8; 64];
            pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, iterations, &mut salted);
            let client_key = hmac_sha512(&salted, b"Client Key");
            let stored_key = Sha512::digest(client_key);
            let signature = hmac_sha512(stored_key.as_slice(), auth_message.as_bytes());
            xor(&client_key, &signature)
        }
    };
    format!("{without_proof},p={}", STANDARD.encode(proof))
}

#[test]
fn recognizes_kafka_delegation_token_extension() {
    let (username, token_authenticated) =
        ScramSession::identity(b"n,,n=token-id,r=client-nonce,tokenauth=true").unwrap();
    assert_eq!(username, "token-id");
    assert!(token_authenticated);

    let (_, token_authenticated) =
        ScramSession::identity(b"n,,n=alice,r=client-nonce,tokenauth=false").unwrap();
    assert!(!token_authenticated);
}
