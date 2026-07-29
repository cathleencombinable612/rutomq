use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub user: String,
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl fmt::Debug for ScramCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramCredential")
            .field("user", &self.user)
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"[hidden]")
            .field("stored_key", &"[hidden]")
            .field("server_key", &"[hidden]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScramCredentialAlteration {
    Upsert(ScramCredential),
    Delete { user: String, mechanism: i8 },
}

impl ScramCredentialAlteration {
    pub fn user(&self) -> &str {
        match self {
            Self::Upsert(credential) => &credential.user,
            Self::Delete { user, .. } => user,
        }
    }
}
