use crate::{ControlError, ScramCredential, ScramCredentialAlteration};
use sqlx::{PgPool, Row};
use std::collections::HashSet;

pub(crate) async fn describe(
    pool: &PgPool,
    users: Option<&[String]>,
) -> Result<Vec<ScramCredential>, ControlError> {
    let rows = match users {
        Some(users) if !users.is_empty() => {
            sqlx::query(
                "SELECT username, mechanism, iterations, salt, stored_key, server_key
                 FROM scram_credentials
                 WHERE username = ANY($1)
                 ORDER BY username, mechanism",
            )
            .bind(users.to_vec())
            .fetch_all(pool)
            .await?
        }
        _ => {
            sqlx::query(
                "SELECT username, mechanism, iterations, salt, stored_key, server_key
                 FROM scram_credentials
                 ORDER BY username, mechanism",
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows
        .into_iter()
        .map(|row| {
            let mechanism: i16 = row.get("mechanism");
            ScramCredential {
                user: row.get("username"),
                mechanism: mechanism as i8,
                iterations: row.get("iterations"),
                salt: row.get("salt"),
                stored_key: row.get("stored_key"),
                server_key: row.get("server_key"),
            }
        })
        .collect())
}

pub(crate) async fn alter(
    pool: &PgPool,
    alterations: Vec<ScramCredentialAlteration>,
) -> Result<HashSet<String>, ControlError> {
    let mut transaction = pool.begin().await?;
    let mut missing = HashSet::new();
    for alteration in alterations {
        match alteration {
            ScramCredentialAlteration::Upsert(credential) => {
                sqlx::query(
                    "INSERT INTO scram_credentials (
                         username, mechanism, iterations, salt, stored_key, server_key
                     )
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (username, mechanism) DO UPDATE SET
                         iterations = EXCLUDED.iterations,
                         salt = EXCLUDED.salt,
                         stored_key = EXCLUDED.stored_key,
                         server_key = EXCLUDED.server_key",
                )
                .bind(credential.user)
                .bind(i16::from(credential.mechanism))
                .bind(credential.iterations)
                .bind(credential.salt)
                .bind(credential.stored_key)
                .bind(credential.server_key)
                .execute(&mut *transaction)
                .await?;
            }
            ScramCredentialAlteration::Delete { user, mechanism } => {
                let result = sqlx::query(
                    "DELETE FROM scram_credentials
                     WHERE username = $1 AND mechanism = $2",
                )
                .bind(&user)
                .bind(i16::from(mechanism))
                .execute(&mut *transaction)
                .await?;
                if result.rows_affected() == 0 {
                    missing.insert(user);
                }
            }
        }
    }
    transaction.commit().await?;
    Ok(missing)
}
