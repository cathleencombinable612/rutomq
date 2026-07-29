use crate::ControlError;
use serde::{Deserialize, Serialize};
use sqlx::{Row, postgres::PgRow};

pub const LEGACY_OBJECT_FORMAT_VERSION: i16 = 0;
pub const CURRENT_OBJECT_FORMAT_VERSION: i16 = 1;
pub type SpanChecksum = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanIntegrity {
    pub format_version: i16,
    pub checksum: Option<SpanChecksum>,
}

impl SpanIntegrity {
    pub const fn legacy() -> Self {
        Self {
            format_version: LEGACY_OBJECT_FORMAT_VERSION,
            checksum: None,
        }
    }

    pub const fn current(checksum: SpanChecksum) -> Self {
        Self {
            format_version: CURRENT_OBJECT_FORMAT_VERSION,
            checksum: Some(checksum),
        }
    }

    pub const fn from_checksum(checksum: Option<SpanChecksum>) -> Self {
        match checksum {
            Some(checksum) => Self::current(checksum),
            None => Self::legacy(),
        }
    }
}

pub(crate) fn from_row(row: &PgRow) -> Result<SpanIntegrity, ControlError> {
    let format_version = row.get("format_version");
    let checksum = row.get::<Option<Vec<u8>>, _>("checksum");
    match (format_version, checksum) {
        (LEGACY_OBJECT_FORMAT_VERSION, None) => Ok(SpanIntegrity::legacy()),
        (version, Some(checksum)) if version > 0 => {
            let checksum = checksum.try_into().map_err(|value: Vec<u8>| {
                ControlError::InvalidRequest(format!(
                    "object span checksum has {} bytes instead of 32",
                    value.len()
                ))
            })?;
            Ok(SpanIntegrity {
                format_version: version,
                checksum: Some(checksum),
            })
        }
        (version, _) => Err(ControlError::InvalidRequest(format!(
            "object span has invalid integrity metadata for format {version}"
        ))),
    }
}
