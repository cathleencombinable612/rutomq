use anyhow::{Result, bail};
use rutomq_control::{
    CURRENT_OBJECT_FORMAT_VERSION, LEGACY_OBJECT_FORMAT_VERSION, SpanChecksum, StoredSpan,
};
use sha2::{Digest, Sha256};

pub(crate) fn checksum(bytes: &[u8]) -> SpanChecksum {
    Sha256::digest(bytes).into()
}

pub(crate) fn verify(span: &StoredSpan, bytes: &[u8]) -> Result<()> {
    match (span.integrity.format_version, span.integrity.checksum) {
        (LEGACY_OBJECT_FORMAT_VERSION, None) => Ok(()),
        (CURRENT_OBJECT_FORMAT_VERSION, Some(expected)) if checksum(bytes) == expected => Ok(()),
        (CURRENT_OBJECT_FORMAT_VERSION, Some(_)) => bail!(
            "object span checksum mismatch for {}:{}..{}",
            span.object_key,
            span.byte_start,
            span.byte_end
        ),
        (version, _) => bail!("unsupported object span format version {version}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_control::{PartitionKey, SpanIntegrity};

    fn span(integrity: SpanIntegrity) -> StoredSpan {
        StoredSpan {
            partition: PartitionKey::new("topic", 0),
            object_key: "data/object".to_owned(),
            byte_start: 0,
            byte_end: 3,
            base_offset: 0,
            last_offset: 0,
            record_count: 1,
            timestamp_ms: 0,
            integrity,
            producer: None,
            transaction_id: None,
            offsets_preserved: false,
        }
    }

    #[test]
    fn verifies_current_and_allows_legacy_spans() {
        let bytes = b"abc";
        assert!(verify(&span(SpanIntegrity::current(checksum(bytes))), bytes).is_ok());
        assert!(verify(&span(SpanIntegrity::legacy()), b"legacy").is_ok());
        assert!(verify(&span(SpanIntegrity::current(checksum(bytes))), b"abd").is_err());
    }
}
