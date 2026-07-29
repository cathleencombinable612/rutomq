use crate::kafka_error::{
    CORRUPT_MESSAGE, INVALID_RECORD, INVALID_TIMESTAMP, MESSAGE_TOO_LARGE,
    UNSUPPORTED_COMPRESSION_TYPE,
};
use crate::records::validate_client_batch;
use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;
use kafka_protocol::compression::{Compressor, Snappy};
use kafka_protocol::records::{
    Compression, NO_TIMESTAMP, RecordBatchDecoder, RecordBatchEncoder, RecordCrcError,
    RecordEncodeOptions, RecordSet, TimestampType,
};
use lz4::{BlockMode, EncoderBuilder};
use rutomq_control::TopicConfig;
use std::io::{Cursor, Write};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct RecordAdmissionError {
    pub code: i16,
    message: String,
}

impl RecordAdmissionError {
    fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[cfg(test)]
fn admit_records(
    records: &Bytes,
    config: &TopicConfig,
    now_ms: i64,
) -> Result<Bytes, RecordAdmissionError> {
    admit_records_for_version(records, config, now_ms, 13)
}

pub fn admit_records_for_version(
    records: &Bytes,
    config: &TopicConfig,
    now_ms: i64,
    produce_version: i16,
) -> Result<Bytes, RecordAdmissionError> {
    let mut sets = decode(records)?;
    if produce_version < 7 && sets.iter().any(|set| set.compression == Compression::Zstd) {
        return Err(RecordAdmissionError::new(
            UNSUPPORTED_COMPRESSION_TYPE,
            format!("Produce v{produce_version} does not support Zstandard record batches"),
        ));
    }
    validate_client_batch(records, &sets)
        .map_err(|error| RecordAdmissionError::new(INVALID_RECORD, error.to_string()))?;
    if sets.iter().all(|set| set.records.is_empty()) {
        return Err(RecordAdmissionError::new(
            INVALID_RECORD,
            "produce request contains no records",
        ));
    }
    validate_compacted_keys(&sets, config)?;
    let rewrite_log_append_time = config.message_timestamp_type == "LogAppendTime";
    let rewrite_create_time = validate_timestamps(&sets, config, now_ms)?;
    if rewrite_log_append_time {
        for set in &mut sets {
            for record in &mut set.records {
                record.timestamp_type = TimestampType::LogAppend;
                record.timestamp = now_ms;
            }
        }
    } else if rewrite_create_time {
        for set in &mut sets {
            let target_compression =
                configured_compression(&config.compression_type, set.compression)?;
            let rebuild_timestamps = target_compression != set.compression;
            for record in &mut set.records {
                if record.timestamp_type == TimestampType::LogAppend {
                    record.timestamp_type = TimestampType::Creation;
                    if rebuild_timestamps {
                        record.timestamp = set.max_timestamp;
                    }
                }
            }
        }
    }

    if !rewrite_log_append_time && !rewrite_create_time && config.compression_type == "producer" {
        validate_batch_sizes(records, config.max_message_bytes)?;
        return Ok(records.clone());
    }

    let rewritten = encode_sets(&sets, config)?;
    validate_batch_sizes(&rewritten, config.max_message_bytes)?;
    Ok(rewritten)
}

fn validate_compacted_keys(
    sets: &[RecordSet],
    config: &TopicConfig,
) -> Result<(), RecordAdmissionError> {
    let compacted = matches!(
        config.cleanup_policy.as_str(),
        "compact" | "compact,delete" | "delete,compact"
    );
    if !compacted {
        return Ok(());
    }
    if let Some(index) = sets
        .iter()
        .flat_map(|set| &set.records)
        .position(|record| record.key.is_none())
    {
        return Err(RecordAdmissionError::new(
            INVALID_RECORD,
            format!("compacted topic cannot accept record {index} without a key"),
        ));
    }
    Ok(())
}

fn decode(records: &Bytes) -> Result<Vec<RecordSet>, RecordAdmissionError> {
    let mut input = records.clone();
    RecordBatchDecoder::decode_all(&mut input).map_err(|error| {
        let code = if error.downcast_ref::<RecordCrcError>().is_some() {
            CORRUPT_MESSAGE
        } else {
            INVALID_RECORD
        };
        RecordAdmissionError::new(code, format!("invalid Kafka record batch: {error}"))
    })
}

fn encode_sets(sets: &[RecordSet], config: &TopicConfig) -> Result<Bytes, RecordAdmissionError> {
    let mut output = BytesMut::new();
    for set in sets {
        let compression = configured_compression(&config.compression_type, set.compression)?;
        let options = RecordEncodeOptions {
            version: set.version,
            compression,
        };
        let result = if config.compression_type == "producer" {
            RecordBatchEncoder::encode(&mut output, set.records.iter(), &options)
        } else {
            RecordBatchEncoder::encode_with_custom_compression(
                &mut output,
                set.records.iter(),
                &options,
                Some(
                    |records: &mut BytesMut, output: &mut BytesMut, compression: Compression| {
                        compress_records(records, output, compression, config)
                    },
                ),
            )
        };
        result.map_err(|error| {
            RecordAdmissionError::new(
                INVALID_RECORD,
                format!("failed to rewrite Kafka record batch: {error}"),
            )
        })?;
    }
    Ok(output.freeze())
}

fn compress_records(
    records: &mut BytesMut,
    output: &mut BytesMut,
    compression: Compression,
    config: &TopicConfig,
) -> anyhow::Result<()> {
    match compression {
        Compression::None => output.extend_from_slice(records),
        Compression::Snappy => {
            Snappy::compress(output, |compressed| {
                compressed.extend_from_slice(records);
                Ok(())
            })?;
        }
        Compression::Gzip => {
            let level = if config.compression_gzip_level == -1 {
                GzipLevel::default()
            } else {
                GzipLevel::new(config.compression_gzip_level as u32)
            };
            let mut encoder = GzEncoder::new(output.writer(), level);
            encoder
                .write_all(records)
                .context("failed to compress gzip record batch")?;
            encoder
                .finish()
                .context("failed to finish gzip record batch")?;
        }
        Compression::Lz4 => {
            let mut encoder = EncoderBuilder::new()
                .level(config.compression_lz4_level as u32)
                .block_mode(BlockMode::Independent)
                .build(output.writer())
                .context("failed to create LZ4 record-batch encoder")?;
            encoder
                .write_all(records)
                .context("failed to compress LZ4 record batch")?;
            encoder
                .finish()
                .1
                .context("failed to finish LZ4 record batch")?;
        }
        Compression::Zstd => {
            zstd::stream::copy_encode(
                Cursor::new(records.as_ref()),
                output.writer(),
                config.compression_zstd_level,
            )
            .context("failed to compress Zstandard record batch")?;
        }
    }
    Ok(())
}

fn configured_compression(
    compression_type: &str,
    producer_compression: Compression,
) -> Result<Compression, RecordAdmissionError> {
    match compression_type {
        "producer" => Ok(producer_compression),
        "uncompressed" => Ok(Compression::None),
        "gzip" => Ok(Compression::Gzip),
        "snappy" => Ok(Compression::Snappy),
        "lz4" => Ok(Compression::Lz4),
        "zstd" => Ok(Compression::Zstd),
        other => Err(RecordAdmissionError::new(
            INVALID_RECORD,
            format!("unsupported topic compression.type {other}"),
        )),
    }
}

fn validate_batch_sizes(
    records: &Bytes,
    max_message_bytes: i32,
) -> Result<(), RecordAdmissionError> {
    let mut input = records.clone();
    while input.has_remaining() {
        if input.remaining() < 12 {
            return Err(RecordAdmissionError::new(
                INVALID_RECORD,
                "record batch is shorter than its size header",
            ));
        }
        let batch_length = i32::from_be_bytes(input[8..12].try_into().expect("four-byte slice"));
        if batch_length < 0 {
            return Err(RecordAdmissionError::new(
                INVALID_RECORD,
                format!("record batch has negative size {batch_length}"),
            ));
        }
        let batch_size = 12usize.saturating_add(batch_length as usize);
        if batch_size > input.remaining() {
            return Err(RecordAdmissionError::new(
                INVALID_RECORD,
                "record batch size exceeds the supplied records",
            ));
        }
        if batch_size > max_message_bytes as usize {
            return Err(RecordAdmissionError::new(
                MESSAGE_TOO_LARGE,
                format!(
                    "record batch is {batch_size} bytes, exceeding max.message.bytes={max_message_bytes}"
                ),
            ));
        }
        input.advance(batch_size);
    }
    Ok(())
}

fn validate_timestamps(
    sets: &[RecordSet],
    config: &TopicConfig,
    now_ms: i64,
) -> Result<bool, RecordAdmissionError> {
    let topic_uses_log_append_time = config.message_timestamp_type == "LogAppendTime";
    let mut rewrite_create_time = false;
    for set in sets {
        let client_uses_log_append_time = set
            .records
            .first()
            .is_some_and(|record| record.timestamp_type == TimestampType::LogAppend);
        if topic_uses_log_append_time || client_uses_log_append_time {
            if client_uses_log_append_time && topic_uses_log_append_time {
                return Err(RecordAdmissionError::new(
                    INVALID_TIMESTAMP,
                    "producer must not set timestamp type to LogAppendTime",
                ));
            }
            if client_uses_log_append_time {
                if set.max_timestamp == NO_TIMESTAMP {
                    return Err(RecordAdmissionError::new(
                        INVALID_TIMESTAMP,
                        "producer must not set timestamp type to LogAppendTime",
                    ));
                }
                validate_timestamp(
                    set.max_timestamp,
                    now_ms,
                    config.message_timestamp_before_max_ms,
                    config.message_timestamp_after_max_ms,
                )?;
                rewrite_create_time = true;
            }
            continue;
        }
        for record in &set.records {
            if record.timestamp != NO_TIMESTAMP {
                validate_timestamp(
                    record.timestamp,
                    now_ms,
                    config.message_timestamp_before_max_ms,
                    config.message_timestamp_after_max_ms,
                )?;
            }
        }
    }
    Ok(rewrite_create_time)
}

fn validate_timestamp(
    timestamp: i64,
    now_ms: i64,
    before_max_ms: i64,
    after_max_ms: i64,
) -> Result<(), RecordAdmissionError> {
    let timestamp_diff = now_ms.wrapping_sub(timestamp);
    if timestamp_diff > before_max_ms || timestamp_diff.wrapping_neg() > after_max_ms {
        let earliest = now_ms.wrapping_sub(before_max_ms);
        let latest = now_ms.wrapping_add(after_max_ms);
        return Err(RecordAdmissionError::new(
            INVALID_TIMESTAMP,
            format!("record timestamp {timestamp} is outside [{earliest}, {latest}]"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::{Compression, Record};

    fn encoded(timestamp: i64, compression: Compression, value: Bytes) -> Bytes {
        encoded_with_timestamp_type(timestamp, TimestampType::Creation, compression, value)
    }

    fn encoded_with_timestamp_type(
        timestamp: i64,
        timestamp_type: TimestampType,
        compression: Compression,
        value: Bytes,
    ) -> Bytes {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type,
            offset: 0,
            sequence: -1,
            timestamp,
            key: None,
            value: Some(value),
            headers: Vec::new(),
        };
        encoded_records(&[record], compression)
    }

    fn encoded_records(records: &[Record], compression: Compression) -> Bytes {
        let mut output = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut output,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        output.freeze()
    }

    #[test]
    fn applies_max_message_bytes_after_producer_compression() {
        let records = encoded(1_000, Compression::Gzip, Bytes::from(vec![7; 8 * 1024]));
        assert!(records.len() < 8 * 1024);
        let accepted = TopicConfig {
            max_message_bytes: records.len() as i32,
            ..TopicConfig::default()
        };
        assert_eq!(admit_records(&records, &accepted, 1_000).unwrap(), records);

        let rejected = TopicConfig {
            max_message_bytes: records.len() as i32 - 1,
            ..TopicConfig::default()
        };
        assert_eq!(
            admit_records(&records, &rejected, 1_000).unwrap_err().code,
            MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn enforces_create_time_bounds() {
        let config = TopicConfig {
            message_timestamp_before_max_ms: 10,
            message_timestamp_after_max_ms: 20,
            ..TopicConfig::default()
        };
        assert_eq!(
            admit_records(
                &encoded(989, Compression::None, Bytes::from_static(b"old")),
                &config,
                1_000,
            )
            .unwrap_err()
            .code,
            INVALID_TIMESTAMP
        );
        assert_eq!(
            admit_records(
                &encoded(1_021, Compression::None, Bytes::from_static(b"future")),
                &config,
                1_000,
            )
            .unwrap_err()
            .code,
            INVALID_TIMESTAMP
        );
    }

    #[test]
    fn matches_java_long_timestamp_window_wrapping() {
        let future_wrap = TopicConfig {
            message_timestamp_before_max_ms: 0,
            message_timestamp_after_max_ms: i64::MAX,
            ..TopicConfig::default()
        };
        let records = encoded(
            i64::MIN,
            Compression::None,
            Bytes::from_static(b"future-wrap"),
        );
        assert_eq!(
            admit_records(&records, &future_wrap, 1_000).unwrap(),
            records
        );

        let negation_wrap = TopicConfig {
            message_timestamp_before_max_ms: i64::MAX,
            message_timestamp_after_max_ms: 0,
            ..TopicConfig::default()
        };
        let timestamp = 1_000_i64.wrapping_sub(i64::MIN);
        let records = encoded(
            timestamp,
            Compression::None,
            Bytes::from_static(b"negation-wrap"),
        );
        assert_eq!(
            admit_records(&records, &negation_wrap, 1_000).unwrap(),
            records
        );
    }

    #[test]
    fn matches_kafka_client_timestamp_type_matrix() {
        let create_time = TopicConfig {
            message_timestamp_before_max_ms: 10,
            message_timestamp_after_max_ms: 20,
            ..TopicConfig::default()
        };
        let missing_timestamp = encoded(
            NO_TIMESTAMP,
            Compression::None,
            Bytes::from_static(b"missing"),
        );
        assert_eq!(
            admit_records(&missing_timestamp, &create_time, 1_000).unwrap(),
            missing_timestamp
        );

        let client_log_append = encoded_with_timestamp_type(
            1_001,
            TimestampType::LogAppend,
            Compression::Snappy,
            Bytes::from_static(b"normalize"),
        );
        let normalized = admit_records(&client_log_append, &create_time, 1_000).unwrap();
        let sets = RecordBatchDecoder::decode_all(&mut normalized.clone()).unwrap();
        assert_eq!(sets[0].compression, Compression::Snappy);
        assert_eq!(sets[0].records[0].timestamp_type, TimestampType::Creation);
        assert_eq!(sets[0].records[0].timestamp, 1_001);

        let missing_log_append = encoded_with_timestamp_type(
            NO_TIMESTAMP,
            TimestampType::LogAppend,
            Compression::None,
            Bytes::from_static(b"invalid"),
        );
        assert_eq!(
            admit_records(&missing_log_append, &create_time, 1_000)
                .unwrap_err()
                .code,
            INVALID_TIMESTAMP
        );

        let log_append_time = TopicConfig {
            message_timestamp_type: "LogAppendTime".to_owned(),
            ..TopicConfig::default()
        };
        assert_eq!(
            admit_records(&client_log_append, &log_append_time, 1_000)
                .unwrap_err()
                .code,
            INVALID_TIMESTAMP
        );
    }

    #[test]
    fn validates_log_append_batches_by_max_timestamp_and_matches_codec_rebuilds() {
        let record = |offset, timestamp, value| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::LogAppend,
            offset,
            sequence: -1,
            timestamp,
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: Vec::new(),
        };
        let config = TopicConfig {
            message_timestamp_before_max_ms: 10,
            message_timestamp_after_max_ms: 20,
            ..TopicConfig::default()
        };
        let source = encoded_records(
            &[
                record(0, 980, b"inner-before-window"),
                record(1, 1_000, b"batch-max-in-window"),
            ],
            Compression::Snappy,
        );

        let admitted = admit_records(&source, &config, 1_000).unwrap();
        let decoded = RecordBatchDecoder::decode_all(&mut admitted.clone()).unwrap();
        assert_eq!(decoded[0].max_timestamp, 1_000);
        assert_eq!(
            decoded[0]
                .records
                .iter()
                .map(|record| record.timestamp)
                .collect::<Vec<_>>(),
            [980, 1_000]
        );
        assert!(
            decoded[0]
                .records
                .iter()
                .all(|record| record.timestamp_type == TimestampType::Creation)
        );

        let recompress = TopicConfig {
            compression_type: "zstd".to_owned(),
            ..config.clone()
        };
        let admitted = admit_records(&source, &recompress, 1_000).unwrap();
        let decoded = RecordBatchDecoder::decode_all(&mut admitted.clone()).unwrap();
        assert_eq!(decoded[0].compression, Compression::Zstd);
        assert_eq!(decoded[0].max_timestamp, 1_000);
        assert!(
            decoded[0]
                .records
                .iter()
                .all(|record| record.timestamp == 1_000)
        );

        let invalid = encoded_records(
            &[
                record(0, 1_000, b"inner-in-window"),
                record(1, 1_021, b"batch-max-after-window"),
            ],
            Compression::Snappy,
        );
        assert_eq!(
            admit_records(&invalid, &config, 1_000).unwrap_err().code,
            INVALID_TIMESTAMP
        );
    }

    #[test]
    fn rewrites_log_append_time_with_compression_and_valid_crc() {
        let records = encoded(123, Compression::Snappy, Bytes::from_static(b"log-append"));
        let config = TopicConfig {
            message_timestamp_type: "LogAppendTime".to_owned(),
            ..TopicConfig::default()
        };
        let admitted = admit_records(&records, &config, 9_876).unwrap();
        let mut input = admitted;
        let sets = RecordBatchDecoder::decode_all(&mut input).unwrap();
        assert_eq!(sets[0].compression, Compression::Snappy);
        assert_eq!(sets[0].records[0].timestamp_type, TimestampType::LogAppend);
        assert_eq!(sets[0].records[0].timestamp, 9_876);
    }

    #[test]
    fn applies_each_topic_compression_policy_and_preserves_records() {
        let original = encoded(
            1_234,
            Compression::Gzip,
            Bytes::from_static(b"codec-policy"),
        );
        let mut input = original.clone();
        let original_records = RecordBatchDecoder::decode_all(&mut input).unwrap()[0]
            .records
            .clone();
        for (policy, expected) in [
            ("producer", Compression::Gzip),
            ("uncompressed", Compression::None),
            ("gzip", Compression::Gzip),
            ("snappy", Compression::Snappy),
            ("lz4", Compression::Lz4),
            ("zstd", Compression::Zstd),
        ] {
            let config = TopicConfig {
                compression_type: policy.to_owned(),
                ..TopicConfig::default()
            };
            let admitted = admit_records(&original, &config, 1_234).unwrap();
            let mut input = admitted;
            let sets = RecordBatchDecoder::decode_all(&mut input).unwrap();
            assert_eq!(sets[0].compression, expected, "{policy}");
            assert_eq!(sets[0].records, original_records, "{policy}");
        }
    }

    #[test]
    fn applies_topic_compression_levels_to_valid_record_batches() {
        let value = Bytes::from(
            (0..256 * 1024)
                .map(|index| {
                    if index % 251 < 211 {
                        (index % 29) as u8
                    } else {
                        ((index * 73 + index / 17) % 256) as u8
                    }
                })
                .collect::<Vec<_>>(),
        );
        let records = encoded(1_234, Compression::None, value.clone());
        for (codec, low, high) in [("gzip", 1, 9), ("lz4", 1, 17), ("zstd", -5, 22)] {
            let mut low_config = TopicConfig {
                compression_type: codec.to_owned(),
                ..TopicConfig::default()
            };
            let mut high_config = low_config.clone();
            match codec {
                "gzip" => {
                    low_config.compression_gzip_level = low;
                    high_config.compression_gzip_level = high;
                }
                "lz4" => {
                    low_config.compression_lz4_level = low;
                    high_config.compression_lz4_level = high;
                }
                "zstd" => {
                    low_config.compression_zstd_level = low;
                    high_config.compression_zstd_level = high;
                }
                _ => unreachable!(),
            }
            let low_records = admit_records(&records, &low_config, 1_234).unwrap();
            let high_records = admit_records(&records, &high_config, 1_234).unwrap();
            assert_ne!(low_records, high_records, "{codec} levels were ignored");
            for encoded in [low_records, high_records] {
                let decoded = RecordBatchDecoder::decode_all(&mut encoded.clone()).unwrap();
                assert_eq!(decoded[0].records[0].value.as_ref(), Some(&value));
            }
        }
    }

    #[test]
    fn validates_max_message_bytes_after_topic_recompression() {
        let records = encoded(1_000, Compression::Gzip, Bytes::from(vec![3; 8 * 1024]));
        let config = TopicConfig {
            compression_type: "uncompressed".to_owned(),
            max_message_bytes: records.len() as i32,
            ..TopicConfig::default()
        };
        assert_eq!(
            admit_records(&records, &config, 1_000).unwrap_err().code,
            MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn preserves_transactional_producer_metadata_during_recompression() {
        let records = [
            Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 4,
                producer_id: 42,
                producer_epoch: 3,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 7,
                timestamp: 1_000,
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"v1")),
                headers: Vec::new(),
            },
            Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 4,
                producer_id: 42,
                producer_epoch: 3,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                sequence: 8,
                timestamp: 1_001,
                key: Some(Bytes::from_static(b"k2")),
                value: Some(Bytes::from_static(b"v2")),
                headers: Vec::new(),
            },
        ];
        let original = encoded_records(&records, Compression::Gzip);
        let config = TopicConfig {
            compression_type: "zstd".to_owned(),
            ..TopicConfig::default()
        };
        let admitted = admit_records(&original, &config, 1_001).unwrap();
        let mut input = admitted;
        let sets = RecordBatchDecoder::decode_all(&mut input).unwrap();
        assert_eq!(sets[0].compression, Compression::Zstd);
        assert_eq!(sets[0].records, records);
    }
}
