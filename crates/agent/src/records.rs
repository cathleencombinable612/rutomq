use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use rutomq_control::{ProducerBatch, increment_producer_sequence};
use rutomq_protocol::records::Record;
use rutomq_protocol::records::{
    Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, RecordBatchDecoder,
    RecordBatchEncoder, RecordEncodeOptions, RecordSet, TimestampType,
};

use crate::server::partition_state_api::VIRTUAL_LEADER_EPOCH;

#[cfg(test)]
const MAGIC_TWO_ATTRIBUTES_OFFSET: usize = 21;
const MAGIC_TWO_BASE_OFFSET_OFFSET: usize = 0;
const MAGIC_TWO_LAST_OFFSET_DELTA_OFFSET: usize = 23;
const MAGIC_TWO_PRODUCER_ID_OFFSET: usize = 43;
const MAGIC_TWO_BASE_SEQUENCE_OFFSET: usize = 53;
const MAGIC_TWO_RECORD_COUNT_OFFSET: usize = 57;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordsMetadata {
    pub record_count: i32,
    pub producer: Option<ProducerBatch>,
    pub transactional: bool,
    pub timestamp_type: Option<TimestampType>,
    pub max_timestamp_ms: Option<i64>,
}

pub fn analyze_records(records: &Bytes) -> Result<RecordsMetadata> {
    let mut input = records.clone();
    let sets = RecordBatchDecoder::decode_all(&mut input)
        .map_err(|error| anyhow!("invalid Kafka record batch: {error}"))?;
    validate_client_batch(records, &sets)?;
    analyze_decoded_records(&sets)
}

pub(crate) fn validate_client_batch(records: &Bytes, sets: &[RecordSet]) -> Result<()> {
    if sets.len() > 1 {
        return Err(anyhow!("record payload contains more than one batch"));
    }
    if sets.is_empty() {
        return Ok(());
    }

    // These fixed fields belong to the magic-v2 client batch header. Keep this
    // validation out of the general decoder because compacted stored batches
    // legitimately retain sparse offset ranges.
    let base_offset = read_i64(records, MAGIC_TWO_BASE_OFFSET_OFFSET)?;
    let last_offset_delta = read_i32(records, MAGIC_TWO_LAST_OFFSET_DELTA_OFFSET)?;
    let producer_id = read_i64(records, MAGIC_TWO_PRODUCER_ID_OFFSET)?;
    let base_sequence = read_i32(records, MAGIC_TWO_BASE_SEQUENCE_OFFSET)?;
    let record_count = read_i32(records, MAGIC_TWO_RECORD_COUNT_OFFSET)?;
    let count_from_offsets = i64::from(last_offset_delta) + 1;

    if base_offset != 0 {
        return Err(anyhow!(
            "client record batch base offset must be 0, got {base_offset}"
        ));
    }
    if count_from_offsets <= 0 {
        return Err(anyhow!(
            "record batch has invalid offset range with last offset delta {last_offset_delta}"
        ));
    }
    if record_count <= 0 {
        return Err(anyhow!(
            "record batch reports invalid record count {record_count}"
        ));
    }
    if count_from_offsets != i64::from(record_count) {
        return Err(anyhow!(
            "record batch offset range contains {count_from_offsets} records but reports {record_count}"
        ));
    }
    if producer_id != NO_PRODUCER_ID && base_sequence < 0 {
        return Err(anyhow!(
            "record batch with producer id {producer_id} has negative base sequence {base_sequence}"
        ));
    }
    Ok(())
}

fn analyze_decoded_records(sets: &[RecordSet]) -> Result<RecordsMetadata> {
    let count = sets.iter().map(|set| set.records.len()).sum::<usize>();
    let record_count =
        i32::try_from(count).map_err(|_| anyhow!("record batch contains too many records"))?;
    let mut records = sets.iter().flat_map(|set| set.records.iter());
    let Some(first) = records.next() else {
        return Ok(RecordsMetadata {
            record_count,
            producer: None,
            transactional: false,
            timestamp_type: None,
            max_timestamp_ms: None,
        });
    };
    if first.control {
        return Err(anyhow!("clients cannot append Kafka control records"));
    }
    let idempotent = first.producer_id != NO_PRODUCER_ID;
    if idempotent && (first.producer_epoch == NO_PRODUCER_EPOCH || first.sequence == NO_SEQUENCE) {
        return Err(anyhow!(
            "producer id requires a producer epoch and sequence"
        ));
    }
    if !idempotent
        && (first.producer_epoch != NO_PRODUCER_EPOCH
            || first.sequence != NO_SEQUENCE
            || first.transactional)
    {
        return Err(anyhow!("record batch has inconsistent producer metadata"));
    }
    let mut last_sequence = first.sequence;
    let timestamp_type = first.timestamp_type;
    let mut max_timestamp_ms = first.timestamp;
    for record in records {
        if record.control {
            return Err(anyhow!("clients cannot append Kafka control records"));
        }
        if record.producer_id != first.producer_id
            || record.producer_epoch != first.producer_epoch
            || record.transactional != first.transactional
            || record.timestamp_type != timestamp_type
        {
            return Err(anyhow!(
                "one partition append must use one producer and transaction mode"
            ));
        }
        max_timestamp_ms = max_timestamp_ms.max(record.timestamp);
        if idempotent {
            let expected = increment_producer_sequence(last_sequence, 1);
            if record.sequence != expected {
                return Err(anyhow!(
                    "producer sequence is not contiguous: expected {expected}, got {}",
                    record.sequence
                ));
            }
            last_sequence = record.sequence;
        }
    }
    Ok(RecordsMetadata {
        record_count,
        producer: idempotent.then_some(ProducerBatch {
            producer_id: first.producer_id,
            producer_epoch: first.producer_epoch,
            first_sequence: first.sequence,
            last_sequence,
        }),
        transactional: first.transactional,
        timestamp_type: Some(timestamp_type),
        max_timestamp_ms: Some(max_timestamp_ms),
    })
}

fn read_i32(records: &Bytes, offset: usize) -> Result<i32> {
    let value = records
        .get(offset..offset + size_of::<i32>())
        .ok_or_else(|| anyhow!("record batch is shorter than the magic-v2 header"))?;
    Ok(i32::from_be_bytes(
        value.try_into().expect("validated four-byte slice"),
    ))
}

fn read_i64(records: &Bytes, offset: usize) -> Result<i64> {
    let value = records
        .get(offset..offset + size_of::<i64>())
        .ok_or_else(|| anyhow!("record batch is shorter than the magic-v2 header"))?;
    Ok(i64::from_be_bytes(
        value.try_into().expect("validated eight-byte slice"),
    ))
}

pub fn rewrite_offsets(records: &Bytes, base_offset: i64) -> Result<Bytes> {
    let mut input = records.clone();
    let mut sets = RecordBatchDecoder::decode_all(&mut input)
        .map_err(|error| anyhow!("invalid Kafka record batch: {error}"))?;
    let mut next_offset = base_offset;
    let mut output = BytesMut::with_capacity(records.len());
    for set in &mut sets {
        for record in &mut set.records {
            record.offset = next_offset;
            record.partition_leader_epoch = VIRTUAL_LEADER_EPOCH;
            next_offset += 1;
        }
        RecordBatchEncoder::encode(
            &mut output,
            set.records.iter(),
            &RecordEncodeOptions {
                version: set.version,
                compression: set.compression,
            },
        )
        .map_err(|error| anyhow!("failed to rewrite Kafka record batch: {error}"))?;
    }
    Ok(output.freeze())
}

pub fn materialize_records(
    records: &Bytes,
    base_offset: i64,
    offsets_preserved: bool,
) -> Result<Bytes> {
    if !offsets_preserved {
        return rewrite_offsets(records, base_offset);
    }
    let mut input = records.clone();
    RecordBatchDecoder::decode_all(&mut input)
        .map_err(|error| anyhow!("invalid Kafka record batch: {error}"))?;
    Ok(records.clone())
}

pub fn decode_stored_record_batches(
    records: &Bytes,
    base_offset: i64,
    offsets_preserved: bool,
) -> Result<Vec<Vec<Record>>> {
    let mut input = records.clone();
    let mut sets = RecordBatchDecoder::decode_all(&mut input)
        .map_err(|error| anyhow!("invalid Kafka record batch: {error}"))?;
    if offsets_preserved {
        return Ok(sets.into_iter().map(|set| set.records).collect());
    }
    let mut next_offset = base_offset;
    for set in &mut sets {
        for record in &mut set.records {
            record.offset = next_offset;
            next_offset += 1;
        }
    }
    Ok(sets.into_iter().map(|set| set.records).collect())
}

pub fn decode_stored_records(
    records: &Bytes,
    base_offset: i64,
    offsets_preserved: bool,
) -> Result<Vec<Record>> {
    Ok(
        decode_stored_record_batches(records, base_offset, offsets_preserved)?
            .into_iter()
            .flatten()
            .collect(),
    )
}

pub fn encode_records(records: &[Record]) -> Result<Bytes> {
    if records.is_empty() {
        return Ok(Bytes::new());
    }
    let mut output = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut output,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|error| anyhow!("failed to encode Kafka record batch: {error}"))?;
    Ok(output.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_protocol::records::{Record, TimestampType};

    fn sample_records() -> Bytes {
        sample_records_with_compression(Compression::None)
    }

    fn sample_records_with_compression(compression: Compression) -> Bytes {
        let records = [Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"hello")),
            headers: Vec::new(),
        }];
        let mut bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut bytes,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        bytes.freeze()
    }

    fn producer_batch(sequence: i32, compression: Compression) -> Bytes {
        let records = [Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: 7,
            producer_epoch: 2,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence,
            timestamp: i64::from(sequence + 1),
            key: None,
            value: Some(Bytes::from_static(b"idempotent")),
            headers: Vec::new(),
        }];
        let mut bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut bytes,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        bytes.freeze()
    }

    fn with_header_i32(records: Bytes, offset: usize, value: i32) -> Bytes {
        let mut bytes = records.to_vec();
        bytes[offset..offset + size_of::<i32>()].copy_from_slice(&value.to_be_bytes());
        let crc = crc32c(&bytes[MAGIC_TWO_ATTRIBUTES_OFFSET..]);
        bytes[17..MAGIC_TWO_ATTRIBUTES_OFFSET].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(bytes)
    }

    fn with_base_offset(records: Bytes, value: i64) -> Bytes {
        let mut bytes = records.to_vec();
        bytes[MAGIC_TWO_BASE_OFFSET_OFFSET..size_of::<i64>()].copy_from_slice(&value.to_be_bytes());
        Bytes::from(bytes)
    }

    fn crc32c(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }

    #[test]
    fn counts_and_rewrites_records() {
        let records = sample_records();
        assert_eq!(analyze_records(&records).unwrap().record_count, 1);
        let rewritten = rewrite_offsets(&records, 8).unwrap();
        let mut input = rewritten.clone();
        let decoded = RecordBatchDecoder::decode_all(&mut input).unwrap();
        assert_eq!(decoded[0].records[0].offset, 8);
        assert_eq!(
            decoded[0].records[0].partition_leader_epoch,
            VIRTUAL_LEADER_EPOCH
        );
        let decoded = decode_stored_records(&records, 12, false).unwrap();
        assert_eq!(decoded[0].offset, 12);
        let encoded = encode_records(&decoded).unwrap();
        let mut input = encoded;
        let round_trip = RecordBatchDecoder::decode_all(&mut input).unwrap();
        assert_eq!(round_trip[0].records[0].offset, 12);
    }

    #[test]
    fn preserves_compression_while_rewriting_offsets() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let rewritten =
                rewrite_offsets(&sample_records_with_compression(compression), 8).unwrap();
            let mut input = rewritten;
            let decoded = RecordBatchDecoder::decode_all(&mut input).unwrap();
            assert_eq!(decoded[0].compression, compression);
            assert_eq!(decoded[0].records[0].offset, 8);
        }
    }

    #[test]
    fn preserves_sparse_offsets_for_compacted_records() {
        let mut first = RecordBatchDecoder::decode_all(&mut sample_records())
            .unwrap()
            .remove(0)
            .records
            .remove(0);
        first.offset = 4;
        let mut second = first.clone();
        second.offset = 9;
        second.value = Some(Bytes::from_static(b"later"));
        let encoded = encode_records(&[first, second]).unwrap();
        assert_eq!(
            i32::from_be_bytes(encoded[53..57].try_into().unwrap()),
            NO_SEQUENCE
        );

        let materialized = materialize_records(&encoded, 100, true).unwrap();
        let decoded = decode_stored_records(&materialized, 100, true).unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![4, 9]
        );
        assert!(decoded.iter().all(|record| record.sequence == NO_SEQUENCE));
    }

    #[test]
    fn preserves_stored_record_batch_boundaries() {
        let first = sample_records();
        let second = sample_records();
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&first);
        combined.extend_from_slice(&second);
        let batches = decode_stored_record_batches(&combined.freeze(), 7, false).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0].offset, 7);
        assert_eq!(batches[1][0].offset, 8);
    }

    #[test]
    fn rejects_multiple_magic_two_batches_in_one_produce_payload() {
        let first = producer_batch(4, Compression::None);
        let second = producer_batch(5, Compression::Gzip);
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&first);
        combined.extend_from_slice(&second);
        let error = analyze_records(&combined.freeze()).unwrap_err();
        assert!(error.to_string().contains("more than one batch"));
    }

    #[test]
    fn rejects_invalid_magic_two_batch_headers() {
        for base_offset in [-1, 1] {
            let records = with_base_offset(sample_records(), base_offset);
            assert!(
                analyze_records(&records)
                    .unwrap_err()
                    .to_string()
                    .contains(&format!("base offset must be 0, got {base_offset}"))
            );
        }

        let zero_count = with_header_i32(sample_records(), MAGIC_TWO_RECORD_COUNT_OFFSET, 0);
        assert!(
            analyze_records(&zero_count)
                .unwrap_err()
                .to_string()
                .contains("invalid record count")
        );

        let invalid_range =
            with_header_i32(sample_records(), MAGIC_TWO_LAST_OFFSET_DELTA_OFFSET, -1);
        assert!(
            analyze_records(&invalid_range)
                .unwrap_err()
                .to_string()
                .contains("invalid offset range")
        );

        let inconsistent_range =
            with_header_i32(sample_records(), MAGIC_TWO_LAST_OFFSET_DELTA_OFFSET, 1);
        assert!(
            analyze_records(&inconsistent_range)
                .unwrap_err()
                .to_string()
                .contains("offset range contains 2 records but reports 1")
        );

        let negative_sequence = with_header_i32(
            producer_batch(0, Compression::None),
            MAGIC_TWO_BASE_SEQUENCE_OFFSET,
            -2,
        );
        assert!(
            analyze_records(&negative_sequence)
                .unwrap_err()
                .to_string()
                .contains("negative base sequence -2")
        );
    }

    #[test]
    fn extracts_idempotent_producer_metadata_across_sequence_rollover() {
        let records = [
            Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: 7,
                producer_epoch: 2,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: i32::MAX,
                timestamp: 1,
                key: None,
                value: Some(Bytes::from_static(b"a")),
                headers: Vec::new(),
            },
            Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: 7,
                producer_epoch: 2,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                sequence: 0,
                timestamp: 1,
                key: None,
                value: Some(Bytes::from_static(b"b")),
                headers: Vec::new(),
            },
        ];
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        let metadata = analyze_records(&encoded.freeze()).unwrap();
        assert_eq!(metadata.record_count, 2);
        assert!(metadata.transactional);
        assert_eq!(
            metadata.producer,
            Some(ProducerBatch {
                producer_id: 7,
                producer_epoch: 2,
                first_sequence: i32::MAX,
                last_sequence: 0,
            })
        );
    }
}
