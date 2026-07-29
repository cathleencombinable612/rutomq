//! Provides utilities for working with records (Kafka messages).
//!
//! [`FetchResponse`](crate::messages::fetch_response::FetchResponse) and associated APIs for interacting with reading and writing
//! contain records in a raw format, allowing the user to implement their own logic for interacting
//! with those values.
//!
//! # Example
//!
//! Decoding a set of records from a [`FetchResponse`](crate::messages::fetch_response::FetchResponse):
//! ```rust
//! use kafka_protocol::messages::FetchResponse;
//! use kafka_protocol::protocol::Decodable;
//! use kafka_protocol::records::RecordBatchDecoder;
//! use bytes::Bytes;
//! use kafka_protocol::records::Compression;
//!
//! # const HEADER: [u8; 45] = [ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,];
//! # const RECORD: [u8; 79] = [ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x43, 0x0, 0x0, 0x0, 0x0, 0x2, 0x73, 0x6d, 0x29, 0x7b, 0x0, 0b00000000, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x22, 0x1, 0xd0, 0xf, 0x2, 0xa, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0xa, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x0,];
//! # let mut res = vec![];
//! # res.extend_from_slice(&HEADER[..]);
//! # res.extend_from_slice(&[0x00, 0x00, 0x00, 0x4f]);
//! # res.extend_from_slice(&RECORD[..]);
//! # let mut buf = Bytes::from(res);
//!
//! let res = FetchResponse::decode(&mut buf, 4).unwrap();
//!
//! for topic in res.responses {
//!     for partition in topic.partitions {
//!          let mut records = partition.records.unwrap();
//!          let records = RecordBatchDecoder::decode_with_custom_compression(&mut records, Some(decompress_record_batch_data)).unwrap();
//!     }
//! }
//!
//! fn decompress_record_batch_data(compressed_buffer: &mut bytes::Bytes, compression: Compression) -> anyhow::Result<Bytes> {
//!         match compression {
//!             Compression::None => Ok(compressed_buffer.to_vec().into()),
//!             _ => { panic!("Compression not implemented") }
//!         }
//!  }
//! ```
use anyhow::{anyhow, bail, Result};
use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_ISO_HDLC};
use crc32c::crc32c;

use crate::protocol::{
    buf::{gap, ByteBuf, ByteBufMut},
    types, Decoder, Encoder, StrBytes,
};

use super::compression::{self as cmpr, Compressor, Decompressor};
use std::convert::TryFrom;
use std::fmt;
/// IEEE (checksum) cyclic redundancy check.
pub const IEEE: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// A record batch whose stored CRC does not match its encoded contents.
#[derive(Debug)]
pub struct RecordCrcError {
    supplied: u32,
    actual: u32,
}

impl fmt::Display for RecordCrcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Cyclic redundancy check failed ({} != {})",
            self.supplied, self.actual
        )
    }
}

impl std::error::Error for RecordCrcError {}

/// The different types of compression supported by Kafka.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    None = 0,
    /// gzip compression library.
    Gzip = 1,
    /// Google's Snappy compression library.
    Snappy = 2,
    /// The LZ4 compression library.
    Lz4 = 3,
    /// Facebook's ZStandard compression library.
    Zstd = 4,
}

/// Indicates the meaning of the timestamp field on a record.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimestampType {
    /// The timestamp represents when the record was created by the client.
    Creation = 0,
    /// The timestamp represents when the record was appended to the log.
    LogAppend = 1,
}

/// Options for encoding and compressing a batch of records. Note, not all compression algorithms
/// are currently implemented by this library.
pub struct RecordEncodeOptions {
    /// Record version, 0, 1, or 2.
    pub version: i8,

    /// The compression algorithm to use.
    pub compression: Compression,
}

/// Value to indicate missing producer id.
pub const NO_PRODUCER_ID: i64 = -1;
/// Value to indicate missing producer epoch.
pub const NO_PRODUCER_EPOCH: i16 = -1;
/// Value to indicated missing leader epoch.
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;
/// Value to indicate missing sequence id.
pub const NO_SEQUENCE: i32 = -1;
/// Value to indicate missing timestamp.
pub const NO_TIMESTAMP: i64 = -1;

fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    const MODULUS: i64 = i32::MAX as i64 + 1;
    (i64::from(sequence) + i64::from(increment)).rem_euclid(MODULUS) as i32
}

#[derive(Debug, Clone)]
/// Batch encoder for Kafka records.
pub struct RecordBatchEncoder;

#[derive(Debug, Clone)]
/// Batch decoder for Kafka records.
pub struct RecordBatchDecoder;

struct BatchDecodeInfo {
    record_count: usize,
    timestamp_type: TimestampType,
    min_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    transactional: bool,
    control: bool,
    delete_horizon: bool,
    partition_leader_epoch: i32,
    producer_id: i64,
    producer_epoch: i16,
}

/// Decoded records plus information about compression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSet {
    /// Compression used for this set of records
    pub compression: Compression,
    /// Version used to encode the set of records
    pub version: i8,
    /// Maximum timestamp stored in the batch header.
    pub max_timestamp: i64,
    /// Records decoded in this set
    pub records: Vec<Record>,
}

/// A Kafka message containing key, payload value, and all associated metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    // Batch properties
    /// Whether this record is transactional.
    pub transactional: bool,
    /// Whether this record is a control message, which should not be exposed to the client.
    pub control: bool,
    /// Whether this record has the delete horizon flag set.
    pub delete_horizon: bool,
    /// Epoch of the leader for this record 's partition.
    pub partition_leader_epoch: i32,
    /// The identifier of the producer.
    pub producer_id: i64,
    /// Producer metadata used to implement transactional writes.
    pub producer_epoch: i16,

    // Record properties
    /// Indicates whether timestamp represents record creation or appending to the log.
    pub timestamp_type: TimestampType,
    /// Message offset within a partition.
    pub offset: i64,
    /// Sequence identifier used for idempotent delivery.
    pub sequence: i32,
    /// Timestamp the record. See also `timestamp_type`.
    pub timestamp: i64,
    /// The key of the record.
    pub key: Option<Bytes>,
    /// The payload of the record.
    pub value: Option<Bytes>,
    /// Headers associated with the record's payload.
    pub headers: Vec<(StrBytes, Option<Bytes>)>,
}

const MAGIC_BYTE_OFFSET: usize = 16;

impl RecordBatchEncoder {
    /// Encode records into given buffer, using provided encoding options that select the encoding
    /// strategy based on version.
    pub fn encode<'a, B, I>(buf: &mut B, records: I, options: &RecordEncodeOptions) -> Result<()>
    where
        B: ByteBufMut,
        I: IntoIterator<Item = &'a Record>,
        I::IntoIter: Clone,
    {
        Self::encode_with_custom_compression(
            buf,
            records,
            options,
            None::<fn(&mut BytesMut, &mut B, Compression) -> Result<()>>,
        )
    }

    /// Encode records into given buffer, using provided encoding options that select the encoding
    /// strategy based on version.
    /// # Arguments
    /// * `compressor` - A function that compresses the given batch of records.
    ///
    /// If `None`, the right compression algorithm will automatically be selected and applied.
    pub fn encode_with_custom_compression<'a, B, I, CF>(
        buf: &mut B,
        records: I,
        options: &RecordEncodeOptions,
        compressor: Option<CF>,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: IntoIterator<Item = &'a Record>,
        I::IntoIter: Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        let records = records.into_iter();
        match options.version {
            0..=1 => bail!("message sets v{} are unsupported", options.version),
            2 => Self::encode_new(buf, records, options, compressor),
            _ => bail!("Unknown record batch version"),
        }
    }

    fn encode_new_records<'a, B, I>(
        buf: &mut B,
        records: I,
        base_offset: i64,
        base_timestamp: i64,
        options: &RecordEncodeOptions,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record>,
    {
        for record in records {
            record.encode_new(buf, base_offset, base_timestamp, options)?;
        }
        Ok(())
    }

    fn encode_new_batch<'a, B, I, CF>(
        buf: &mut B,
        records: &mut I,
        options: &RecordEncodeOptions,
        compressor: Option<&CF>,
    ) -> Result<bool>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record> + Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        let mut record_peeker = records.clone();

        // Get first record
        let first_record = match record_peeker.next() {
            Some(record) => record,
            None => return Ok(false),
        };

        // Determine how many additional records can be included in the batch
        let num_records = record_peeker
            .take_while(|record| {
                record.transactional == first_record.transactional
                    && record.control == first_record.control
                    && record.delete_horizon == first_record.delete_horizon
                    && record.timestamp_type == first_record.timestamp_type
                    && record.partition_leader_epoch == first_record.partition_leader_epoch
                    && record.producer_id == first_record.producer_id
                    && record.producer_epoch == first_record.producer_epoch
                    && (first_record.producer_id == NO_PRODUCER_ID || {
                        let offset_delta = record.offset.wrapping_sub(first_record.offset);
                        (0..=i64::from(i32::MAX)).contains(&offset_delta)
                            && record.sequence
                                == increment_sequence(first_record.sequence, offset_delta as i32)
                    })
            })
            .count()
            + 1;

        // Aggregate various record properties
        let base_offset = first_record.offset;
        let last_offset = records
            .clone()
            .take(num_records)
            .last()
            .map(|record| record.offset)
            .expect("Batch contains at least one element");
        let last_offset_delta = last_offset.wrapping_sub(base_offset);
        if last_offset_delta > i32::MAX as i64 || last_offset_delta < i32::MIN as i64 {
            bail!("Offsets within batch are too far apart ({base_offset}, {last_offset})");
        }
        let base_timestamp = first_record.timestamp;
        let max_timestamp = records
            .clone()
            .take(num_records)
            .map(|r| r.timestamp)
            .max()
            .expect("Batch contains at least one element");
        let base_sequence = if first_record.producer_id == NO_PRODUCER_ID {
            NO_SEQUENCE
        } else {
            first_record.sequence
        };

        // Base offset
        types::Int64.encode(buf, base_offset)?;

        // Batch length
        let size_gap = buf.put_typed_gap(gap::I32);
        let batch_start = buf.offset();

        // Partition leader epoch
        types::Int32.encode(buf, first_record.partition_leader_epoch)?;

        // Magic byte
        types::Int8.encode(buf, options.version)?;

        // CRC
        let crc_gap = buf.put_typed_gap(gap::U32);
        let content_start = buf.offset();

        // Attributes
        let mut attributes = options.compression as i16;
        if first_record.timestamp_type == TimestampType::LogAppend {
            attributes |= 1 << 3;
        }
        if first_record.transactional {
            attributes |= 1 << 4;
        }
        if first_record.control {
            attributes |= 1 << 5;
        }
        if first_record.delete_horizon {
            attributes |= 1 << 6;
        }
        types::Int16.encode(buf, attributes)?;

        // Last offset delta
        types::Int32.encode(buf, last_offset_delta as i32)?;

        // First timestamp
        types::Int64.encode(buf, base_timestamp)?;

        // Last timestamp
        types::Int64.encode(buf, max_timestamp)?;

        // Producer ID
        types::Int64.encode(buf, first_record.producer_id)?;

        // Producer epoch
        types::Int16.encode(buf, first_record.producer_epoch)?;

        // Base sequence
        types::Int32.encode(buf, base_sequence)?;

        // Record count
        if num_records > i32::MAX as usize {
            bail!("Too many records to encode in one batch ({num_records} records)");
        }
        types::Int32.encode(buf, num_records as i32)?;

        // Records
        let records = records.take(num_records);

        if let Some(compressor) = compressor {
            let mut record_buf = BytesMut::new();
            Self::encode_new_records(
                &mut record_buf,
                records,
                base_offset,
                base_timestamp,
                options,
            )?;
            compressor(&mut record_buf, buf, options.compression)?;
        } else {
            match options.compression {
                Compression::None => cmpr::None::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, base_offset, base_timestamp, options)
                })?,
                #[cfg(feature = "snappy")]
                Compression::Snappy => cmpr::Snappy::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, base_offset, base_timestamp, options)
                })?,
                #[cfg(feature = "gzip")]
                Compression::Gzip => cmpr::Gzip::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, base_offset, base_timestamp, options)
                })?,
                #[cfg(feature = "lz4")]
                Compression::Lz4 => cmpr::Lz4::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, base_offset, base_timestamp, options)
                })?,
                #[cfg(feature = "zstd")]
                Compression::Zstd => cmpr::Zstd::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, base_offset, base_timestamp, options)
                })?,
                #[allow(unreachable_patterns)]
                c => {
                    return Err(anyhow!(
                        "Support for {c:?} is not enabled as a cargo feature"
                    ))
                }
            }
        }
        let batch_end = buf.offset();

        // Fill size gap
        let batch_size = batch_end - batch_start;
        if batch_size > i32::MAX as usize {
            bail!("Record batch was too large to encode ({batch_size} bytes)");
        }

        buf.fill_typed_gap(size_gap, batch_size as i32);

        // Fill CRC gap
        let crc = crc32c(buf.range(content_start..batch_end));
        buf.fill_typed_gap(crc_gap, crc);

        Ok(true)
    }

    fn encode_new<'a, B, I, CF>(
        buf: &mut B,
        mut records: I,
        options: &RecordEncodeOptions,
        compressor: Option<CF>,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record> + Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        while Self::encode_new_batch(buf, &mut records, options, compressor.as_ref())? {}
        Ok(())
    }
}

impl RecordBatchDecoder {
    /// Decode one RecordSet from the provided buffer.
    /// # Arguments
    /// * `decompressor` - A function that decompresses the given batch of records.
    ///
    /// If `None`, the right decompression algorithm will automatically be selected and applied.
    pub fn decode_with_custom_compression<B: ByteBuf, F>(
        buf: &mut B,
        decompressor: Option<F>,
    ) -> Result<RecordSet>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        let mut records = Vec::new();
        let (version, compression, max_timestamp) =
            Self::decode_into_vec(buf, &mut records, decompressor.as_ref())?;
        Ok(RecordSet {
            version,
            compression,
            max_timestamp,
            records,
        })
    }

    /// Decode the entire buffer into a vec of RecordSets.
    pub fn decode_all<B: ByteBuf>(buf: &mut B) -> Result<Vec<RecordSet>> {
        let mut batches = Vec::new();
        while buf.has_remaining() {
            batches.push(Self::decode(buf)?);
        }
        Ok(batches)
    }

    /// Decode one RecordSet from the provided buffer.
    pub fn decode<B: ByteBuf>(buf: &mut B) -> Result<RecordSet> {
        Self::decode_with_custom_compression(
            buf,
            None::<fn(&mut bytes::Bytes, Compression) -> Result<B>>.as_ref(),
        )
    }

    fn decode_into_vec<B: ByteBuf, F>(
        buf: &mut B,
        records: &mut Vec<Record>,
        decompress_func: Option<&F>,
    ) -> Result<(i8, Compression, i64)>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        let version = buf.try_peek_bytes(MAGIC_BYTE_OFFSET..(MAGIC_BYTE_OFFSET + 1))?[0] as i8;
        let (compression, max_timestamp) = match version {
            0..=1 => bail!("message sets v{version} are unsupported"),
            2 => Self::decode_new_batch(buf, version, records, decompress_func),
            _ => {
                bail!("Unknown record batch version ({version})");
            }
        }?;
        Ok((version, compression, max_timestamp))
    }
    fn decode_new_records<B: ByteBuf>(
        buf: &mut B,
        batch_decode_info: &BatchDecodeInfo,
        version: i8,
        records: &mut Vec<Record>,
    ) -> Result<()> {
        if batch_decode_info.record_count > buf.remaining() {
            bail!(
                "record count {} exceeds remaining record payload size {}",
                batch_decode_info.record_count,
                buf.remaining()
            );
        }
        records.reserve(batch_decode_info.record_count);
        for _ in 0..batch_decode_info.record_count {
            records.push(Record::decode_new(buf, batch_decode_info, version)?);
        }
        if batch_decode_info.record_count > 0 && buf.has_remaining() {
            let trailing = buf.remaining();
            bail!(
                "record batch has {trailing} trailing byte{} after its declared record count",
                if trailing == 1 { "" } else { "s" }
            );
        }
        Ok(())
    }
    fn decode_new_batch<B: ByteBuf, F>(
        buf: &mut B,
        version: i8,
        records: &mut Vec<Record>,
        decompress_func: Option<&F>,
    ) -> Result<(Compression, i64)>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        // Base offset
        let min_offset = types::Int64.decode(buf)?;

        // Batch length
        let batch_length: i32 = types::Int32.decode(buf)?;
        if batch_length < 0 {
            bail!("Unexpected negative batch size: {batch_length}");
        }

        // Convert buf to bytes
        let buf = &mut buf.try_get_bytes(batch_length as usize)?;

        // Partition leader epoch
        let partition_leader_epoch = types::Int32.decode(buf)?;

        // Magic byte
        let magic: i8 = types::Int8.decode(buf)?;
        if magic != version {
            bail!("Version mismatch ({magic} != {version})");
        }

        // CRC
        let supplied_crc: u32 = types::UInt32.decode(buf)?;
        let actual_crc = crc32c(buf);

        if supplied_crc != actual_crc {
            return Err(RecordCrcError {
                supplied: supplied_crc,
                actual: actual_crc,
            }
            .into());
        }

        // Attributes
        let attributes: i16 = types::Int16.decode(buf)?;
        let transactional = (attributes & (1 << 4)) != 0;
        let control = (attributes & (1 << 5)) != 0;
        let delete_horizon = (attributes & (1 << 6)) != 0;
        let compression = match attributes & 0x7 {
            0 => Compression::None,
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            other => {
                bail!("Unknown compression algorithm used: {other}");
            }
        };
        let timestamp_type = if (attributes & (1 << 3)) != 0 {
            TimestampType::LogAppend
        } else {
            TimestampType::Creation
        };

        // Last offset delta
        let _max_offset_delta: i32 = types::Int32.decode(buf)?;

        // First timestamp
        let base_timestamp = types::Int64.decode(buf)?;

        // Last timestamp
        let max_timestamp: i64 = types::Int64.decode(buf)?;

        // Producer ID
        let producer_id = types::Int64.decode(buf)?;

        // Producer epoch
        let producer_epoch = types::Int16.decode(buf)?;

        // Base sequence
        let base_sequence = types::Int32.decode(buf)?;

        // Record count
        let record_count: i32 = types::Int32.decode(buf)?;
        if record_count < 0 {
            bail!("Unexpected negative record count ({record_count})");
        }
        let record_count = record_count as usize;

        let batch_decode_info = BatchDecodeInfo {
            record_count,
            timestamp_type,
            min_offset,
            base_timestamp,
            base_sequence,
            transactional,
            control,
            delete_horizon,
            partition_leader_epoch,
            producer_id,
            producer_epoch,
        };

        if let Some(decompress_func) = decompress_func {
            let mut decompressed_buf = decompress_func(buf, compression)?;

            Self::decode_new_records(&mut decompressed_buf, &batch_decode_info, version, records)?;
        } else {
            match compression {
                Compression::None => cmpr::None::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "snappy")]
                Compression::Snappy => cmpr::Snappy::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "gzip")]
                Compression::Gzip => cmpr::Gzip::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "zstd")]
                Compression::Zstd => cmpr::Zstd::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "lz4")]
                Compression::Lz4 => cmpr::Lz4::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[allow(unreachable_patterns)]
                c => {
                    return Err(anyhow!(
                        "Support for {c:?} is not enabled as a cargo feature"
                    ))
                }
            };
        }

        Ok((compression, max_timestamp))
    }
}

impl Record {
    fn encode_new<B: ByteBufMut>(
        &self,
        buf: &mut B,
        base_offset: i64,
        base_timestamp: i64,
        options: &RecordEncodeOptions,
    ) -> Result<()> {
        // Size
        let size = self.compute_size_new(base_offset, base_timestamp, options)?;
        if size > i32::MAX as usize {
            bail!("Record was too large to encode ({size} bytes)");
        }
        types::VarInt.encode(buf, size as i32)?;

        // Attributes
        types::Int8.encode(buf, 0)?;

        // Timestamp delta
        let timestamp_delta = self.timestamp.wrapping_sub(base_timestamp);
        types::VarLong.encode(buf, timestamp_delta)?;

        // Offset delta
        let offset_delta = self.offset.wrapping_sub(base_offset);
        if offset_delta > i32::MAX as i64 || offset_delta < i32::MIN as i64 {
            bail!(
                "Offsets within batch are too far apart ({}, {})",
                base_offset,
                self.offset
            );
        }
        types::VarInt.encode(buf, offset_delta as i32)?;

        // Key
        if let Some(k) = self.key.as_ref() {
            if k.len() > i32::MAX as usize {
                bail!("Record key was too large to encode ({} bytes)", k.len());
            }
            types::VarInt.encode(buf, k.len() as i32)?;
            buf.put_slice(k);
        } else {
            types::VarInt.encode(buf, -1)?;
        }

        // Value
        if let Some(v) = self.value.as_ref() {
            if v.len() > i32::MAX as usize {
                bail!("Record value was too large to encode ({} bytes)", v.len());
            }
            types::VarInt.encode(buf, v.len() as i32)?;
            buf.put_slice(v);
        } else {
            types::VarInt.encode(buf, -1)?;
        }

        // Headers
        if self.headers.len() > i32::MAX as usize {
            bail!("Too many record headers encode ({})", self.headers.len());
        }
        types::VarInt.encode(buf, self.headers.len() as i32)?;
        for (k, v) in &self.headers {
            // Key len
            if k.len() > i32::MAX as usize {
                bail!(
                    "Record header key was too large to encode ({} bytes)",
                    k.len()
                );
            }
            types::VarInt.encode(buf, k.len() as i32)?;

            // Key
            buf.put_slice(k.as_ref());

            // Value
            if let Some(v) = v.as_ref() {
                if v.len() > i32::MAX as usize {
                    bail!(
                        "Record header value was too large to encode ({} bytes)",
                        v.len()
                    );
                }
                types::VarInt.encode(buf, v.len() as i32)?;
                buf.put_slice(v);
            } else {
                types::VarInt.encode(buf, -1)?;
            }
        }

        Ok(())
    }
    fn compute_size_new(
        &self,
        base_offset: i64,
        base_timestamp: i64,
        _options: &RecordEncodeOptions,
    ) -> Result<usize> {
        let mut total_size = 0;

        // Attributes
        total_size += types::Int8.compute_size(0)?;

        // Timestamp delta
        let timestamp_delta = self.timestamp.wrapping_sub(base_timestamp);
        total_size += types::VarLong.compute_size(timestamp_delta)?;

        // Offset delta
        let offset_delta = self.offset.wrapping_sub(base_offset);
        if offset_delta > i32::MAX as i64 || offset_delta < i32::MIN as i64 {
            bail!(
                "Offsets within batch are too far apart ({}, {})",
                base_offset,
                self.offset
            );
        }
        total_size += types::VarInt.compute_size(offset_delta as i32)?;

        // Key
        if let Some(k) = self.key.as_ref() {
            if k.len() > i32::MAX as usize {
                bail!("Record key was too large to encode ({} bytes)", k.len());
            }
            total_size += types::VarInt.compute_size(k.len() as i32)?;
            total_size += k.len();
        } else {
            total_size += types::VarInt.compute_size(-1)?;
        }

        // Value len
        if let Some(v) = self.value.as_ref() {
            if v.len() > i32::MAX as usize {
                bail!("Record value was too large to encode ({} bytes)", v.len());
            }
            total_size += types::VarInt.compute_size(v.len() as i32)?;
            total_size += v.len();
        } else {
            total_size += types::VarInt.compute_size(-1)?;
        }

        // Headers
        if self.headers.len() > i32::MAX as usize {
            bail!("Too many record headers encode ({})", self.headers.len());
        }
        total_size += types::VarInt.compute_size(self.headers.len() as i32)?;
        for (k, v) in &self.headers {
            // Key len
            if k.len() > i32::MAX as usize {
                bail!(
                    "Record header key was too large to encode ({} bytes)",
                    k.len()
                );
            }
            total_size += types::VarInt.compute_size(k.len() as i32)?;

            // Key
            total_size += k.len();

            // Value
            if let Some(v) = v.as_ref() {
                if v.len() > i32::MAX as usize {
                    bail!(
                        "Record header value was too large to encode ({} bytes)",
                        v.len()
                    );
                }
                total_size += types::VarInt.compute_size(v.len() as i32)?;
                total_size += v.len();
            } else {
                total_size += types::VarInt.compute_size(-1)?;
            }
        }

        Ok(total_size)
    }
    fn decode_new<B: ByteBuf>(
        buf: &mut B,
        batch_decode_info: &BatchDecodeInfo,
        _version: i8,
    ) -> Result<Self> {
        // Size
        let size: i32 = types::VarInt.decode(buf)?;
        if size < 0 {
            bail!("Unexpected negative record size: {size}");
        }

        // Ensure we don't over-read
        let buf = &mut buf.try_get_bytes(size as usize)?;

        // Attributes
        let _attributes: i8 = types::Int8.decode(buf)?;

        // Timestamp delta
        let timestamp_delta: i64 = types::VarLong.decode(buf)?;
        let timestamp = batch_decode_info
            .base_timestamp
            .wrapping_add(timestamp_delta);

        // Offset delta
        let offset_delta: i32 = types::VarInt.decode(buf)?;
        let offset = batch_decode_info
            .min_offset
            .wrapping_add(i64::from(offset_delta));
        let sequence = if batch_decode_info.base_sequence < 0 {
            NO_SEQUENCE
        } else {
            increment_sequence(batch_decode_info.base_sequence, offset_delta)
        };

        // Key
        let key_len: i32 = types::VarInt.decode(buf)?;
        let key = if key_len < 0 {
            None
        } else {
            Some(buf.try_get_bytes(key_len as usize)?)
        };

        // Value
        let value_len: i32 = types::VarInt.decode(buf)?;
        let value = if value_len < 0 {
            None
        } else {
            Some(buf.try_get_bytes(value_len as usize)?)
        };

        // Headers
        let num_headers: i32 = types::VarInt.decode(buf)?;
        if num_headers < 0 {
            bail!("Unexpected negative record header count: {num_headers}");
        }
        let num_headers = num_headers as usize;
        if num_headers > buf.remaining() {
            bail!(
                "record header count {num_headers} exceeds remaining record body size {}",
                buf.remaining()
            );
        }

        let mut headers = Vec::with_capacity(num_headers);
        for _ in 0..num_headers {
            // Key len
            let key_len: i32 = types::VarInt.decode(buf)?;
            if key_len < 0 {
                bail!("Unexpected negative record header key length ({key_len} bytes)");
            }

            // Key
            let key = StrBytes::try_from(buf.try_get_bytes(key_len as usize)?)?;

            // Key len
            let value_len: i32 = types::VarInt.decode(buf)?;

            // Value
            let value = if value_len < 0 {
                None
            } else {
                Some(buf.try_get_bytes(value_len as usize)?)
            };

            headers.push((key, value));
        }

        if buf.has_remaining() {
            let trailing = buf.remaining();
            bail!(
                "record body has {trailing} trailing byte{} after its declared fields",
                if trailing == 1 { "" } else { "s" }
            );
        }

        Ok(Self {
            transactional: batch_decode_info.transactional,
            control: batch_decode_info.control,
            delete_horizon: batch_decode_info.delete_horizon,
            timestamp_type: batch_decode_info.timestamp_type,
            partition_leader_epoch: batch_decode_info.partition_leader_epoch,
            producer_id: batch_decode_info.producer_id,
            producer_epoch: batch_decode_info.producer_epoch,
            sequence,
            offset,
            timestamp,
            key,
            value,
            headers,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn round_trips_duplicate_headers_in_order() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: Default::default(),
            value: Default::default(),
            headers: [
                ("duplicate".into(), Some("first".into())),
                ("other-header".into(), None),
                ("duplicate".into(), Some("second".into())),
            ]
            .into(),
        };
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        let decoded = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap();
        assert_eq!(decoded[0].records[0].headers, record.headers);
    }

    #[test]
    fn round_trips_producer_sequence_rollover_in_one_batch() {
        let records = [
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: 7,
                producer_epoch: 2,
                sequence: i32::MAX,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                timestamp: 1,
                key: None,
                value: Some(Bytes::from_static(b"before-rollover")),
                headers: Vec::new(),
            },
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: 7,
                producer_epoch: 2,
                sequence: 0,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                timestamp: 2,
                key: None,
                value: Some(Bytes::from_static(b"after-rollover")),
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

        assert_eq!(i32::from_be_bytes(encoded[23..27].try_into().unwrap()), 1);
        assert_eq!(
            i32::from_be_bytes(encoded[53..57].try_into().unwrap()),
            i32::MAX
        );
        assert_eq!(i32::from_be_bytes(encoded[57..61].try_into().unwrap()), 2);
        let decoded = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].records, records);
    }

    #[test]
    fn decode_record_header_no_value() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: Default::default(),
            value: Default::default(),
            headers: [("other-header".into(), None)].into(),
        };
        let mut buf = &mut bytes::BytesMut::new();
        record
            .encode_new(
                buf,
                0,
                0,
                &RecordEncodeOptions {
                    version: 2,
                    compression: super::Compression::None,
                },
            )
            .expect("encode works");

        Record::decode_new(
            &mut buf,
            &BatchDecodeInfo {
                record_count: 1,
                timestamp_type: TimestampType::Creation,
                min_offset: 0,
                base_timestamp: 0,
                base_sequence: 0,
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: 0,
                producer_epoch: 0,
            },
            2,
        )
        .expect("decode works");
    }

    #[test]
    fn decodes_any_negative_nullable_record_length_as_null() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 0,
            key: None,
            value: None,
            headers: [("h".into(), None)].into(),
        };
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        const RECORDS_OFFSET: usize = 61;
        assert_eq!(
            &encoded[RECORDS_OFFSET..],
            &[18, 0, 0, 0, 1, 1, 2, 2, b'h', 1]
        );
        encoded[RECORDS_OFFSET + 4] = 3;
        encoded[RECORDS_OFFSET + 5] = 3;
        encoded[RECORDS_OFFSET + 9] = 3;
        let checksum = crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&checksum.to_be_bytes());

        let decoded = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap();
        let decoded = &decoded[0].records[0];
        assert_eq!(decoded.key, None);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.headers, [("h".into(), None)]);
    }

    #[test]
    fn round_trips_timestamp_delta_beyond_i32() {
        let timestamp_delta = i64::from(i32::MAX) + 1;
        let first_timestamp = 1_000 + timestamp_delta;
        let records = [
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: -1,
                producer_epoch: -1,
                sequence: -1,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                timestamp: first_timestamp,
                key: None,
                value: Some(Bytes::from_static(b"first")),
                headers: Vec::new(),
            },
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: -1,
                producer_epoch: -1,
                sequence: -1,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                timestamp: 1_000,
                key: None,
                value: Some(Bytes::from_static(b"second")),
                headers: Vec::new(),
            },
        ];
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::Snappy,
            },
        )
        .unwrap();

        assert_eq!(
            i64::from_be_bytes(encoded[27..35].try_into().unwrap()),
            first_timestamp
        );
        let decoded = RecordBatchDecoder::decode_all(&mut encoded.clone().freeze()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].compression, Compression::Snappy);
        assert_eq!(decoded[0].max_timestamp, first_timestamp);
        assert_eq!(decoded[0].records, records);

        encoded[..8].copy_from_slice(&i64::MAX.to_be_bytes());
        let decoded = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap();
        assert_eq!(
            decoded[0]
                .records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![i64::MAX, i64::MIN]
        );

        let mut reencoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut reencoded,
            decoded[0].records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::Zstd,
            },
        )
        .unwrap();
        assert_eq!(
            i64::from_be_bytes(reencoded[..8].try_into().unwrap()),
            i64::MAX
        );
        assert_eq!(i32::from_be_bytes(reencoded[23..27].try_into().unwrap()), 1);
        let round_trip = RecordBatchDecoder::decode_all(&mut reencoded.freeze()).unwrap();
        assert_eq!(round_trip[0].records, decoded[0].records);
    }

    #[test]
    fn rejects_valid_crc_record_and_batch_padding() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 1_000,
            key: None,
            value: Some(Bytes::from_static(b"padding")),
            headers: Vec::new(),
        };
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        for pad_record_body in [true, false] {
            let mut malformed = encoded.to_vec();
            if pad_record_body {
                const RECORDS_OFFSET: usize = 61;
                assert_eq!(malformed[RECORDS_OFFSET] & 0x80, 0);
                malformed[RECORDS_OFFSET] += 2;
            }
            malformed.push(0);
            let batch_length = i32::from_be_bytes(malformed[8..12].try_into().unwrap()) + 1;
            malformed[8..12].copy_from_slice(&batch_length.to_be_bytes());
            let checksum = crc32c(&malformed[21..]);
            malformed[17..21].copy_from_slice(&checksum.to_be_bytes());

            let error = RecordBatchDecoder::decode_all(&mut Bytes::from(malformed)).unwrap_err();
            let expected = if pad_record_body {
                "record body has 1 trailing byte"
            } else {
                "record batch has 1 trailing byte"
            };
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn rejects_impossible_header_count_before_allocation() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 1_000,
            key: None,
            value: Some(Bytes::from_static(b"headers")),
            headers: Vec::new(),
        };
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        const RECORDS_OFFSET: usize = 61;
        assert_eq!(encoded[RECORDS_OFFSET] & 0x80, 0);
        assert_eq!(encoded.last(), Some(&0));
        encoded[RECORDS_OFFSET] += 8;
        encoded.truncate(encoded.len() - 1);
        encoded.extend_from_slice(&[254, 255, 255, 255, 15]);
        let batch_length = i32::from_be_bytes(encoded[8..12].try_into().unwrap()) + 4;
        encoded[8..12].copy_from_slice(&batch_length.to_be_bytes());
        let crc = crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&crc.to_be_bytes());

        let error = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap_err();
        assert!(error
            .to_string()
            .contains("record header count 2147483647 exceeds remaining record body size 0"));
    }

    #[test]
    fn rejects_impossible_record_count_before_allocation() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 1_000,
            key: None,
            value: Some(Bytes::from_static(b"records")),
            headers: Vec::new(),
        };
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        encoded[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
        let crc = crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&crc.to_be_bytes());

        let error = RecordBatchDecoder::decode_all(&mut encoded.freeze()).unwrap_err();
        assert!(error
            .to_string()
            .starts_with("record count 2147483647 exceeds remaining record payload size "));
    }
}
