use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    INVALID_RECORD, INVALID_TIMESTAMP, MESSAGE_TOO_LARGE, UNSUPPORTED_COMPRESSION_TYPE,
};
use chrono::Utc;
use kafka_protocol::messages::ProduceResponse;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::records::{
    Compression, NO_TIMESTAMP, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType,
};
use rutomq_control::TopicConfig;

fn records(timestamp: i64, value_size: usize) -> Bytes {
    records_with_compression(timestamp, value_size, Compression::None)
}

fn records_with_compression(timestamp: i64, value_size: usize, compression: Compression) -> Bytes {
    records_with_timestamp_type(timestamp, value_size, compression, TimestampType::Creation)
}

fn records_with_timestamp_type(
    timestamp: i64,
    value_size: usize,
    compression: Compression,
    timestamp_type: TimestampType,
) -> Bytes {
    records_with_timestamps_and_type(&[timestamp], value_size, compression, timestamp_type)
}

fn records_with_timestamps_and_type(
    timestamps: &[i64],
    value_size: usize,
    compression: Compression,
    timestamp_type: TimestampType,
) -> Bytes {
    let records = timestamps
        .iter()
        .enumerate()
        .map(|(index, timestamp)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type,
            offset: index as i64,
            sequence: -1,
            timestamp: *timestamp,
            key: None,
            value: Some(Bytes::from(vec![index as u8 + 1; value_size])),
            headers: Vec::new(),
        })
        .collect::<Vec<_>>();
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

fn records_with_wide_timestamp_delta(first_timestamp: i64, delta: i64) -> Bytes {
    let records = [
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
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
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: 0,
            timestamp: first_timestamp + delta,
            key: None,
            value: Some(Bytes::from_static(b"second")),
            headers: vec![
                ("duplicate".into(), Some(Bytes::from_static(b"first"))),
                ("duplicate".into(), Some(Bytes::from_static(b"second"))),
            ],
        },
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 2,
            sequence: 1,
            timestamp: first_timestamp - 1,
            key: None,
            value: Some(Bytes::from_static(b"third")),
            headers: Vec::new(),
        },
    ];
    let mut output = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut output,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::Snappy,
        },
    )
    .unwrap();
    output.freeze()
}

fn fetch_partition(topic: &str) -> FetchRequest {
    FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(16 * 1_024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name(topic))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(16 * 1_024),
                ]),
        ])
}

fn produce(topic: &str, partitions: Vec<PartitionProduceData>) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name(topic))
                .with_partition_data(partitions),
        ])
}

#[tokio::test]
async fn produce_admission_returns_partition_errors_without_allocating_offsets() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-size", 2)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "admission-size",
            TopicConfig {
                max_message_bytes: 200,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let now = Utc::now().timestamp_millis();
    let request = produce(
        "admission-size",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(now, 8))),
            PartitionProduceData::default()
                .with_index(1)
                .with_records(Some(records(now, 1_024))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 1, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(response.responses.len(), 1);
    let partitions = &response.responses[0].partition_responses;
    assert_eq!(partitions.len(), 2);
    assert_eq!(
        partitions
            .iter()
            .find(|partition| partition.index == 0)
            .unwrap()
            .error_code,
        NO_ERROR
    );
    assert_eq!(
        partitions
            .iter()
            .find(|partition| partition.index == 1)
            .unwrap()
            .error_code,
        MESSAGE_TOO_LARGE
    );

    let retry = produce(
        "admission-size",
        vec![
            PartitionProduceData::default()
                .with_index(1)
                .with_records(Some(records(now, 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 2, &retry))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 0);
}

#[tokio::test]
async fn produce_rejects_keyless_records_only_while_compacted() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-compacted-key", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "admission-compacted-key",
            TopicConfig {
                cleanup_policy: "delete,compact".to_owned(),
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let request = produce(
        "admission-compacted-key",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(Utc::now().timestamp_millis(), 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 20, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, INVALID_RECORD);
    assert_eq!(partition.base_offset, -1);
    assert!(
        partition
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("without a key"))
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("admission-compacted-key", 0), -1)
            .await
            .unwrap(),
        0
    );

    broker
        .metadata
        .set_topic_config("admission-compacted-key", TopicConfig::default())
        .await
        .unwrap();
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 21, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 0);
}

#[tokio::test]
async fn produce_enforces_create_time_and_rewrites_log_append_time() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-time", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "admission-time",
            TopicConfig {
                message_timestamp_before_max_ms: 60_000,
                message_timestamp_after_max_ms: 60_000,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let stale = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(1, 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 3, &stale))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TIMESTAMP
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, -1);

    let missing = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(NO_TIMESTAMP, 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 4, &missing))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 0);

    let client_timestamp = Utc::now().timestamp_millis();
    let client_log_append = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_timestamps_and_type(
                    &[client_timestamp - 120_000, client_timestamp],
                    8,
                    Compression::Snappy,
                    TimestampType::LogAppend,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 5, &client_log_append))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 1);

    let invalid_client_log_append = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_timestamps_and_type(
                    &[client_timestamp, client_timestamp + 120_000],
                    8,
                    Compression::Snappy,
                    TimestampType::LogAppend,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(
            ApiKey::Produce,
            3,
            6,
            &invalid_client_log_append,
        ))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TIMESTAMP
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, -1);

    let missing_log_append = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_timestamp_type(
                    NO_TIMESTAMP,
                    8,
                    Compression::None,
                    TimestampType::LogAppend,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 7, &missing_log_append))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TIMESTAMP
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, -1);

    broker
        .metadata
        .set_topic_config(
            "admission-time",
            TopicConfig {
                message_timestamp_type: "LogAppendTime".to_owned(),
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let invalid_log_append = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_timestamp_type(
                    Utc::now().timestamp_millis(),
                    8,
                    Compression::None,
                    TimestampType::LogAppend,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 8, &invalid_log_append))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_TIMESTAMP
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, -1);

    let appended = produce(
        "admission-time",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(1, 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 9, &appended))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 3);
    let log_append_time = partition.log_append_time_ms;
    assert!(log_append_time > 1);

    let fetch = fetch_partition("admission-time");
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 10, &fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let mut fetched = response.responses[0].partitions[0].records.clone().unwrap();
    let batches = RecordBatchDecoder::decode_all(&mut fetched).unwrap();
    let fetched_records = batches
        .iter()
        .flat_map(|batch| &batch.records)
        .collect::<Vec<_>>();
    assert_eq!(fetched_records.len(), 4);
    assert_eq!(fetched_records[0].timestamp_type, TimestampType::Creation);
    assert_eq!(fetched_records[0].timestamp, NO_TIMESTAMP);
    assert_eq!(fetched_records[1].timestamp_type, TimestampType::Creation);
    assert_eq!(fetched_records[1].timestamp, client_timestamp - 120_000);
    assert_eq!(fetched_records[2].timestamp_type, TimestampType::Creation);
    assert_eq!(fetched_records[2].timestamp, client_timestamp);
    assert_eq!(fetched_records[3].timestamp_type, TimestampType::LogAppend);
    assert_eq!(fetched_records[3].timestamp, log_append_time);
}

#[tokio::test]
async fn produce_uses_kafka_wrapping_timestamp_window_arithmetic() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-time-wrap", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "admission-time-wrap",
            TopicConfig {
                message_timestamp_before_max_ms: 0,
                message_timestamp_after_max_ms: i64::MAX,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();

    let request = produce(
        "admission-time-wrap",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records(i64::MIN, 8))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 10, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 0);

    let response = broker
        .handle_request(request_frame(
            ApiKey::Fetch,
            4,
            11,
            &fetch_partition("admission-time-wrap"),
        ))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let mut fetched = response.responses[0].partitions[0].records.clone().unwrap();
    let batches = RecordBatchDecoder::decode_all(&mut fetched).unwrap();
    assert_eq!(batches[0].records[0].timestamp, i64::MIN);
}

#[tokio::test]
async fn produce_applies_topic_compression_to_fetched_batches() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-compression", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "admission-compression",
            TopicConfig {
                compression_type: "zstd".to_owned(),
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let now = Utc::now().timestamp_millis();
    let zstd_records = records_with_compression(now, 4 * 1_024, Compression::Zstd);
    let unsupported = produce(
        "admission-compression",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(zstd_records.clone())),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 6, 6, &unsupported))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 6, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, UNSUPPORTED_COMPRESSION_TYPE);
    assert_eq!(partition.base_offset, -1);
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("admission-compression", 0), -1)
            .await
            .unwrap(),
        0
    );
    assert!(broker.objects.list("").await.unwrap().is_empty());

    let request = produce(
        "admission-compression",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_compression(
                    now,
                    4 * 1_024,
                    Compression::Gzip,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 6, 7, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 6, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );

    let supported = produce(
        "admission-compression",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(zstd_records)),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 7, 8, &supported))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 7, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 1);

    let response = broker
        .handle_request(request_frame(
            ApiKey::Fetch,
            4,
            9,
            &fetch_partition("admission-compression"),
        ))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let mut fetched = response.responses[0].partitions[0].records.clone().unwrap();
    let batches = RecordBatchDecoder::decode_all(&mut fetched).unwrap();
    assert_eq!(batches.len(), 2);
    assert!(
        batches
            .iter()
            .all(|batch| batch.compression == Compression::Zstd)
    );
    assert!(batches.iter().all(|batch| {
        batch.records.len() == 1 && batch.records[0].value.as_ref().unwrap().len() == 4 * 1_024
    }));
}

#[tokio::test]
async fn produce_fetches_timestamp_delta_beyond_i32() {
    let broker = broker();
    broker
        .metadata
        .create_topic("admission-wide-timestamp", 1)
        .await
        .unwrap();
    let timestamp_delta = i64::from(i32::MAX) + 1;
    let latest_timestamp = Utc::now().timestamp_millis();
    let first_timestamp = latest_timestamp - timestamp_delta;
    broker
        .metadata
        .set_topic_config(
            "admission-wide-timestamp",
            TopicConfig {
                message_timestamp_before_max_ms: timestamp_delta + 60_000,
                compression_type: "zstd".to_owned(),
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let request = produce(
        "admission-wide-timestamp",
        vec![
            PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_with_wide_timestamp_delta(
                    first_timestamp,
                    timestamp_delta,
                ))),
        ],
    );
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 10, &request))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 0);

    let response = broker
        .handle_request(request_frame(
            ApiKey::Fetch,
            4,
            11,
            &fetch_partition("admission-wide-timestamp"),
        ))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let mut fetched = response.responses[0].partitions[0].records.clone().unwrap();
    assert_eq!(
        i64::from_be_bytes(fetched[27..35].try_into().unwrap()),
        first_timestamp
    );
    let batches = RecordBatchDecoder::decode_all(&mut fetched).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].compression, Compression::Zstd);
    assert_eq!(
        batches[0]
            .records
            .iter()
            .map(|record| (record.offset, record.timestamp))
            .collect::<Vec<_>>(),
        vec![
            (0, first_timestamp),
            (1, latest_timestamp),
            (2, first_timestamp - 1),
        ]
    );
    assert_eq!(
        batches[0].records[1].headers,
        vec![
            ("duplicate".into(), Some(Bytes::from_static(b"first"))),
            ("duplicate".into(), Some(Bytes::from_static(b"second"))),
        ]
    );
}
