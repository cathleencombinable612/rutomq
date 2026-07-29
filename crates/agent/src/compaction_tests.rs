use super::*;
use crate::compaction_rewrite::compact_plan;
use crate::object_integrity;
use crate::records::{decode_stored_records, encode_records};
use bytes::{Bytes, BytesMut};
use rutomq_control::{
    BatchDraft, FetchIsolation, MemoryMetadataStore, ObjectRef, PartitionKey, TopicConfig,
};
use rutomq_protocol::records::{Record, TimestampType};
use rutomq_storage::OpenDalObjectStore;
use std::collections::HashSet;
use std::sync::Mutex;

fn record(key: &'static [u8], value: Option<&'static [u8]>) -> Record {
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
        timestamp: 1,
        key: Some(Bytes::from_static(key)),
        value: value.map(Bytes::from_static),
        headers: Vec::new(),
    }
}

async fn append(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    object_key: &str,
    timestamp_ms: i64,
    records: Vec<Record>,
) {
    let mut object = BytesMut::new();
    let mut drafts = Vec::new();
    for record in records {
        let encoded = encode_records(&[record]).unwrap();
        let byte_start = object.len() as u64;
        object.extend_from_slice(&encoded);
        drafts.push(BatchDraft {
            partition: PartitionKey::new("events", 0),
            byte_start,
            byte_end: object.len() as u64,
            record_count: 1,
            timestamp_ms,
            checksum: Some(object_integrity::checksum(&encoded)),
            producer: None,
            transactional_id: None,
            verify_transaction_partition: true,
        });
    }
    objects
        .put_immutable(object_key, object.freeze())
        .await
        .unwrap();
    metadata
        .commit_object(
            ObjectRef {
                key: object_key.to_owned(),
                size: drafts.last().unwrap().byte_end,
            },
            drafts,
        )
        .await
        .unwrap();
}

async fn fixture(
    delete_retention_ms: i64,
    timestamp_ms: i64,
    records: Vec<Record>,
) -> (Arc<dyn MetadataStore>, Arc<dyn ObjectStore>, PendingObjects) {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("events", 1).await.unwrap();
    metadata
        .set_topic_config(
            "events",
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                delete_retention_ms,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let objects: Arc<dyn ObjectStore> = Arc::new(OpenDalObjectStore::memory().unwrap());
    append(
        &metadata,
        &objects,
        "data/test/source.rlog",
        timestamp_ms,
        records,
    )
    .await;
    let pending = Arc::new(Mutex::new(HashSet::new()));
    (metadata, objects, pending)
}

async fn read_records(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
) -> (Vec<Record>, i64, i64) {
    let fetched = metadata
        .fetch(
            &PartitionKey::new("events", 0),
            0,
            usize::MAX,
            FetchIsolation::ReadUncommitted,
        )
        .await
        .unwrap();
    let mut records = Vec::new();
    for span in &fetched.spans {
        let raw = objects
            .get_range(&span.object_key, span.byte_start..span.byte_end)
            .await
            .unwrap();
        records
            .extend(decode_stored_records(&raw, span.base_offset, span.offsets_preserved).unwrap());
    }
    (records, fetched.high_watermark, fetched.log_start_offset)
}

#[tokio::test]
async fn compaction_keeps_latest_key_without_renumbering_offsets() {
    let (metadata, objects, pending) = fixture(
        10_000,
        900,
        vec![record(b"same", Some(b"old")), record(b"same", Some(b"new"))],
    )
    .await;
    let plan = metadata
        .claim_compaction(1_000, 1_000)
        .await
        .unwrap()
        .unwrap();
    let outcome = compact_plan(&metadata, &objects, "test", &pending, &plan, 1_000, 1024)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(outcome.removed_records, 1);
    let (records, high_watermark, log_start_offset) = read_records(&metadata, &objects).await;
    assert_eq!(high_watermark, 2);
    assert_eq!(log_start_offset, 0);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 1);
    assert_eq!(records[0].value.as_deref(), Some(&b"new"[..]));
}

#[tokio::test]
async fn compaction_waits_for_dirty_ratio_and_maximum_lag_forces_progress() {
    let (metadata, objects, pending) = fixture(
        10_000,
        900,
        vec![record(b"same", Some(b"old")), record(b"same", Some(b"new"))],
    )
    .await;
    let plan = metadata
        .claim_compaction(1_000, 1_000)
        .await
        .unwrap()
        .unwrap();
    compact_plan(&metadata, &objects, "test", &pending, &plan, 1_000, 1024)
        .await
        .unwrap()
        .unwrap();
    append(
        &metadata,
        &objects,
        "data/test/dirty.rlog",
        1_100,
        vec![record(b"other", Some(b"dirty"))],
    )
    .await;
    metadata
        .set_topic_config(
            "events",
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.9,
                max_compaction_lag_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();

    assert!(
        metadata
            .claim_compaction(1_499, 1_000)
            .await
            .unwrap()
            .is_none()
    );
    metadata
        .set_topic_config(
            "events",
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.4,
                max_compaction_lag_ms: 10_000,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let ratio_plan = metadata
        .claim_compaction(1_499, 1_000)
        .await
        .unwrap()
        .unwrap();
    metadata
        .release_compaction(&ratio_plan.partition, ratio_plan.lease_id)
        .await
        .unwrap();

    metadata
        .set_topic_config(
            "events",
            TopicConfig {
                cleanup_policy: "compact".to_owned(),
                min_cleanable_dirty_ratio: 0.9,
                max_compaction_lag_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(
        metadata
            .claim_compaction(1_599, 1_000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        metadata
            .claim_compaction(1_600, 1_000)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn tombstone_is_rechecked_and_removed_after_delete_retention() {
    let (metadata, objects, pending) = fixture(
        100,
        950,
        vec![record(b"same", Some(b"value")), record(b"same", None)],
    )
    .await;
    let plan = metadata
        .claim_compaction(1_000, 1_000)
        .await
        .unwrap()
        .unwrap();
    compact_plan(&metadata, &objects, "test", &pending, &plan, 1_000, 1024)
        .await
        .unwrap()
        .unwrap();
    let (records, _, _) = read_records(&metadata, &objects).await;
    assert_eq!(records.len(), 1);
    assert!(records[0].value.is_none());
    assert!(
        metadata
            .claim_compaction(1_049, 1_000)
            .await
            .unwrap()
            .is_none()
    );

    let plan = metadata
        .claim_compaction(1_050, 1_000)
        .await
        .unwrap()
        .unwrap();
    compact_plan(&metadata, &objects, "test", &pending, &plan, 1_050, 1024)
        .await
        .unwrap()
        .unwrap();
    let (records, high_watermark, log_start_offset) = read_records(&metadata, &objects).await;
    assert!(records.is_empty());
    assert_eq!(high_watermark, 2);
    assert_eq!(log_start_offset, 0);
}
