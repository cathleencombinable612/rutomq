use super::*;
use crate::Metrics;
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn shutdown_flushes_queued_requests_and_rejects_new_ones() {
    let mut config = AgentConfig {
        flush_interval: Duration::from_secs(60 * 60),
        ..AgentConfig::default()
    };
    config.cluster_id = "batcher-shutdown-test".to_owned();
    let batcher = ProduceBatcher::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
        Arc::new(Mutex::new(HashSet::new())),
    );
    let (response, received) = oneshot::channel();
    batcher
        .sender
        .send(BatcherCommand::Produce(ProduceCommand {
            request: ProduceRequest::default(),
            version: 3,
            flush_policy: ProduceFlushPolicy::default(),
            verify_transaction_partition: true,
            response,
        }))
        .await
        .unwrap();

    timeout(Duration::from_secs(1), batcher.shutdown())
        .await
        .expect("shutdown must bypass the one-hour flush window")
        .unwrap();
    received
        .await
        .expect("queued request must receive a response");

    let error = batcher
        .submit(
            ProduceRequest::default(),
            3,
            ProduceFlushPolicy::default(),
            true,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shutting down"));
}

#[test]
fn topic_flush_deadline_can_only_shorten_the_agent_window() {
    let now = Instant::now();
    let mut policy = ProduceFlushPolicy::default();
    policy.add_partition(PartitionKey::new("events", 0), 1, i64::MAX, 40);
    policy.add_partition(PartitionKey::new("other", 0), 1, i64::MAX, 10);
    assert_eq!(
        flush_deadline(Duration::from_millis(250), &policy, now),
        now + Duration::from_millis(10)
    );

    let default_policy = ProduceFlushPolicy::default();
    assert_eq!(
        flush_deadline(Duration::from_millis(250), &default_policy, now),
        now + Duration::from_millis(250)
    );
}

#[test]
fn message_threshold_is_accumulated_per_partition() {
    let mut state = FlushPolicyState::default();
    let mut first = ProduceFlushPolicy::default();
    first.add_partition(PartitionKey::new("events", 0), 2, 3, i64::MAX);
    assert!(!state.add(&first));

    let mut other_partition = ProduceFlushPolicy::default();
    other_partition.add_partition(PartitionKey::new("events", 1), 2, 3, i64::MAX);
    assert!(!state.add(&other_partition));

    let mut threshold = ProduceFlushPolicy::default();
    threshold.add_partition(PartitionKey::new("events", 0), 1, 3, i64::MAX);
    assert!(state.add(&threshold));
}

#[test]
fn zero_flush_interval_requests_immediate_commit() {
    let mut state = FlushPolicyState::default();
    let mut policy = ProduceFlushPolicy::default();
    policy.add_partition(PartitionKey::new("events", 0), 1, i64::MAX, 0);
    assert!(state.add(&policy));
}
