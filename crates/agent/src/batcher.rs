use crate::config::AgentConfig;
use crate::health::Metrics;
use crate::kafka_error::{
    INVALID_RECORD, NO_ERROR, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION,
    control_error_code,
};
use crate::object_integrity;
use crate::records::analyze_records;
use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{BatchDraft, ControlError, MetadataStore, ObjectRef, PartitionKey};
use rutomq_protocol::records::TimestampType;
use rutomq_storage::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::debug;
use uuid::Uuid;

pub type PendingObjects = Arc<Mutex<HashSet<String>>>;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct ProduceInputError {
    code: i16,
    message: String,
}

struct ProduceCommand {
    request: ProduceRequest,
    version: i16,
    flush_policy: ProduceFlushPolicy,
    verify_transaction_partition: bool,
    response: oneshot::Sender<ProduceResponse>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProduceFlushPolicy {
    max_wait: Duration,
    partitions: Vec<PartitionFlushPolicy>,
}

#[derive(Debug, Clone)]
struct PartitionFlushPolicy {
    partition: PartitionKey,
    records: u64,
    max_messages: u64,
}

impl Default for ProduceFlushPolicy {
    fn default() -> Self {
        Self {
            max_wait: Duration::MAX,
            partitions: Vec::new(),
        }
    }
}

impl ProduceFlushPolicy {
    pub(crate) fn add_partition(
        &mut self,
        partition: PartitionKey,
        records: i32,
        max_messages: i64,
        max_wait_ms: i64,
    ) {
        debug_assert!(records > 0);
        debug_assert!(max_messages > 0);
        debug_assert!(max_wait_ms >= 0);
        self.max_wait = self.max_wait.min(Duration::from_millis(max_wait_ms as u64));
        self.partitions.push(PartitionFlushPolicy {
            partition,
            records: records as u64,
            max_messages: max_messages as u64,
        });
    }
}

#[derive(Default)]
struct FlushPolicyState {
    partitions: HashMap<PartitionKey, (u64, u64)>,
}

impl FlushPolicyState {
    fn add(&mut self, policy: &ProduceFlushPolicy) -> bool {
        let mut flush = policy.max_wait.is_zero();
        for partition in &policy.partitions {
            let state = self
                .partitions
                .entry(partition.partition.clone())
                .or_insert((0, partition.max_messages));
            state.0 = state.0.saturating_add(partition.records);
            state.1 = state.1.min(partition.max_messages);
            flush |= state.0 >= state.1;
        }
        flush
    }
}

enum BatcherCommand {
    Produce(ProduceCommand),
    Shutdown(oneshot::Sender<()>),
}

struct ProduceTarget {
    response: oneshot::Sender<ProduceResponse>,
    shape: Vec<(ProduceTopic, Vec<i32>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ProduceTopic {
    Name(String),
    Id(Uuid),
}

#[derive(Default)]
struct FlushBuffers {
    object: BytesMut,
    drafts: Vec<BatchDraft>,
    requested: Vec<RequestedPartition>,
}

struct RequestedPartition {
    request_index: usize,
    topic: ProduceTopic,
    partition_index: i32,
    log_append_time_ms: i64,
}

struct AppendContext<'a> {
    object: &'a mut BytesMut,
    drafts: &'a mut Vec<BatchDraft>,
    requested: &'a mut Vec<RequestedPartition>,
    request_index: usize,
    request_records: i32,
    transactional_id: Option<String>,
    verify_transaction_partition: bool,
}

#[derive(Clone)]
pub struct ProduceBatcher {
    sender: mpsc::Sender<BatcherCommand>,
    accepting: Arc<AtomicBool>,
}

#[derive(Clone)]
struct Backend {
    metadata: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    config: AgentConfig,
    metrics: Arc<Metrics>,
    pending: PendingObjects,
}

impl ProduceBatcher {
    pub fn new(
        metadata: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        config: AgentConfig,
        metrics: Arc<Metrics>,
        pending: PendingObjects,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let backend = Backend {
            metadata,
            objects,
            config,
            metrics,
            pending,
        };
        tokio::spawn(run(receiver, backend));
        Self {
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn submit(
        &self,
        request: ProduceRequest,
        version: i16,
        flush_policy: ProduceFlushPolicy,
        verify_transaction_partition: bool,
    ) -> Result<ProduceResponse> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(anyhow!("produce batcher is shutting down"));
        }
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(BatcherCommand::Produce(ProduceCommand {
                request,
                version,
                flush_policy,
                verify_transaction_partition,
                response,
            }))
            .await
            .map_err(|_| anyhow!("produce batcher is shut down"))?;
        receiver
            .await
            .map_err(|_| anyhow!("produce batcher dropped a request"))
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.stop_accepting();
        let (completed, receiver) = oneshot::channel();
        self.sender
            .send(BatcherCommand::Shutdown(completed))
            .await
            .map_err(|_| anyhow!("produce batcher is already shut down"))?;
        receiver
            .await
            .map_err(|_| anyhow!("produce batcher shutdown was interrupted"))
    }
}

async fn run(mut receiver: mpsc::Receiver<BatcherCommand>, backend: Backend) {
    let mut shutting_down = false;
    let mut shutdown_waiters = Vec::new();
    loop {
        let Some(first) =
            next_produce(&mut receiver, &mut shutting_down, &mut shutdown_waiters).await
        else {
            break;
        };
        let mut batch = vec![first];
        let mut size = request_size(&batch[0].request);
        let mut policies = FlushPolicyState::default();
        let mut should_flush = policies.add(&batch[0].flush_policy);
        let deadline = sleep_until_flush(
            backend.config.flush_interval,
            &batch[0].flush_policy,
            Instant::now(),
        );
        tokio::pin!(deadline);
        while !should_flush && size < backend.config.max_batch_bytes.max(1) {
            let command = if shutting_down {
                receiver.recv().await
            } else {
                tokio::select! {
                    command = receiver.recv() => command,
                    _ = &mut deadline => break,
                }
            };
            match command {
                Some(BatcherCommand::Produce(command)) => {
                    let command_deadline = flush_deadline(
                        backend.config.flush_interval,
                        &command.flush_policy,
                        Instant::now(),
                    );
                    if command_deadline < deadline.deadline() {
                        deadline.as_mut().reset(command_deadline);
                    }
                    should_flush = policies.add(&command.flush_policy);
                    size += request_size(&command.request);
                    batch.push(command);
                }
                Some(BatcherCommand::Shutdown(waiter)) => {
                    shutting_down = true;
                    receiver.close();
                    shutdown_waiters.push(waiter);
                }
                None => break,
            }
        }
        flush(batch, &backend).await;
    }
    for waiter in shutdown_waiters {
        let _ = waiter.send(());
    }
}

fn sleep_until_flush(
    agent_interval: Duration,
    policy: &ProduceFlushPolicy,
    now: Instant,
) -> tokio::time::Sleep {
    tokio::time::sleep_until(flush_deadline(agent_interval, policy, now))
}

fn flush_deadline(agent_interval: Duration, policy: &ProduceFlushPolicy, now: Instant) -> Instant {
    now + agent_interval.min(policy.max_wait)
}

async fn next_produce(
    receiver: &mut mpsc::Receiver<BatcherCommand>,
    shutting_down: &mut bool,
    shutdown_waiters: &mut Vec<oneshot::Sender<()>>,
) -> Option<ProduceCommand> {
    loop {
        match receiver.recv().await {
            Some(BatcherCommand::Produce(command)) => return Some(command),
            Some(BatcherCommand::Shutdown(waiter)) => {
                *shutting_down = true;
                receiver.close();
                shutdown_waiters.push(waiter);
            }
            None => return None,
        }
    }
}

async fn flush(commands: Vec<ProduceCommand>, backend: &Backend) {
    let _flush_timer = backend.metrics.produce_flush_duration.start_timer();
    let mut buffers = FlushBuffers::default();
    let mut targets = Vec::new();

    for command in commands {
        let request_index = targets.len();
        let shape = request_shape(&command.request, command.version);
        match append_request(
            backend,
            command.request,
            command.version,
            command.verify_transaction_partition,
            &mut buffers,
            request_index,
        )
        .await
        {
            Ok(()) => targets.push(ProduceTarget {
                response: command.response,
                shape,
            }),
            Err(error) => {
                let code = error
                    .downcast_ref::<ProduceInputError>()
                    .map(|error| error.code)
                    .unwrap_or(INVALID_RECORD);
                let _ = command
                    .response
                    .send(error_response(shape, code, &error.to_string()));
            }
        }
    }
    if buffers.drafts.is_empty() {
        return;
    }
    let FlushBuffers {
        object,
        drafts,
        requested,
    } = buffers;

    let object_key = format!("data/{}/{}.rlog", backend.config.cluster_id, Uuid::new_v4());
    let staged_object = ObjectRef {
        key: object_key.clone(),
        size: u64::try_from(object.len()).expect("batch size fits in u64"),
    };
    if let Err(error) = backend
        .metadata
        .stage_object(staged_object)
        .await
        .context("stage object upload intent")
    {
        send_errors(targets, UNKNOWN_SERVER_ERROR, &error.to_string());
        return;
    }
    backend
        .pending
        .lock()
        .expect("pending object lock is not poisoned")
        .insert(object_key.clone());
    let object_metadata = match backend
        .objects
        .put_immutable(&object_key, object.freeze())
        .await
        .context("persist produce object to object storage")
    {
        Ok(metadata) => metadata,
        Err(error) => {
            backend
                .pending
                .lock()
                .expect("pending object lock is not poisoned")
                .remove(&object_key);
            send_errors(targets, UNKNOWN_SERVER_ERROR, &error.to_string());
            return;
        }
    };
    let commit_timer = backend
        .metrics
        .produce_metadata_commit_duration
        .start_timer();
    let committed = backend
        .metadata
        .commit_object(
            ObjectRef {
                key: object_metadata.key,
                size: object_metadata.size,
            },
            drafts,
        )
        .await
        .context("commit object metadata");
    commit_timer.observe_duration();
    let spans = match committed {
        Ok(spans) => spans,
        Err(error) => {
            backend
                .pending
                .lock()
                .expect("pending object lock is not poisoned")
                .remove(&object_key);
            let code = error
                .downcast_ref::<ControlError>()
                .map(control_error_code)
                .unwrap_or(UNKNOWN_SERVER_ERROR);
            send_errors(targets, code, &error.to_string());
            return;
        }
    };
    backend
        .pending
        .lock()
        .expect("pending object lock is not poisoned")
        .remove(&object_key);
    if spans.len() != requested.len() {
        send_errors(
            targets,
            UNKNOWN_SERVER_ERROR,
            "metadata commit returned an incomplete span set",
        );
        return;
    }
    if spans.iter().any(|span| span.object_key == object_key) {
        backend.metrics.committed_objects.inc();
    }
    let mut grouped = (0..targets.len())
        .map(|_| HashMap::<ProduceTopic, Vec<PartitionProduceResponse>>::new())
        .collect::<Vec<_>>();
    for (span, requested) in spans.into_iter().zip(requested) {
        grouped[requested.request_index]
            .entry(requested.topic)
            .or_default()
            .push(
                PartitionProduceResponse::default()
                    .with_index(requested.partition_index)
                    .with_error_code(NO_ERROR)
                    .with_base_offset(span.base_offset)
                    .with_log_append_time_ms(requested.log_append_time_ms)
                    .with_log_start_offset(0),
            );
    }
    for (target, topics) in targets.into_iter().zip(grouped) {
        let responses = topics
            .into_iter()
            .map(|(topic, partitions)| topic_response(topic, partitions))
            .collect();
        let _ = target
            .response
            .send(ProduceResponse::default().with_responses(responses));
    }
}

async fn append_request(
    backend: &Backend,
    request: ProduceRequest,
    version: i16,
    verify_transaction_partition: bool,
    buffers: &mut FlushBuffers,
    request_index: usize,
) -> Result<()> {
    let object_start = buffers.object.len();
    let drafts_start = buffers.drafts.len();
    let requested_start = buffers.requested.len();
    let result = async {
        let transactional_id = request
            .transactional_id
            .as_ref()
            .map(|transactional_id| transactional_id.as_str().to_owned());
        let mut context = AppendContext {
            object: &mut buffers.object,
            drafts: &mut buffers.drafts,
            requested: &mut buffers.requested,
            request_index,
            request_records: 0,
            transactional_id,
            verify_transaction_partition,
        };
        for topic in request.topic_data {
            let (topic_info, response_topic) = if version >= 13 {
                let topic_info = backend
                    .metadata
                    .topic_by_id(topic.topic_id)
                    .await?
                    .ok_or_else(|| {
                        produce_input_error(
                            UNKNOWN_TOPIC_ID,
                            format!("topic ID {} was not found", topic.topic_id),
                        )
                    })?;
                (topic_info, ProduceTopic::Id(topic.topic_id))
            } else {
                let topic_name = topic.name.as_str().to_owned();
                let topic_info = backend.metadata.topic(&topic_name).await?.ok_or_else(|| {
                    produce_input_error(
                        UNKNOWN_TOPIC_OR_PARTITION,
                        format!("topic {topic_name} was not found"),
                    )
                })?;
                (topic_info, ProduceTopic::Name(topic_name))
            };
            append_topic(
                topic,
                &topic_info.name,
                topic_info.partitions,
                response_topic,
                &mut context,
            )?;
        }
        if context.request_records == 0 {
            return Err(produce_input_error(
                INVALID_RECORD,
                "produce request contains no records",
            ));
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        buffers.object.truncate(object_start);
        buffers.drafts.truncate(drafts_start);
        buffers.requested.truncate(requested_start);
    }
    result
}

fn append_topic(
    topic: TopicProduceData,
    topic_name: &str,
    partition_count: i32,
    response_topic: ProduceTopic,
    context: &mut AppendContext<'_>,
) -> Result<()> {
    for partition in topic.partition_data {
        if partition.index < 0 || partition.index >= partition_count {
            return Err(produce_input_error(
                UNKNOWN_TOPIC_OR_PARTITION,
                format!(
                    "partition {} is outside topic {} with {} partitions",
                    partition.index, topic_name, partition_count
                ),
            ));
        }
        let key = PartitionKey::new(topic_name, partition.index);
        let records: Bytes = partition.records.unwrap_or_default();
        let metadata = analyze_records(&records).map_err(|error| {
            debug!(
                topic = topic_name,
                partition = partition.index,
                %error,
                "invalid produce records"
            );
            produce_input_error(INVALID_RECORD, error.to_string())
        })?;
        if metadata.record_count <= 0 {
            return Err(produce_input_error(
                INVALID_RECORD,
                "produce request contains no records",
            ));
        }
        match (&context.transactional_id, metadata.transactional) {
            (Some(_), false) => {
                return Err(produce_input_error(
                    INVALID_RECORD,
                    "transactional produce requires transactional record batches",
                ));
            }
            (None, true) => {
                return Err(produce_input_error(
                    INVALID_RECORD,
                    "transactional record batch requires transactional_id",
                ));
            }
            _ => {}
        }
        let start = context.object.len() as u64;
        context.object.extend_from_slice(&records);
        let end = context.object.len() as u64;
        context.drafts.push(BatchDraft {
            partition: key.clone(),
            byte_start: start,
            byte_end: end,
            record_count: metadata.record_count,
            timestamp_ms: metadata
                .max_timestamp_ms
                .expect("non-empty record batches have a timestamp"),
            checksum: Some(object_integrity::checksum(&records)),
            producer: metadata.producer,
            transactional_id: context.transactional_id.clone(),
            verify_transaction_partition: context.verify_transaction_partition,
        });
        context.requested.push(RequestedPartition {
            request_index: context.request_index,
            topic: response_topic.clone(),
            partition_index: partition.index,
            log_append_time_ms: if metadata.timestamp_type == Some(TimestampType::LogAppend) {
                metadata
                    .max_timestamp_ms
                    .expect("LogAppendTime records have a timestamp")
            } else {
                -1
            },
        });
        context.request_records += metadata.record_count;
    }
    Ok(())
}

fn produce_input_error(code: i16, message: impl Into<String>) -> anyhow::Error {
    ProduceInputError {
        code,
        message: message.into(),
    }
    .into()
}

fn request_size(request: &ProduceRequest) -> usize {
    request
        .topic_data
        .iter()
        .flat_map(|topic| topic.partition_data.iter())
        .filter_map(|partition| partition.records.as_ref())
        .map(Bytes::len)
        .sum()
}

fn request_shape(request: &ProduceRequest, version: i16) -> Vec<(ProduceTopic, Vec<i32>)> {
    request
        .topic_data
        .iter()
        .map(|topic| {
            (
                if version >= 13 {
                    ProduceTopic::Id(topic.topic_id)
                } else {
                    ProduceTopic::Name(topic.name.as_str().to_owned())
                },
                topic
                    .partition_data
                    .iter()
                    .map(|partition| partition.index)
                    .collect(),
            )
        })
        .collect()
}

fn error_response(
    shape: Vec<(ProduceTopic, Vec<i32>)>,
    code: i16,
    message: &str,
) -> ProduceResponse {
    ProduceResponse::default().with_responses(
        shape
            .into_iter()
            .map(|(topic, partitions)| {
                let partitions = partitions
                    .into_iter()
                    .map(|partition| {
                        PartitionProduceResponse::default()
                            .with_index(partition)
                            .with_error_code(code)
                            .with_error_message(Some(StrBytes::from_string(message.to_owned())))
                    })
                    .collect();
                topic_response(topic, partitions)
            })
            .collect(),
    )
}

fn send_errors(targets: Vec<ProduceTarget>, code: i16, error: &str) {
    for target in targets {
        let _ = target
            .response
            .send(error_response(target.shape, code, error));
    }
}

fn topic_name(value: &str) -> kafka_protocol::messages::TopicName {
    kafka_protocol::messages::TopicName::from(StrBytes::from_string(value.to_owned()))
}

fn topic_response(
    topic: ProduceTopic,
    partitions: Vec<PartitionProduceResponse>,
) -> TopicProduceResponse {
    let response = TopicProduceResponse::default().with_partition_responses(partitions);
    match topic {
        ProduceTopic::Name(name) => response.with_name(topic_name(&name)),
        ProduceTopic::Id(topic_id) => response.with_topic_id(topic_id),
    }
}

#[cfg(test)]
#[path = "batcher_tests.rs"]
mod tests;
