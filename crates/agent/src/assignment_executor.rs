use crate::health::Metrics;
use rutomq_control::{AssignmentProtocol, GroupAssignmentTask, MetadataStore};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Clone)]
pub(crate) struct AssignmentExecutor {
    sender: mpsc::Sender<QueuedAssignment>,
    in_flight: Arc<Mutex<HashSet<(AssignmentProtocol, String)>>>,
    metrics: Arc<Metrics>,
}

struct QueuedAssignment {
    task: GroupAssignmentTask,
    queued_at: Instant,
}

impl AssignmentExecutor {
    pub(crate) fn new(
        metadata: Arc<dyn MetadataStore>,
        worker_count: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        let capacity = worker_count.saturating_mul(64).max(1);
        let (sender, receiver) = mpsc::channel::<QueuedAssignment>(capacity);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let in_flight = Arc::new(Mutex::new(HashSet::new()));
        let active = Arc::new(AtomicUsize::new(0));
        metrics.group_assignment_background_idle_ratio.set(1.0);
        for _ in 0..worker_count {
            spawn_worker(
                receiver.clone(),
                metadata.clone(),
                in_flight.clone(),
                active.clone(),
                worker_count,
                metrics.clone(),
            );
        }
        Self {
            sender,
            in_flight,
            metrics,
        }
    }

    pub(crate) fn submit(&self, task: GroupAssignmentTask) {
        let key = (task.protocol, task.group_id.clone());
        {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !in_flight.insert(key.clone()) {
                return;
            }
        }
        let protocol = task.protocol.as_str();
        match self.sender.try_send(QueuedAssignment {
            task,
            queued_at: Instant::now(),
        }) {
            Ok(()) => self.metrics.group_assignment_background_queued.inc(),
            Err(error) => {
                self.in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&key);
                self.metrics
                    .group_assignment_background_completions
                    .with_label_values(&[protocol, "queue_rejected"])
                    .inc();
                warn!(protocol, reason = %error, "group assignment background queue rejected work");
            }
        }
    }
}

fn spawn_worker(
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<QueuedAssignment>>>,
    metadata: Arc<dyn MetadataStore>,
    in_flight: Arc<Mutex<HashSet<(AssignmentProtocol, String)>>>,
    active: Arc<AtomicUsize>,
    worker_count: usize,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        loop {
            let Some(queued) = receiver.lock().await.recv().await else {
                break;
            };
            metrics.group_assignment_background_queued.dec();
            let protocol = queued.task.protocol;
            let protocol_name = protocol.as_str();
            metrics
                .group_assignment_background_queue_duration
                .with_label_values(&[protocol_name])
                .observe(queued.queued_at.elapsed().as_secs_f64());
            let active_count = active.fetch_add(1, Ordering::AcqRel) + 1;
            update_worker_metrics(&metrics, active_count, worker_count);

            let group_id = queued.task.group_id.clone();
            let task = queued.task;
            let handle = Handle::current();
            let metadata = metadata.clone();
            let started = Instant::now();
            let completion = tokio::task::spawn_blocking(move || {
                handle.block_on(metadata.complete_group_assignment(task))
            })
            .await;
            metrics
                .group_assignment_background_processing_duration
                .with_label_values(&[protocol_name])
                .observe(started.elapsed().as_secs_f64());
            match completion {
                Ok(Ok(result)) => metrics
                    .group_assignment_background_completions
                    .with_label_values(&[protocol_name, result.as_str()])
                    .inc(),
                Ok(Err(error)) => {
                    metrics
                        .group_assignment_background_completions
                        .with_label_values(&[protocol_name, "error"])
                        .inc();
                    warn!(
                        protocol = protocol_name,
                        group = group_id,
                        reason = %error,
                        "group assignment background work failed"
                    );
                }
                Err(error) => {
                    metrics
                        .group_assignment_background_completions
                        .with_label_values(&[protocol_name, "worker_failure"])
                        .inc();
                    warn!(
                        protocol = protocol_name,
                        group = group_id,
                        reason = %error,
                        "group assignment background worker failed"
                    );
                }
            }
            in_flight
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&(protocol, group_id));
            let active_count = active.fetch_sub(1, Ordering::AcqRel) - 1;
            update_worker_metrics(&metrics, active_count, worker_count);
        }
    });
}

fn update_worker_metrics(metrics: &Metrics, active: usize, workers: usize) {
    metrics
        .group_assignment_background_active
        .set(i64::try_from(active).unwrap_or(i64::MAX));
    metrics
        .group_assignment_background_idle_ratio
        .set((workers.saturating_sub(active) as f64) / workers as f64);
}
