use crate::batcher::PendingObjects;
use crate::compaction_rewrite::compact_plan;
use crate::config::AgentConfig;
use crate::health::Metrics;
use anyhow::Result;
use chrono::Utc;
use rutomq_control::MetadataStore;
use rutomq_storage::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const MAX_PARTITIONS_PER_SWEEP: usize = 32;

pub fn spawn(
    metadata: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    config: AgentConfig,
    pending: PendingObjects,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        let interval = config.compaction_interval.max(Duration::from_millis(1));
        loop {
            tokio::time::sleep(interval).await;
            for _ in 0..MAX_PARTITIONS_PER_SWEEP {
                match compact_once(
                    &metadata,
                    &objects,
                    &config.cluster_id,
                    &pending,
                    config.compaction_lease,
                    config.compaction_max_object_bytes,
                    &metrics,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(error) => {
                        metrics.compaction_errors.inc();
                        warn!(%error, "record-key compaction failed");
                        break;
                    }
                }
            }
        }
    });
}

async fn compact_once(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    cluster_id: &str,
    pending: &PendingObjects,
    lease: Duration,
    max_object_bytes: usize,
    metrics: &Metrics,
) -> Result<bool> {
    let now_ms = Utc::now().timestamp_millis();
    let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX).max(1);
    let Some(plan) = metadata.claim_compaction(now_ms, lease_ms).await? else {
        return Ok(false);
    };
    let result = compact_plan(
        metadata,
        objects,
        cluster_id,
        pending,
        &plan,
        now_ms,
        max_object_bytes.max(1),
    )
    .await;
    match result {
        Ok(Some(outcome)) => {
            metrics.compaction_runs.inc();
            metrics
                .compaction_removed_records
                .inc_by(outcome.removed_records);
            metrics
                .compaction_bytes_written
                .inc_by(outcome.bytes_written);
            Ok(true)
        }
        Ok(None) => {
            metrics.compaction_conflicts.inc();
            metadata
                .release_compaction(&plan.partition, plan.lease_id)
                .await?;
            Ok(false)
        }
        Err(error) => {
            if let Err(release_error) = metadata
                .release_compaction(&plan.partition, plan.lease_id)
                .await
            {
                warn!(%release_error, "failed to release compaction lease");
            }
            Err(error)
        }
    }
}

#[cfg(test)]
#[path = "compaction_tests.rs"]
mod tests;
