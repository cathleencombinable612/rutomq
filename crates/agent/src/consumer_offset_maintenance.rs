use crate::health::Metrics;
use chrono::Utc;
use rutomq_control::MetadataStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const SWEEP_LIMIT: usize = 1_000;

pub fn spawn(
    metadata: Arc<dyn MetadataStore>,
    retention_minutes: i32,
    check_interval: Duration,
    metrics: Arc<Metrics>,
) {
    let retention_ms = i64::from(retention_minutes) * 60_000;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(check_interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match metadata
                .expire_consumer_offsets(Utc::now().timestamp_millis(), retention_ms, SWEEP_LIMIT)
                .await
            {
                Ok(expired) if expired > 0 => {
                    metrics.expired_consumer_offsets.inc_by(expired);
                }
                Ok(_) => {}
                Err(error) => {
                    metrics.consumer_offset_maintenance_errors.inc();
                    warn!(%error, "consumer offset expiration sweep failed");
                }
            }
        }
    });
}
