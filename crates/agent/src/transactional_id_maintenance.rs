use crate::health::Metrics;
use chrono::Utc;
use rutomq_control::MetadataStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const SWEEP_LIMIT: usize = 1_000;

pub fn spawn(
    metadata: Arc<dyn MetadataStore>,
    expiration_ms: i64,
    check_interval: Duration,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(check_interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match metadata
                .expire_transactional_ids(Utc::now().timestamp_millis(), expiration_ms, SWEEP_LIMIT)
                .await
            {
                Ok(expired) if expired > 0 => {
                    metrics.expired_transactional_ids.inc_by(expired);
                }
                Ok(_) => {}
                Err(error) => {
                    metrics.transactional_id_maintenance_errors.inc();
                    warn!(%error, "transactional id expiry sweep failed");
                }
            }
        }
    });
}
