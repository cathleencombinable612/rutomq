use chrono::Utc;
use rutomq_control::MetadataStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const SWEEP_LIMIT: usize = 1_000;

pub fn spawn(metadata: Arc<dyn MetadataStore>, expiration_ms: i64, check_interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(check_interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = metadata
                .expire_producer_sequences(
                    Utc::now().timestamp_millis(),
                    expiration_ms,
                    SWEEP_LIMIT,
                )
                .await
            {
                warn!(%error, "producer state expiry sweep failed");
            }
        }
    });
}
