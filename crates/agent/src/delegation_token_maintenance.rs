use chrono::Utc;
use rutomq_control::MetadataStore;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tracing::warn;

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const SWEEP_LIMIT: usize = 1_000;

pub fn spawn(metadata: Arc<dyn MetadataStore>) {
    tokio::spawn(async move {
        let mut ticker = interval(SWEEP_INTERVAL);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = metadata
                .delete_expired_delegation_tokens(Utc::now().timestamp_millis(), SWEEP_LIMIT)
                .await
            {
                warn!(%error, "delegation token expiry sweep failed");
            }
        }
    });
}
