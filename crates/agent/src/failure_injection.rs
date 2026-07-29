use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::warn;

const PRODUCE_POST_COMMIT_CLIENT_ID: &str = "RUTOMQ_TEST_PRODUCE_DISCONNECT_AFTER_COMMIT_CLIENT_ID";

#[derive(Clone, Default)]
pub(crate) struct FailureInjection {
    produce_post_commit: Option<Arc<ProducePostCommit>>,
}

struct ProducePostCommit {
    client_id: String,
    armed: AtomicBool,
}

impl FailureInjection {
    pub(crate) fn from_env() -> Self {
        let produce_post_commit = std::env::var(PRODUCE_POST_COMMIT_CLIENT_ID)
            .ok()
            .filter(|client_id| !client_id.is_empty())
            .map(|client_id| {
                warn!(
                    client_id,
                    env = PRODUCE_POST_COMMIT_CLIENT_ID,
                    "test-only Produce post-commit disconnect is armed"
                );
                Arc::new(ProducePostCommit {
                    client_id,
                    armed: AtomicBool::new(true),
                })
            });
        Self {
            produce_post_commit,
        }
    }

    pub(crate) fn disconnect_after_committed_produce(&self, client_id: &str) -> bool {
        self.produce_post_commit.as_ref().is_some_and(|failpoint| {
            failpoint.client_id == client_id && failpoint.armed.swap(false, Ordering::AcqRel)
        })
    }
}
