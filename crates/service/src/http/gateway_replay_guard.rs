//! Replay protection belongs to one inbound request, across its account and URL attempts.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) const AMBIGUOUS_UPSTREAM_DELIVERY_MESSAGE: &str =
    "upstream request may already have been processed; automatic replay stopped after transport failure before response headers";

#[derive(Debug, Default, Clone)]
pub(crate) struct UpstreamReplayGuard {
    blocked: Arc<AtomicBool>,
}

tokio::task_local! {
    static CURRENT_REPLAY_GUARD: UpstreamReplayGuard;
}

pub(crate) async fn scope_upstream_replay_guard<F: Future>(
    guard: UpstreamReplayGuard,
    future: F,
) -> F::Output {
    CURRENT_REPLAY_GUARD.scope(guard, future).await
}

pub(crate) fn current_upstream_replay_guard() -> Option<UpstreamReplayGuard> {
    CURRENT_REPLAY_GUARD.try_with(Clone::clone).ok()
}

impl UpstreamReplayGuard {
    pub(crate) fn block_after_ambiguous_delivery(&self) {
        self.blocked.store(true, Ordering::Release);
    }

    pub(crate) fn is_blocked(&self) -> bool {
        self.blocked.load(Ordering::Acquire)
    }
}
