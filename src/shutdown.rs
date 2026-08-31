//! Process shutdown coordination shared by the HTTP readiness path and the
//! durable ingest replayer.

use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ShutdownPhase {
    Running,
    Stopping,
    Draining,
    Finished,
}

#[derive(Clone)]
pub struct ShutdownController {
    phase: watch::Sender<ShutdownPhase>,
}

impl ShutdownController {
    pub fn new() -> Self {
        let (phase, _) = watch::channel(ShutdownPhase::Running);
        Self { phase }
    }

    pub fn phase(&self) -> ShutdownPhase {
        *self.phase.borrow()
    }

    pub fn is_requested(&self) -> bool {
        self.phase() >= ShutdownPhase::Stopping
    }

    /// Stop readiness and reject new work. Returns true only for the first
    /// shutdown request.
    pub fn request(&self) -> bool {
        if self.phase() != ShutdownPhase::Running {
            return false;
        }
        self.phase.send_replace(ShutdownPhase::Stopping);
        true
    }

    pub fn begin_drain(&self) {
        if self.phase() < ShutdownPhase::Draining {
            self.phase.send_replace(ShutdownPhase::Draining);
        }
    }

    pub fn finish(&self) {
        self.phase.send_replace(ShutdownPhase::Finished);
    }

    pub async fn wait_for_request(&self) {
        self.wait_until(|phase| phase >= ShutdownPhase::Stopping)
            .await;
    }

    pub async fn wait_for_drain(&self) {
        self.wait_until(|phase| phase >= ShutdownPhase::Draining)
            .await;
    }

    async fn wait_until(&self, predicate: impl Fn(ShutdownPhase) -> bool) {
        let mut rx = self.phase.subscribe();
        loop {
            if predicate(*rx.borrow()) {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for ShutdownController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{ShutdownController, ShutdownPhase};

    #[tokio::test]
    async fn transitions_are_monotonic_and_idempotent() {
        let shutdown = ShutdownController::new();
        assert_eq!(shutdown.phase(), ShutdownPhase::Running);
        assert!(shutdown.request());
        assert!(!shutdown.request());
        assert_eq!(shutdown.phase(), ShutdownPhase::Stopping);
        shutdown.begin_drain();
        shutdown.begin_drain();
        assert_eq!(shutdown.phase(), ShutdownPhase::Draining);
        shutdown.finish();
        assert_eq!(shutdown.phase(), ShutdownPhase::Finished);
    }
}
