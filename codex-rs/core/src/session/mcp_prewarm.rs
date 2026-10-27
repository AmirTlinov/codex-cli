//! Best-effort MCP prewarming.
//!
//! A bounded channel coalesces refresh requests. The worker only prepares the
//! newest thread state; exact model steps remain the correctness path.

use super::*;

impl Session {
    pub(crate) fn request_mcp_runtime_refresh(&self) {
        // Plugin changes can reuse connections but still change their skill resources.
        self.services.mcp_runtime.invalidate_resource_caches();
        self.request_mcp_runtime_reprojection();
    }

    /// Reproject contributor state without invalidating cached MCP resources.
    pub(crate) fn request_mcp_runtime_reprojection(&self) {
        self.mark_mcp_runtime_dirty();
        self.schedule_mcp_prewarm();
    }

    pub(super) fn start_mcp_prewarm_worker(
        self: &Arc<Self>,
        requests: async_channel::Receiver<()>,
    ) {
        let session = Arc::downgrade(self);
        let shutdown = self.mcp_prewarm_shutdown.clone();
        let worker = self.services.runtime_handle.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    request = requests.recv() => {
                        if request.is_err() {
                            break;
                        }
                    },
                };
                let Some(session) = session.upgrade() else {
                    break;
                };
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = session.refresh_mcp_if_dirty() => {},
                }
            }
        });
        *self
            .mcp_prewarm_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
    }

    pub(super) fn schedule_mcp_prewarm(&self) {
        let _ = self.mcp_prewarm_tx.try_send(());
    }

    pub(super) async fn stop_mcp_prewarm_worker(&self) {
        self.mcp_prewarm_shutdown.cancel();
        let worker = self
            .mcp_prewarm_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker
            && let Err(error) = worker.await
        {
            warn!(%error, "MCP prewarm worker stopped unexpectedly");
        }
    }
}
