use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::AcquireError;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;

/// Owns MCP invalidation and the single gate used to publish runtime updates.
pub(super) struct McpRefresh {
    pending: AtomicBool,
    source_revision: AtomicU64,
    auth_generation: AtomicU64,
    gate: Semaphore,
}

impl McpRefresh {
    pub(super) fn new(source_revision: u64, auth_generation: u64) -> Self {
        Self {
            pending: AtomicBool::new(false),
            source_revision: AtomicU64::new(source_revision),
            auth_generation: AtomicU64::new(auth_generation),
            gate: Semaphore::new(/*permits*/ 1),
        }
    }

    pub(super) fn invalidate(&self) {
        self.pending.store(true, Ordering::Release);
    }

    /// Called under the publication gate. A concurrent invalidation remains
    /// visible on the next iteration; failed publication restores pending work.
    pub(super) fn observe_source_revision(&self, revision: u64) -> bool {
        self.source_revision.swap(revision, Ordering::AcqRel) != revision
    }

    pub(super) fn observe_auth_generation(&self, generation: u64) -> bool {
        self.auth_generation.swap(generation, Ordering::AcqRel) != generation
    }

    #[cfg(test)]
    pub(super) fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    pub(super) fn claim(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    #[tracing::instrument(name = "mcp.runtime.refresh_wait", skip_all)]
    pub(super) async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        self.gate.acquire().await
    }

    pub(super) fn close(&self) {
        self.gate.close();
    }
}

/// Restores a claimed refresh when its task is cancelled before publication.
pub(super) struct McpRefreshInvalidationGuard<'a> {
    pub(super) refresh: &'a McpRefresh,
    pub(super) published: bool,
}

impl Drop for McpRefreshInvalidationGuard<'_> {
    fn drop(&mut self) {
        if !self.published {
            self.refresh.invalidate();
        }
    }
}

#[cfg(test)]
#[path = "mcp_refresh_tests.rs"]
mod tests;
