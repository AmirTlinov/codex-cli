//! Runs V8 work on threads created by its initialization thread.

use futures::future::BoxFuture;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tokio::sync::mpsc;

pub(crate) struct RuntimeExecutor {
    tasks: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
    disposal: tokio::runtime::Handle,
}

impl RuntimeExecutor {
    /// Must run on the thread that has just initialized V8. On Linux with
    /// protection keys, children inherit the access V8 granted that thread.
    pub(crate) fn new() -> Result<Self, String> {
        let (tasks, mut incoming) = mpsc::unbounded_channel::<BoxFuture<'static, ()>>();
        let (ready, initialized) = std_mpsc::sync_channel(1);
        thread::Builder::new()
            .name("code-mode-executor".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .thread_name("code-mode-v8")
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                        return;
                    }
                };
                // Cleanup must progress even when every JS worker is busy. A
                // separate blocking pool owns queued values until their drop
                // completes; its idle worker can retire like other Tokio workers.
                let disposal = match tokio::runtime::Builder::new_current_thread()
                    .thread_name("code-mode-dispose")
                    .max_blocking_threads(1)
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                        return;
                    }
                };
                if ready.send(Ok(disposal.handle().clone())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    while let Some(task) = incoming.recv().await {
                        tokio::spawn(task);
                    }
                });
                // Keep the disposal runtime alive through coordinator shutdown,
                // and join its blocking work outside the async context.
                drop(disposal);
            })
            .map_err(|error| format!("failed to start code-mode executor: {error}"))?;
        let disposal = initialized
            .recv()
            .map_err(|_| "code-mode executor stopped during initialization".to_string())??;
        Ok(Self { tasks, disposal })
    }

    pub(crate) fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), String> {
        self.tasks
            .send(task)
            .map_err(|_| "code-mode executor stopped".to_string())
    }

    /// Called from the post-initialization coordinator so new blocking workers
    /// inherit V8's memory access. Disposal is independent of cell cancellation.
    pub(crate) async fn dispose<T: Send + 'static>(
        &self,
        value: T,
    ) -> Result<(), tokio::task::JoinError> {
        self.disposal.spawn_blocking(move || drop(value)).await
    }
}
