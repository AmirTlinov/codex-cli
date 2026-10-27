//! Runs V8 work on threads created by its initialization thread.

use futures::future::BoxFuture;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tokio::sync::mpsc;

pub(crate) struct RuntimeExecutor {
    tasks: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
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
                if ready.send(Ok(())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    while let Some(task) = incoming.recv().await {
                        tokio::spawn(task);
                    }
                });
            })
            .map_err(|error| format!("failed to start code-mode executor: {error}"))?;
        initialized
            .recv()
            .map_err(|_| "code-mode executor stopped during initialization".to_string())??;
        Ok(Self { tasks })
    }

    pub(crate) fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), String> {
        self.tasks
            .send(task)
            .map_err(|_| "code-mode executor stopped".to_string())
    }
}
