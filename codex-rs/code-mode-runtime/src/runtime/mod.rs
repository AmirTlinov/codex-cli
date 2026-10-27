mod audio;
mod callbacks;
mod globals;
mod instance;
mod module_loader;
#[cfg(test)]
mod scheduling_tests;
mod timers;
mod value;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

use codex_code_mode_protocol::CodeModeToolKind;
use codex_code_mode_protocol::EnabledToolMetadata;
use codex_code_mode_protocol::ExecuteRequest;
use codex_code_mode_protocol::FunctionCallOutputContentItem;
use codex_code_mode_protocol::normalize_code_mode_identifier;
use codex_protocol::ToolName;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use self::instance::RuntimeInstance;
use crate::TaskFailureHandler;
use crate::executor::RuntimeExecutor;
use crate::v8_init::runtime_executor;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    Start(String),
    ToolResponse { id: String, result: JsonValue },
    ToolError { id: String, error_text: String },
    TimeoutFired { id: u64 },
    ObservePendingFrontier,
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum PendingRuntimeMode {
    #[cfg(test)]
    Continue,
    PauseUntilResumed,
}

#[derive(Debug)]
pub(crate) enum RuntimeControlCommand {
    Continue,
    Resume,
    Terminate,
}

#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    Started,
    Pending,
    ContentItem(FunctionCallOutputContentItem),
    YieldRequested,
    ToolCall {
        id: String,
        name: ToolName,
        kind: CodeModeToolKind,
        input: Option<JsonValue>,
    },
    Notify {
        call_id: String,
        text: String,
    },
    Result {
        stored_value_writes: HashMap<String, Arc<JsonValue>>,
        error_text: Option<String>,
    },
    ThreadPanicked,
}

pub(crate) async fn spawn_runtime(
    stored_values: HashMap<String, Arc<JsonValue>>,
    request: ExecuteRequest,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_mode: PendingRuntimeMode,
    cancellation: CancellationToken,
    task_failure_handler: Option<TaskFailureHandler>,
) -> Result<
    (
        mpsc::UnboundedSender<RuntimeCommand>,
        mpsc::UnboundedSender<RuntimeControlCommand>,
        v8::IsolateHandle,
    ),
    String,
> {
    let executor = runtime_executor()?;
    let cancellation = cancellation.child_token();
    let startup_guard = cancellation.clone().drop_guard();
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let (control_tx, mut control_rx) = mpsc::unbounded_channel();
    let runtime_command_tx = command_tx.clone();
    let (isolate_handle_tx, isolate_handle_rx) = oneshot::channel();
    let config = RuntimeConfig {
        tool_call_id: request.tool_call_id,
        enabled_tools: request
            .enabled_tools
            .into_iter()
            .map(|definition| EnabledToolMetadata {
                global_name: normalize_code_mode_identifier(&definition.name),
                tool_name: definition.tool_name,
                description: definition.description,
                kind: definition.kind,
            })
            .collect(),
        stored_values,
    };
    let source = request.source;
    let runtime_handle = tokio::runtime::Handle::current();
    executor.spawn(Box::pin(async move {
        let initialization_events = event_tx.clone();
        let caller_runtime = runtime_handle.clone();
        let Some(mut instance) = run_runtime_job(
            executor,
            event_tx.clone(),
            task_failure_handler.clone(),
            &cancellation,
            move || {
                if isolate_handle_tx.is_closed() {
                    return None;
                }
                let _runtime_guard = caller_runtime.enter();
                let instance =
                    match RuntimeInstance::new(config, initialization_events, runtime_command_tx) {
                        Ok(instance) => instance,
                        Err(error) => {
                            let _ = isolate_handle_tx.send(Err(error));
                            return None;
                        }
                    };
                if isolate_handle_tx
                    .send(Ok(instance.termination_handle()))
                    .is_err()
                {
                    return None;
                }
                Some(instance)
            },
        )
        .await
        .flatten() else {
            return;
        };
        // Start is sent only after the caller receives the termination handle.
        // If startup is cancelled, closing the control channel disposes the
        // parked isolate without ever running user code.
        let mut command = receive_runtime_command(&mut command_rx, &mut control_rx).await;
        while let Some(next_command) = command {
            let caller_runtime = runtime_handle.clone();
            let next = run_runtime_job(
                executor,
                event_tx.clone(),
                task_failure_handler.clone(),
                &cancellation,
                move || {
                    let _runtime_guard = caller_runtime.enter();
                    instance.advance(next_command)
                },
            )
            .await
            .flatten();
            let Some(next) = next else {
                return;
            };
            instance = next;
            command =
                next_runtime_command(&event_tx, &mut command_rx, &mut control_rx, pending_mode)
                    .await;
        }
        // Teardown can run V8 cleanup. Keep it off the shared async coordinator.
        cancellation.cancel();
        if executor.dispose(instance).await.is_err() {
            report_runtime_failure(&event_tx, task_failure_handler.as_ref());
        }
    }))?;
    let isolate_handle = isolate_handle_rx
        .await
        .map_err(|_| "failed to initialize code mode runtime".to_string())??;
    command_tx
        .send(RuntimeCommand::Start(source))
        .map_err(|_| "code mode runtime closed during initialization".to_string())?;
    startup_guard.disarm();
    Ok((command_tx, control_tx, isolate_handle))
}

async fn run_runtime_job<T: Send + 'static>(
    executor: &RuntimeExecutor,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    task_failure_handler: Option<TaskFailureHandler>,
    cancellation: &CancellationToken,
    runtime: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    if cancellation.is_cancelled() {
        if executor.dispose(runtime).await.is_err() {
            report_runtime_failure(&event_tx, task_failure_handler.as_ref());
        }
        return None;
    }
    // Retain ownership until a worker actually starts. Cancelling a queued
    // Tokio blocking task alone leaves its captures resident behind running JS.
    let queued = Arc::new(Mutex::new(Some(runtime)));
    let worker_job = Arc::clone(&queued);
    let worker_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let job = worker_job
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if worker_cancellation.is_cancelled() {
            None
        } else {
            job.map(|job| job())
        }
    });
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            let job = queued.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            if job.is_some() {
                task.abort();
                executor.dispose(job).await.map(|()| None)
            } else {
                // A running isolate is interrupted through its IsolateHandle.
                task.await
            }
        },
        result = &mut task => result,
    };
    match result {
        Ok(result) => result,
        Err(_) => {
            report_runtime_failure(&event_tx, task_failure_handler.as_ref());
            None
        }
    }
}

fn report_runtime_failure(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    task_failure_handler: Option<&TaskFailureHandler>,
) {
    if let Some(task_failure_handler) = task_failure_handler {
        task_failure_handler("code-mode V8 runtime thread panicked".to_string());
    }
    let _ = event_tx.send(RuntimeEvent::ThreadPanicked);
}

#[derive(Clone)]
struct RuntimeConfig {
    tool_call_id: String,
    enabled_tools: Vec<EnabledToolMetadata>,
    stored_values: HashMap<String, Arc<JsonValue>>,
}

// Callback indices refer to this cell's original catalog for its entire lifetime.
struct ToolCallbackMetadata {
    tool_name: ToolName,
    kind: CodeModeToolKind,
}

pub(super) struct RuntimeState {
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_tool_calls: HashMap<String, v8::Global<v8::PromiseResolver>>,
    pending_timeouts: HashMap<u64, timers::ScheduledTimeout>,
    stored_values: HashMap<String, Arc<JsonValue>>,
    stored_value_writes: HashMap<String, Arc<JsonValue>>,
    enabled_tools: Vec<ToolCallbackMetadata>,
    next_tool_call_id: u64,
    next_timeout_id: u64,
    tool_call_id: String,
    runtime_command_tx: mpsc::UnboundedSender<RuntimeCommand>,
    exit_requested: bool,
}

pub(super) enum CompletionState {
    Pending,
    Completed { error_text: Option<String> },
}

async fn next_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &mut mpsc::UnboundedReceiver<RuntimeCommand>,
    control_rx: &mut mpsc::UnboundedReceiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
) -> Option<RuntimeCommand> {
    loop {
        match command_rx.try_recv() {
            Ok(command) => return Some(command),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        let _ = event_tx.send(RuntimeEvent::Pending);
        match pending_mode {
            #[cfg(test)]
            PendingRuntimeMode::Continue => {
                return receive_runtime_command(command_rx, control_rx).await;
            }
            PendingRuntimeMode::PauseUntilResumed => match control_rx.recv().await? {
                RuntimeControlCommand::Continue => {
                    return receive_runtime_command(command_rx, control_rx).await;
                }
                RuntimeControlCommand::Resume => continue,
                RuntimeControlCommand::Terminate => return Some(RuntimeCommand::Terminate),
            },
        }
    }
}

async fn receive_runtime_command(
    command_rx: &mut mpsc::UnboundedReceiver<RuntimeCommand>,
    control_rx: &mut mpsc::UnboundedReceiver<RuntimeControlCommand>,
) -> Option<RuntimeCommand> {
    loop {
        tokio::select! {
            biased;
            control = control_rx.recv() => match control? {
                RuntimeControlCommand::Terminate => return Some(RuntimeCommand::Terminate),
                RuntimeControlCommand::Continue | RuntimeControlCommand::Resume => {}
            },
            command = command_rx.recv() => return command,
        }
    }
}

fn send_scope_result(
    scope: &mut v8::PinScope<'_, '_>,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    error_text: Option<String>,
) {
    // Error rendering can itself run JS getters, including exit(). Capture
    // the result only after every call into the exiting isolate has returned.
    let state = scope.get_slot::<RuntimeState>();
    let stored_value_writes = state
        .map(|state| state.stored_value_writes.clone())
        .unwrap_or_default();
    let error_text = error_text.filter(|_| !state.is_some_and(|state| state.exit_requested));
    send_result(event_tx, stored_value_writes, error_text);
}

fn send_result(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    stored_value_writes: HashMap<String, Arc<JsonValue>>,
    error_text: Option<String>,
) {
    let _ = event_tx.send(RuntimeEvent::Result {
        stored_value_writes,
        error_text,
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;

    use tokio_util::sync::CancellationToken;

    use super::ExecuteRequest;
    use super::PendingRuntimeMode;
    use super::RuntimeCommand;
    use super::RuntimeControlCommand;
    use super::RuntimeEvent;
    use super::spawn_runtime;
    use crate::FunctionCallOutputContentItem;

    fn execute_request(source: &str) -> ExecuteRequest {
        ExecuteRequest {
            tool_call_id: "call_1".to_string(),
            enabled_tools: Vec::new(),
            source: source.to_string(),
            yield_time_ms: Some(1),
            max_output_tokens: None,
        }
    }

    #[test]
    fn linked_v8_has_sandbox_enabled() {
        unsafe extern "C" {
            fn v8__V8__IsSandboxEnabled() -> bool;
        }

        // `rusty_v8` exposes this symbol for verifying linked sandbox support.
        assert!(
            unsafe { v8__V8__IsSandboxEnabled() },
            "code mode must link against sandbox-enabled V8"
        );
    }

    #[tokio::test]
    async fn terminate_execution_stops_javascript() {
        for source in [
            r#"notify("started"); while (true) {}"#,
            // The failed first read enters JSON fallback, whose TryCatch must preserve termination.
            r#"
let reads = 0;
const value = {
    get image_url() {
        if (++reads === 1) throw new Error("enter fallback");
        notify("started");
        while (true) {}
    }
};
try { image(value); } catch {}
text("continued after cancellation");
"#,
        ] {
            let (event_tx, mut event_rx) = mpsc::unbounded_channel();
            let (_runtime_tx, _runtime_control_tx, runtime_terminate_handle) = spawn_runtime(
                HashMap::new(),
                execute_request(source),
                event_tx,
                PendingRuntimeMode::Continue,
                CancellationToken::new(),
                /*task_failure_handler*/ None,
            )
            .await
            .unwrap();

            let started_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(started_event, RuntimeEvent::Started));

            // Started precedes evaluation; the notification proves the workload is running.
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                    .await
                    .unwrap(),
                Some(RuntimeEvent::Notify { .. })
            ));
            assert!(runtime_terminate_handle.terminate_execution());

            let result_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let RuntimeEvent::Result { error_text, .. } = result_event else {
                panic!("expected runtime result after termination");
            };
            assert!(error_text.is_some());

            assert!(
                tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn pending_mode_freezes_runtime_commands_until_resume() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (runtime_tx, runtime_control_tx, _runtime_terminate_handle) = spawn_runtime(
            HashMap::new(),
            execute_request(
                r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
text("after");
await new Promise(() => {});
"#,
            ),
            event_tx,
            PendingRuntimeMode::PauseUntilResumed,
            CancellationToken::new(),
            /*task_failure_handler*/ None,
        )
        .await
        .unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Started
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Pending
        ));

        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .is_err()
        );

        runtime_control_tx
            .send(RuntimeControlCommand::Resume)
            .unwrap();

        let content_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let RuntimeEvent::ContentItem(FunctionCallOutputContentItem::InputText { text }) =
            content_event
        else {
            panic!("expected resumed runtime output");
        };
        assert_eq!(text, "after");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Pending
        ));

        runtime_control_tx
            .send(RuntimeControlCommand::Terminate)
            .unwrap();
    }

    #[tokio::test]
    async fn timers_release_tasks_when_cleared_or_the_cell_finishes() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (_runtime_tx, _runtime_control_tx, _runtime_terminate_handle) = spawn_runtime(
            HashMap::new(),
            execute_request(
                r#"
clearTimeout(setTimeout(() => text("cancelled"), 3_600_000));
await new Promise((resolve) => setTimeout(resolve, 3_600_000));
text("done");
setTimeout(() => text("late"), 3_600_000);
"#,
            ),
            event_tx,
            PendingRuntimeMode::Continue,
            CancellationToken::new(),
            /*task_failure_handler*/ None,
        )
        .await
        .unwrap();

        loop {
            match tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
            {
                Some(RuntimeEvent::Pending) => break,
                Some(_) => {}
                None => panic!("runtime closed before the timer was pending"),
            }
        }
        tokio::task::yield_now().await;
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            1
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3_600)).await;
        tokio::time::resume();

        let mut output = Vec::new();
        while let Some(event) = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("timer runtime should finish")
        {
            match event {
                RuntimeEvent::ContentItem(item) => output.push(item),
                RuntimeEvent::Result { error_text, .. } => assert_eq!(error_text, None),
                _ => {}
            }
        }
        assert_eq!(
            output,
            vec![FunctionCallOutputContentItem::InputText {
                text: "done".to_string()
            }]
        );
        tokio::task::yield_now().await;
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            0
        );
    }
}
