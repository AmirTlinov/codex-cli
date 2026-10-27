//! An isolated JavaScript cell, resident without a dedicated system thread.

use std::collections::HashMap;

use tokio::sync::mpsc;

use super::CompletionState;
use super::RuntimeCommand;
use super::RuntimeConfig;
use super::RuntimeEvent;
use super::RuntimeState;
use super::ToolCallbackMetadata;
use super::globals;
use super::module_loader;
use super::send_scope_result;
use super::timers;

pub(super) struct RuntimeInstance {
    context: v8::Global<v8::Context>,
    pending_promise: Option<v8::Global<v8::Promise>>,
    isolate: v8::SharedIsolate,
    events: mpsc::UnboundedSender<RuntimeEvent>,
}

impl RuntimeInstance {
    pub(super) fn new(
        config: RuntimeConfig,
        events: mpsc::UnboundedSender<RuntimeEvent>,
        commands: mpsc::UnboundedSender<RuntimeCommand>,
    ) -> Result<Self, String> {
        fn assert_send<T: Send>() {}
        assert_send::<RuntimeState>();
        let isolate = v8::Isolate::new(v8::CreateParams::default());
        // SAFETY: the fresh isolate has no embedder state. Its only later slot
        // is RuntimeState (checked Send above); all callbacks are plain functions.
        // No Local or borrowed Global escapes a Locker's handle scopes.
        let isolate = unsafe { isolate.try_into_shared() }.map_err(|error| {
            let message = error.to_string();
            drop(error.into_isolate());
            message
        })?;
        let context = {
            let mut entered = isolate.lock();
            entered.set_host_import_module_dynamically_callback(
                module_loader::dynamic_import_callback,
            );
            v8::scope!(let scope, &mut *entered);
            let context = v8::Context::new(scope, Default::default());
            let saved_context = v8::Global::new(scope, context);
            let scope = &mut v8::ContextScope::new(scope, context);
            globals::install_globals(scope, &config.enabled_tools)?;
            scope.set_slot(RuntimeState {
                event_tx: events.clone(),
                pending_tool_calls: HashMap::new(),
                pending_timeouts: HashMap::new(),
                stored_values: config.stored_values,
                stored_value_writes: HashMap::new(),
                enabled_tools: config
                    .enabled_tools
                    .into_iter()
                    .map(|tool| ToolCallbackMetadata {
                        tool_name: tool.tool_name,
                        kind: tool.kind,
                    })
                    .collect(),
                next_tool_call_id: 1,
                next_timeout_id: 1,
                tool_call_id: config.tool_call_id,
                runtime_command_tx: commands,
                exit_requested: false,
            });
            saved_context
        };
        Ok(Self {
            context,
            pending_promise: None,
            isolate,
            events,
        })
    }

    pub(super) fn termination_handle(&self) -> v8::IsolateHandle {
        self.isolate.thread_safe_handle()
    }

    /// Runs to the next asynchronous boundary, retaining the exact JS heap.
    pub(super) fn advance(mut self, command: RuntimeCommand) -> Option<Self> {
        if matches!(command, RuntimeCommand::Terminate) {
            return None;
        }
        {
            let mut entered = self.isolate.lock();
            v8::scope!(let scope, &mut *entered);
            let context = v8::Local::new(scope, &self.context);
            let scope = &mut v8::ContextScope::new(scope, context);
            let starting = matches!(command, RuntimeCommand::Start(_));
            let result = match command {
                RuntimeCommand::Start(source) => {
                    let _ = self.events.send(RuntimeEvent::Started);
                    module_loader::evaluate_main_module(scope, &source).map(|promise| {
                        self.pending_promise = promise;
                    })
                }
                RuntimeCommand::ToolResponse { id, result } => {
                    module_loader::resolve_tool_response(scope, &id, Ok(result))
                }
                RuntimeCommand::ToolError { id, error_text } => {
                    module_loader::resolve_tool_response(scope, &id, Err(error_text))
                }
                RuntimeCommand::TimeoutFired { id } => timers::invoke_timeout_callback(scope, id),
                RuntimeCommand::ObservePendingFrontier => Ok(()),
                RuntimeCommand::Terminate => unreachable!("handled before entering V8"),
            };
            if let Err(error_text) = result {
                send_scope_result(scope, &self.events, Some(error_text));
                return None;
            }
            if !starting {
                scope.perform_microtask_checkpoint();
            }
            match module_loader::completion_state(scope, self.pending_promise.as_ref()) {
                CompletionState::Completed { error_text } => {
                    send_scope_result(scope, &self.events, error_text);
                    return None;
                }
                CompletionState::Pending => {}
            }
            if let Some(promise) = self.pending_promise.as_ref() {
                let promise = v8::Local::new(scope, promise);
                if promise.state() != v8::PromiseState::Pending {
                    self.pending_promise = None;
                }
            }
        }
        Some(self)
    }
}
