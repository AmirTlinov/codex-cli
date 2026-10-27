use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use crate::session::session::Session;
use anyhow::Context;
use anyhow::bail;
use codex_hooks::HookMcpCall;
use codex_hooks::HookMcpExecutor;
use codex_hooks::HookMcpOutput;
use codex_mcp::McpRuntime;
use codex_protocol::ThreadId;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::Value;

pub(crate) struct CoreHookMcpExecutor {
    pub(crate) runtime: Arc<McpRuntime>,
    // Session-scoped MCP tools require the owning thread ID in request metadata.
    pub(crate) thread_id: ThreadId,
    // Hooks are validated before the fallible session initialization completes.
    // Bind their owner before publishing the session, without a reference cycle.
    pub(crate) session: OnceLock<Weak<Session>>,
}

impl HookMcpExecutor for CoreHookMcpExecutor {
    fn execute(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<HookMcpOutput>> {
        async move {
            let mut metadata = call.metadata.unwrap_or_default();
            metadata.insert(
                "threadId".to_string(),
                Value::String(self.thread_id.to_string()),
            );

            let call_result = tokio::time::timeout(call.timeout, async {
                let session = self.session.get().and_then(Weak::upgrade);
                if let Some(session) = session.as_ref() {
                    session.refresh_mcp_if_dirty().await;
                } else if self.session.get().is_some() {
                    bail!("MCP hook session has shut down");
                }
                self.runtime
                    .latest_call_tool(
                        &call.server,
                        &call.tool,
                        call.environment_id.as_deref(),
                        Some(Value::Object(call.input)),
                        Some(Value::Object(metadata)),
                        Some(call.timeout),
                        /*wait_for_server*/ true,
                    )
                    .await
            })
            .await
            .context("MCP hook timed out")??;
            let result = call_result.result;
            let text = result
                .content
                .iter()
                .filter_map(|content| {
                    (content.get("type").and_then(Value::as_str) == Some("text"))
                        .then(|| content.get("text").and_then(Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>()
                .join("\n");
            if result.is_error == Some(true) {
                bail!("MCP tool returned an error: {text}");
            }

            Ok(HookMcpOutput {
                text,
                source_tool_namespace: call_result.source_tool_namespace,
            })
        }
        .boxed()
    }
}
