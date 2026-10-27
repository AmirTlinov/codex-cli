//! Reconciles shared plugin changes only when a session admits hook work.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use codex_hooks::Hooks;
use codex_protocol::protocol::SessionSource;
use tokio::sync::Mutex;

use super::Session;
use super::build_hooks_config;
use crate::config::Config;

pub(crate) struct SessionHooks {
    current: ArcSwap<Hooks>,
    generation: AtomicU64,
    dirty: AtomicBool,
    refresh: Mutex<()>,
}

impl SessionHooks {
    pub(crate) fn new(hooks: Hooks, generation: u64) -> Self {
        Self {
            current: ArcSwap::from_pointee(hooks),
            generation: AtomicU64::new(generation),
            dirty: AtomicBool::new(false),
            refresh: Mutex::new(()),
        }
    }

    /// Call while publishing hook inputs under the session state lock.
    pub(crate) fn invalidate(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn is_current(&self, generation: u64) -> bool {
        !self.dirty.load(Ordering::Acquire) && self.generation.load(Ordering::Acquire) == generation
    }

    pub(crate) async fn shutdown(&self) {
        // Reconfiguration preserves the command runtime. Drain its admitted work
        // without discovery; new teardown events still go through Session::hooks.
        self.current.load_full().shutdown().await;
    }

    #[cfg(test)]
    pub(crate) fn store(&self, hooks: Arc<Hooks>) {
        self.current.store(hooks);
    }
}

impl Session {
    pub(crate) async fn session_end_hooks(&self) -> Option<Arc<Hooks>> {
        // SessionEnd is root-only. ThreadSpawn uses SubagentStart/SubagentStop;
        // other subagents are internal implementation details.
        let is_subagent = matches!(
            &self.state.lock().await.session_configuration.session_source,
            SessionSource::SubAgent(_)
        );
        if is_subagent {
            return None;
        }
        Some(self.hooks().await)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "hook discovery and publication must be serialized for this session"
    )]
    pub(crate) async fn hooks(&self) -> Arc<Hooks> {
        let hooks = &self.services.hooks;
        let plugins = &self.services.plugins_manager;
        if !hooks.is_current(plugins.loaded_plugins_cache_generation()) {
            let _refresh = hooks.refresh.lock().await;
            if !hooks.is_current(plugins.loaded_plugins_cache_generation()) {
                self.refresh_hooks_inner(self.get_config().await).await;
            }
        }
        hooks.current.load_full()
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "explicit hook refresh shares the same publication gate as lazy admission"
    )]
    pub(crate) async fn refresh_hooks(&self, config: Arc<Config>) {
        let _refresh = self.services.hooks.refresh.lock().await;
        self.refresh_hooks_inner(config).await;
    }

    async fn refresh_hooks_inner(&self, mut config: Arc<Config>) {
        loop {
            let generation = self
                .services
                .plugins_manager
                .loaded_plugins_cache_generation();
            let disabled_plugin_ids = self.state.lock().await.active_disabled_plugin_ids.clone();
            // Host hook discovery only needs a ready local shell. Remote readiness
            // belongs to the environment hooks captured by each event's caller.
            let environments = self.services.turn_environments.snapshot_now();
            let hooks_config = build_hooks_config(
                config.as_ref(),
                self.services.plugins_manager.as_ref(),
                environments.single_local_environment(),
                &disabled_plugin_ids,
            )
            .await;
            let refreshed = self
                .services
                .hooks
                .current
                .load()
                .reconfigured(hooks_config);

            let state = self.state.lock().await;
            if Arc::ptr_eq(
                &state.session_configuration.original_config_do_not_use,
                &config,
            ) && state.active_disabled_plugin_ids == disabled_plugin_ids
                && generation
                    == self
                        .services
                        .plugins_manager
                        .loaded_plugins_cache_generation()
            {
                let hooks = &self.services.hooks;
                hooks.current.store(Arc::new(refreshed));
                hooks.generation.store(generation, Ordering::Release);
                hooks.dirty.store(false, Ordering::Release);
                return;
            }
            config = Arc::clone(&state.session_configuration.original_config_do_not_use);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::TurnContext;
    use crate::session::tests::make_session_and_context;
    use codex_config::CONFIG_TOML_FILE;
    use codex_features::Feature;
    use codex_hooks::HooksConfig;
    use codex_protocol::protocol::SubAgentSource;
    use futures::FutureExt;
    use pretty_assertions::assert_eq;

    async fn session_with_hooks(
        enabled: bool,
    ) -> (Arc<Session>, Arc<TurnContext>, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let (session, turn_context) = make_session_and_context().await;
        let mut config = (*session.get_config().await).clone();
        config.features.enable(Feature::CodexHooks).unwrap();
        let config_path = config.codex_home.join(CONFIG_TOML_FILE);
        let mut user_config: codex_config::TomlValue = serde_json::from_value(serde_json::json!({
            "hooks": {
                "SessionEnd": [{"hooks": [{
                    "type": "command",
                    "command": format!("echo hook > \"{}\"", directory.path().join("session-end").display()),
                }]}],
                "Interrupt": [{"hooks": [{
                    "type": "command",
                    "command": format!("echo hook > \"{}\"", directory.path().join("interrupt").display()),
                }]}],
            },
        }))
        .unwrap();
        let layers = config
            .config_layer_stack
            .with_user_config(&config_path, user_config.clone())
            .unwrap();
        let listed = codex_hooks::list_hooks(HooksConfig {
            feature_enabled: true,
            config_layer_stack: Some(layers),
            ..HooksConfig::default()
        });
        assert_eq!(listed.hooks.len(), 2);
        let state = listed
            .hooks
            .into_iter()
            .map(|hook| {
                (
                    hook.key,
                    serde_json::json!({"trusted_hash": hook.current_hash, "enabled": enabled}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        user_config["hooks"].as_table_mut().unwrap().insert(
            "state".to_string(),
            serde_json::from_value(state.into()).unwrap(),
        );
        config.config_layer_stack = config
            .config_layer_stack
            .with_user_config(&config_path, user_config)
            .unwrap();
        session
            .state
            .lock()
            .await
            .session_configuration
            .original_config_do_not_use = Arc::new(config);
        session.services.hooks.invalidate();
        assert_eq!(
            session.hooks().await.preview_session_end().len(),
            usize::from(enabled)
        );
        (Arc::new(session), Arc::new(turn_context), directory)
    }

    fn with_hooks_enabled(config: &Config, enabled: bool) -> Config {
        let mut config = config.clone();
        let mut user_config = config
            .config_layer_stack
            .get_active_user_layer()
            .unwrap()
            .config
            .clone();
        for (_, hook) in user_config["hooks"]["state"]
            .as_table_mut()
            .unwrap()
            .iter_mut()
        {
            hook.as_table_mut()
                .unwrap()
                .insert("enabled".to_string(), enabled.into());
        }
        config.config_layer_stack = config
            .config_layer_stack
            .with_user_config(&config.codex_home.join(CONFIG_TOML_FILE), user_config)
            .unwrap();
        config
    }

    async fn set_hooks_enabled(session: &Session, enabled: bool) {
        let config = session.get_config().await;
        let next = with_hooks_enabled(&config, enabled);
        assert_eq!(
            session.refresh_runtime_config(config, next).await,
            crate::ConfigRefreshOutcome::Published
        );
    }

    #[tokio::test]
    async fn draining_hooks_does_not_wait_for_discovery() {
        let (session, _) = make_session_and_context().await;
        let previous = session.hooks().await;
        session.services.plugins_manager.clear_cache();
        let _refresh = session.services.hooks.refresh.try_lock().unwrap();

        assert!(session.services.hooks.shutdown().now_or_never().is_some());
        assert!(Arc::ptr_eq(
            &previous,
            &session.services.hooks.current.load_full()
        ));
        assert!(
            !session.services.hooks.is_current(
                session
                    .services
                    .plugins_manager
                    .loaded_plugins_cache_generation()
            )
        );
    }

    #[tokio::test]
    async fn current_empty_teardown_hooks_skip_discovery() {
        let (session, turn_context) = make_session_and_context().await;
        let session = Arc::new(session);
        session.hooks().await;
        let _refresh = session.services.hooks.refresh.try_lock().unwrap();
        assert!(
            crate::hook_runtime::run_session_end_hooks(&session)
                .now_or_never()
                .is_some()
        );
        assert!(
            crate::hook_runtime::run_turn_interrupt_hooks(
                &session,
                &Arc::new(turn_context),
                &Mutex::new(crate::state::TurnState::default()),
            )
            .now_or_never()
            .is_some()
        );
    }

    #[tokio::test]
    async fn subagent_session_end_skips_even_pending_discovery() {
        let (session, _) = make_session_and_context().await;
        let session = Arc::new(session);
        session
            .state
            .lock()
            .await
            .session_configuration
            .session_source = SessionSource::SubAgent(SubAgentSource::Other("test".to_string()));
        session.services.plugins_manager.clear_cache();
        let _refresh = session.services.hooks.refresh.try_lock().unwrap();
        assert!(
            crate::hook_runtime::run_session_end_hooks(&session)
                .now_or_never()
                .is_some()
        );
    }

    #[tokio::test]
    async fn session_end_uses_current_hook_configuration_after_draining() {
        for enabled in [false, true] {
            let (session, _, directory) = session_with_hooks(!enabled).await;
            set_hooks_enabled(&session, enabled).await;
            session.services.hooks.shutdown().await;
            crate::hook_runtime::run_session_end_hooks(&session).await;

            assert_eq!(directory.path().join("session-end").exists(), enabled);
            assert!(!directory.path().join("interrupt").exists());
        }
    }

    #[tokio::test]
    async fn interrupt_uses_current_hook_configuration_including_shutdown() {
        for shutting_down in [false, true] {
            for enabled in [false, true] {
                let (session, turn_context, directory) = session_with_hooks(!enabled).await;
                set_hooks_enabled(&session, enabled).await;
                session.state.lock().await.shutting_down = shutting_down;
                crate::hook_runtime::run_turn_interrupt_hooks(
                    &session,
                    &turn_context,
                    &Mutex::new(crate::state::TurnState::default()),
                )
                .await;

                assert_eq!(directory.path().join("interrupt").exists(), enabled);
                assert!(!directory.path().join("session-end").exists());
            }
        }
    }

    #[tokio::test]
    async fn plugin_invalidation_is_lazy_and_concurrent_admissions_share_the_refresh() {
        let (session, _) = make_session_and_context().await;
        let previous = session.hooks().await;
        session.services.plugins_manager.clear_cache();
        assert!(Arc::ptr_eq(
            &previous,
            &session.services.hooks.current.load_full()
        ));

        let (first, second) = tokio::join!(session.hooks(), session.hooks());
        assert!(!Arc::ptr_eq(&previous, &first));
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&first, &session.hooks().await));
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the held state lock deliberately pauses refresh before cancelling it"
    )]
    async fn cancelled_refresh_keeps_invalidation_pending() {
        let (session, _) = make_session_and_context().await;
        let previous = session.hooks().await;
        session.services.plugins_manager.clear_cache();
        let state = session.state.lock().await;
        {
            let refresh = session.hooks();
            tokio::pin!(refresh);
            assert!(futures::poll!(&mut refresh).is_pending());
        }
        assert!(Arc::ptr_eq(
            &previous,
            &session.services.hooks.current.load_full()
        ));
        drop(state);
        assert!(!Arc::ptr_eq(&previous, &session.hooks().await));
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the held turn lock pauses config refresh after publishing its hook inputs"
    )]
    async fn cancelled_config_refresh_preserves_hook_invalidation() {
        let (session, _, _directory) = session_with_hooks(true).await;
        let previous = session.hooks().await;
        let config = session.get_config().await;
        let next = with_hooks_enabled(&config, false);
        let generation = session
            .services
            .plugins_manager
            .loaded_plugins_cache_generation();
        // Stop after config publication, before the refresh can finish its active-turn check.
        let active_turn = session.active_turn.lock().await;
        {
            let refresh = session.refresh_runtime_config(config, next);
            tokio::pin!(refresh);
            assert!(futures::poll!(&mut refresh).is_pending());
        }
        assert!(session.services.hooks.dirty.load(Ordering::Acquire));
        assert_eq!(
            session
                .services
                .plugins_manager
                .loaded_plugins_cache_generation(),
            generation
        );
        drop(active_turn);
        assert!(!Arc::ptr_eq(&previous, &session.hooks().await));
        assert!(session.hooks().await.preview_session_end().is_empty());
    }
}
