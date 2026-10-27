//! Reconciles shared plugin changes only when a session admits hook work.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use codex_hooks::Hooks;
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

    /// Cancellation and teardown use admitted declarations without starting discovery.
    pub(crate) fn snapshot(&self) -> Arc<Hooks> {
        self.current.load_full()
    }

    pub(crate) async fn shutdown(&self) -> Arc<Hooks> {
        // Teardown drains admitted work without starting discovery again.
        let hooks = self.snapshot();
        hooks.shutdown().await;
        hooks
    }

    #[cfg(test)]
    pub(crate) fn store(&self, hooks: Arc<Hooks>) {
        self.current.store(hooks);
    }
}

impl Session {
    pub(crate) async fn hooks_for_interrupt(&self) -> Arc<Hooks> {
        let shutting_down = self.state.lock().await.shutting_down;
        if shutting_down {
            self.services.hooks.snapshot()
        } else {
            self.hooks().await
        }
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
    use crate::session::tests::make_session_and_context;
    use futures::FutureExt;

    #[tokio::test]
    async fn teardown_uses_captured_hooks_without_waiting_for_refresh() {
        let (session, turn_context) = make_session_and_context().await;
        let session = Arc::new(session);
        let previous = session.services.hooks.shutdown().await;
        session.services.plugins_manager.clear_cache();
        session.state.lock().await.shutting_down = true;
        let _refresh = session.services.hooks.refresh.try_lock().unwrap();

        assert!(
            crate::hook_runtime::run_session_end_hooks(&session, Arc::clone(&previous))
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
        assert!(Arc::ptr_eq(&previous, &session.services.hooks.snapshot()));
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
    async fn ordinary_interrupt_reconciles_pending_hook_configuration() {
        let (session, turn_context) = make_session_and_context().await;
        let session = Arc::new(session);
        let previous = session.hooks().await;
        let config = session.get_config().await;
        let mut next = config.as_ref().clone();
        next.features
            .disable(codex_features::Feature::CodexHooks)
            .unwrap();
        session.refresh_runtime_config(config, next).await;
        assert!(session.services.hooks.dirty.load(Ordering::Acquire));

        crate::hook_runtime::run_turn_interrupt_hooks(
            &session,
            &Arc::new(turn_context),
            &Mutex::new(crate::state::TurnState::default()),
        )
        .await;

        assert!(!session.services.hooks.dirty.load(Ordering::Acquire));
        assert!(!Arc::ptr_eq(&previous, &session.services.hooks.snapshot()));
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
        let (session, _) = make_session_and_context().await;
        let previous = session.hooks().await;
        let config = session.get_config().await;
        let mut next = config.as_ref().clone();
        next.features
            .disable(codex_features::Feature::CodexHooks)
            .unwrap();
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
    }
}
