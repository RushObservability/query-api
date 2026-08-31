//! Request-scoped session authorization reuse.
//!
//! Tenant middleware validates a browser session before dispatching the
//! handler. Protected handlers historically repeated that same ClickHouse
//! session/user/role lookup, and dashboard handlers sometimes performed it a
//! third time. This module shares only within one request task. No result
//! survives the response, so logout, user-version changes, role changes, and
//! revocation are checked again on the next request by every API replica.

use std::cell::RefCell;
use std::future::Future;
use std::time::Instant;

use crate::AppState;
use crate::clickhouse_config::SessionUser;

#[derive(Default)]
struct RequestSessionCache {
    /// HMAC-derived storage key; never the raw bearer token.
    entry: Option<(String, Option<SessionUser>)>,
}

tokio::task_local! {
    static REQUEST_SESSION_CACHE: RefCell<RequestSessionCache>;
}

/// Run one HTTP request with an empty authorization reuse scope.
pub async fn scope<F: Future>(future: F) -> F::Output {
    REQUEST_SESSION_CACHE
        .scope(RefCell::new(RequestSessionCache::default()), future)
        .await
}

async fn cached_session<F, Fut>(key: String, load: F) -> (Option<SessionUser>, bool)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Option<SessionUser>>,
{
    if let Ok(Some(user)) = REQUEST_SESSION_CACHE.try_with(|cache| {
        let cache = cache.borrow();
        cache
            .entry
            .as_ref()
            .filter(|(cached_key, _)| cached_key == &key)
            .map(|(_, user)| user.clone())
    }) {
        return (user, true);
    }

    let user = load().await;
    let _ = REQUEST_SESSION_CACHE.try_with(|cache| {
        cache.borrow_mut().entry = Some((key, user.clone()));
    });
    (user, false)
}

/// Resolve the authenticated user once per HTTP request. A missing task-local
/// scope (for direct unit calls or background work) safely falls back to the
/// database and retains the previous behavior.
pub async fn resolve_session_user(state: &AppState, token: &str) -> Option<SessionUser> {
    let key = state.config_db.session_request_key(token);
    let started = Instant::now();
    let (user, cache_hit) = cached_session(key, || {
        state
            .config_db
            .get_session_user_observed(token, &state.self_metrics)
    })
    .await;
    state
        .self_metrics
        .record_auth_cache("session", if cache_hit { "hit" } else { "miss" });
    if !cache_hit {
        state.self_metrics.record_auth_lookup(
            "session",
            started.elapsed().as_secs_f64() * 1_000.0,
            u64::from(user.is_some()),
            if user.is_some() { "ok" } else { "not_found" },
        );
    }
    user
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn user() -> SessionUser {
        (
            "user-1".into(),
            "admin".into(),
            "Admin".into(),
            "default".into(),
            "admin".into(),
        )
    }

    #[tokio::test]
    async fn three_authorization_consumers_execute_one_backend_lookup_per_request() {
        let baseline_calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let calls = baseline_calls.clone();
            let _ = cached_session("same-session".into(), move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Some(user())
            })
            .await;
        }
        assert_eq!(baseline_calls.load(Ordering::SeqCst), 3);

        let optimized_calls = Arc::new(AtomicUsize::new(0));
        scope(async {
            for _ in 0..3 {
                let calls = optimized_calls.clone();
                let (resolved, _) = cached_session("same-session".into(), move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(user())
                })
                .await;
                assert_eq!(resolved, Some(user()));
            }
        })
        .await;
        assert_eq!(optimized_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_new_request_always_revalidates_the_session() {
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = calls.clone();
        let first = scope(async move {
            cached_session("same-session".into(), move || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Some(user())
            })
            .await
            .0
        })
        .await;
        assert_eq!(first, Some(user()));

        // Simulate logout, password-version invalidation, role disablement, or
        // replica-visible revocation between requests.
        let second_calls = calls.clone();
        let second = scope(async move {
            cached_session("same-session".into(), move || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                None
            })
            .await
            .0
        })
        .await;
        assert!(second.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalid_sessions_are_reused_only_inside_the_current_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        scope(async {
            for _ in 0..2 {
                let calls = calls.clone();
                let (resolved, _) = cached_session("invalid-session".into(), move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    None
                })
                .await;
                assert!(resolved.is_none());
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
