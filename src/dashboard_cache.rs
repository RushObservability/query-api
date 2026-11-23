//! Short, bounded reuse of successful telemetry summaries. Authorization,
//! signal policy, admission and range validation run before this middleware.

use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    AppState, RequestIdentity, TenantContext,
    query_governor::{QueryBudget, WorkloadClass},
    self_metrics::SelfMetrics,
};

const TTL: Duration = Duration::from_secs(5);
const MAX_ENTRIES: usize = 256;
const MAX_BYTES: usize = 32 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
type Key = [u8; 32];

struct Cached {
    body: Bytes,
    started: Instant,
    unified: bool,
}

struct Slot {
    gate: Arc<Mutex<()>>,
    cached: Option<Cached>,
    touched: Instant,
}

#[derive(Default)]
struct Inner {
    slots: HashMap<Key, Slot>,
    bytes: usize,
    generation: u64,
}

pub struct DashboardCache {
    inner: std::sync::Mutex<Inner>,
    max_entries: usize,
    max_bytes: usize,
}

impl Default for DashboardCache {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            max_entries: MAX_ENTRIES,
            max_bytes: MAX_BYTES,
        }
    }
}

fn record(metrics: &SelfMetrics, result: &'static str) {
    metrics.inc_counter(
        "rush_dashboard_cache_requests_total",
        &[("result", result)],
        1,
    );
}

impl Inner {
    fn gauges(&self, metrics: &SelfMetrics) {
        metrics.set_gauge("rush_dashboard_cache_entries", &[], self.slots.len() as f64);
        metrics.set_gauge("rush_dashboard_cache_bytes", &[], self.bytes as f64);
    }

    fn remove(&mut self, key: &Key, metrics: &SelfMetrics, reason: &'static str) {
        if let Some(slot) = self.slots.remove(key) {
            self.bytes -= slot.cached.map_or(0, |cached| cached.body.len());
            metrics.inc_counter(
                "rush_dashboard_cache_evictions_total",
                &[("reason", reason)],
                1,
            );
        }
    }

    fn evict_oldest(&mut self, metrics: &SelfMetrics) -> bool {
        let key = self
            .slots
            .iter()
            // Never detach an active execution or its waiters from their gate.
            .filter(|(_, slot)| Arc::strong_count(&slot.gate) == 1)
            .min_by_key(|(_, slot)| slot.touched)
            .map(|(key, _)| *key);
        if let Some(key) = key {
            self.remove(&key, metrics, "capacity");
            true
        } else {
            false
        }
    }
}

impl DashboardCache {
    pub fn invalidate(&self, metrics: &SelfMetrics) {
        let mut inner = self.inner.lock().expect("dashboard cache poisoned");
        inner.generation = inner.generation.wrapping_add(1);
        inner.slots.clear();
        inner.bytes = 0;
        inner.gauges(metrics);
        metrics.inc_counter("rush_dashboard_cache_invalidations_total", &[], 1);
    }

    fn slot(&self, key: Key, metrics: &SelfMetrics) -> Option<(Arc<Mutex<()>>, u64)> {
        let mut inner = self.inner.lock().expect("dashboard cache poisoned");
        let expired: Vec<_> = inner
            .slots
            .iter()
            .filter(|(_, slot)| {
                Arc::strong_count(&slot.gate) == 1
                    && slot
                        .cached
                        .as_ref()
                        .is_none_or(|cached| cached.started.elapsed() >= TTL)
            })
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            inner.remove(&key, metrics, "expired");
        }
        if !inner.slots.contains_key(&key) {
            while inner.slots.len() >= self.max_entries {
                if !inner.evict_oldest(metrics) {
                    return None;
                }
            }
        }
        let generation = inner.generation;
        let slot = inner.slots.entry(key).or_insert_with(|| Slot {
            gate: Arc::new(Mutex::new(())),
            cached: None,
            touched: Instant::now(),
        });
        slot.touched = Instant::now();
        let gate = slot.gate.clone();
        inner.gauges(metrics);
        Some((gate, generation))
    }

    fn hit(&self, key: &Key, generation: u64) -> Option<(Bytes, bool, Duration)> {
        let inner = self.inner.lock().expect("dashboard cache poisoned");
        if inner.generation != generation {
            return None;
        }
        let cached = inner.slots.get(key)?.cached.as_ref()?;
        let age = cached.started.elapsed();
        (age < TTL).then(|| (cached.body.clone(), cached.unified, age))
    }

    fn store(&self, key: Key, generation: u64, cached: Cached, metrics: &SelfMetrics) {
        if cached.started.elapsed() >= TTL || cached.body.len() > self.max_bytes {
            return;
        }
        let mut inner = self.inner.lock().expect("dashboard cache poisoned");
        if inner.generation != generation || !inner.slots.contains_key(&key) {
            return;
        }
        let old_bytes = inner
            .slots
            .get_mut(&key)
            .and_then(|slot| slot.cached.take())
            .map_or(0, |old| old.body.len());
        inner.bytes -= old_bytes;
        while inner.bytes + cached.body.len() > self.max_bytes {
            if !inner.evict_oldest(metrics) {
                inner.gauges(metrics);
                return;
            }
        }
        inner.bytes += cached.body.len();
        inner.slots.get_mut(&key).expect("active cache slot").cached = Some(cached);
        inner.gauges(metrics);
    }

    async fn run<F, Fut>(
        &self,
        mut req: Request,
        budget: QueryBudget,
        metrics: &SelfMetrics,
        next: F,
    ) -> Response
    where
        F: FnOnce(Request) -> Fut,
        Fut: Future<Output = Response>,
    {
        let started = Instant::now();
        let Some(unified) = eligible(&req) else {
            return next(req).await;
        };
        if bypass_requested(&req) {
            record(metrics, "bypass");
            return tagged(next(req).await, "bypass", Duration::ZERO);
        }
        let bytes = if req.method() == Method::POST {
            let body = std::mem::replace(req.body_mut(), Body::empty());
            match axum::body::to_bytes(body, 512 * 1024).await {
                Ok(bytes) => bytes,
                Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
            }
        } else {
            Bytes::new()
        };
        if req.method() == Method::POST {
            *req.body_mut() = Body::from(bytes.clone());
        }
        if unified && !aggregate_operation(&bytes) {
            return next(req).await;
        }
        let Some(key) = key(&req, &budget, &bytes) else {
            return next(req).await;
        };
        let Some((gate, generation)) = self.slot(key, metrics) else {
            record(metrics, "bypass");
            return tagged(next(req).await, "bypass", Duration::ZERO);
        };
        let (_guard, waited) = match gate.try_lock() {
            Ok(guard) => (guard, false),
            Err(_) => (gate.lock().await, true),
        };
        if let Some((body, unified, age)) = self.hit(&key, generation) {
            let result = if waited { "coalesced" } else { "hit" };
            record(metrics, result);
            let body = if unified {
                refresh_metadata(body, started)
            } else {
                body
            };
            return tagged(
                ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
                result,
                age,
            );
        }
        record(metrics, "miss");
        let query_started = Instant::now();
        let response = next(req).await;
        let cacheable_headers = response.status() == StatusCode::OK
            && response
                .headers()
                .get(header::CONTENT_TYPE)
                .is_some_and(|v| v == "application/json")
            && response
                .headers()
                .keys()
                .all(|name| matches!(name.as_str(), "content-type" | "content-length"));
        if !cacheable_headers
            || response
                .body()
                .size_hint()
                .exact()
                .is_none_or(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return tagged(response, "bypass", Duration::ZERO);
        }
        let (parts, body) = response.into_parts();
        let body = match axum::body::to_bytes(body, MAX_RESPONSE_BYTES).await {
            Ok(body) => body,
            Err(error) => {
                return crate::api_error::ApiError::internal("dashboard.cache.response", error)
                    .into_response();
            }
        };
        if successful_payload(&body) {
            self.store(
                key,
                generation,
                Cached {
                    body: body.clone(),
                    started: query_started,
                    unified,
                },
                metrics,
            );
        }
        tagged(
            Response::from_parts(parts, Body::from(body)),
            "miss",
            Duration::ZERO,
        )
    }
}

fn bypass_requested(req: &Request) -> bool {
    req.headers()
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "no-cache" | "no-store" | "max-age=0"
            )
        })
        || req
            .headers()
            .get(header::PRAGMA)
            .is_some_and(|v| v == "no-cache")
}

fn eligible(req: &Request) -> Option<bool> {
    let dashboard = req.extensions().get::<WorkloadClass>() == Some(&WorkloadClass::Dashboard);
    if dashboard
        && req.method() == Method::GET
        && matches!(
            req.uri().path(),
            "/prom/api/v1/query" | "/prom/api/v1/query_range"
        )
    {
        return Some(false);
    }
    if req.method() == Method::GET
        && matches!(
            req.uri().path(),
            "/api/v1/services/graph"
                | "/api/v1/services/time-breakdown"
                | "/api/v1/services/time-breakdown/timeseries"
                | "/api/v1/services/latency-histogram"
                | "/api/v1/services/endpoints"
        )
    {
        return Some(false);
    }
    if req.method() != Method::POST
        || !req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
            })
    {
        return None;
    }
    if matches!(req.uri().path(), "/api/v1/rum/pages" | "/api/v1/rum/vitals") {
        return Some(false);
    }
    if !dashboard {
        return None;
    }
    match req.uri().path() {
        "/api/v1/query/execute" => Some(true),
        "/api/v1/query/count"
        | "/api/v1/query/group"
        | "/api/v1/query/timeseries"
        | "/api/v1/logs/count"
        | "/api/v1/logs/group"
        | "/api/v1/logs/histogram"
        | "/api/v1/logs/patterns" => Some(false),
        _ => None,
    }
}

fn aggregate_operation(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body).ok().is_some_and(|v| {
        matches!(
            (v["signal"].as_str(), v["operation"].as_str()),
            (
                Some("logs" | "spans"),
                Some("count" | "group" | "timeseries")
            ) | (Some("metrics"), Some("instant" | "range"))
                | (Some("logs"), Some("patterns"))
        )
    })
}

fn key(req: &Request, budget: &QueryBudget, body: &[u8]) -> Option<Key> {
    let tenant = req.extensions().get::<TenantContext>()?;
    let identity = req.extensions().get::<RequestIdentity>()?;
    let class = req.extensions().get::<WorkloadClass>()?;
    if tenant.tenant_id != identity.tenant_id {
        return None;
    }
    let mut hash = Sha256::new();
    let scope = serde_json::to_vec(&(
        &tenant.tenant_id,
        &identity.actor_id,
        &identity.actor_type,
        &identity.credential_type,
        identity.authenticated,
        class,
        budget,
        req.method().as_str(),
        req.uri().to_string(),
        req.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        req.headers()
            .get("x-rush-workload-source")
            .and_then(|v| v.to_str().ok()),
    ))
    .ok()?;
    hash.update((scope.len() as u64).to_be_bytes());
    hash.update(scope);
    hash.update(body);
    Some(hash.finalize().into())
}

fn successful_payload(body: &[u8]) -> bool {
    fn complete(value: &Value) -> bool {
        value
            .get("errors")
            .is_none_or(|v| v.as_object().is_some_and(|v| v.is_empty()))
            && value
                .get("warnings")
                .is_none_or(|v| v.as_array().is_some_and(|v| v.is_empty()))
            && value.get("status").is_none_or(|v| v == "success")
    }
    serde_json::from_slice::<Value>(body)
        .ok()
        .is_some_and(|value| {
            complete(&value)
                && value.get("data").is_none_or(complete)
                && value.get("meta").is_none_or(|meta| {
                    meta.get("partial").is_none_or(|v| v == false) && complete(meta)
                })
        })
}

fn refresh_metadata(body: Bytes, started: Instant) -> Bytes {
    let mut value: Value = serde_json::from_slice(&body).expect("cached JSON response");
    let Some(meta) = value.get_mut("meta").and_then(Value::as_object_mut) else {
        return body;
    };
    meta.insert(
        "request_id".into(),
        crate::api_error::current_request_id().into(),
    );
    meta.insert(
        "elapsed_ms".into(),
        (started.elapsed().as_millis() as u64).into(),
    );
    serde_json::to_vec(&value)
        .expect("cached JSON serialization")
        .into()
}

fn tagged(mut response: Response, result: &'static str, age: Duration) -> Response {
    response
        .headers_mut()
        .insert("x-rush-cache", result.parse().expect("static cache result"));
    response
        .headers_mut()
        .insert("x-rush-cache-age-ms", (age.as_millis() as u64).into());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

fn invalidates(req: &Request) -> bool {
    matches!(
        *req.method(),
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    ) && (req.uri().path().starts_with("/api/v1/settings/")
        || req.uri().path() == "/api/v1/tenants"
        || req.uri().path().starts_with("/api/v1/tenants/")
        || req.uri().path().starts_with("/api/v1/retention/"))
}

pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let invalidate = invalidates(&req);
    let response = match crate::query_governor::active_budget() {
        Some(budget) => {
            state
                .dashboard_cache
                .run(req, budget, &state.self_metrics, |req| next.run(req))
                .await
        }
        None => next.run(req).await,
    };
    if invalidate && response.status().is_success() {
        state.dashboard_cache.invalidate(&state.self_metrics);
    }
    response
}

#[cfg(test)]
mod tests;
