use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Json;
use serde_json::json;

fn budget() -> QueryBudget {
    crate::query_governor::QueryGovernorConfig::default().dashboard
}

fn request(tenant: &str, user: &str, body: Value) -> Request {
    let mut req = Request::builder()
        .method("POST")
        .uri("/api/v1/query/execute")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    req.extensions_mut().insert(TenantContext {
        tenant_id: tenant.into(),
    });
    req.extensions_mut().insert(RequestIdentity {
        tenant_id: tenant.into(),
        authenticated: true,
        actor_id: user.into(),
        actor_name: user.into(),
        actor_type: "user".into(),
        credential_type: "session".into(),
    });
    req.extensions_mut().insert(WorkloadClass::Dashboard);
    req
}

fn query() -> Value {
    json!({"version":"v1", "signal":"spans", "operation":"count",
        "time_range":{"from":"2026-11-22T00:00:00Z", "to":"2026-11-22T01:00:00Z"},
        "filters":[], "options":{"interval":"1m"}})
}

fn response(count: usize) -> Response {
    Json(
        json!({"version":"v1", "signal":"spans", "operation":"count", "data":[{"count":count}],
        "meta":{"request_id":"old-request", "elapsed_ms":999999, "partial":false, "warnings":[]}}),
    )
    .into_response()
}

async fn json_body(response: Response) -> Value {
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn exact_requests_reuse_data_with_current_request_metadata() {
    let cache = Arc::new(DashboardCache::default());
    let metrics = Arc::new(SelfMetrics::new());
    let calls = AtomicUsize::new(0);
    let first = cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            response(calls.fetch_add(1, Ordering::SeqCst) + 1)
        })
        .await;
    assert_eq!(first.headers()["x-rush-cache"], "miss");
    let mut hit_request = request("a", "u", query());
    hit_request
        .headers_mut()
        .insert("x-request-id", "current-request-123".parse().unwrap());
    // Exercise the public request-id scope used by the real HTTP router.
    let app = axum::Router::new()
        .route(
            "/api/v1/query/execute",
            axum::routing::post(move |req: Request| {
                let cache = cache.clone();
                let metrics = metrics.clone();
                async move {
                    cache
                        .run(req, budget(), &metrics, |_| async {
                            panic!("cached query ran twice")
                        })
                        .await
                }
            }),
        )
        .layer(axum::middleware::from_fn(
            crate::api_error::public_error_middleware,
        ));
    use tower::ServiceExt;
    let second = app.oneshot(hit_request).await.unwrap();
    assert_eq!(second.headers()["x-rush-cache"], "hit");
    assert_eq!(second.headers()[header::CACHE_CONTROL], "no-store");
    let request_id = second.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let value = json_body(second).await;
    assert_eq!(value["data"][0]["count"], 1);
    assert_eq!(value["meta"]["request_id"], request_id);
    assert_ne!(value["meta"]["request_id"], "current-request-123");
    assert!(value["meta"]["elapsed_ms"].as_u64().unwrap() < 999999);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tenant_identity_filters_bounds_options_uri_and_budgets_do_not_share() {
    let cache = DashboardCache::default();
    let metrics = SelfMetrics::new();
    let calls = AtomicUsize::new(0);
    let mut changed_filter = query();
    changed_filter["filters"] = json!([{"field":"service_name", "op":"eq", "value":"shop"}]);
    let mut changed_range = query();
    changed_range["time_range"]["to"] = "2026-11-22T01:00:00.001Z".into();
    let mut changed_options = query();
    changed_options["options"]["interval"] = "5m".into();
    let mut requests = vec![
        request("a", "u", query()),
        request("b", "u", query()),
        request("a", "v", query()),
        request("a", "u", changed_filter),
        request("a", "u", changed_range),
        request("a", "u", changed_options),
    ];
    let mut credential = request("a", "u", query());
    credential
        .extensions_mut()
        .get_mut::<RequestIdentity>()
        .unwrap()
        .credential_type = "api_key".into();
    requests.push(credential);
    let mut source = request("a", "u", query());
    source
        .headers_mut()
        .insert("x-rush-workload-source", "dashboard:other".parse().unwrap());
    requests.push(source);
    let mut uri = request("a", "u", query());
    *uri.uri_mut() = "/api/v1/query/execute?v=other".parse().unwrap();
    requests.push(uri);
    for req in requests {
        let resp = cache
            .run(req, budget(), &metrics, |_| async {
                response(calls.fetch_add(1, Ordering::SeqCst) + 1)
            })
            .await;
        assert_eq!(resp.headers()["x-rush-cache"], "miss");
    }
    let mut limited = budget();
    limited.max_result_rows /= 2;
    let resp = cache
        .run(request("a", "u", query()), limited, &metrics, |_| async {
            response(calls.fetch_add(1, Ordering::SeqCst) + 1)
        })
        .await;
    assert_eq!(resp.headers()["x-rush-cache"], "miss");
    assert_eq!(calls.load(Ordering::SeqCst), 10);
}

#[tokio::test]
async fn concurrent_reads_share_execution_and_cancellation_does_not_poison_the_gate() {
    let cache = DashboardCache::default();
    let metrics = SelfMetrics::new();
    let calls = AtomicUsize::new(0);
    let make = || {
        cache.run(request("a", "u", query()), budget(), &metrics, |_| async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            response(7)
        })
    };
    let results = futures_util::future::join_all((0..20).map(|_| make())).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| r.headers()["x-rush-cache"] == "miss")
            .count(),
        1
    );
    assert!(
        metrics
            .render_prometheus()
            .contains("rush_dashboard_cache_requests_total{result=\"coalesced\"} 19")
    );
    for result in results {
        assert_eq!(json_body(result).await["data"][0]["count"], 7);
    }
    cache.invalidate(&metrics);
    let started = tokio::sync::Notify::new();
    let mut cancelled =
        Box::pin(
            cache.run(request("a", "u", query()), budget(), &metrics, |_| async {
                started.notify_one();
                std::future::pending::<Response>().await
            }),
        );
    tokio::select! { _ = &mut cancelled => panic!("unexpected completion"), _ = started.notified() => {} }
    // Dropping the leader releases the mutex, so its follower can compute.
    drop(cancelled);
    let recovered = cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            response(9)
        })
        .await;
    assert_eq!(json_body(recovered).await["data"][0]["count"], 9);
}

#[tokio::test]
async fn invalidation_during_execution_prevents_repopulation() {
    let cache = DashboardCache::default();
    let metrics = SelfMetrics::new();
    cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            cache.invalidate(&metrics);
            response(1)
        })
        .await;
    assert!(cache.inner.lock().unwrap().slots.is_empty());
    let fresh = cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            response(2)
        })
        .await;
    assert_eq!(json_body(fresh).await["data"][0]["count"], 2);
}

#[tokio::test]
async fn expiry_and_manual_bypass_read_fresh_data_without_caching_errors_or_partial_results() {
    let cache = DashboardCache::default();
    let metrics = SelfMetrics::new();
    cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            response(1)
        })
        .await;
    cache
        .inner
        .lock()
        .unwrap()
        .slots
        .values_mut()
        .next()
        .unwrap()
        .cached
        .as_mut()
        .unwrap()
        .started = Instant::now() - TTL;
    let fresh = cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            response(2)
        })
        .await;
    assert_eq!(fresh.headers()["x-rush-cache"], "miss");
    for directive in ["no-cache", "no-store", "max-age=0"] {
        let mut req = request("a", "u", query());
        req.headers_mut()
            .insert(header::CACHE_CONTROL, directive.parse().unwrap());
        let response = cache
            .run(req, budget(), &metrics, |_| async { response(3) })
            .await;
        assert_eq!(response.headers()["x-rush-cache"], "bypass");
        assert_eq!(json_body(response).await["data"][0]["count"], 3);
    }
    for bad in [
        json!({"errors":{"heatmap":"unavailable"}}),
        json!({"meta":{"partial":true}}),
        json!({"status":"error"}),
        json!({"warnings":["incomplete"]}),
    ] {
        cache.invalidate(&metrics);
        cache
            .run(request("a", "u", query()), budget(), &metrics, |_| async {
                Json(bad).into_response()
            })
            .await;
        assert!(
            cache
                .inner
                .lock()
                .unwrap()
                .slots
                .values()
                .all(|slot| slot.cached.is_none())
        );
    }
    cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })
        .await;
    assert!(
        cache
            .inner
            .lock()
            .unwrap()
            .slots
            .values()
            .all(|slot| slot.cached.is_none())
    );
}

#[tokio::test]
async fn capacity_is_bounded_and_busy_slots_or_large_responses_bypass() {
    let cache = DashboardCache {
        max_entries: 2,
        max_bytes: 1000,
        ..Default::default()
    };
    let metrics = SelfMetrics::new();
    for tenant in ["a", "b", "c"] {
        cache
            .run(
                request(tenant, "u", query()),
                budget(),
                &metrics,
                |_| async { response(1) },
            )
            .await;
        let inner = cache.inner.lock().unwrap();
        assert!(inner.slots.len() <= 2);
        assert!(inner.bytes <= 1000);
    }
    cache.invalidate(&metrics);
    cache
        .run(request("a", "u", query()), budget(), &metrics, |_| async {
            Json(json!({"large":"x".repeat(MAX_RESPONSE_BYTES)})).into_response()
        })
        .await;
    assert_eq!(cache.inner.lock().unwrap().bytes, 0);
    cache.invalidate(&metrics);
    let _a = cache.slot([1; 32], &metrics).unwrap();
    let _b = cache.slot([2; 32], &metrics).unwrap();
    assert!(cache.slot([3; 32], &metrics).is_none());
    let bypass = cache
        .run(request("c", "u", query()), budget(), &metrics, |_| async {
            response(8)
        })
        .await;
    assert_eq!(bypass.headers()["x-rush-cache"], "bypass");
    assert_eq!(json_body(bypass).await["data"][0]["count"], 8);
}

#[tokio::test]
async fn byte_pressure_evicts_idle_entries_and_slow_results_are_not_retained() {
    let body = axum::body::to_bytes(response(1).into_body(), MAX_RESPONSE_BYTES)
        .await
        .unwrap();
    let cache = DashboardCache {
        max_entries: 16,
        max_bytes: body.len() + 1,
        ..Default::default()
    };
    let metrics = SelfMetrics::new();
    for tenant in ["a", "b", "c"] {
        cache
            .run(
                request(tenant, "u", query()),
                budget(),
                &metrics,
                |_| async { response(1) },
            )
            .await;
        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.bytes, body.len());
        assert_eq!(inner.slots.len(), 1);
    }
    cache.invalidate(&metrics);
    let (_gate, generation) = cache.slot([0; 32], &metrics).unwrap();
    cache.store(
        [0; 32],
        generation,
        Cached {
            body,
            started: Instant::now() - TTL,
            unified: true,
        },
        &metrics,
    );
    assert_eq!(cache.inner.lock().unwrap().bytes, 0);
}

#[test]
fn only_summary_routes_and_verified_dashboard_aggregates_are_eligible() {
    let mut req = request("a", "u", query());
    assert_eq!(eligible(&req), Some(true));
    let original_uri = req.uri().clone();
    *req.uri_mut() = "/prom/api/v1/query_range?query=up&start=1&end=2&step=1"
        .parse()
        .unwrap();
    *req.method_mut() = Method::GET;
    assert_eq!(eligible(&req), Some(false));
    req.extensions_mut().insert(WorkloadClass::Integration);
    assert_eq!(eligible(&req), None);
    *req.uri_mut() = original_uri;
    *req.method_mut() = Method::POST;
    req.extensions_mut().insert(WorkloadClass::Interactive);
    assert_eq!(eligible(&req), None);
    req.headers_mut()
        .insert("x-rush-workload", "dashboard".parse().unwrap());
    assert_eq!(eligible(&req), None);
    for path in [
        "/api/v1/query",
        "/api/v1/query/export",
        "/api/v1/auth/login",
        "/api/v1/rum/query",
        "/api/v1/rum/replay",
        "/api/v1/settings/rum",
    ] {
        *req.uri_mut() = path.parse().unwrap();
        assert_eq!(eligible(&req), None);
    }
    for operation in ["search", "explore", "scatter", "export"] {
        let mut q = query();
        q["operation"] = operation.into();
        assert!(!aggregate_operation(&serde_json::to_vec(&q).unwrap()));
    }
    *req.uri_mut() = "/api/v1/services/graph?minutes=60".parse().unwrap();
    *req.method_mut() = Method::GET;
    assert_eq!(eligible(&req), Some(false));
    *req.uri_mut() = "/api/v1/retention/global".parse().unwrap();
    *req.method_mut() = Method::PUT;
    assert!(invalidates(&req));
    *req.uri_mut() = "/otlp/v1/traces".parse().unwrap();
    assert!(!invalidates(&req));
}
