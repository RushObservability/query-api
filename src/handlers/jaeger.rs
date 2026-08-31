//! Jaeger-compatible query API over the `spans` table.
//!
//! Implements the subset of the Jaeger Query HTTP API that Grafana's built-in
//! Jaeger data source calls, so traces can be browsed in Grafana with no custom
//! plugin. Mount behind `/t/{tenant}/jaeger` (the tenant middleware strips the
//! prefix) and authenticate with a tenant-scoped API key:
//!
//!   GET /jaeger/api/services
//!   GET /jaeger/api/services/{service}/operations
//!   GET /jaeger/api/traces?service=&operation=&start=&end=&minDuration=&maxDuration=&tags=&limit=
//!   GET /jaeger/api/traces/{trace_id}
//!   GET /jaeger/api/dependencies   (stub — empty)
//!
//! Span model mapping (Jaeger uses MICROSECONDS for time/duration):
//!   traceID←trace_id, spanID←span_id, CHILD_OF ref←parent_span_id,
//!   operationName←span_name, startTime←timestamp(ns)/1000, duration←duration_ns/1000,
//!   process.serviceName←service_name, tags←attributes(JSON) + kind/status.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::models::trace::WideEvent;
use crate::query_builder::escape_string_literal;
use crate::{AppState, TenantContext};

const EMPTY_PARENT: &str = "0000000000000000";

fn ch_err<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    tracing::error!(error = %e, handler = "jaeger", "ClickHouse query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
}

/// Jaeger response envelope: `{ data, total, limit, offset, errors }`.
fn envelope<T: serde::Serialize>(data: Vec<T>) -> Json<Value> {
    Json(json!({
        "data": data,
        "total": 0,
        "limit": 0,
        "offset": 0,
        "errors": Value::Null,
    }))
}

/// GET /jaeger/api/services → list of service names.
pub async fn services(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let esc_t = escape_string_literal(tenant_id);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct R {
        s: String,
    }
    let rows = crate::tenant_query(
        &state.ch,
        &format!(
            "SELECT DISTINCT service_name AS s FROM services \
             WHERE tenant_id = '{esc_t}' AND service_name != '' ORDER BY service_name"
        ),
        tenant_id,
    )
    .fetch_all::<R>()
    .await
    .map_err(ch_err)?;

    Ok(envelope(rows.into_iter().map(|r| r.s).collect::<Vec<_>>()))
}

/// GET /jaeger/api/services/{service}/operations → operation (span) names.
pub async fn operations(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(service): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let esc_t = escape_string_literal(tenant_id);
    let esc_s = escape_string_literal(&service);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct R {
        s: String,
    }
    let rows = crate::tenant_query(
        &state.ch,
        &format!(
            "SELECT DISTINCT span_name AS s FROM spans \
             WHERE tenant_id = '{esc_t}' AND service_name = '{esc_s}' AND span_name != '' \
             ORDER BY span_name LIMIT 2000"
        ),
        tenant_id,
    )
    .fetch_all::<R>()
    .await
    .map_err(ch_err)?;

    Ok(envelope(rows.into_iter().map(|r| r.s).collect::<Vec<_>>()))
}

/// GET /jaeger/api/traces — trace search.
pub async fn search(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(p): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let esc_t = escape_string_literal(tenant_id);

    // ── Build the per-span match conditions (a trace matches if any of its
    //    spans satisfies all of them — standard Jaeger search semantics). ──
    let mut conds = vec![format!("tenant_id = '{esc_t}'")];

    if let Some(svc) = p
        .get("service")
        .filter(|s| !s.is_empty() && s.as_str() != "all")
    {
        conds.push(format!("service_name = '{}'", escape_string_literal(svc)));
    }
    if let Some(op) = p
        .get("operation")
        .filter(|s| !s.is_empty() && s.as_str() != "all")
    {
        conds.push(format!("span_name = '{}'", escape_string_literal(op)));
    }

    // Jaeger sends start/end in microseconds since epoch.
    let start_us = p.get("start").and_then(|s| s.parse::<i64>().ok());
    let end_us = p.get("end").and_then(|s| s.parse::<i64>().ok());
    if let Some(st) = start_us {
        conds.push(format!("timestamp >= fromUnixTimestamp64Micro({st})"));
    }
    if let Some(en) = end_us {
        conds.push(format!("timestamp <= fromUnixTimestamp64Micro({en})"));
    }

    if let Some(min_ns) = p.get("minDuration").and_then(|s| parse_go_duration_ns(s)) {
        conds.push(format!("duration_ns >= {min_ns}"));
    }
    if let Some(max_ns) = p.get("maxDuration").and_then(|s| parse_go_duration_ns(s)) {
        conds.push(format!("duration_ns <= {max_ns}"));
    }

    // Grafana sends `tags` as a JSON object string, e.g. {"error":"true"}.
    if let Some(raw) = p.get("tags") {
        if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(raw) {
            for (k, v) in map {
                let vs = match v {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                conds.push(format!(
                    "JSONExtractString(attributes, '{}') = '{}'",
                    escape_string_literal(&k),
                    escape_string_literal(&vs)
                ));
            }
        }
    }

    let limit = p
        .get("limit")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(20)
        .clamp(1, 1500);

    let where_clause = conds.join(" AND ");

    // Step 1: candidate trace ids (most recent first).
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TR {
        trace_id: String,
    }
    let id_sql = format!(
        "SELECT trace_id FROM spans WHERE {where_clause} \
         GROUP BY trace_id ORDER BY max(timestamp) DESC LIMIT {limit}"
    );
    let ids = crate::tenant_query(&state.ch, &id_sql, tenant_id)
        .fetch_all::<TR>()
        .await
        .map_err(ch_err)?;

    if ids.is_empty() {
        return Ok(envelope(Vec::<Value>::new()));
    }

    // Step 2: fetch ALL spans for those traces (full waterfalls), bounded to the
    // search window ±5 min so ClickHouse prunes granules.
    let in_list = ids
        .iter()
        .map(|r| format!("'{}'", escape_string_literal(&r.trace_id)))
        .collect::<Vec<_>>()
        .join(",");
    let mut span_conds = vec![
        format!("tenant_id = '{esc_t}'"),
        format!("trace_id IN ({in_list})"),
    ];
    const PAD_US: i64 = 300_000_000; // 5 minutes in microseconds
    if let Some(st) = start_us {
        span_conds.push(format!(
            "timestamp >= fromUnixTimestamp64Micro({})",
            st - PAD_US
        ));
    }
    if let Some(en) = end_us {
        span_conds.push(format!(
            "timestamp <= fromUnixTimestamp64Micro({})",
            en + PAD_US
        ));
    }
    let span_sql = format!(
        "SELECT * FROM spans WHERE {} ORDER BY trace_id, timestamp ASC",
        span_conds.join(" AND ")
    );
    let rows = crate::tenant_query(&state.ch, &span_sql, tenant_id)
        .fetch_all::<WideEvent>()
        .await
        .map_err(ch_err)?;

    let mut by_trace: HashMap<String, Vec<WideEvent>> = HashMap::new();
    for r in rows {
        by_trace.entry(r.trace_id.clone()).or_default().push(r);
    }
    // Preserve the recency order from step 1.
    let traces: Vec<Value> = ids
        .iter()
        .filter_map(|r| {
            by_trace
                .remove(&r.trace_id)
                .map(|spans| build_jaeger_trace(&r.trace_id, spans))
        })
        .collect();

    Ok(envelope(traces))
}

/// GET /jaeger/api/traces/{trace_id} — single trace by id.
pub async fn get_trace(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(trace_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "trace_id must be a 32-character hex string".to_string(),
        ));
    }
    let tenant_id = &tenant.tenant_id;
    let esc_t = escape_string_literal(tenant_id);
    let esc_id = escape_string_literal(&trace_id);

    let rows = crate::tenant_query(
        &state.ch,
        &format!(
            "SELECT * FROM spans WHERE tenant_id = '{esc_t}' AND trace_id = '{esc_id}' \
             ORDER BY timestamp ASC"
        ),
        tenant_id,
    )
    .fetch_all::<WideEvent>()
    .await
    .map_err(ch_err)?;

    if rows.is_empty() {
        return Ok(envelope(Vec::<Value>::new()));
    }
    Ok(envelope(vec![build_jaeger_trace(&trace_id, rows)]))
}

/// GET /jaeger/api/dependencies — stub (no precomputed dependency graph).
pub async fn dependencies(Extension(_tenant): Extension<TenantContext>) -> impl IntoResponse {
    envelope(Vec::<Value>::new())
}

/// Assemble a single Jaeger trace JSON object from a trace's spans.
fn build_jaeger_trace(trace_id: &str, spans: Vec<WideEvent>) -> Value {
    // Deduplicate by span_id (keep first).
    let mut seen = std::collections::HashSet::new();
    let mut uniq: Vec<WideEvent> = Vec::with_capacity(spans.len());
    for s in spans {
        if seen.insert(s.span_id.clone()) {
            uniq.push(s);
        }
    }

    // One processID per distinct service.
    let mut proc_ids: HashMap<String, String> = HashMap::new();
    for e in &uniq {
        if !proc_ids.contains_key(&e.service_name) {
            let id = format!("p{}", proc_ids.len() + 1);
            proc_ids.insert(e.service_name.clone(), id);
        }
    }
    let mut processes = serde_json::Map::new();
    for (svc, pid) in &proc_ids {
        processes.insert(pid.clone(), json!({ "serviceName": svc, "tags": [] }));
    }

    let spans_json: Vec<Value> = uniq
        .iter()
        .map(|e| {
            let mut tags: Vec<Value> = Vec::new();

            // Span attributes (all stored as JSON strings → "string" tags).
            if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&e.attributes) {
                for (k, v) in map {
                    let (ty, val) = match v {
                        Value::String(s) => ("string", Value::String(s)),
                        Value::Bool(b) => ("bool", Value::Bool(b)),
                        Value::Number(n) => (
                            if n.is_f64() { "float64" } else { "int64" },
                            Value::Number(n),
                        ),
                        other => ("string", Value::String(other.to_string())),
                    };
                    tags.push(json!({ "key": k, "type": ty, "value": val }));
                }
            }
            // Span-level fields not present in the attributes map.
            if !e.kind.is_empty() {
                tags.push(json!({ "key": "span.kind", "type": "string", "value": e.kind }));
            }
            if !e.status.is_empty() {
                tags.push(
                    json!({ "key": "otel.status_code", "type": "string", "value": e.status }),
                );
                if e.status.to_uppercase().contains("ERROR") {
                    tags.push(json!({ "key": "error", "type": "bool", "value": true }));
                }
            }

            let references = if e.parent_span_id.is_empty() || e.parent_span_id == EMPTY_PARENT {
                Vec::new()
            } else {
                vec![json!({
                    "refType": "CHILD_OF",
                    "traceID": trace_id,
                    "spanID": e.parent_span_id,
                })]
            };

            json!({
                "traceID": trace_id,
                "spanID": e.span_id,
                "operationName": e.span_name,
                "references": references,
                "startTime": e.timestamp / 1000,            // ns → µs
                "duration": (e.duration_ns / 1000) as i64,  // ns → µs
                "tags": tags,
                "logs": [],
                "processID": proc_ids.get(&e.service_name).cloned().unwrap_or_default(),
                "flags": 0,
                "warnings": Value::Null,
            })
        })
        .collect();

    json!({
        "traceID": trace_id,
        "spans": spans_json,
        "processes": Value::Object(processes),
        "warnings": Value::Null,
    })
}

/// Parse a Go-style duration string (e.g. `100ms`, `1.5s`, `500us`, `2m`) into
/// nanoseconds. Returns None if it doesn't parse.
fn parse_go_duration_ns(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let idx = s.find(|c: char| c.is_ascii_alphabetic() || c == 'µ')?;
    let (num, unit) = s.split_at(idx);
    let val: f64 = num.parse().ok()?;
    let mult: f64 = match unit {
        "ns" => 1.0,
        "us" | "µs" => 1_000.0,
        "ms" => 1_000_000.0,
        "s" => 1_000_000_000.0,
        "m" => 60_000_000_000.0,
        "h" => 3_600_000_000_000.0,
        _ => return None,
    };
    Some((val * mult) as u64)
}
