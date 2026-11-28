use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::collections::HashMap;

use crate::AppState;
use crate::TenantContext;
use crate::models::trace::{SpanEvent, SpanNode, TraceResponse, WideEvent, nanos_to_string};

pub async fn get_trace(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(trace_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    // Validate trace_id is hex and correct length
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "trace_id must be a 32-character hex string".to_string(),
        ));
    }

    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);

    // Fast path: the spans_by_trace MV is ORDER BY (tenant_id, trace_id, timestamp), so
    // resolving the trace's time bounds is a primary-key lookup instead of a
    // whole-retention bloom-filter probe. The wide-column spans fetch is then bounded
    // to that window (±5 min for clock skew), pruning to a handful of granules.
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TraceTimeBounds {
        min_ns: i64,
        max_ns: i64,
        cnt: u64,
    }

    let bounds = crate::tenant_query(
        &state.ch,
        &format!(
            "SELECT min(toUnixTimestamp64Nano(timestamp)) AS min_ns, \
                 max(toUnixTimestamp64Nano(timestamp)) AS max_ns, count() AS cnt \
                 FROM spans_by_trace \
                 PREWHERE tenant_id = '{escaped_tenant}' WHERE trace_id = ?"
        ),
        tenant_id,
    )
    .bind(&trace_id)
    .fetch_one::<TraceTimeBounds>()
    .await;

    // Time bound for the spans fetch, when the MV knows the trace. lo/hi are
    // server-computed i64 nanoseconds (not user input). ±5 min pad for clock skew.
    const PAD_NS: i64 = 300_000_000_000;
    let time_bound = match &bounds {
        Ok(b) if b.cnt > 0 => {
            let lo = b.min_ns.saturating_sub(PAD_NS);
            let hi = b.max_ns.saturating_add(PAD_NS);
            format!(
                " AND timestamp >= fromUnixTimestamp64Nano({lo}) AND timestamp <= fromUnixTimestamp64Nano({hi})"
            )
        }
        Ok(_) => String::new(),
        Err(e) => {
            // MV missing/unhealthy: fall back to the unbounded scan rather than 404ing.
            tracing::warn!(error = %e, signal = "traces", handler = "get_trace", "spans_by_trace bounds lookup failed; falling back to unbounded scan");
            String::new()
        }
    };

    let fetch_spans = |time_bound: String| {
        // Single WHERE (no explicit PREWHERE): `trace_id` is served by the
        // `idx_trace_id` bloom filter, which an explicit PREWHERE on tenant/time
        // would defeat (CH would read the whole bloom instead of pruning) — most
        // damaging on the MV-miss fallback where `time_bound` is empty.
        // `optimize_move_to_prewhere` still promotes tenant/time to prewhere.
        let sql = trace_spans_sql(&escaped_tenant, &time_bound);
        let q = crate::tenant_query(&state.ch, &sql, tenant_id).bind(&trace_id);
        async move { q.fetch_all::<WideEvent>().await }
    };

    let mut rows = fetch_spans(time_bound.clone())
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "get_trace", "ClickHouse query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    // Defensive fallback: the MV is populated asynchronously, so if the bounded fetch
    // found nothing while the MV claimed knowledge (or the MV had no rows but the
    // trace exists only in spans, e.g. data ingested before the MV was created),
    // retry without the time bound before declaring 404.
    if rows.is_empty() && !time_bound.is_empty() {
        rows = fetch_spans(String::new())
            .await
            .map_err(|e| {
                tracing::error!(error = %e, signal = "traces", handler = "get_trace", "ClickHouse fallback query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;
    }

    if rows.is_empty() {
        return Err((StatusCode::NOT_FOUND, "trace not found".to_string()));
    }

    let truncated = cap_trace_rows(&mut rows);
    let mut trace = assemble_trace(&trace_id, rows);
    trace.truncated = truncated;
    Ok(Json(trace))
}

/// Most spans one trace response carries. The earliest spans are kept and the response
/// says `truncated`, so one huge trace can't build a response of hundreds of MB.
pub(crate) const MAX_TRACE_SPANS: usize = 10_000;

/// Spans of one trace, earliest first, one row past the cap so truncation is detectable.
/// `escaped_tenant` must already be escaped; the trace id is bound as `?`.
pub(crate) fn trace_spans_sql(escaped_tenant: &str, time_bound: &str) -> String {
    format!(
        "SELECT ?fields FROM spans WHERE tenant_id = '{escaped_tenant}' AND trace_id = ?{time_bound} \
         ORDER BY timestamp ASC LIMIT {}",
        MAX_TRACE_SPANS + 1
    )
}

/// Drop rows past `MAX_TRACE_SPANS`; true when any were dropped.
pub(crate) fn cap_trace_rows(rows: &mut Vec<WideEvent>) -> bool {
    let truncated = rows.len() > MAX_TRACE_SPANS;
    rows.truncate(MAX_TRACE_SPANS);
    truncated
}

/// Deepest nesting level in the assembled tree; deeper spans are attached flat at this level.
const MAX_TREE_DEPTH: usize = 256;

/// Build the span tree from a flat list of wide events.
fn assemble_trace(trace_id: &str, events: Vec<WideEvent>) -> TraceResponse {
    // Deduplicate by span_id, keeping the first occurrence
    let mut best: std::collections::HashMap<String, WideEvent> = std::collections::HashMap::new();
    for e in events {
        best.entry(e.span_id.clone()).or_insert(e);
    }
    let events: Vec<WideEvent> = best.into_values().collect();

    let mut nodes: Vec<Option<SpanNode>> = events
        .iter()
        .map(|e| {
            let attributes: serde_json::Value =
                serde_json::from_str(&e.attributes).unwrap_or(serde_json::Value::Null);

            let span_events: Vec<SpanEvent> = e
                .event_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let ts = e.event_timestamps.get(i).copied().unwrap_or_default();
                    let attrs: serde_json::Value = e
                        .event_attributes
                        .get(i)
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(serde_json::Value::Null);
                    SpanEvent {
                        timestamp: nanos_to_string(ts),
                        name: name.clone(),
                        attributes: attrs,
                    }
                })
                .collect();

            Some(SpanNode {
                span_id: e.span_id.clone(),
                parent_span_id: e.parent_span_id.clone(),
                service_name: e.service_name.clone(),
                span_name: e.span_name.clone(),
                kind: e.kind.clone(),
                service_version: String::new(),
                http_method: e.http_method.clone(),
                http_path: e.http_path.clone(),
                http_status_code: e.http_status_code,
                duration_ns: e.duration_ns,
                status: e.status.clone(),
                timestamp: nanos_to_string(e.timestamp),
                attributes,
                events: span_events,
                children: vec![],
            })
        })
        .collect();

    let span_count = nodes.len();

    // Collect services and max duration before taking ownership
    let mut services: Vec<String> = nodes
        .iter()
        .filter_map(|n| n.as_ref().map(|n| n.service_name.clone()))
        .collect();
    services.sort_unstable();
    services.dedup();

    let total_duration = nodes
        .iter()
        .filter_map(|n| n.as_ref().map(|n| n.duration_ns))
        .max()
        .unwrap_or(0);

    // Build parent -> children map using references to span_id/parent_span_id
    let empty_parent = "0000000000000000";
    let mut children_map: HashMap<String, Vec<usize>> = HashMap::new();
    let mut root_indices: Vec<usize> = Vec::new();

    for (i, node) in nodes.iter().enumerate() {
        if let Some(n) = node {
            if n.parent_span_id == empty_parent || n.parent_span_id.is_empty() {
                root_indices.push(i);
            } else {
                children_map
                    .entry(n.parent_span_id.clone())
                    .or_default()
                    .push(i);
            }
        }
    }

    // Iterative build: recursion here (and in serde/Drop of the nested result) overflowed
    // the worker stack on a deep parent chain. Breadth-first order gives each node its
    // attach target; spans below MAX_TREE_DEPTH attach flat to their depth-cap ancestor,
    // so the nested response stays shallow while every span is kept.
    let mut order: Vec<usize> = Vec::with_capacity(span_count);
    let mut attach_to: Vec<usize> = vec![usize::MAX; nodes.len()];
    let mut depth: Vec<usize> = vec![0; nodes.len()];
    let mut queue: std::collections::VecDeque<usize> = root_indices.iter().copied().collect();
    while let Some(i) = queue.pop_front() {
        order.push(i);
        let Some(node) = nodes[i].as_ref() else {
            continue;
        };
        if let Some(child_indices) = children_map.get(&node.span_id) {
            let (target, child_depth) = if depth[i] < MAX_TREE_DEPTH {
                (i, depth[i] + 1)
            } else {
                (attach_to[i], depth[i])
            };
            for &ci in child_indices {
                attach_to[ci] = target;
                depth[ci] = child_depth;
                queue.push_back(ci);
            }
        }
    }

    // Reverse BFS order: a node's children are all attached before it moves to its parent.
    for &i in order.iter().rev() {
        if attach_to[i] == usize::MAX {
            continue; // root
        }
        let mut node = nodes[i].take().expect("node already taken");
        node.children.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        if let Some(parent) = nodes[attach_to[i]].as_mut() {
            parent.children.push(node);
        }
    }

    let spans: Vec<SpanNode> = root_indices
        .iter()
        .filter_map(|&ri| nodes[ri].take())
        .map(|mut root| {
            root.children.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            root
        })
        .collect();

    TraceResponse {
        trace_id: trace_id.to_string(),
        spans,
        span_count,
        duration_ns: total_duration,
        services,
        truncated: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ids start at 1: an all-zero parent id means "no parent".
    fn span(i: usize, parent: Option<usize>) -> WideEvent {
        serde_json::from_value(serde_json::json!({
            "tenant_id": "t", "timestamp": 1_800_000_000_000_000_000i64 + i as i64 * 1000,
            "trace_id": "0af7651916cd43dd8448eb211c80319c", "span_id": format!("{:016x}", i + 1),
            "parent_span_id": parent.map(|p| format!("{:016x}", p + 1)).unwrap_or_default(),
            "service_name": "svc", "span_name": "work", "kind": "INTERNAL", "status": "OK",
            "duration_ns": 1, "http_method": "", "http_path": "", "http_status_code": 0,
            "attributes": "{}", "event_names": [], "event_timestamps": [], "event_attributes": [],
            "link_trace_ids": [], "link_span_ids": []
        }))
        .unwrap()
    }

    fn walk(nodes: &[SpanNode], depth: usize, max_depth: &mut usize, count: &mut usize) {
        // Explicit stack so the check itself cannot overflow.
        let mut stack: Vec<(&SpanNode, usize)> = nodes.iter().map(|n| (n, depth)).collect();
        while let Some((n, d)) = stack.pop() {
            *count += 1;
            *max_depth = (*max_depth).max(d);
            stack.extend(n.children.iter().map(|c| (c, d + 1)));
        }
    }

    #[test]
    fn deep_parent_chain_does_not_overflow_the_worker_stack() {
        // 2 MiB = tokio worker default stack. Assemble, serialize and drop must all fit.
        let handle = std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(|| {
                let depth = 100_000;
                let rows: Vec<WideEvent> = (0..depth).map(|i| span(i, i.checked_sub(1))).collect();
                let trace = assemble_trace("0af7651916cd43dd8448eb211c80319c", rows);
                let (mut max_depth, mut count) = (0, 0);
                walk(&trace.spans, 0, &mut max_depth, &mut count);
                let body = serde_json::to_vec(&trace).unwrap();
                drop(trace);
                (max_depth, count, body.len())
            })
            .unwrap();
        let (max_depth, count, len) = handle.join().expect("assembling a deep trace crashed");
        assert_eq!(count, 100_000, "every span is kept");
        assert_eq!(max_depth, MAX_TREE_DEPTH);
        assert!(len > 0);
    }

    #[test]
    fn trace_fetch_is_capped_and_reports_truncation() {
        let sql = trace_spans_sql("t", "");
        assert!(
            sql.ends_with(&format!("LIMIT {}", MAX_TRACE_SPANS + 1)),
            "{sql}"
        );

        let mut rows: Vec<WideEvent> = (0..MAX_TRACE_SPANS + 1).map(|i| span(i, None)).collect();
        assert!(cap_trace_rows(&mut rows));
        assert_eq!(rows.len(), MAX_TRACE_SPANS);
        assert!(!cap_trace_rows(&mut rows));

        let trace = assemble_trace("0af7651916cd43dd8448eb211c80319c", vec![span(0, None)]);
        let json = serde_json::to_value(&trace).unwrap();
        assert_eq!(json["truncated"], false);
    }

    #[test]
    fn shallow_tree_keeps_nesting_and_child_order() {
        // root 0 -> children 2, 1 (out of order) -> 3 under 1
        let rows = vec![
            span(3, Some(1)),
            span(2, Some(0)),
            span(0, None),
            span(1, Some(0)),
        ];
        let trace = assemble_trace("0af7651916cd43dd8448eb211c80319c", rows);
        assert_eq!(trace.spans.len(), 1);
        let root = &trace.spans[0];
        let ids: Vec<&str> = root.children.iter().map(|c| c.span_id.as_str()).collect();
        assert_eq!(ids, ["0000000000000002", "0000000000000003"]);
        assert_eq!(root.children[0].children[0].span_id, "0000000000000004");
        assert_eq!(trace.span_count, 4);
    }
}
