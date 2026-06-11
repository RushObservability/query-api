use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Extension,
};
use std::collections::HashMap;

use crate::AppState;
use crate::TenantContext;
use crate::models::trace::{nanos_to_string, SpanEvent, SpanNode, TraceResponse, WideEvent};

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
            format!(" AND timestamp >= fromUnixTimestamp64Nano({lo}) AND timestamp <= fromUnixTimestamp64Nano({hi})")
        }
        Ok(_) => String::new(),
        Err(e) => {
            // MV missing/unhealthy: fall back to the unbounded scan rather than 404ing.
            tracing::warn!(error = %e, signal = "traces", handler = "get_trace", "spans_by_trace bounds lookup failed; falling back to unbounded scan");
            String::new()
        }
    };

    let fetch_spans = |time_bound: String| {
        let sql = format!(
            "SELECT * FROM spans PREWHERE tenant_id = '{escaped_tenant}'{time_bound} WHERE trace_id = ? ORDER BY timestamp ASC"
        );
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

    let trace = assemble_trace(&trace_id, rows);
    Ok(Json(trace))
}

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
                    let ts = e
                        .event_timestamps
                        .get(i)
                        .copied()
                        .unwrap_or_default();
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
    let mut services: Vec<String> = nodes.iter()
        .filter_map(|n| n.as_ref().map(|n| n.service_name.clone()))
        .collect();
    services.sort_unstable();
    services.dedup();

    let total_duration = nodes.iter()
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

    // Take ownership via Option::take — no cloning
    fn build_tree(
        index: usize,
        nodes: &mut Vec<Option<SpanNode>>,
        children_map: &HashMap<String, Vec<usize>>,
    ) -> SpanNode {
        let mut node = nodes[index].take().expect("node already taken");
        if let Some(child_indices) = children_map.get(&node.span_id) {
            node.children = child_indices
                .iter()
                .map(|&ci| build_tree(ci, nodes, children_map))
                .collect();
            node.children.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        node
    }

    let spans: Vec<SpanNode> = root_indices
        .iter()
        .map(|&ri| build_tree(ri, &mut nodes, &children_map))
        .collect();

    TraceResponse {
        trace_id: trace_id.to_string(),
        spans,
        span_count,
        duration_ns: total_duration,
        services,
    }
}
