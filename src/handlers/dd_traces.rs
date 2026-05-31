use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
    Extension,
};
use prost::Message;
use serde::Deserialize;
use std::collections::HashMap;

use crate::AppState;
use crate::TenantContext;
use crate::ch_writer::{SpoolBatch, WriteError};
use crate::models::ingest::TraceInsertRow;
use super::dd_common::{validate_api_key, decompress_body};

// ═══ DD Agent protobuf types (AgentPayload) ═══
// The DD agent v1 trace writer sends protobuf-encoded AgentPayload
// to /api/v0.2/traces, NOT the Vec<Vec<Span>> msgpack format.

#[derive(Clone, PartialEq, Message)]
struct AgentPayload {
    #[prost(string, tag = "1")]
    host_name: String,
    #[prost(string, tag = "2")]
    env: String,
    #[prost(message, repeated, tag = "5")]
    tracer_payloads: Vec<TracerPayload>,
    // Note: we intentionally skip fields 7-11 (tags, agentVersion, targetTPS,
    // errorTPS, rareSamplerEnabled) — prost will skip unknown fields.
}

#[derive(Clone, PartialEq, Message)]
struct TracerPayload {
    #[prost(string, tag = "1")]
    container_id: String,
    #[prost(string, tag = "2")]
    language_name: String,
    #[prost(string, tag = "3")]
    language_version: String,
    #[prost(string, tag = "4")]
    tracer_version: String,
    #[prost(message, repeated, tag = "5")]
    chunks: Vec<TraceChunk>,
    // DD agent v1 writer uses tag 6 (deprecated "traces" field) with TraceChunk format
    #[prost(message, repeated, tag = "6")]
    traces: Vec<TraceChunk>,
    #[prost(string, tag = "8")]
    app_version: String,
    #[prost(string, tag = "9")]
    hostname: String,
}

#[derive(Clone, PartialEq, Message)]
struct TraceChunk {
    #[prost(int32, tag = "1")]
    priority: i32,
    #[prost(string, tag = "2")]
    origin: String,
    #[prost(message, repeated, tag = "3")]
    spans: Vec<PbSpan>,
}

#[derive(Clone, PartialEq, Message)]
struct PbSpan {
    #[prost(string, tag = "1")]
    service: String,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(string, tag = "3")]
    resource: String,
    #[prost(uint64, tag = "4")]
    trace_id: u64,
    #[prost(uint64, tag = "5")]
    span_id: u64,
    #[prost(uint64, tag = "6")]
    parent_id: u64,
    #[prost(int64, tag = "7")]
    start: i64,
    #[prost(int64, tag = "8")]
    duration: i64,
    #[prost(int32, tag = "9")]
    error: i32,
    #[prost(map = "string, string", tag = "10")]
    meta: HashMap<String, String>,
    #[prost(map = "string, double", tag = "11")]
    metrics: HashMap<String, f64>,
    #[prost(string, tag = "12")]
    r#type: String,
}

// ═══ V0.4 msgpack trace payload (from dd-trace libraries) ═══

/// A span as sent by dd-trace libraries in v0.3/v0.4 format.
#[derive(Debug, Deserialize)]
struct DdSpan {
    service: String,
    name: String,
    #[serde(default)]
    resource: String,
    #[serde(rename = "trace_id")]
    trace_id: u64,
    #[serde(rename = "span_id")]
    span_id: u64,
    #[serde(rename = "parent_id", default)]
    parent_id: u64,
    start: i64,    // nanoseconds
    duration: i64, // nanoseconds
    #[serde(default)]
    error: i32,
    #[serde(default)]
    meta: std::collections::HashMap<String, String>,
    #[serde(default)]
    metrics: std::collections::HashMap<String, f64>,
    #[serde(rename = "type", default)]
    span_type: String,
}


/// Convert a 64-bit DD trace/span ID to a hex string.
fn id_to_hex(id: u64, width: usize) -> String {
    format!("{:0>width$x}", id, width = width)
}

/// Map DD span type to OTEL SpanKind.
fn dd_type_to_span_kind(span_type: &str) -> &'static str {
    match span_type {
        "web" | "http" => "SPAN_KIND_SERVER",
        "client" | "dns" | "grpc" => "SPAN_KIND_CLIENT",
        "db" | "cache" | "memcached" | "redis" | "sql" | "cassandra" | "elasticsearch" => "SPAN_KIND_CLIENT",
        "worker" | "consumer" => "SPAN_KIND_CONSUMER",
        "producer" => "SPAN_KIND_PRODUCER",
        _ => "SPAN_KIND_INTERNAL",
    }
}

/// Convert a DdSpan into an spans_raw insert row.
/// The materialized view `spans_mv` handles populating spans automatically.
fn convert_span(
    span: &DdSpan,
    env: &str,
    hostname: &str,
    tenant_id: &str,
) -> TraceInsertRow {
    let trace_id = id_to_hex(span.trace_id, 32);
    let span_id = id_to_hex(span.span_id, 16);
    let parent_span_id = if span.parent_id == 0 {
        String::new()
    } else {
        id_to_hex(span.parent_id, 16)
    };

    let span_kind = dd_type_to_span_kind(&span.span_type);
    let status_code = if span.error != 0 { "STATUS_CODE_ERROR" } else { "STATUS_CODE_OK" };
    let status_message = span.meta.get("error.message").cloned().unwrap_or_default();

    // Build resource attributes (OTEL standard keys for the MV to extract)
    let mut resource_attrs = Vec::new();
    resource_attrs.push(("service.name".to_string(), span.service.clone()));
    if !hostname.is_empty() {
        resource_attrs.push(("host.name".to_string(), hostname.to_string()));
    }
    if !env.is_empty() {
        resource_attrs.push(("deployment.environment".to_string(), env.to_string()));
    }
    // Map DD version to OTEL service.version
    if let Some(ver) = span.meta.get("version") {
        if !ver.is_empty() {
            resource_attrs.push(("service.version".to_string(), ver.clone()));
        }
    }

    // Build span attributes from DD meta + metrics + resource
    let mut span_attrs: Vec<(String, String)> = span.meta.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (k, v) in &span.metrics {
        span_attrs.push((k.clone(), v.to_string()));
    }
    if !span.resource.is_empty() {
        span_attrs.push(("dd.resource".to_string(), span.resource.clone()));
    }
    if !span.span_type.is_empty() {
        span_attrs.push(("dd.type".to_string(), span.span_type.clone()));
    }

    TraceInsertRow {
        tenant_id: tenant_id.to_string(),
        timestamp: span.start,
        trace_id,
        span_id,
        parent_span_id,
        trace_state: String::new(),
        span_name: span.name.clone(),
        span_kind: span_kind.to_string(),
        service_name: span.service.clone(),
        resource_attributes: resource_attrs,
        scope_name: "datadog".to_string(),
        scope_version: String::new(),
        span_attributes: span_attrs,
        duration: span.duration.max(0) as u64,
        status_code: status_code.to_string(),
        status_message,
        events_timestamp: Vec::new(),
        events_name: Vec::new(),
        events_attributes: Vec::new(),
        links_trace_id: Vec::new(),
        links_span_id: Vec::new(),
        links_trace_state: Vec::new(),
        links_attributes: Vec::new(),
    }
}

/// PUT /datadog/v0.4/traces — Accept traces from dd-trace libraries (msgpack).
///
/// Payload: array of traces, each trace is array of spans (msgpack-encoded).
pub async fn ingest_v04(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    // DD trace libs don't send DD-API-KEY (they send to the local agent)
    // but we still accept it if present
    let _ = validate_api_key(&headers);

    let raw = decompress_body(&headers, body)?;

    // Decode msgpack: Vec<Vec<DdSpan>> (array of traces, each trace is array of spans)
    let traces: Vec<Vec<DdSpan>> = rmp_serde::from_slice(&raw).map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("msgpack decode failed: {e}"))
    })?;

    let span_count: usize = traces.iter().map(|t| t.len()).sum();
    if span_count == 0 {
        return Ok(Json(serde_json::json!({"rate_by_service": {}})));
    }

    let rows: Vec<TraceInsertRow> = traces.iter().flat_map(|trace| {
        trace.iter().map(|span| {
            let env = span.meta.get("env").cloned().unwrap_or_default();
            let hostname = span.meta.get("_dd.hostname").cloned().unwrap_or_default();
            convert_span(span, &env, &hostname, tenant_id)
        })
    }).collect();

    state.writer.write(SpoolBatch::SpansRaw(rows)).await.map_err(|e| match e {
        WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
        WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
    })?;

    // Record usage for per-tenant ingest metering
    state.usage_accumulator.record(tenant_id, "traces", span_count as u64, raw.len() as u64);

    tracing::info!(
        signal = "traces",
        tenant_id = %tenant_id,
        spans_count = span_count,
        traces_count = traces.len(),
        source = "datadog",
        endpoint = "v0.4",
        "ingested spans"
    );

    // Return empty rate_by_service (Rush doesn't do agent-side sampling)
    Ok(Json(serde_json::json!({"rate_by_service": {}})))
}

/// PUT /datadog/v0.3/traces — Accept traces (JSON or msgpack, legacy format).
pub async fn ingest_v03(
    state: State<AppState>,
    tenant: Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // v0.3 uses the same span format as v0.4, just with different response
    ingest_v04(state, tenant, headers, body).await
}

/// /datadog/api/v0.2/traces — Accept traces from the DD agent trace writer.
///
/// The DD agent v1 trace writer sends protobuf-encoded AgentPayload.
/// We try protobuf first, then msgpack as fallback.
pub async fn ingest_agent(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let _ = validate_api_key(&headers);
    let raw = decompress_body(&headers, body)?;

    // Log content-type for debugging
    let ct = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("none");
    tracing::debug!(
        endpoint = "v0.2",
        bytes = raw.len(),
        content_type = ct,
        "datadog traces payload received"
    );


    // Try protobuf AgentPayload first
    match AgentPayload::decode(raw.as_slice()) {
        Err(e) => {
            tracing::debug!(error = %e, endpoint = "v0.2", "protobuf decode failed, trying msgpack");
        }
        Ok(payload) => {
        let env = payload.env.clone();
        let hostname = payload.host_name.clone();
        let tp_count = payload.tracer_payloads.len();
        let chunk_count: usize = payload.tracer_payloads.iter()
            .map(|tp| tp.chunks.len() + tp.traces.len()).sum();
        let total_spans: usize = payload.tracer_payloads.iter()
            .flat_map(|tp| tp.chunks.iter().chain(tp.traces.iter()))
            .map(|c| c.spans.len())
            .sum();
        tracing::debug!(
            endpoint = "v0.2",
            host = %hostname,
            env = %env,
            tracer_payloads = tp_count,
            chunks = chunk_count,
            spans = total_spans,
            "protobuf payload decoded"
        );
        let mut span_count = 0usize;
        // Collect all spans from the protobuf payload
        let mut all_spans: Vec<DdSpan> = Vec::new();
        for tp in &payload.tracer_payloads {
            let tp_hostname = if tp.hostname.is_empty() { &hostname } else { &tp.hostname };
            // DD agent uses both tag 5 (chunks) and tag 6 (traces/deprecated) for TraceChunks
            let all_chunks = tp.chunks.iter().chain(tp.traces.iter());
            for chunk in all_chunks {
                for pb_span in &chunk.spans {
                    span_count += 1;
                    all_spans.push(DdSpan {
                        service: pb_span.service.clone(),
                        name: pb_span.name.clone(),
                        resource: pb_span.resource.clone(),
                        trace_id: pb_span.trace_id,
                        span_id: pb_span.span_id,
                        parent_id: pb_span.parent_id,
                        start: pb_span.start,
                        duration: pb_span.duration,
                        error: pb_span.error,
                        meta: pb_span.meta.clone(),
                        metrics: pb_span.metrics.clone(),
                        span_type: pb_span.r#type.clone(),
                    });
                    // Inject env/hostname into meta if not present
                    if let Some(span) = all_spans.last_mut() {
                        if !env.is_empty() && !span.meta.contains_key("env") {
                            span.meta.insert("env".to_string(), env.clone());
                        }
                        if !tp_hostname.is_empty() && !span.meta.contains_key("_dd.hostname") {
                            span.meta.insert("_dd.hostname".to_string(), tp_hostname.clone());
                        }
                        if !tp.app_version.is_empty() && !span.meta.contains_key("version") {
                            span.meta.insert("version".to_string(), tp.app_version.clone());
                        }
                    }
                }
            }
        }

        if span_count == 0 {
            return Ok(Json(serde_json::json!({"rate_by_service": {}})));
        }

        let rows: Vec<TraceInsertRow> = all_spans.iter().map(|span| {
            let span_env = span.meta.get("env").cloned().unwrap_or_default();
            let span_host = span.meta.get("_dd.hostname").cloned().unwrap_or_default();
            convert_span(span, &span_env, &span_host, tenant_id)
        }).collect();

        state.writer.write(SpoolBatch::SpansRaw(rows)).await.map_err(|e| match e {
            WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
            WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
        })?;

        // Record usage for per-tenant ingest metering (protobuf path)
        state.usage_accumulator.record(tenant_id, "traces", span_count as u64, raw.len() as u64);

        tracing::info!(
            signal = "traces",
            tenant_id = %tenant_id,
            spans_count = span_count,
            source = "datadog",
            endpoint = "v0.2",
            encoding = "protobuf",
            "ingested spans"
        );
        return Ok(Json(serde_json::json!({"rate_by_service": {}})));
        }
    }

    // Fallback: try msgpack decode (Vec<Vec<DdSpan>> format)
    match rmp_serde::from_slice::<Vec<Vec<DdSpan>>>(&raw) {
        Ok(traces) => {
            let span_count: usize = traces.iter().map(|t| t.len()).sum();
            if span_count == 0 {
                return Ok(Json(serde_json::json!({"rate_by_service": {}})));
            }

            let rows: Vec<TraceInsertRow> = traces.iter().flat_map(|trace| {
                trace.iter().map(|span| {
                    let env = span.meta.get("env").cloned().unwrap_or_default();
                    let hostname = span.meta.get("_dd.hostname").cloned().unwrap_or_default();
                    convert_span(span, &env, &hostname, tenant_id)
                })
            }).collect();

            state.writer.write(SpoolBatch::SpansRaw(rows)).await.map_err(|e| match e {
                WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
                WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
            })?;

            // Record usage for per-tenant ingest metering (msgpack fallback path)
            state.usage_accumulator.record(tenant_id, "traces", span_count as u64, raw.len() as u64);

            tracing::info!(
                signal = "traces",
                tenant_id = %tenant_id,
                spans_count = span_count,
                traces_count = traces.len(),
                source = "datadog",
                endpoint = "v0.2",
                encoding = "msgpack",
                "ingested spans"
            );
            Ok(Json(serde_json::json!({"rate_by_service": {}})))
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                signal = "traces",
                handler = "dd_traces",
                endpoint = "v0.2",
                bytes = raw.len(),
                "payload decode failed"
            );
            Ok(Json(serde_json::json!({})))
        }
    }
}
