use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::AppState;
use super::dd_common::{validate_api_key, decompress_body, parse_dd_tags, dd_status_to_severity};

/// A single Datadog log entry from the JSON payload.
#[derive(Debug, Deserialize)]
struct DdLogEntry {
    #[serde(default)]
    message: String,
    #[serde(default)]
    ddsource: String,
    #[serde(default)]
    ddtags: String,
    #[serde(default)]
    hostname: String,
    #[serde(default)]
    service: String,
    #[serde(default)]
    status: String,
    /// Unix timestamp in milliseconds (optional — defaults to now)
    #[serde(default)]
    timestamp: Option<i64>,
}

/// ClickHouse row matching the otel_logs schema.
#[derive(Debug, Clone, Serialize, Row)]
struct LogInsertRow {
    #[serde(rename = "Timestamp")]
    timestamp: i64, // DateTime64(9) — nanoseconds
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "TraceFlags")]
    trace_flags: u8,
    #[serde(rename = "SeverityText")]
    severity_text: String,
    #[serde(rename = "SeverityNumber")]
    severity_number: u8,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "Body")]
    body: String,
    #[serde(rename = "ResourceSchemaUrl")]
    resource_schema_url: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeSchemaUrl")]
    scope_schema_url: String,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    scope_attributes: Vec<(String, String)>,
    #[serde(rename = "LogAttributes")]
    log_attributes: Vec<(String, String)>,
    #[serde(rename = "EventName")]
    event_name: String,
}

/// POST /datadog/v1/input — Datadog log intake endpoint.
///
/// Accepts a JSON array of log entries, optionally gzip-compressed.
/// Maps DD log fields to the otel_logs ClickHouse schema.
pub async fn ingest_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_api_key(&headers)?;

    let raw = decompress_body(&headers, body)?;

    // The DD agent sends either a JSON array or a single object
    let entries: Vec<DdLogEntry> = if raw.first() == Some(&b'[') {
        serde_json::from_slice(&raw).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("invalid JSON array: {e}"))
        })?
    } else {
        let single: DdLogEntry = serde_json::from_slice(&raw).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}"))
        })?;
        vec![single]
    };

    if entries.is_empty() {
        return Ok(Json(serde_json::json!({})));
    }

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let mut insert = state
        .ch
        .insert("otel_logs")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;

    let mut count = 0u64;

    for entry in &entries {
        let (severity_text, severity_number) = if entry.status.is_empty() {
            ("INFO".into(), 9u8)
        } else {
            dd_status_to_severity(&entry.status)
        };

        // Timestamp: DD sends Unix ms; if absent use now
        let ts_ns = match entry.timestamp {
            Some(ms) if ms > 1_000_000_000_000_000 => ms, // already nanoseconds
            Some(ms) if ms > 1_000_000_000_000 => ms * 1_000_000, // microseconds
            Some(ms) if ms > 1_000_000_000 => ms * 1_000_000, // milliseconds
            Some(s) => s * 1_000_000_000, // seconds
            None => now_ns,
        };

        // Build resource attributes from DD metadata
        let mut resource_attrs = Vec::new();
        if !entry.hostname.is_empty() {
            resource_attrs.push(("host.name".to_string(), entry.hostname.clone()));
        }
        if !entry.ddsource.is_empty() {
            resource_attrs.push(("dd.source".to_string(), entry.ddsource.clone()));
        }

        // Parse ddtags into resource and log attributes
        let mut log_attrs = Vec::new();
        for (k, v) in parse_dd_tags(&entry.ddtags) {
            match k.as_str() {
                "env" => resource_attrs.push(("deployment.environment".to_string(), v)),
                "version" => resource_attrs.push(("service.version".to_string(), v)),
                _ => log_attrs.push((k, v)),
            }
        }

        let row = LogInsertRow {
            timestamp: ts_ns,
            trace_id: String::new(),
            span_id: String::new(),
            trace_flags: 0,
            severity_text,
            severity_number,
            service_name: entry.service.clone(),
            body: entry.message.clone(),
            resource_schema_url: String::new(),
            resource_attributes: resource_attrs,
            scope_schema_url: String::new(),
            scope_name: "datadog".to_string(),
            scope_version: String::new(),
            scope_attributes: Vec::new(),
            log_attributes: log_attrs,
            event_name: String::new(),
        };

        insert.write(&row).await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
        })?;
        count += 1;
    }

    insert.end().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
    })?;

    tracing::info!("datadog logs: ingested {count} entries");

    Ok(Json(serde_json::json!({})))
}
