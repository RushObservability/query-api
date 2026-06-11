use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
    Extension,
};
use serde::Deserialize;

use crate::AppState;
use crate::TenantContext;
use crate::ch_writer::{SpoolBatch, WriteError};
use crate::models::ingest::LogInsertRow;
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


/// POST /datadog/v1/input — Datadog log intake endpoint.
///
/// Accepts a JSON array of log entries, optionally gzip-compressed.
/// Maps DD log fields to the logs ClickHouse schema.
pub async fn ingest_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    ingest_logs_inner(state, tenant.tenant_id.clone(), headers, body).await
}

/// Tenant-override variant: the tenant is taken from the URL path instead of the
/// middleware. Used when the DD agent's log forwarder can't send the DD-API-KEY header.
/// Route: POST /api/v2/logs/t/{tenant}
pub async fn ingest_logs_with_tenant(
    State(state): State<AppState>,
    axum::extract::Path(tenant_override): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !state.config_db.is_tenant_enabled(&tenant_override).await {
        return Err((StatusCode::BAD_REQUEST, format!("tenant '{}' not found or disabled", tenant_override)));
    }
    ingest_logs_inner(state, tenant_override, headers, body).await
}

async fn ingest_logs_inner(
    state: AppState,
    tenant_id: String,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_api_key(&headers)?;

    let raw = decompress_body(&headers, body).await?;

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

    // Arc refactor: tenant_id + the constant scope fields are shared across all
    // entries in this request — allocate each once and Arc-clone per row.
    let tenant_arc: std::sync::Arc<str> = tenant_id.as_str().into();
    let empty_str: std::sync::Arc<str> = "".into();
    let scope_dd: std::sync::Arc<str> = "datadog".into();
    let empty_attrs: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(Vec::new());

    let mut rows: Vec<LogInsertRow> = Vec::with_capacity(entries.len());

    for entry in &entries {
        // Determine severity: prefer parsing from the log body (more accurate)
        // over the DD agent's status field (which is often just stderr=error).
        let (severity_text, severity_number) = {
            let body = entry.message.as_str();
            if body.contains(" ERROR ") || body.contains(" error ") || body.contains("\\bERROR\\b") {
                ("ERROR".into(), 17u8)
            } else if body.contains(" WARN ") || body.contains(" WARNING ") || body.contains(" warn ") {
                ("WARN".into(), 13u8)
            } else if body.contains(" DEBUG ") || body.contains(" debug ") {
                ("DEBUG".into(), 5u8)
            } else if body.contains(" FATAL ") || body.contains(" fatal ") || body.contains(" CRITICAL ") {
                ("FATAL".into(), 21u8)
            } else if body.contains(" INFO ") || body.contains(" info ") {
                ("INFO".into(), 9u8)
            } else if !entry.status.is_empty() {
                // Fall back to DD status if body doesn't have a recognizable level
                dd_status_to_severity(&entry.status)
            } else {
                ("INFO".into(), 9u8)
            }
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

        rows.push(LogInsertRow {
            tenant_id: tenant_arc.clone(),
            timestamp: ts_ns,
            trace_id: String::new(),
            span_id: String::new(),
            trace_flags: 0,
            severity_text,
            severity_number,
            service_name: entry.service.clone(),
            body: entry.message.clone(),
            resource_schema_url: empty_str.clone(),
            resource_attributes: std::sync::Arc::new(resource_attrs),
            scope_schema_url: empty_str.clone(),
            scope_name: scope_dd.clone(),
            scope_version: empty_str.clone(),
            scope_attributes: empty_attrs.clone(),
            log_attributes: log_attrs,
            event_name: String::new(),
        });
    }

    let count = rows.len() as u64;
    state.writer.write(SpoolBatch::Logs(rows)).await.map_err(|e| match e {
        WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
        WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
    })?;

    // Record usage for per-tenant ingest metering
    state.usage_accumulator.record(&tenant_id, "logs", count, raw.len() as u64);

    tracing::debug!(
        signal = "logs",
        tenant_id = %tenant_id,
        count = count,
        source = "datadog",
        "ingested logs"
    );

    Ok(Json(serde_json::json!({})))
}
