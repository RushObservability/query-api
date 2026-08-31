use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::Engine;
use serde::Deserialize;

use crate::AppState;
use crate::ch_writer::{SpoolBatch, WriteError};
use crate::models::ingest::LogInsertRow;

/// Outer Kinesis Data Firehose HTTP-endpoint envelope.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FirehoseRequest {
    #[serde(default, rename = "requestId")]
    request_id: String,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    records: Vec<FirehoseRecord>,
}

#[derive(Debug, Deserialize)]
struct FirehoseRecord {
    /// base64-encoded, gzip-compressed CloudWatch Logs payload.
    #[serde(default)]
    data: String,
}

/// CloudWatch Logs payload (after base64-decode + gunzip of a Firehose record).
#[derive(Debug, Deserialize)]
struct CwlPayload {
    #[serde(default)]
    owner: String,
    #[serde(default, rename = "logGroup")]
    log_group: String,
    #[serde(default, rename = "logStream")]
    log_stream: String,
    #[serde(default, rename = "messageType")]
    message_type: String,
    #[serde(default, rename = "logEvents")]
    log_events: Vec<CwlEvent>,
}

#[derive(Debug, Deserialize)]
struct CwlEvent {
    /// Event timestamp in Unix MILLISECONDS.
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    message: String,
}

/// Gunzip a buffer with a hard size cap.
async fn gunzip_capped(
    limits: &crate::ingest_limits::IngestLimits,
    input: Vec<u8>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    limits.check_compressed("cloudwatch", input.len())?;
    let permit = limits.acquire_decode("cloudwatch").await?;
    let limits = limits.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        gunzip_capped_sync(&limits, &input)
    })
    .await
    .map_err(|e| crate::api_error::internal_legacy("cloudwatch.decode_task", e))?
}

fn gunzip_capped_sync(
    limits: &crate::ingest_limits::IngestLimits,
    input: &[u8],
) -> Result<Vec<u8>, (StatusCode, String)> {
    use std::io::Read;
    let decoder = flate2::read::GzDecoder::new(input);
    let mut out = Vec::new();
    decoder
        .take(limits.max_decompressed_bytes as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| limits.malformed("cloudwatch", "invalid compressed CloudWatch payload"))?;
    limits.check_decompressed("cloudwatch", out.len())?;
    Ok(out)
}

/// Classify a log line's severity by finding a level keyword as a standalone
/// token (boundaries are start-of-string or any non-alphanumeric on both sides).
/// This catches the level at the start of the line (`ERROR ...`), bracketed
/// (`[WARN] ...`), or tab-separated (`ERROR\tmsg`) — the common CloudWatch /
/// Lambda shapes — not just the space-surrounded form. Only the first 200 chars
/// are scanned (levels appear early; avoids matching prose later in the body).
/// Priority high→low so `FATAL` wins over an `INFO` later in the same line.
fn classify_severity(message: &str) -> (&'static str, u8) {
    // Take up to ~200 bytes without splitting a UTF-8 char.
    let mut cut = message.len().min(200);
    while cut > 0 && !message.is_char_boundary(cut) {
        cut -= 1;
    }
    let upper = message[..cut].to_ascii_uppercase();
    let has = |needle: &str| has_token(&upper, needle);
    if has("FATAL") || has("CRITICAL") || has("EMERG") || has("PANIC") {
        ("FATAL", 21)
    } else if has("ERROR") || has("ERR") || has("SEVERE") {
        ("ERROR", 17)
    } else if has("WARNING") || has("WARN") {
        ("WARN", 13)
    } else if has("DEBUG") {
        ("DEBUG", 5)
    } else if has("TRACE") {
        ("TRACE", 1)
    } else if has("INFO") || has("NOTICE") {
        ("INFO", 9)
    } else {
        ("INFO", 9)
    }
}

/// True when `needle` (already uppercase) appears in `haystack` (already
/// uppercase) bounded by non-alphanumeric chars (or string ends) on both sides.
fn has_token(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let n = needle.len();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let i = start + pos;
        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        let after = i + n;
        let after_ok = after >= bytes.len() || !bytes[after].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        start = i + 1;
    }
    false
}

/// True when CloudWatch ingest is enabled: env CLOUDWATCH_ENABLED == "true"/"1"
/// OR the `cloudwatch_enabled` setting == "true". Mirrors the kubernetes_enabled
/// check shape used in get_features.
async fn cloudwatch_enabled(state: &AppState) -> bool {
    std::env::var("CLOUDWATCH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
        || state
            .config_db
            .get_setting("cloudwatch_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false)
}

/// Build the Firehose JSON response body. Firehose REQUIRES a JSON body with the
/// echoed requestId and a current timestamp; `error` adds an errorMessage and is
/// treated by Firehose as a delivery failure (it retries).
fn firehose_response(request_id: &str, error: Option<&str>) -> Json<serde_json::Value> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    match error {
        Some(msg) => Json(serde_json::json!({
            "requestId": request_id,
            "timestamp": now_ms,
            "errorMessage": msg,
        })),
        None => Json(serde_json::json!({
            "requestId": request_id,
            "timestamp": now_ms,
        })),
    }
}

/// POST /cloudwatch/firehose/t/{tenant} — AWS CloudWatch Logs via Kinesis Data
/// Firehose HTTP-endpoint delivery. The tenant is taken from the URL path. An
/// The tenant always comes from the URL path. If that tenant is NOT auth-required,
/// no key is needed. If it IS auth-required, the caller must present a tenant-scoped
/// API key as the Firehose access key (`X-Amz-Firehose-Access-Key`, or an
/// `Authorization: Bearer` header) and it must resolve to this same tenant. Mirrors
/// dd_logs::ingest_logs_with_tenant.
pub async fn ingest_firehose_with_tenant(
    State(state): State<AppState>,
    axum::extract::Path(tenant_override): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // No request_id parsed yet; echo whatever the Firehose header carries.
    let request_id = headers
        .get("x-amz-firehose-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !state.config_db.is_tenant_enabled(&tenant_override).await {
        return (
            StatusCode::BAD_REQUEST,
            firehose_response(
                &request_id,
                Some(&format!(
                    "tenant '{}' not found or disabled",
                    tenant_override
                )),
            ),
        );
    }

    // Authentication, tenant binding, signal scope, source restrictions, and
    // rate limits are enforced once by the outer tenant middleware.
    ingest_firehose_inner(state, tenant_override, headers, body).await
}

async fn ingest_firehose_inner(
    state: AppState,
    tenant_id: String,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<serde_json::Value>) {
    let started = std::time::Instant::now();

    // Echo the Firehose request id back in every response.
    let request_id = headers
        .get("x-amz-firehose-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Gate: refuse ingest unless CloudWatch is enabled (env or setting).
    if !cloudwatch_enabled(&state).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            firehose_response(&request_id, Some("CloudWatch ingest is disabled")),
        );
    }
    if let Err((status, message)) = state.ingest_limits.check_body("cloudwatch", &body) {
        return (status, firehose_response(&request_id, Some(&message)));
    }

    // The outer Firehose body may be gzip-compressed (Content-Encoding: gzip).
    let gzipped = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|e| e.contains("gzip"))
        .unwrap_or(false);
    let raw_len = body.len() as u64;
    let outer: Vec<u8> = if gzipped {
        match gunzip_capped(&state.ingest_limits, body.to_vec()).await {
            Ok(b) => b,
            Err((status, message)) => {
                return (status, firehose_response(&request_id, Some(&message)));
            }
        }
    } else {
        body.to_vec()
    };

    // Parse the outer Firehose envelope.
    let req: FirehoseRequest = match serde_json::from_slice(&outer) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                firehose_response(&request_id, Some("invalid Firehose JSON payload")),
            );
        }
    };
    // Prefer the body's requestId if the header was missing.
    let request_id = if request_id.is_empty() && !req.request_id.is_empty() {
        req.request_id.clone()
    } else {
        request_id
    };

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    // Arc-share the constant fields across all rows in this request.
    let tenant_arc: std::sync::Arc<str> = tenant_id.as_str().into();
    let empty_str: std::sync::Arc<str> = "".into();
    let scope_cw: std::sync::Arc<str> = "cloudwatch".into();
    let empty_attrs: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(Vec::new());

    let mut rows: Vec<LogInsertRow> = Vec::new();
    let mut decoded_entities = req.records.len();
    let mut decompressed_total = outer.len();
    if let Err((status, message)) = state
        .ingest_limits
        .check_entities("cloudwatch", decoded_entities)
    {
        return (status, firehose_response(&request_id, Some(&message)));
    }

    for record in &req.records {
        // Each record's data is base64 → gzip → CloudWatch Logs JSON.
        let decoded = match base64::engine::general_purpose::STANDARD.decode(record.data.as_bytes())
        {
            Ok(d) => d,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    firehose_response(&request_id, Some("invalid base64 Firehose record")),
                );
            }
        };
        let inflated = match gunzip_capped(&state.ingest_limits, decoded).await {
            Ok(b) => b,
            Err((status, message)) => {
                return (status, firehose_response(&request_id, Some(&message)));
            }
        };
        decompressed_total = decompressed_total.saturating_add(inflated.len());
        if let Err((status, message)) = state
            .ingest_limits
            .check_decompressed("cloudwatch", decompressed_total)
        {
            return (status, firehose_response(&request_id, Some(&message)));
        }
        let payload: CwlPayload = match serde_json::from_slice(&inflated) {
            Ok(p) => p,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    firehose_response(&request_id, Some("invalid CloudWatch Logs JSON payload")),
                );
            }
        };
        decoded_entities = decoded_entities.saturating_add(payload.log_events.len());
        if let Err((status, message)) = state
            .ingest_limits
            .check_entities("cloudwatch", decoded_entities)
        {
            return (status, firehose_response(&request_id, Some(&message)));
        }

        // CONTROL_MESSAGE records are CWL/Firehose health checks — skip them.
        if payload.message_type == "CONTROL_MESSAGE" {
            continue;
        }

        // Resource attributes are constant across this record's events.
        let resource_attrs: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(vec![
            (
                "aws.cloudwatch.log_group".to_string(),
                payload.log_group.clone(),
            ),
            (
                "aws.cloudwatch.log_stream".to_string(),
                payload.log_stream.clone(),
            ),
            ("cloud.account.id".to_string(), payload.owner.clone()),
            ("cloud.provider".to_string(), "aws".to_string()),
        ]);

        for event in &payload.log_events {
            // Severity: classify the log level as a standalone token. Unlike the
            // dd_logs space-surrounded heuristic, CloudWatch lines commonly START
            // with the level or bracket it (e.g. "ERROR ...", "[WARN] ...",
            // "ERROR\tmessage" from Lambda), so we match on word boundaries.
            let (level, num) = classify_severity(&event.message);
            let (severity_text, severity_number): (String, u8) = (level.into(), num);

            // CloudWatch event timestamps are Unix MILLISECONDS → nanoseconds.
            let ts_ns = if event.timestamp > 0 {
                event.timestamp * 1_000_000
            } else {
                now_ns
            };

            rows.push(LogInsertRow {
                tenant_id: tenant_arc.clone(),
                timestamp: ts_ns,
                trace_id: String::new(),
                span_id: String::new(),
                trace_flags: 0,
                severity_text,
                severity_number,
                service_name: payload.log_group.clone(),
                body: event.message.clone(),
                resource_schema_url: empty_str.clone(),
                resource_attributes: resource_attrs.clone(),
                scope_schema_url: empty_str.clone(),
                scope_name: scope_cw.clone(),
                scope_version: empty_str.clone(),
                scope_attributes: empty_attrs.clone(),
                log_attributes: Vec::new(),
                event_name: String::new(),
            });
        }
    }

    if rows.is_empty() {
        // Nothing to write (e.g. all CONTROL_MESSAGE). Still a successful delivery.
        return (StatusCode::OK, firehose_response(&request_id, None));
    }

    let count = rows.len() as u64;
    if let Err(e) =
        crate::handlers::ingest_gate::write_gated(&state, &tenant_id, SpoolBatch::Logs(rows)).await
    {
        let (status, msg) = match e {
            WriteError::Backpressure => (
                StatusCode::TOO_MANY_REQUESTS,
                "ingest backpressure: clickhouse unavailable, spool full".to_string(),
            ),
            WriteError::Fatal(s) => crate::api_error::internal_legacy("cloudwatch.write", s),
        };
        return (status, firehose_response(&request_id, Some(&msg)));
    }

    // Record usage for per-tenant ingest metering.
    state
        .usage_accumulator
        .record(&tenant_id, "logs", count, raw_len);

    tracing::info!(
        signal = "logs",
        tenant_id = %tenant_id,
        count = count,
        source = "cloudwatch",
        duration_ms = started.elapsed().as_millis() as u64,
        "ingested logs"
    );

    (StatusCode::OK, firehose_response(&request_id, None))
}
