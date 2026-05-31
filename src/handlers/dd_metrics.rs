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
use crate::models::ingest::{GaugeRow, SumRow};
use super::dd_common::{validate_api_key, decompress_body};

// ═══ V1 Series payload ═══

#[derive(Debug, Deserialize)]
struct V1SeriesPayload {
    series: Vec<V1Series>,
}

#[derive(Debug, Deserialize)]
struct V1Series {
    metric: String,
    /// [[timestamp, value], ...]
    points: Vec<Vec<f64>>,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    interval: Option<i64>,
}

// ═══ V2 Series payload ═══

#[derive(Debug, Deserialize)]
struct V2SeriesPayload {
    series: Vec<V2Series>,
}

#[derive(Debug, Deserialize)]
struct V2Series {
    metric: String,
    #[serde(default)]
    r#type: Option<serde_json::Value>, // can be int (3) or string ("gauge")
    points: Vec<V2Point>,
    #[serde(default)]
    resources: Vec<V2Resource>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    unit: String,
    #[serde(default)]
    interval: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct V2Point {
    timestamp: i64,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct V2Resource {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    name: String,
}

// ═══ Service check payload ═══

#[derive(Debug, Deserialize)]
struct ServiceCheck {
    check: String,
    #[serde(default)]
    host_name: String,
    #[serde(default)]
    status: i32,
    #[serde(default, deserialize_with = "deserialize_nullable_tags")]
    tags: Vec<String>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

fn deserialize_nullable_tags<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<Vec<String>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}


/// Parse DD tags list into (service_name, attributes).
fn extract_tags(tags: &[String]) -> (String, Vec<(String, String)>) {
    let mut service_name = String::new();
    let mut attrs = Vec::new();
    for tag in tags {
        if let Some((k, v)) = tag.split_once(':') {
            match k {
                "service" => service_name = v.to_string(),
                _ => attrs.push((k.to_string(), v.to_string())),
            }
        } else {
            attrs.push((tag.clone(), String::new()));
        }
    }
    (service_name, attrs)
}

/// Determine which table to insert into based on DD metric type.
/// Returns "metrics_gauge" or "metrics_sum".
fn table_for_type(dd_type: &str) -> &'static str {
    match dd_type.to_lowercase().as_str() {
        "count" | "1" => "metrics_sum",
        "rate" | "2" => "metrics_sum",
        _ => "metrics_gauge", // gauge (3), unspecified (0), default
    }
}

fn build_template(
    service_name: String,
    metric_name: String,
    unit: String,
    attrs: Vec<(String, String)>,
    host: &str,
    tenant_id: &str,
) -> GaugeRow {
    let mut resource_attributes = Vec::new();
    if !host.is_empty() {
        resource_attributes.push(("host.name".to_string(), host.to_string()));
    }
    GaugeRow {
        tenant_id: tenant_id.to_string(),
        resource_attributes,
        resource_schema_url: String::new(),
        scope_name: "datadog".to_string(),
        scope_version: String::new(),
        scope_attributes: Vec::new(),
        scope_dropped_attr_count: 0,
        scope_schema_url: String::new(),
        service_name,
        metric_name,
        metric_description: String::new(),
        metric_unit: unit,
        attributes: attrs,
        start_time_unix: 0,
        time_unix: 0,
        value: 0.0,
        flags: 0,
        exemplars_filtered_attributes: Vec::new(),
        exemplars_time_unix: Vec::new(),
        exemplars_value: Vec::new(),
        exemplars_span_id: Vec::new(),
        exemplars_trace_id: Vec::new(),
    }
}

/// POST /datadog/api/v1/series — Datadog V1 metrics intake (JSON).
pub async fn ingest_v1(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    validate_api_key(&headers)?;
    let raw = decompress_body(&headers, body)?;

    let payload: V1SeriesPayload = serde_json::from_slice(&raw).map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}"))
    })?;

    if payload.series.is_empty() {
        return Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))));
    }

    // Group by target table
    let mut gauge_rows: Vec<GaugeRow> = Vec::new();
    let mut sum_rows: Vec<SumRow> = Vec::new();

    for series in &payload.series {
        let (svc, attrs) = extract_tags(&series.tags);
        let is_sum = table_for_type(&series.r#type) == "metrics_sum";
        let template = build_template(
            svc,
            series.metric.clone(),
            String::new(),
            attrs,
            &series.host,
            tenant_id,
        );

        // Clone template once per series; only time_unix/value (scalars) change per point.
        let mut row = template.clone();
        let is_count = series.r#type.to_lowercase() == "count";
        for point in &series.points {
            if point.len() < 2 { continue; }
            row.time_unix = (point[0] as i64) * 1_000_000_000; // seconds → ns
            row.value = point[1];

            if is_sum {
                sum_rows.push(SumRow::from_gauge(&row, is_count));
            } else {
                gauge_rows.push(row.clone());
            }
        }
    }

    let total = gauge_rows.len() + sum_rows.len();
    let gauge_len = gauge_rows.len();
    let sum_len = sum_rows.len();

    // Write gauges and sums in parallel through the durable writer.
    let map_err = |e: WriteError| match e {
        WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
        WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
    };
    let gauge_fut = async {
        if !gauge_rows.is_empty() {
            state.writer.write(SpoolBatch::Gauge(gauge_rows)).await.map_err(map_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let sum_fut = async {
        if !sum_rows.is_empty() {
            state.writer.write(SpoolBatch::Sum(sum_rows)).await.map_err(map_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let (gauge_res, sum_res) = tokio::join!(gauge_fut, sum_fut);
    gauge_res?;
    sum_res?;

    // Record usage for per-tenant ingest metering
    state.usage_accumulator.record(tenant_id, "metrics", total as u64, raw.len() as u64);

    tracing::info!(
        signal = "metrics",
        tenant_id = %tenant_id,
        series_count = payload.series.len(),
        datapoints = total,
        gauge_count = gauge_len,
        sum_count = sum_len,
        source = "datadog",
        endpoint = "v1",
        "ingested metrics"
    );

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))))
}

/// POST /datadog/api/v2/series — Datadog V2 metrics intake.
/// The DD agent sends either JSON or protobuf (default). We parse JSON and
/// gracefully accept protobuf payloads we can't yet decode.
pub async fn ingest_v2(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    validate_api_key(&headers)?;
    let raw = decompress_body(&headers, body)?;

    // DD agent v7 sends protobuf by default for v2/series.
    // Try JSON first; if it fails, accept gracefully (protobuf support TODO).
    let payload: V2SeriesPayload = match serde_json::from_slice(&raw) {
        Ok(p) => p,
        Err(_) => {
            tracing::debug!(
                signal = "metrics",
                endpoint = "v2",
                bytes = raw.len(),
                content_type = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("none"),
                "received non-JSON payload, accepting"
            );
            return Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))));
        }
    };

    if payload.series.is_empty() {
        return Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))));
    }

    let mut gauge_rows: Vec<GaugeRow> = Vec::new();
    let mut sum_rows: Vec<SumRow> = Vec::new();

    for series in &payload.series {
        let (svc, attrs) = extract_tags(&series.tags);
        let host = series.resources.iter()
            .find(|r| r.r#type == "host")
            .map(|r| r.name.as_str())
            .unwrap_or("");

        // Determine type from the type field (can be int or string)
        let dd_type = match &series.r#type {
            Some(serde_json::Value::Number(n)) => match n.as_i64().unwrap_or(0) {
                1 => "count",
                2 => "rate",
                3 => "gauge",
                _ => "gauge",
            },
            Some(serde_json::Value::String(s)) => s.as_str(),
            _ => "gauge",
        };
        let is_sum = table_for_type(dd_type) == "metrics_sum";

        let template = build_template(
            svc,
            series.metric.clone(),
            series.unit.clone(),
            attrs,
            host,
            tenant_id,
        );

        // Clone template once per series; only time_unix/value (scalars) change per point.
        let mut row = template.clone();
        let is_count = dd_type == "count";
        for point in &series.points {
            row.time_unix = point.timestamp * 1_000_000_000; // seconds → ns
            row.value = point.value;

            if is_sum {
                sum_rows.push(SumRow::from_gauge(&row, is_count));
            } else {
                gauge_rows.push(row.clone());
            }
        }
    }

    let total = gauge_rows.len() + sum_rows.len();

    let map_err = |e: WriteError| match e {
        WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
        WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
    };
    let gauge_fut = async {
        if !gauge_rows.is_empty() {
            state.writer.write(SpoolBatch::Gauge(gauge_rows)).await.map_err(map_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let sum_fut = async {
        if !sum_rows.is_empty() {
            state.writer.write(SpoolBatch::Sum(sum_rows)).await.map_err(map_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let (gauge_res, sum_res) = tokio::join!(gauge_fut, sum_fut);
    gauge_res?;
    sum_res?;

    // Record usage for per-tenant ingest metering
    state.usage_accumulator.record(tenant_id, "metrics", total as u64, raw.len() as u64);

    tracing::info!(
        signal = "metrics",
        tenant_id = %tenant_id,
        series_count = payload.series.len(),
        datapoints = total,
        source = "datadog",
        endpoint = "v2",
        "ingested metrics"
    );

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))))
}

/// POST /datadog/api/v1/check_run — Datadog service checks.
/// Maps check status to a gauge metric: dd.check.{check_name} = status.
pub async fn check_run(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    validate_api_key(&headers)?;
    let raw = decompress_body(&headers, body)?;

    // Agent may send null or non-array — handle gracefully
    let checks: Vec<ServiceCheck> = match serde_json::from_slice(&raw) {
        Ok(c) => c,
        Err(_) => {
            // Try to parse as single check, or return empty
            match serde_json::from_slice::<ServiceCheck>(&raw) {
                Ok(c) => vec![c],
                Err(_) => {
                    tracing::debug!(signal = "metrics", endpoint = "check_run", "ignoring unparseable payload");
                    return Ok(Json(serde_json::json!({"status": "ok"})));
                }
            }
        }
    };

    if checks.is_empty() {
        return Ok(Json(serde_json::json!({"status": "ok"})));
    }

    let now_s = chrono::Utc::now().timestamp();

    let rows: Vec<GaugeRow> = checks.iter().map(|check| {
        let (svc, attrs) = extract_tags(&check.tags);
        let ts = check.timestamp.unwrap_or(now_s) * 1_000_000_000;
        let mut row = build_template(
            svc,
            format!("dd.check.{}", check.check),
            String::new(),
            attrs,
            &check.host_name,
            tenant_id,
        );
        row.time_unix = ts;
        row.value = check.status as f64;
        row
    }).collect();

    state.writer.write(SpoolBatch::Gauge(rows)).await.map_err(|e| match e {
        WriteError::Backpressure => (StatusCode::TOO_MANY_REQUESTS, "ingest backpressure: clickhouse unavailable, spool full".to_string()),
        WriteError::Fatal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
    })?;

    // Record usage for per-tenant ingest metering
    state.usage_accumulator.record(tenant_id, "metrics", checks.len() as u64, raw.len() as u64);

    tracing::info!(
        signal = "metrics",
        tenant_id = %tenant_id,
        count = checks.len(),
        source = "datadog",
        endpoint = "check_run",
        "ingested service checks"
    );

    Ok(Json(serde_json::json!({"status": "ok"})))
}
