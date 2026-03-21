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

// ═══ ClickHouse gauge row ═══

#[derive(Debug, Clone, Serialize, Row)]
struct GaugeRow {
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "MetricName")]
    metric_name: String,
    #[serde(rename = "MetricDescription")]
    metric_description: String,
    #[serde(rename = "MetricUnit")]
    metric_unit: String,
    #[serde(rename = "Attributes")]
    attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    time_unix: i64,
    #[serde(rename = "Value")]
    value: f64,
    #[serde(rename = "Flags")]
    flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    exemplars_trace_id: Vec<String>,
}

/// ClickHouse row for otel_metrics_sum (has AggregationTemporality + IsMonotonic).
#[derive(Debug, Clone, Serialize, Row)]
struct SumRow {
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "MetricName")]
    metric_name: String,
    #[serde(rename = "MetricDescription")]
    metric_description: String,
    #[serde(rename = "MetricUnit")]
    metric_unit: String,
    #[serde(rename = "Attributes")]
    attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    time_unix: i64,
    #[serde(rename = "Value")]
    value: f64,
    #[serde(rename = "Flags")]
    flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    exemplars_trace_id: Vec<String>,
    #[serde(rename = "AggregationTemporality")]
    aggregation_temporality: i32,
    #[serde(rename = "IsMonotonic")]
    is_monotonic: bool,
}

impl SumRow {
    fn from_gauge(g: &GaugeRow, monotonic: bool) -> Self {
        SumRow {
            resource_attributes: g.resource_attributes.clone(),
            resource_schema_url: g.resource_schema_url.clone(),
            scope_name: g.scope_name.clone(),
            scope_version: g.scope_version.clone(),
            scope_attributes: g.scope_attributes.clone(),
            scope_dropped_attr_count: g.scope_dropped_attr_count,
            scope_schema_url: g.scope_schema_url.clone(),
            service_name: g.service_name.clone(),
            metric_name: g.metric_name.clone(),
            metric_description: g.metric_description.clone(),
            metric_unit: g.metric_unit.clone(),
            attributes: g.attributes.clone(),
            start_time_unix: g.start_time_unix,
            time_unix: g.time_unix,
            value: g.value,
            flags: g.flags,
            exemplars_filtered_attributes: g.exemplars_filtered_attributes.clone(),
            exemplars_time_unix: g.exemplars_time_unix.clone(),
            exemplars_value: g.exemplars_value.clone(),
            exemplars_span_id: g.exemplars_span_id.clone(),
            exemplars_trace_id: g.exemplars_trace_id.clone(),
            aggregation_temporality: 2, // CUMULATIVE
            is_monotonic: monotonic,
        }
    }
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
/// Returns "otel_metrics_gauge" or "otel_metrics_sum".
fn table_for_type(dd_type: &str) -> &'static str {
    match dd_type.to_lowercase().as_str() {
        "count" | "1" => "otel_metrics_sum",
        "rate" | "2" => "otel_metrics_sum",
        _ => "otel_metrics_gauge", // gauge (3), unspecified (0), default
    }
}

fn build_template(
    service_name: String,
    metric_name: String,
    unit: String,
    attrs: Vec<(String, String)>,
    host: &str,
) -> GaugeRow {
    let mut resource_attributes = Vec::new();
    if !host.is_empty() {
        resource_attributes.push(("host.name".to_string(), host.to_string()));
    }
    GaugeRow {
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
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
        let is_sum = table_for_type(&series.r#type) == "otel_metrics_sum";
        let template = build_template(
            svc,
            series.metric.clone(),
            String::new(),
            attrs,
            &series.host,
        );

        for point in &series.points {
            if point.len() < 2 { continue; }
            let mut row = template.clone();
            row.time_unix = (point[0] as i64) * 1_000_000_000; // seconds → ns
            row.value = point[1];

            if is_sum {
                sum_rows.push(SumRow::from_gauge(&row, series.r#type.to_lowercase() == "count"));
            } else {
                gauge_rows.push(row);
            }
        }
    }

    // Insert gauges
    if !gauge_rows.is_empty() {
        let mut insert = state.ch.insert("otel_metrics_gauge")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;
        for row in &gauge_rows {
            insert.write(row).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
            })?;
        }
        insert.end().await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
        })?;
    }

    // Insert sums
    if !sum_rows.is_empty() {
        let mut insert = state.ch.insert("otel_metrics_sum")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;
        for row in &sum_rows {
            insert.write(row).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
            })?;
        }
        insert.end().await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
        })?;
    }

    let total = gauge_rows.len() + sum_rows.len();
    tracing::info!("datadog metrics v1: ingested {total} samples ({} gauge, {} sum)", gauge_rows.len(), sum_rows.len());

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))))
}

/// POST /datadog/api/v2/series — Datadog V2 metrics intake.
/// The DD agent sends either JSON or protobuf (default). We parse JSON and
/// gracefully accept protobuf payloads we can't yet decode.
pub async fn ingest_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_api_key(&headers)?;
    let raw = decompress_body(&headers, body)?;

    // DD agent v7 sends protobuf by default for v2/series.
    // Try JSON first; if it fails, accept gracefully (protobuf support TODO).
    let payload: V2SeriesPayload = match serde_json::from_slice(&raw) {
        Ok(p) => p,
        Err(_) => {
            tracing::debug!(
                "datadog metrics v2: received non-JSON payload ({} bytes, content-type: {:?}), accepting",
                raw.len(),
                headers.get("content-type").and_then(|v| v.to_str().ok())
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
        let is_sum = table_for_type(dd_type) == "otel_metrics_sum";

        let template = build_template(
            svc,
            series.metric.clone(),
            series.unit.clone(),
            attrs,
            host,
        );

        for point in &series.points {
            let mut row = template.clone();
            row.time_unix = point.timestamp * 1_000_000_000; // seconds → ns
            row.value = point.value;

            if is_sum {
                sum_rows.push(SumRow::from_gauge(&row, dd_type == "count"));
            } else {
                gauge_rows.push(row);
            }
        }
    }

    if !gauge_rows.is_empty() {
        let mut insert = state.ch.insert("otel_metrics_gauge")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;
        for row in &gauge_rows {
            insert.write(row).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
            })?;
        }
        insert.end().await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
        })?;
    }

    if !sum_rows.is_empty() {
        let mut insert = state.ch.insert("otel_metrics_sum")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;
        for row in &sum_rows {
            insert.write(row).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
            })?;
        }
        insert.end().await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
        })?;
    }

    let total = gauge_rows.len() + sum_rows.len();
    tracing::info!("datadog metrics v2: ingested {total} samples");

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"status": "ok"}))))
}

/// POST /datadog/api/v1/check_run — Datadog service checks.
/// Maps check status to a gauge metric: dd.check.{check_name} = status.
pub async fn check_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
                    tracing::debug!("datadog check_run: ignoring unparseable payload");
                    return Ok(Json(serde_json::json!({"status": "ok"})));
                }
            }
        }
    };

    if checks.is_empty() {
        return Ok(Json(serde_json::json!({"status": "ok"})));
    }

    let now_s = chrono::Utc::now().timestamp();

    let mut insert = state.ch.insert("otel_metrics_gauge")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;

    for check in &checks {
        let (svc, attrs) = extract_tags(&check.tags);
        let ts = check.timestamp.unwrap_or(now_s) * 1_000_000_000;

        let mut row = build_template(
            svc,
            format!("dd.check.{}", check.check),
            String::new(),
            attrs,
            &check.host_name,
        );
        row.time_unix = ts;
        row.value = check.status as f64;

        insert.write(&row).await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
        })?;
    }

    insert.end().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
    })?;

    tracing::info!("datadog check_run: ingested {} checks", checks.len());

    Ok(Json(serde_json::json!({"status": "ok"})))
}
