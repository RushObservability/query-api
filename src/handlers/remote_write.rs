use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    Extension,
};
use clickhouse::Row;
use prost::Message;
use serde::Serialize;

use crate::AppState;
use crate::TenantContext;

// ═══ Prometheus remote write protobuf types ═══
// Defined manually to avoid requiring protoc at build time.

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Summary = 3,
    Histogram = 4,
    GaugeHistogram = 5,
    Info = 6,
    StateSet = 7,
}

// ═══ ClickHouse row for metrics_gauge ═══

#[derive(Debug, Clone, Serialize, Row)]
struct GaugeRow {
    tenant_id: String,
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

// ═══ Handler ═══

/// POST /prom/api/v1/write — Prometheus remote write receiver.
///
/// Accepts snappy-compressed protobuf `prometheus.WriteRequest` and inserts
/// samples into `metrics_gauge` in ClickHouse.
pub async fn prom_remote_write(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    // Verify content type (optional — some clients don't set it)
    if let Some(ct) = headers.get("content-type") {
        let ct_str = ct.to_str().unwrap_or("");
        if !ct_str.is_empty()
            && !ct_str.contains("x-protobuf")
            && !ct_str.contains("protobuf")
            && !ct_str.contains("snappy")
            && !ct_str.contains("octet-stream")
        {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format!("unsupported content-type: {ct_str}"),
            ));
        }
    }

    // Check content-encoding for snappy (some clients use this header)
    let is_snappy = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("snappy"));

    // Decompress snappy — Prometheus remote write always uses snappy framing
    // Try snappy decompression; if it fails and content-encoding isn't snappy,
    // treat as raw protobuf (for flexibility with custom senders).
    let decompressed = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(data) => data,
        Err(_) if !is_snappy => body.to_vec(),
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("snappy decompression failed: {e}"),
            ));
        }
    };

    // Decode protobuf
    let write_req = WriteRequest::decode(decompressed.as_slice()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("protobuf decode failed: {e}"),
        )
    })?;

    if write_req.timeseries.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    // Build metadata lookup (metric_name → description/unit)
    let mut meta_map = std::collections::HashMap::new();
    for m in &write_req.metadata {
        meta_map.insert(
            m.metric_family_name.clone(),
            (m.help.clone(), m.unit.clone()),
        );
    }

    // Count samples for logging
    let sample_count: usize = write_req
        .timeseries
        .iter()
        .map(|ts| ts.samples.len())
        .sum();
    tracing::debug!(
        signal = "metrics",
        source = "prometheus",
        timeseries = write_req.timeseries.len(),
        samples = sample_count,
        "remote write payload decoded"
    );

    // Insert into metrics_gauge
    let mut insert = state
        .ch
        .insert("metrics_gauge")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "insert failed".into()))?;

    for ts in &write_req.timeseries {
        // Extract __name__ (metric name) and job (service name) from labels
        let mut metric_name = String::new();
        let mut service_name = String::new();
        let mut attrs = Vec::new();

        for label in &ts.labels {
            match label.name.as_str() {
                "__name__" => metric_name = label.value.clone(),
                "job" => service_name = label.value.clone(),
                _ => {
                    attrs.push((label.name.clone(), label.value.clone()));
                }
            }
        }

        if metric_name.is_empty() {
            continue; // Skip timeseries without a metric name
        }

        let (description, unit) = meta_map
            .get(&metric_name)
            .cloned()
            .unwrap_or_default();

        // P1: Build template row once per timeseries, only update time+value per sample
        let template = GaugeRow {
            tenant_id: tenant_id.clone(),
            resource_attributes: Vec::new(),
            resource_schema_url: String::new(),
            scope_name: "prometheus".to_string(),
            scope_version: String::new(),
            scope_attributes: Vec::new(),
            scope_dropped_attr_count: 0,
            scope_schema_url: String::new(),
            service_name,
            metric_name,
            metric_description: description,
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
        };

        for sample in &ts.samples {
            let mut row = template.clone();
            row.time_unix = sample.timestamp * 1_000_000;
            row.value = sample.value;

            insert.write(&row).await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "insert failed".into(),
                )
            })?;
        }
    }

    insert.end().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "insert failed".into(),
        )
    })?;

    // Record usage for per-tenant ingest metering (use decompressed size for bytes)
    state.usage_accumulator.record(tenant_id, "metrics", sample_count as u64, decompressed.len() as u64);

    tracing::info!(
        signal = "metrics",
        tenant_id = %tenant_id,
        series_count = write_req.timeseries.len(),
        samples = sample_count,
        source = "prometheus",
        "ingested remote write"
    );

    Ok(StatusCode::NO_CONTENT)
}
