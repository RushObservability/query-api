use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// A single wide event span as stored in ClickHouse.
/// timestamp/event_timestamps are i64 nanoseconds since epoch (DateTime64(9)).
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct WideEvent {
    pub timestamp: i64,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub service_version: String,
    pub environment: String,
    pub host_name: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status_code: u16,
    pub duration_ns: u64,
    pub status: String,
    pub attributes: String,
    pub event_timestamps: Vec<i64>,
    pub event_names: Vec<String>,
    pub event_attributes: Vec<String>,
    pub link_trace_ids: Vec<String>,
    pub link_span_ids: Vec<String>,
}

/// A lightweight span row from the trace_index materialized view.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TraceIndexRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status_code: u16,
    pub duration_ns: u64,
    pub status: String,
    pub timestamp: i64,
}

/// A span event (log entry within a span).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: String,
    pub name: String,
    pub attributes: serde_json::Value,
}

/// A fully assembled trace with nested span tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceResponse {
    pub trace_id: String,
    pub spans: Vec<SpanNode>,
    pub span_count: usize,
    pub duration_ns: u64,
    pub services: Vec<String>,
}

/// A span node in the assembled trace tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanNode {
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub service_version: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status_code: u16,
    pub duration_ns: u64,
    pub status: String,
    pub timestamp: String,
    pub attributes: serde_json::Value,
    pub events: Vec<SpanEvent>,
    pub children: Vec<SpanNode>,
}

/// Convert nanoseconds since epoch to a human-readable UTC timestamp string.
pub fn nanos_to_string(nanos: i64) -> String {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, subsec_nanos)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string())
        .unwrap_or_else(|| nanos.to_string())
}
