use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// A single wide event span as stored in ClickHouse (v2 schema).
/// Column order must match the `spans` table exactly for `SELECT *`.
/// timestamp/event_timestamps are i64 nanoseconds since epoch (DateTime64(9)).
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct WideEvent {
    pub tenant_id: String,
    pub timestamp: i64,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub span_name: String,
    pub kind: String,
    pub status: String,
    pub duration_ns: u64,
    pub http_method: String,
    pub http_path: String,
    pub http_status_code: u16,
    pub attributes: String,
    pub event_names: Vec<String>,
    pub event_timestamps: Vec<i64>,
    pub event_attributes: Vec<String>,
    pub link_trace_ids: Vec<String>,
    pub link_span_ids: Vec<String>,
}

/// Look up an OTel attribute value by key in a `(key, value)` list.
fn attr_lookup<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Serialize an OTel attribute list to a JSON object string — matches ClickHouse's
/// `toJSONString(Map(...))` so `JSONExtractString(attributes, key)` queries behave
/// identically to the former `spans_mv` output.
fn attrs_to_json(attrs: &[(String, String)]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".into())
}

impl From<crate::models::ingest::TraceInsertRow> for WideEvent {
    /// Reshape an OTel-native ingest row into the wide `spans` row. This is the Rust
    /// port of the former `spans_mv` materialized view — applied at ingest so spans land
    /// directly in the single `spans` table (no `spans_raw` copy + SQL MV).
    fn from(r: crate::models::ingest::TraceInsertRow) -> Self {
        let a = &r.span_attributes;
        let http_method = attr_lookup(a, "http.method").unwrap_or("").to_string();
        // COALESCE(http.route, http.target, url.path, SpanName) — first non-empty.
        let http_path = attr_lookup(a, "http.route")
            .filter(|s| !s.is_empty())
            .or_else(|| attr_lookup(a, "http.target").filter(|s| !s.is_empty()))
            .or_else(|| attr_lookup(a, "url.path").filter(|s| !s.is_empty()))
            .map(str::to_string)
            .unwrap_or_else(|| r.span_name.clone());
        // toUInt16OrZero(COALESCE(http.status_code, http.response.status_code, '0')).
        let http_status_code = attr_lookup(a, "http.status_code")
            .filter(|s| !s.is_empty())
            .or_else(|| attr_lookup(a, "http.response.status_code").filter(|s| !s.is_empty()))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);
        let attributes = attrs_to_json(a);
        let event_attributes = r
            .events_attributes
            .iter()
            .map(|e| attrs_to_json(e))
            .collect();
        WideEvent {
            tenant_id: r.tenant_id.to_string(),
            timestamp: r.timestamp,
            trace_id: r.trace_id,
            span_id: r.span_id,
            parent_span_id: r.parent_span_id,
            service_name: r.service_name.to_string(),
            span_name: r.span_name,
            kind: r.span_kind,
            status: r.status_code,
            duration_ns: r.duration,
            http_method,
            http_path,
            http_status_code,
            attributes,
            event_names: r.events_name,
            event_timestamps: r.events_timestamp,
            event_attributes,
            link_trace_ids: r.links_trace_id,
            link_span_ids: r.links_span_id,
        }
    }
}

/// A slim span row for the Explore list view — only the columns the table renders.
/// Used when a query requests `columns: "list"`. Field names match `WideEvent` so a
/// slim row is a forward-compatible subset on the wire (the wide-only fields are simply
/// absent from the JSON). Column order must match the slim SELECT list exactly.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct SlimEvent {
    pub timestamp: i64,
    pub service_name: String,
    pub span_name: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status_code: u16,
    pub duration_ns: u64,
    pub status: String,
    pub trace_id: String,
    pub span_id: String,
}

/// A lightweight span row from the spans_by_trace materialized view.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TraceIndexRow {
    pub tenant_id: String,
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
