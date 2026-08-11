use clickhouse::Row;
use serde::{Deserialize, Serialize, Serializer};

fn vec_pairs_as_map<S: Serializer>(v: &Vec<(String, String)>, s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = s.serialize_map(Some(v.len()))?;
    for (k, val) in v {
        map.serialize_entry(k, val)?;
    }
    map.end()
}

/// A single log record from the logs table.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogRecord {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "SeverityText")]
    pub severity_text: String,
    #[serde(rename = "SeverityNumber")]
    pub severity_number: u8,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "ResourceAttributes", serialize_with = "vec_pairs_as_map")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "LogAttributes", serialize_with = "vec_pairs_as_map")]
    pub log_attributes: Vec<(String, String)>,
}

/// A slim log row used by the interactive list endpoint.
///
/// Attribute maps are intentionally excluded: they are often much larger than
/// the visible row and are fetched through `/api/v1/logs/detail` only when the
/// user expands a log. The block coordinates provide the fast detail locator;
/// the body hash is used by the stable primary-key fallback after part merges.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogListRecord {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "SeverityText")]
    pub severity_text: String,
    #[serde(rename = "SeverityNumber")]
    pub severity_number: u8,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "TimestampNs")]
    pub timestamp_ns: String,
    #[serde(rename = "BlockNumber")]
    pub block_number: String,
    #[serde(rename = "BlockOffset")]
    pub block_offset: String,
    #[serde(rename = "BodyHash")]
    pub body_hash: String,
    /// Deterministic pagination tie-breaker. Separate from BodyHash because the
    /// latter is the compact cityHash locator used by lazy detail lookup.
    #[serde(rename = "CursorHash")]
    pub cursor_hash: String,
}
