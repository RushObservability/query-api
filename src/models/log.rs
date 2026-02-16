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

/// A single log record from the otel_logs table.
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
