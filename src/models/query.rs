use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// A structured query request against wide_events.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub time_range: TimeRange,
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default = "default_aggregation")]
    pub aggregation: String,
    #[serde(default = "default_limit")]
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
    /// Free-text search across http_path, attributes, event_names, event_attributes
    #[serde(default)]
    pub search: Option<String>,
}

fn default_aggregation() -> String {
    "count".to_string()
}

fn default_limit() -> u64 {
    100
}

#[derive(Debug, Deserialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize)]
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub enum FilterOp {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = "LIKE")]
    Like,
    #[serde(rename = "NOT LIKE")]
    NotLike,
    #[serde(rename = "IN")]
    In,
    #[serde(rename = "NOT IN")]
    NotIn,
}

/// Count query — returns event counts bucketed by time interval.
#[derive(Debug, Deserialize)]
pub struct CountQueryRequest {
    pub time_range: TimeRange,
    pub filters: Vec<Filter>,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub search: Option<String>,
}

fn default_interval() -> String {
    "1m".to_string()
}

/// Count result — time bucketed counts (ClickHouse Row).
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct CountBucket {
    pub bucket: String,
    pub count: u64,
    pub error_count: u64,
}

/// A single string value row from ClickHouse.
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct StringValueRow {
    pub val: String,
}

/// A single count row from ClickHouse.
#[derive(Debug, Deserialize, Row)]
pub struct CountRow {
    pub count: u64,
}

/// Query result wrapper.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub total: u64,
}

/// Timeseries query — returns time-bucketed aggregations including duration percentiles.
#[derive(Debug, Deserialize)]
pub struct TimeseriesRequest {
    pub time_range: TimeRange,
    pub filters: Vec<Filter>,
    #[serde(default = "default_interval")]
    pub interval: String,
    /// Optional group_by field for multi-series (e.g. "service_name")
    #[serde(default)]
    pub group_by: Option<String>,
    #[serde(default)]
    pub search: Option<String>,
}

/// A single timeseries bucket with RED metrics.
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TimeseriesBucket {
    pub bucket: String,
    pub count: u64,
    pub error_count: u64,
    pub avg_duration_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

/// A grouped timeseries bucket (with a group key).
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct GroupedTimeseriesBucket {
    pub bucket: String,
    pub group_key: String,
    pub count: u64,
    pub error_count: u64,
    pub avg_duration_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}
