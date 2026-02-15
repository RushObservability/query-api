use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Raw sample from ClickHouse otel_metrics_gauge or otel_metrics_sum
#[derive(Debug, Clone, Row, Deserialize)]
pub struct MetricSample {
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "ts_ms")]
    pub ts_ms: i64,
    #[serde(rename = "Value")]
    pub value: f64,
}

/// Row for label discovery queries
#[derive(Debug, Clone, Row, Deserialize)]
pub struct LabelNameRow {
    pub name: String,
}

#[derive(Debug, Clone, Row, Deserialize)]
pub struct LabelValueRow {
    pub value: String,
}

#[derive(Debug, Clone, Row, Deserialize)]
pub struct SeriesRow {
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
}

// ── Prometheus JSON response types ──

#[derive(Debug, Serialize)]
pub struct PromResponse<T: Serialize> {
    pub status: &'static str,
    pub data: T,
}

#[derive(Debug, Serialize)]
pub struct VectorData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<VectorResult>,
}

#[derive(Debug, Serialize)]
pub struct VectorResult {
    pub metric: BTreeMap<String, String>,
    pub value: (f64, String), // (timestamp, value_string)
}

#[derive(Debug, Serialize)]
pub struct MatrixData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<MatrixResult>,
}

#[derive(Debug, Serialize)]
pub struct MatrixResult {
    pub metric: BTreeMap<String, String>,
    pub values: Vec<(f64, String)>, // [(timestamp, value_string), ...]
}
