/// Shared ClickHouse insert row types used by the write path and spool/replayer.
///
/// These structs are intentionally `pub` so that `ch_writer` can reference them
/// in the `SpoolBatch` enum without depending on the handler crates.
use clickhouse::Row;
use serde::{Deserialize, Serialize};

// ═══ spans_raw (Datadog traces) ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TraceInsertRow {
    pub tenant_id: String,
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "ParentSpanId")]
    pub parent_span_id: String,
    #[serde(rename = "TraceState")]
    pub trace_state: String,
    #[serde(rename = "SpanName")]
    pub span_name: String,
    #[serde(rename = "SpanKind")]
    pub span_kind: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "SpanAttributes")]
    pub span_attributes: Vec<(String, String)>,
    #[serde(rename = "Duration")]
    pub duration: u64,
    #[serde(rename = "StatusCode")]
    pub status_code: String,
    #[serde(rename = "StatusMessage")]
    pub status_message: String,
    #[serde(rename = "Events.Timestamp")]
    pub events_timestamp: Vec<i64>,
    #[serde(rename = "Events.Name")]
    pub events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub events_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Links.TraceId")]
    pub links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub links_attributes: Vec<Vec<(String, String)>>,
}

// ═══ logs (Datadog logs) ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogInsertRow {
    pub tenant_id: String,
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "TraceFlags")]
    pub trace_flags: u32,
    #[serde(rename = "SeverityText")]
    pub severity_text: String,
    #[serde(rename = "SeverityNumber")]
    pub severity_number: u8,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "LogAttributes")]
    pub log_attributes: Vec<(String, String)>,
    #[serde(rename = "EventName")]
    pub event_name: String,
}

// ═══ metrics_gauge ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct GaugeRow {
    pub tenant_id: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Value")]
    pub value: f64,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
}

// ═══ metrics_sum ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct SumRow {
    pub tenant_id: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Value")]
    pub value: f64,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
    #[serde(rename = "IsMonotonic")]
    pub is_monotonic: bool,
}

impl SumRow {
    pub fn from_gauge(g: &GaugeRow, monotonic: bool) -> Self {
        SumRow {
            tenant_id: g.tenant_id.clone(),
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

// ═══ metrics_histogram ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct HistogramRow {
    pub tenant_id: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "BucketCounts")]
    pub bucket_counts: Vec<u64>,
    #[serde(rename = "ExplicitBounds")]
    pub explicit_bounds: Vec<f64>,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Min")]
    pub min: f64,
    #[serde(rename = "Max")]
    pub max: f64,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
}

// ═══ metrics_exp_histogram ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct ExpHistogramRow {
    pub tenant_id: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "Scale")]
    pub scale: i32,
    #[serde(rename = "ZeroCount")]
    pub zero_count: u64,
    #[serde(rename = "PositiveOffset")]
    pub positive_offset: i32,
    #[serde(rename = "PositiveBucketCounts")]
    pub positive_bucket_counts: Vec<u64>,
    #[serde(rename = "NegativeOffset")]
    pub negative_offset: i32,
    #[serde(rename = "NegativeBucketCounts")]
    pub negative_bucket_counts: Vec<u64>,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Min")]
    pub min: f64,
    #[serde(rename = "Max")]
    pub max: f64,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
}

// ═══ metrics_summary ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct SummaryRow {
    pub tenant_id: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    /// ValueAtQuantiles is stored as a Nested(Quantile Float64, Value Float64) in ClickHouse.
    /// We represent it as parallel arrays: (quantiles, values).
    #[serde(rename = "ValueAtQuantiles.Quantile")]
    pub quantiles: Vec<f64>,
    #[serde(rename = "ValueAtQuantiles.Value")]
    pub quantile_values: Vec<f64>,
    #[serde(rename = "Flags")]
    pub flags: u32,
}

// ═══ Metric firewall integration ═══
// All metric row types expose metric_name + attributes for the ingest-time
// metric firewall (see crate::metric_firewall).
macro_rules! impl_metric_row {
    ($t:ty) => {
        impl crate::metric_firewall::MetricRow for $t {
            fn fw_metric_name(&self) -> &str { &self.metric_name }
            fn fw_attributes(&self) -> &[(String, String)] { &self.attributes }
            fn fw_attributes_mut(&mut self) -> &mut Vec<(String, String)> { &mut self.attributes }
        }
    };
}
impl_metric_row!(GaugeRow);
impl_metric_row!(SumRow);
impl_metric_row!(HistogramRow);
impl_metric_row!(ExpHistogramRow);
impl_metric_row!(SummaryRow);

// ═══ rum_replay ═══

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct RumReplayChunk {
    pub tenant_id: String,
    pub session_id: String,
    pub app_name: String,
    pub chunk_idx: u32,
    pub chunk_ts: i64,
    pub events_json: String,
}
