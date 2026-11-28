/// OTLP/HTTP ingest endpoints and Vector JSON-logs endpoint.
///
/// Routes registered in main.rs:
///   POST /v1/traces          — OTel Collector OTLP/HTTP traces
///   POST /v1/logs            — OTel Collector OTLP/HTTP logs
///   POST /v1/metrics         — OTel Collector OTLP/HTTP metrics
///   POST /api/v1/ingest/logs — Vector JSON log array (or single object)
///
/// All handlers resolve tenant via the same TenantContext extension set by
/// tenant_middleware (Bearer / DD-API-KEY / X-Rush-Tenant / "default").
///
/// Content-type handling:
///   application/x-protobuf  → prost Message::decode (primary)
///   application/json        → 415 Unsupported Media Type (OTLP JSON not supported)
///   Missing / other         → attempt protobuf decode; 400 on failure
use crate::timestamps::TimestampExt;
use axum::{
    Extension,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use prost::Message;
use serde::Deserialize;

use crate::AppState;
use crate::TenantContext;
use crate::ch_writer::{SpoolBatch, WriteError};
use crate::ingest_limits::{ProtoField, counted, nested};
use crate::models::ingest::{
    ExpHistogramRow, GaugeRow, HistogramRow, LogInsertRow, SumRow, SummaryRow, TraceInsertRow,
};

// ─── Re-export the proto types we need ───────────────────────────────────────

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exemplar;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;

// ─── Shared helpers ───────────────────────────────────────────────────────────

/// Map a WriteError to an axum (StatusCode, String) error response.
fn map_write_err(e: WriteError) -> (StatusCode, String) {
    match e {
        WriteError::Backpressure => (
            StatusCode::TOO_MANY_REQUESTS,
            "ingest backpressure: clickhouse unavailable, spool full".to_string(),
        ),
        WriteError::Fatal(s) => crate::api_error::internal_legacy("otlp.write", s),
    }
}

/// Decode an incoming request body as protobuf, returning 415 for JSON or an
/// unsupported Content-Encoding and 400 for other decode failures. Honors
/// gzip, deflate and zstd `Content-Encoding` (the OTel Collector's otlphttp
/// exporter compresses by default), bounded by the decompressed-bytes limit.
pub(crate) async fn decode_proto<T: Message + Default + Send + 'static>(
    limits: &crate::ingest_limits::IngestLimits,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<T, (StatusCode, String)> {
    decode_proto_with(limits, headers, body, &[], |_, req| Ok(req)).await
}

/// `decode_proto` with the entity limit for `entities` checked before the
/// decode, then `build` (row building) in the same blocking task and decode
/// permit, so none of it runs on a tokio worker.
async fn decode_proto_with<T, R, F>(
    limits: &crate::ingest_limits::IngestLimits,
    headers: &HeaderMap,
    body: Bytes,
    entities: &'static [ProtoField],
    build: F,
) -> Result<R, (StatusCode, String)>
where
    T: Message + Default,
    R: Send + 'static,
    F: FnOnce(&crate::ingest_limits::IngestLimits, T) -> Result<R, (StatusCode, String)>
        + Send
        + 'static,
{
    limits.check_body("otlp", &body)?;
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Reject OTLP JSON explicitly — we only support protobuf binary.
    if ct.contains("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "OTLP JSON is not supported; use application/x-protobuf".to_string(),
        ));
    }

    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if !matches!(encoding.as_str(), "" | "identity")
        && !super::dd_common::is_compressed_encoding(&encoding)
    {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported Content-Encoding; use gzip, deflate or zstd".to_string(),
        ));
    }

    // Protobuf decode is synchronous CPU work even for identity bodies. Always
    // admit it through the dedicated semaphore and blocking pool.
    limits
        .spawn_decode("otlp", move |limits| {
            build(
                limits,
                decode_proto_sync::<T>(limits, &encoding, &body, entities)?,
            )
        })
        .await
}

fn decode_proto_sync<T: Message + Default>(
    limits: &crate::ingest_limits::IngestLimits,
    encoding: &str,
    body: &[u8],
    entities: &[ProtoField],
) -> Result<T, (StatusCode, String)> {
    let decoded: std::borrow::Cow<[u8]> = if super::dd_common::is_compressed_encoding(encoding) {
        // Capped at the decompressed-bytes limit (decompression-bomb guard).
        std::borrow::Cow::Owned(super::dd_common::decompress_body_sync(
            limits,
            "otlp",
            "invalid compressed OTLP payload",
            encoding,
            body,
        )?)
    } else {
        limits.check_decompressed("otlp", body.len())?;
        std::borrow::Cow::Borrowed(body)
    };

    if !entities.is_empty() {
        limits.check_proto_entities("otlp", "invalid OTLP payload", &decoded, entities)?;
    }
    T::decode(decoded.as_ref()).map_err(|_| limits.malformed("otlp", "invalid OTLP payload"))
}

/// Convert an OTLP AnyValue to a String for storage as a span/log attribute value.
fn any_value_to_string(v: &AnyValue) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &v.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::BytesValue(b)) => hex::encode(b),
        Some(Value::ArrayValue(a)) => {
            // Serialize array as JSON-ish string
            let parts: Vec<String> = a.values.iter().map(any_value_to_string).collect();
            format!("[{}]", parts.join(","))
        }
        Some(Value::KvlistValue(kv)) => {
            let parts: Vec<String> = kv
                .values
                .iter()
                .map(|kv| {
                    let val = kv
                        .value
                        .as_ref()
                        .map(any_value_to_string)
                        .unwrap_or_default();
                    format!("{}={}", kv.key, val)
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        // This variant references the profiling signal's string table. It is not
        // meaningful for traces, logs, or metrics, so OTLP receivers are expected
        // to treat it as an empty value for those signals.
        Some(Value::StringValueStrindex(_)) => String::new(),
        None => String::new(),
    }
}

/// Convert a slice of OTLP KeyValue into the Vec<(String,String)> used by row types.
fn kv_to_attrs(kvs: &[KeyValue]) -> Vec<(String, String)> {
    kvs.iter()
        .map(|kv| {
            let val = kv
                .value
                .as_ref()
                .map(any_value_to_string)
                .unwrap_or_default();
            (kv.key.clone(), val)
        })
        .collect()
}

/// Extract the service.name from a Resource's attributes, defaulting to "".
fn resource_service_name(resource: &opentelemetry_proto::tonic::resource::v1::Resource) -> String {
    resource
        .attributes
        .iter()
        .find(|kv| kv.key == "service.name")
        .and_then(|kv| kv.value.as_ref())
        .map(any_value_to_string)
        .unwrap_or_default()
}

/// Hex-encode a byte slice (lowercase), matching the convention used in spans_raw.
fn bytes_to_hex(b: &[u8]) -> String {
    hex::encode(b)
}

// ─── OTLP Exemplar helper ────────────────────────────────────────────────────

struct ExemplarData {
    filtered_attributes: Vec<Vec<(String, String)>>,
    time_unix: Vec<i64>,
    value: Vec<f64>,
    span_id: Vec<String>,
    trace_id: Vec<String>,
}

fn extract_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> ExemplarData {
    let mut filtered_attributes = Vec::with_capacity(exemplars.len());
    let mut time_unix = Vec::with_capacity(exemplars.len());
    let mut value = Vec::with_capacity(exemplars.len());
    let mut span_id = Vec::with_capacity(exemplars.len());
    let mut trace_id = Vec::with_capacity(exemplars.len());

    for ex in exemplars {
        filtered_attributes.push(kv_to_attrs(&ex.filtered_attributes));
        time_unix.push(ex.time_unix_nano as i64);
        let v = match &ex.value {
            Some(exemplar::Value::AsDouble(d)) => *d,
            Some(exemplar::Value::AsInt(i)) => *i as f64,
            None => 0.0,
        };
        value.push(v);
        span_id.push(bytes_to_hex(&ex.span_id));
        trace_id.push(bytes_to_hex(&ex.trace_id));
    }

    ExemplarData {
        filtered_attributes,
        time_unix,
        value,
        span_id,
        trace_id,
    }
}

// ─── SpanKind integer to string ───────────────────────────────────────────────

fn span_kind_name(kind: i32) -> &'static str {
    match kind {
        0 => "SPAN_KIND_UNSPECIFIED",
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "SPAN_KIND_UNSPECIFIED",
    }
}

fn status_code_name(code: i32) -> &'static str {
    match code {
        0 => "STATUS_CODE_UNSET",
        1 => "STATUS_CODE_OK",
        2 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
}

// Entities counted before decoding: resources, scopes and records with their
// attributes (span events and links too) for traces and logs; resources,
// scopes, metrics and data points for metrics.
const RESOURCE: ProtoField = nested(1, &[counted(1, &[])]);
const SCOPE: ProtoField = nested(1, &[counted(3, &[])]);
const TRACE_ENTITIES: &[ProtoField] = &[counted(
    1,
    &[
        RESOURCE,
        counted(
            2,
            &[
                SCOPE,
                counted(
                    2,
                    &[
                        counted(9, &[]),
                        counted(11, &[counted(3, &[])]),
                        counted(13, &[counted(4, &[])]),
                    ],
                ),
            ],
        ),
    ],
)];
const LOG_ENTITIES: &[ProtoField] = &[counted(
    1,
    &[
        RESOURCE,
        counted(2, &[SCOPE, counted(2, &[counted(6, &[])])]),
    ],
)];
const POINTS: &[ProtoField] = &[counted(1, &[])];
const METRIC_ENTITIES: &[ProtoField] = &[counted(
    1,
    &[
        RESOURCE,
        counted(
            2,
            &[
                SCOPE,
                counted(
                    2,
                    &[
                        nested(5, POINTS),
                        nested(7, POINTS),
                        nested(9, POINTS),
                        nested(10, POINTS),
                        nested(11, POINTS),
                    ],
                ),
            ],
        ),
    ],
)];

// ─── POST /v1/traces ──────────────────────────────────────────────────────────

pub async fn ingest_otlp_traces(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;

    let tenant = tenant_id.clone();
    let rows = decode_proto_with(
        &state.ingest_limits,
        &headers,
        body.clone(),
        TRACE_ENTITIES,
        move |limits, req: ExportTraceServiceRequest| {
            // Reshape OTel-native rows into wide `spans` rows at ingest (the former
            // spans_mv transform) for the single `spans` table — no spans_raw copy.
            Ok(crate::models::trace::WideEvent::from_rows(otlp_trace_rows(
                limits, &req, &tenant,
            )?))
        },
    )
    .await?;

    if rows.is_empty() {
        return Ok(StatusCode::OK);
    }

    let count = rows.len();
    crate::handlers::ingest_gate::write_gated(&state, tenant_id, SpoolBatch::Spans(rows))
        .await
        .map_err(map_write_err)?;

    state
        .usage_accumulator
        .record(&tenant_id, "traces", count as u64, body.len() as u64);

    tracing::debug!(
        signal = "traces",
        tenant_id = %tenant_id,
        spans_count = count,
        source = "otlp",
        "ingested spans"
    );

    Ok(StatusCode::OK)
}

// ─── POST /v1/logs ────────────────────────────────────────────────────────────

pub async fn ingest_otlp_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;

    let tenant = tenant_id.clone();
    let rows = decode_proto_with(
        &state.ingest_limits,
        &headers,
        body.clone(),
        LOG_ENTITIES,
        move |limits, req: ExportLogsServiceRequest| otlp_log_rows(limits, &req, &tenant),
    )
    .await?;

    if rows.is_empty() {
        return Ok(StatusCode::OK);
    }

    let count = rows.len();
    crate::handlers::ingest_gate::write_gated(&state, &tenant_id, SpoolBatch::Logs(rows))
        .await
        .map_err(map_write_err)?;

    state
        .usage_accumulator
        .record(&tenant_id, "logs", count as u64, body.len() as u64);

    tracing::debug!(
        signal = "logs",
        tenant_id = %tenant_id,
        count = count,
        source = "otlp",
        "ingested logs"
    );

    Ok(StatusCode::OK)
}

// ─── POST /v1/metrics ────────────────────────────────────────────────────────

pub async fn ingest_otlp_metrics(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;

    let tenant = tenant_id.clone();
    let (gauge_rows, sum_rows, histogram_rows, exp_histogram_rows, summary_rows) =
        decode_proto_with(
            &state.ingest_limits,
            &headers,
            body.clone(),
            METRIC_ENTITIES,
            move |_, req: ExportMetricsServiceRequest| Ok(otlp_metric_rows(&req, &tenant)),
        )
        .await?;

    let total = gauge_rows.len()
        + sum_rows.len()
        + histogram_rows.len()
        + exp_histogram_rows.len()
        + summary_rows.len();

    if total == 0 {
        return Ok(StatusCode::OK);
    }

    // Write each non-empty type batch concurrently.
    let gauge_fut = async {
        if !gauge_rows.is_empty() {
            crate::handlers::ingest_gate::write_gated(
                &state,
                &tenant_id,
                SpoolBatch::Gauge(gauge_rows),
            )
            .await
            .map_err(map_write_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let sum_fut = async {
        if !sum_rows.is_empty() {
            crate::handlers::ingest_gate::write_gated(
                &state,
                &tenant_id,
                SpoolBatch::Sum(sum_rows),
            )
            .await
            .map_err(map_write_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let histogram_fut = async {
        if !histogram_rows.is_empty() {
            crate::handlers::ingest_gate::write_gated(
                &state,
                &tenant_id,
                SpoolBatch::Histogram(histogram_rows),
            )
            .await
            .map_err(map_write_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let exp_histogram_fut = async {
        if !exp_histogram_rows.is_empty() {
            crate::handlers::ingest_gate::write_gated(
                &state,
                &tenant_id,
                SpoolBatch::ExpHistogram(exp_histogram_rows),
            )
            .await
            .map_err(map_write_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };
    let summary_fut = async {
        if !summary_rows.is_empty() {
            crate::handlers::ingest_gate::write_gated(
                &state,
                &tenant_id,
                SpoolBatch::Summary(summary_rows),
            )
            .await
            .map_err(map_write_err)?;
        }
        Ok::<_, (StatusCode, String)>(())
    };

    let (r1, r2, r3, r4, r5) = tokio::join!(
        gauge_fut,
        sum_fut,
        histogram_fut,
        exp_histogram_fut,
        summary_fut
    );
    r1?;
    r2?;
    r3?;
    r4?;
    r5?;

    state
        .usage_accumulator
        .record(&tenant_id, "metrics", total as u64, body.len() as u64);

    tracing::debug!(
        signal = "metrics",
        tenant_id = %tenant_id,
        total = total,
        source = "otlp",
        "ingested metrics"
    );

    Ok(StatusCode::OK)
}

// ─── POST /api/v1/ingest/logs  (Vector JSON) ─────────────────────────────────
//
// Vector's `http` sink with `encoding.codec = "json"` posts a JSON array of
// objects shaped by the `parse_logs` transform in vector-kube-logs.yaml:
//
//   {
//     "ServiceName":        "my-svc",
//     "SeverityText":       "ERROR",
//     "SeverityNumber":     17,
//     "Body":               "...",
//     "Timestamp":          1700000000000000000,   // i64 nanos
//     "TraceId":            "abc123",
//     "SpanId":             "deadbeef",
//     "TraceFlags":         1,
//     "ResourceAttributes": { "k8s.namespace.name": "prod" },
//     "LogAttributes":      { "foo": "bar" },
//     "ScopeName":          "opentelemetry.instrumentation.logging",
//     "EventName":          ""
//   }
//
// Supports:  top-level JSON array  (Vector default)
//            single JSON object    (tolerated for convenience)

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VectorLogEntry {
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub severity_text: String,
    #[serde(default)]
    pub severity_number: u8,
    #[serde(default)]
    pub body: String,
    /// Unix nanoseconds as i64; 0 / missing → current time.
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
    #[serde(default)]
    pub trace_flags: u32,
    /// Flat JSON object.  Missing keys → empty map. Deserialized straight into
    /// a map (not a generic Value) so attrs are parsed exactly once.
    #[serde(default)]
    pub resource_attributes: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub log_attributes: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub scope_name: String,
    #[serde(default)]
    pub event_name: String,
}

/// Flatten a JSON object into Vec<(String,String)>.
/// Non-string leaf values are stringified.
fn json_obj_to_attrs(map: &serde_json::Map<String, serde_json::Value>) -> Vec<(String, String)> {
    map.iter()
        .map(|(k, v)| {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (k.clone(), s)
        })
        .collect()
}

pub async fn ingest_vector_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    state.ingest_limits.check_body("otlp", &body)?;
    state.ingest_limits.check_decompressed("otlp", body.len())?;

    let wire_bytes = body.len();
    let tenant = tenant_id.clone();
    let rows = state
        .ingest_limits
        .spawn_decode("otlp", move |limits| {
            vector_log_rows(limits, &tenant, &body)
        })
        .await?;
    if rows.is_empty() {
        return Ok(StatusCode::OK);
    }

    let count = rows.len();
    crate::handlers::ingest_gate::write_gated(&state, tenant_id, SpoolBatch::Logs(rows))
        .await
        .map_err(map_write_err)?;

    state
        .usage_accumulator
        .record(tenant_id, "logs", count as u64, wire_bytes as u64);

    tracing::info!(
        signal = "logs",
        tenant_id = %tenant_id,
        count = count,
        source = "vector",
        "ingested logs"
    );

    Ok(StatusCode::OK)
}

/// Build span rows from a decoded OTLP trace request. Runs on the blocking pool.
fn otlp_trace_rows(
    limits: &crate::ingest_limits::IngestLimits,
    req: &ExportTraceServiceRequest,
    tenant_id: &str,
) -> Result<Vec<TraceInsertRow>, (StatusCode, String)> {
    let mut rows: Vec<TraceInsertRow> = Vec::new();
    let mut fan_out = 0usize;

    // Arc refactor: tenant_id is shared across the whole batch — allocate once.
    let tenant_id: std::sync::Arc<str> = tenant_id.into();

    for rs in &req.resource_spans {
        let resource = rs.resource.as_ref();
        // Per-resource shared data: build the resource attribute Vec + the
        // service.name/schema_url strings ONCE, behind Arc, then hand cheap Arc
        // clones to every span row instead of re-allocating per span.
        let resource_attributes: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(
            resource
                .map(|r| kv_to_attrs(&r.attributes))
                .unwrap_or_default(),
        );
        let service_name: std::sync::Arc<str> = resource
            .map(|r| resource_service_name(r))
            .unwrap_or_default()
            .into();
        let resource_schema_url = rs.schema_url.clone();

        for ss in &rs.scope_spans {
            let scope = ss.scope.as_ref();
            let scope_name: std::sync::Arc<str> =
                scope.map(|s| s.name.as_str()).unwrap_or("").into();
            let scope_version: std::sync::Arc<str> =
                scope.map(|s| s.version.as_str()).unwrap_or("").into();
            // Scope attributes and schema URLs are copied into every span's
            // attributes: build them once per scope and count the copies.
            let mut scope_span_attrs: Vec<(String, String)> = scope
                .map(|s| {
                    s.attributes
                        .iter()
                        .map(|kv| {
                            let val = kv
                                .value
                                .as_ref()
                                .map(any_value_to_string)
                                .unwrap_or_default();
                            (format!("scope.{}", kv.key), val)
                        })
                        .collect()
                })
                .unwrap_or_default();
            if !resource_schema_url.is_empty() {
                scope_span_attrs.push((
                    "resource.schema_url".to_string(),
                    resource_schema_url.clone(),
                ));
            }
            if !ss.schema_url.is_empty() {
                scope_span_attrs.push(("scope.schema_url".to_string(), ss.schema_url.clone()));
            }
            // WideEvent copies service_name into every span row.
            limits.check_fan_out(
                "otlp",
                &mut fan_out,
                service_name.len() + crate::ingest_limits::attr_bytes(&scope_span_attrs),
                ss.spans.len(),
            )?;

            for span in &ss.spans {
                let trace_id = bytes_to_hex(&span.trace_id);
                let span_id = bytes_to_hex(&span.span_id);
                let parent_span_id = if span.parent_span_id.is_empty() {
                    String::new()
                } else {
                    bytes_to_hex(&span.parent_span_id)
                };

                // OTLP times are in nanoseconds; TraceInsertRow.timestamp is i64 nanos.
                let timestamp = span.start_time_unix_nano as i64;
                let end_time = span.end_time_unix_nano as i64;
                let duration = if end_time > timestamp {
                    (end_time - timestamp) as u64
                } else {
                    0u64
                };

                let span_attrs = kv_to_attrs(&span.attributes);
                let status = span.status.as_ref();
                let status_code = status
                    .map(|s| status_code_name(s.code))
                    .unwrap_or("STATUS_CODE_UNSET");
                let status_message = status.map(|s| s.message.clone()).unwrap_or_default();

                // Events
                let mut events_timestamp = Vec::with_capacity(span.events.len());
                let mut events_name = Vec::with_capacity(span.events.len());
                let mut events_attributes = Vec::with_capacity(span.events.len());
                for ev in &span.events {
                    events_timestamp.push(ev.time_unix_nano as i64);
                    events_name.push(ev.name.clone());
                    events_attributes.push(kv_to_attrs(&ev.attributes));
                }

                // Links
                let mut links_trace_id = Vec::with_capacity(span.links.len());
                let mut links_span_id = Vec::with_capacity(span.links.len());
                let mut links_trace_state = Vec::with_capacity(span.links.len());
                let mut links_attributes = Vec::with_capacity(span.links.len());
                for lk in &span.links {
                    links_trace_id.push(bytes_to_hex(&lk.trace_id));
                    links_span_id.push(bytes_to_hex(&lk.span_id));
                    links_trace_state.push(lk.trace_state.clone());
                    links_attributes.push(kv_to_attrs(&lk.attributes));
                }

                // TraceInsertRow has no ScopeAttributes column, so scope attrs
                // ("scope." prefix) and schema URLs go into span_attributes.
                let mut all_span_attrs = span_attrs;
                all_span_attrs.extend(scope_span_attrs.iter().cloned());

                rows.push(TraceInsertRow {
                    tenant_id: tenant_id.clone(),
                    timestamp,
                    trace_id,
                    span_id,
                    parent_span_id,
                    trace_state: span.trace_state.clone(),
                    span_name: span.name.clone(),
                    span_kind: span_kind_name(span.kind).to_string(),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    span_attributes: all_span_attrs,
                    duration,
                    status_code: status_code.to_string(),
                    status_message,
                    events_timestamp,
                    events_name,
                    events_attributes,
                    links_trace_id,
                    links_span_id,
                    links_trace_state,
                    links_attributes,
                });
            }
        }
    }

    Ok(rows)
}

/// Build log rows from a decoded OTLP logs request. Runs on the blocking pool.
fn otlp_log_rows(
    limits: &crate::ingest_limits::IngestLimits,
    req: &ExportLogsServiceRequest,
    tenant_id: &str,
) -> Result<Vec<LogInsertRow>, (StatusCode, String)> {
    let mut rows: Vec<LogInsertRow> = Vec::new();
    let mut fan_out = 0usize;

    let now_ns = time::OffsetDateTime::now_utc().unix_nanos().unwrap_or(0);

    // Arc refactor: tenant_id is shared across the whole batch — allocate once.
    let tenant_id: std::sync::Arc<str> = tenant_id.into();

    for rl in &req.resource_logs {
        let resource = rl.resource.as_ref();
        // Per-resource shared data behind Arc (allocated once, cheaply cloned
        // into each log row).
        let resource_attributes: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(
            resource
                .map(|r| kv_to_attrs(&r.attributes))
                .unwrap_or_default(),
        );
        let service_name = resource
            .map(|r| resource_service_name(r))
            .unwrap_or_default();
        let resource_schema_url: std::sync::Arc<str> = rl.schema_url.as_str().into();

        for sl in &rl.scope_logs {
            let scope = sl.scope.as_ref();
            let scope_name: std::sync::Arc<str> =
                scope.map(|s| s.name.as_str()).unwrap_or("").into();
            let scope_version: std::sync::Arc<str> =
                scope.map(|s| s.version.as_str()).unwrap_or("").into();
            let scope_attributes: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(
                scope
                    .map(|s| kv_to_attrs(&s.attributes))
                    .unwrap_or_default(),
            );
            let scope_schema_url: std::sync::Arc<str> = sl.schema_url.as_str().into();
            // service_name is copied into every log row.
            limits.check_fan_out(
                "otlp",
                &mut fan_out,
                service_name.len(),
                sl.log_records.len(),
            )?;

            for lr in &sl.log_records {
                let ts = if lr.time_unix_nano != 0 {
                    lr.time_unix_nano as i64
                } else if lr.observed_time_unix_nano != 0 {
                    lr.observed_time_unix_nano as i64
                } else {
                    now_ns
                };

                let trace_id = bytes_to_hex(&lr.trace_id);
                let span_id = bytes_to_hex(&lr.span_id);

                let body_str = lr
                    .body
                    .as_ref()
                    .map(any_value_to_string)
                    .unwrap_or_default();

                let log_attrs = kv_to_attrs(&lr.attributes);

                rows.push(LogInsertRow {
                    tenant_id: tenant_id.clone(),
                    timestamp: ts,
                    trace_id,
                    span_id,
                    trace_flags: lr.flags,
                    severity_text: lr.severity_text.clone(),
                    severity_number: lr.severity_number as u8,
                    body: body_str,
                    service_name: service_name.clone(),
                    resource_schema_url: resource_schema_url.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_schema_url: scope_schema_url.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attributes: scope_attributes.clone(),
                    log_attributes: log_attrs,
                    event_name: String::new(),
                });
            }
        }
    }

    Ok(rows)
}

/// Rows built from one OTLP metrics request: gauge, sum, histogram,
/// exponential histogram and summary.
type OtlpMetricRows = (
    Vec<GaugeRow>,
    Vec<SumRow>,
    Vec<HistogramRow>,
    Vec<ExpHistogramRow>,
    Vec<SummaryRow>,
);

/// Build metric rows from a decoded OTLP metrics request. Runs on the blocking pool.
fn otlp_metric_rows(req: &ExportMetricsServiceRequest, tenant_id: &str) -> OtlpMetricRows {
    let mut gauge_rows: Vec<GaugeRow> = Vec::new();
    let mut sum_rows: Vec<SumRow> = Vec::new();
    let mut histogram_rows: Vec<HistogramRow> = Vec::new();
    let mut exp_histogram_rows: Vec<ExpHistogramRow> = Vec::new();
    let mut summary_rows: Vec<SummaryRow> = Vec::new();

    // Arc refactor: tenant_id is shared across the whole batch — allocate once.
    // For a 10k-datapoint batch the per-resource/scope/metric values below are
    // each allocated once and cheaply Arc-cloned into every datapoint row, in
    // place of the previous per-datapoint String/Vec clones.
    let tenant_id: std::sync::Arc<str> = tenant_id.into();

    for rm in &req.resource_metrics {
        let resource = rm.resource.as_ref();
        let resource_attributes: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(
            resource
                .map(|r| kv_to_attrs(&r.attributes))
                .unwrap_or_default(),
        );
        let service_name: std::sync::Arc<str> = resource
            .map(|r| resource_service_name(r))
            .unwrap_or_default()
            .into();
        let resource_schema_url: std::sync::Arc<str> = rm.schema_url.as_str().into();

        for sm in &rm.scope_metrics {
            let scope = sm.scope.as_ref();
            let scope_name: std::sync::Arc<str> =
                scope.map(|s| s.name.as_str()).unwrap_or("").into();
            let scope_version: std::sync::Arc<str> =
                scope.map(|s| s.version.as_str()).unwrap_or("").into();
            let scope_attributes: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(
                scope
                    .map(|s| kv_to_attrs(&s.attributes))
                    .unwrap_or_default(),
            );
            let scope_dropped = scope.map(|s| s.dropped_attributes_count).unwrap_or(0);
            let scope_schema_url: std::sync::Arc<str> = sm.schema_url.as_str().into();

            for metric in &sm.metrics {
                let metric_name: std::sync::Arc<str> = metric.name.as_str().into();
                let metric_description: std::sync::Arc<str> = metric.description.as_str().into();
                let metric_unit: std::sync::Arc<str> = metric.unit.as_str().into();

                match &metric.data {
                    Some(MetricData::Gauge(g)) => {
                        for dp in &g.data_points {
                            let attrs = kv_to_attrs(&dp.attributes);
                            let ex = extract_exemplars(&dp.exemplars);
                            use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
                            let value = match &dp.value {
                                Some(Value::AsDouble(d)) => *d,
                                Some(Value::AsInt(i)) => *i as f64,
                                None => 0.0,
                            };
                            gauge_rows.push(GaugeRow {
                                tenant_id: tenant_id.clone(),
                                resource_attributes: resource_attributes.clone(),
                                resource_schema_url: resource_schema_url.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                                scope_dropped_attr_count: scope_dropped,
                                scope_schema_url: scope_schema_url.clone(),
                                service_name: service_name.clone(),
                                metric_name: metric_name.clone(),
                                metric_description: metric_description.clone(),
                                metric_unit: metric_unit.clone(),
                                attributes: attrs,
                                start_time_unix: dp.start_time_unix_nano as i64,
                                time_unix: dp.time_unix_nano as i64,
                                value,
                                flags: dp.flags,
                                exemplars_filtered_attributes: ex.filtered_attributes,
                                exemplars_time_unix: ex.time_unix,
                                exemplars_value: ex.value,
                                exemplars_span_id: ex.span_id,
                                exemplars_trace_id: ex.trace_id,
                            });
                        }
                    }
                    Some(MetricData::Sum(s)) => {
                        let agg_temp = s.aggregation_temporality;
                        let is_monotonic = s.is_monotonic;
                        for dp in &s.data_points {
                            let attrs = kv_to_attrs(&dp.attributes);
                            let ex = extract_exemplars(&dp.exemplars);
                            use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
                            let value = match &dp.value {
                                Some(Value::AsDouble(d)) => *d,
                                Some(Value::AsInt(i)) => *i as f64,
                                None => 0.0,
                            };
                            sum_rows.push(SumRow {
                                tenant_id: tenant_id.clone(),
                                resource_attributes: resource_attributes.clone(),
                                resource_schema_url: resource_schema_url.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                                scope_dropped_attr_count: scope_dropped,
                                scope_schema_url: scope_schema_url.clone(),
                                service_name: service_name.clone(),
                                metric_name: metric_name.clone(),
                                metric_description: metric_description.clone(),
                                metric_unit: metric_unit.clone(),
                                attributes: attrs,
                                start_time_unix: dp.start_time_unix_nano as i64,
                                time_unix: dp.time_unix_nano as i64,
                                value,
                                flags: dp.flags,
                                exemplars_filtered_attributes: ex.filtered_attributes,
                                exemplars_time_unix: ex.time_unix,
                                exemplars_value: ex.value,
                                exemplars_span_id: ex.span_id,
                                exemplars_trace_id: ex.trace_id,
                                aggregation_temporality: agg_temp,
                                is_monotonic,
                            });
                        }
                    }
                    Some(MetricData::Histogram(h)) => {
                        let agg_temp = h.aggregation_temporality;
                        for dp in &h.data_points {
                            let attrs = kv_to_attrs(&dp.attributes);
                            let ex = extract_exemplars(&dp.exemplars);
                            histogram_rows.push(HistogramRow {
                                tenant_id: tenant_id.clone(),
                                resource_attributes: resource_attributes.clone(),
                                resource_schema_url: resource_schema_url.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                                scope_dropped_attr_count: scope_dropped,
                                scope_schema_url: scope_schema_url.clone(),
                                service_name: service_name.clone(),
                                metric_name: metric_name.clone(),
                                metric_description: metric_description.clone(),
                                metric_unit: metric_unit.clone(),
                                attributes: attrs,
                                start_time_unix: dp.start_time_unix_nano as i64,
                                time_unix: dp.time_unix_nano as i64,
                                count: dp.count,
                                sum: dp.sum.unwrap_or(0.0),
                                bucket_counts: dp.bucket_counts.clone(),
                                explicit_bounds: dp.explicit_bounds.clone(),
                                flags: dp.flags,
                                min: dp.min.unwrap_or(0.0),
                                max: dp.max.unwrap_or(0.0),
                                aggregation_temporality: agg_temp,
                                exemplars_filtered_attributes: ex.filtered_attributes,
                                exemplars_time_unix: ex.time_unix,
                                exemplars_value: ex.value,
                                exemplars_span_id: ex.span_id,
                                exemplars_trace_id: ex.trace_id,
                            });
                        }
                    }
                    Some(MetricData::ExponentialHistogram(eh)) => {
                        let agg_temp = eh.aggregation_temporality;
                        for dp in &eh.data_points {
                            let attrs = kv_to_attrs(&dp.attributes);
                            let ex = extract_exemplars(&dp.exemplars);
                            let (pos_offset, pos_counts) = dp
                                .positive
                                .as_ref()
                                .map(|b| (b.offset, b.bucket_counts.clone()))
                                .unwrap_or((0, Vec::new()));
                            let (neg_offset, neg_counts) = dp
                                .negative
                                .as_ref()
                                .map(|b| (b.offset, b.bucket_counts.clone()))
                                .unwrap_or((0, Vec::new()));
                            exp_histogram_rows.push(ExpHistogramRow {
                                tenant_id: tenant_id.clone(),
                                resource_attributes: resource_attributes.clone(),
                                resource_schema_url: resource_schema_url.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                                scope_dropped_attr_count: scope_dropped,
                                scope_schema_url: scope_schema_url.clone(),
                                service_name: service_name.clone(),
                                metric_name: metric_name.clone(),
                                metric_description: metric_description.clone(),
                                metric_unit: metric_unit.clone(),
                                attributes: attrs,
                                start_time_unix: dp.start_time_unix_nano as i64,
                                time_unix: dp.time_unix_nano as i64,
                                count: dp.count,
                                sum: dp.sum.unwrap_or(0.0),
                                scale: dp.scale,
                                zero_count: dp.zero_count,
                                positive_offset: pos_offset,
                                positive_bucket_counts: pos_counts,
                                negative_offset: neg_offset,
                                negative_bucket_counts: neg_counts,
                                flags: dp.flags,
                                min: dp.min.unwrap_or(0.0),
                                max: dp.max.unwrap_or(0.0),
                                aggregation_temporality: agg_temp,
                                exemplars_filtered_attributes: ex.filtered_attributes,
                                exemplars_time_unix: ex.time_unix,
                                exemplars_value: ex.value,
                                exemplars_span_id: ex.span_id,
                                exemplars_trace_id: ex.trace_id,
                            });
                        }
                    }
                    Some(MetricData::Summary(s)) => {
                        for dp in &s.data_points {
                            let attrs = kv_to_attrs(&dp.attributes);
                            let mut quantiles_vec = Vec::with_capacity(dp.quantile_values.len());
                            let mut values_vec = Vec::with_capacity(dp.quantile_values.len());
                            for qv in &dp.quantile_values {
                                quantiles_vec.push(qv.quantile);
                                values_vec.push(qv.value);
                            }
                            summary_rows.push(SummaryRow {
                                tenant_id: tenant_id.clone(),
                                resource_attributes: resource_attributes.clone(),
                                resource_schema_url: resource_schema_url.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                                scope_dropped_attr_count: scope_dropped,
                                scope_schema_url: scope_schema_url.clone(),
                                service_name: service_name.clone(),
                                metric_name: metric_name.clone(),
                                metric_description: metric_description.clone(),
                                metric_unit: metric_unit.clone(),
                                attributes: attrs,
                                start_time_unix: dp.start_time_unix_nano as i64,
                                time_unix: dp.time_unix_nano as i64,
                                count: dp.count,
                                sum: dp.sum,
                                quantiles: quantiles_vec,
                                quantile_values: values_vec,
                                flags: dp.flags,
                            });
                        }
                    }
                    None => {}
                }
            }
        }
    }

    (
        gauge_rows,
        sum_rows,
        histogram_rows,
        exp_histogram_rows,
        summary_rows,
    )
}

/// Parse a Vector JSON logs body and build its rows. Runs on the blocking pool.
fn vector_log_rows(
    limits: &crate::ingest_limits::IngestLimits,
    tenant_id: &str,
    body: &[u8],
) -> Result<Vec<LogInsertRow>, (StatusCode, String)> {
    // Framing-agnostic: Vector's http sink may send a JSON array, a single
    // object, or newline/whitespace-separated JSON depending on version and
    // framing config. Handle all three.
    let first_non_ws = body.iter().find(|b| !b.is_ascii_whitespace()).copied();
    let entries: Vec<VectorLogEntry> = if first_non_ws == Some(b'[') {
        serde_json::from_slice(body)
            .map_err(|_| limits.malformed("otlp", "invalid Vector log payload"))?
    } else {
        // Stream of concatenated/NDJSON objects (also covers a single object).
        let mut out = Vec::new();
        let mut stream = serde_json::Deserializer::from_slice(body).into_iter::<VectorLogEntry>();
        for item in &mut stream {
            out.push(item.map_err(|_| limits.malformed("otlp", "invalid Vector log payload"))?);
        }
        out
    };

    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let entity_count = entries
        .iter()
        .try_fold(entries.len(), |count, entry| {
            count
                .checked_add(entry.resource_attributes.len())
                .and_then(|count| count.checked_add(entry.log_attributes.len()))
        })
        .ok_or_else(|| limits.check_entities("otlp", usize::MAX).unwrap_err())?;
    limits.check_entities("otlp", entity_count)?;

    let now_ns = time::OffsetDateTime::now_utc().unix_nanos().unwrap_or(0);

    // Arc refactor: tenant_id is shared across all entries; the empty scope
    // strings/attrs are shared constants — allocate each once per request.
    let tenant_arc: std::sync::Arc<str> = tenant_id.into();
    let empty_str: std::sync::Arc<str> = "".into();
    let empty_attrs: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(Vec::new());

    let rows = entries
        .into_iter()
        .map(|e| {
            let ts = if e.timestamp != 0 {
                e.timestamp
            } else {
                now_ns
            };
            let resource_attributes =
                std::sync::Arc::new(json_obj_to_attrs(&e.resource_attributes));
            let log_attributes = json_obj_to_attrs(&e.log_attributes);
            LogInsertRow {
                tenant_id: tenant_arc.clone(),
                timestamp: ts,
                trace_id: e.trace_id,
                span_id: e.span_id,
                trace_flags: e.trace_flags,
                severity_text: e.severity_text,
                severity_number: e.severity_number,
                body: e.body,
                service_name: e.service_name,
                resource_schema_url: empty_str.clone(),
                resource_attributes,
                scope_schema_url: empty_str.clone(),
                scope_name: e.scope_name.into(),
                scope_version: empty_str.clone(),
                scope_attributes: empty_attrs.clone(),
                log_attributes,
                event_name: e.event_name,
            }
        })
        .collect();

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;
    use std::sync::Arc;

    fn limits() -> crate::ingest_limits::IngestLimits {
        crate::ingest_limits::IngestLimits::for_test(Arc::new(
            crate::self_metrics::SelfMetrics::new(),
        ))
    }

    #[test]
    fn otlp_gzip_bomb_is_bounded_and_returns_413() {
        let limits = limits();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&vec![0_u8; 4097]).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < limits.max_compressed_bytes);

        let error =
            decode_proto_sync::<ExportLogsServiceRequest>(&limits, "gzip", &compressed, &[])
                .unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            error.1,
            "decompressed ingest payload exceeds configured limit"
        );
    }

    #[test]
    fn malformed_otlp_error_is_stable() {
        let limits = limits();
        let error =
            decode_proto_sync::<ExportLogsServiceRequest>(&limits, "", &[0xff, 0xff, 0xff], &[])
                .unwrap_err();
        assert_eq!(
            error,
            (StatusCode::BAD_REQUEST, "invalid OTLP payload".into())
        );
    }

    #[tokio::test]
    async fn otlp_accepts_gzip_deflate_and_zstd_bodies() {
        use flate2::write::DeflateEncoder;
        let request = ExportLogsServiceRequest {
            resource_logs: vec![Default::default()],
        };
        let raw = request.encode_to_vec();
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(&raw).unwrap();
        let mut deflate = DeflateEncoder::new(Vec::new(), Compression::default());
        deflate.write_all(&raw).unwrap();
        let zstd = zstd::encode_all(raw.as_slice(), 1).unwrap();
        for (encoding, body) in [
            ("gzip", gzip.finish().unwrap()),
            ("deflate", deflate.finish().unwrap()),
            ("zstd", zstd),
            ("identity", raw.clone()),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", encoding.parse().unwrap());
            let decoded: ExportLogsServiceRequest =
                decode_proto(&limits(), &headers, Bytes::from(body))
                    .await
                    .unwrap_or_else(|e| panic!("{encoding}: {e:?}"));
            assert_eq!(decoded, request, "{encoding}");
        }
    }

    #[tokio::test]
    async fn otlp_unsupported_encoding_returns_415() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "snappy".parse().unwrap());
        let error =
            decode_proto::<ExportLogsServiceRequest>(&limits(), &headers, Bytes::from_static(b"x"))
                .await
                .unwrap_err();
        assert_eq!(error.0, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    fn resource(service: &str) -> Option<opentelemetry_proto::tonic::resource::v1::Resource> {
        Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service.into(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn logs(service: &str, records: usize) -> ExportLogsServiceRequest {
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: resource(service),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default(); records],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn traces(service: &str, spans: usize) -> ExportTraceServiceRequest {
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(service),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default(); spans],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn shared_service_name_copied_into_every_row_is_bounded() {
        let limits = limits();
        // 100 bytes x 50 rows exceeds the 4096-byte test limit.
        let long = "s".repeat(100);
        assert_eq!(
            otlp_log_rows(&limits, &logs(&long, 50), "t").unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            otlp_trace_rows(&limits, &traces(&long, 50), "t")
                .unwrap_err()
                .0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            otlp_log_rows(&limits, &logs("svc", 50), "t").unwrap().len(),
            50
        );
        assert_eq!(
            otlp_trace_rows(&limits, &traces("svc", 50), "t")
                .unwrap()
                .len(),
            50
        );
    }

    #[test]
    fn entity_limit_runs_before_the_protobuf_decode() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        };
        use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
        let limits = limits();
        let attr = || KeyValue {
            key: "k".into(),
            ..Default::default()
        };

        // 1 resource + 1 attr, 1 scope, 2 spans with 1 attr, event + attr and
        // link + attr each.
        let mut trace = traces("svc", 2);
        let scope = &mut trace.resource_spans[0].scope_spans[0];
        for span in &mut scope.spans {
            span.attributes = vec![attr()];
            span.events = vec![Event {
                attributes: vec![attr()],
                ..Default::default()
            }];
            span.links = vec![Link {
                attributes: vec![attr()],
                ..Default::default()
            }];
        }
        assert_eq!(
            crate::ingest_limits::count_proto_entities(&trace.encode_to_vec(), TRACE_ENTITIES),
            Some(15)
        );
        let mut log = logs("svc", 3);
        log.resource_logs[0].scope_logs[0].log_records[0].attributes = vec![attr(), attr()];
        assert_eq!(
            crate::ingest_limits::count_proto_entities(&log.encode_to_vec(), LOG_ENTITIES),
            Some(8)
        );
        let metrics = |points: usize| ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(MetricData::Gauge(Gauge {
                            data_points: vec![NumberDataPoint::default(); points],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_eq!(
            crate::ingest_limits::count_proto_entities(
                &metrics(5).encode_to_vec(),
                METRIC_ENTITIES
            ),
            Some(8)
        );

        // Over the 100-entity test limit: rejected before prost runs.
        for (body, entities) in [
            (logs("svc", 100).encode_to_vec(), LOG_ENTITIES),
            (traces("svc", 100).encode_to_vec(), TRACE_ENTITIES),
            (metrics(100).encode_to_vec(), METRIC_ENTITIES),
        ] {
            let error = decode_proto_sync::<ExportLogsServiceRequest>(&limits, "", &body, entities)
                .unwrap_err();
            assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
        }
    }

    #[test]
    fn profile_string_reference_is_ignored_for_non_profile_signals() {
        let value = AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValueStrindex(7),
            ),
        };

        assert_eq!(any_value_to_string(&value), "");
    }
}
