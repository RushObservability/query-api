use axum::{
    Extension,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use prost::Message;

use crate::AppState;
use crate::TenantContext;
use crate::ch_writer::{SpoolBatch, WriteError};
use crate::ingest_limits::{ProtoField, counted};
use crate::models::ingest::GaugeRow;

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
    state.ingest_limits.check_body("prometheus", &body)?;
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

    // Decompression, protobuf decode, validation and row building are
    // synchronous CPU work; run them on the blocking pool under a decode permit
    // so large remote_write batches don't stall a tokio worker.
    let tenant = tenant_id.clone();
    let Some(decoded) = state
        .ingest_limits
        .spawn_decode("prometheus", move |limits| {
            decode_rows(limits, &tenant, is_snappy, &body)
        })
        .await?
    else {
        return Ok(StatusCode::NO_CONTENT);
    };
    let DecodedRows {
        rows,
        series_count,
        sample_count,
        decompressed_len,
    } = decoded;
    tracing::debug!(
        signal = "metrics",
        source = "prometheus",
        timeseries = series_count,
        samples = sample_count,
        "remote write payload decoded"
    );

    crate::handlers::ingest_gate::write_gated(&state, tenant_id, SpoolBatch::Gauge(rows))
        .await
        .map_err(|e| match e {
            WriteError::Backpressure => (
                StatusCode::TOO_MANY_REQUESTS,
                "ingest backpressure: clickhouse unavailable, spool full".to_string(),
            ),
            WriteError::Fatal(s) => crate::api_error::internal_legacy("remote_write.write", s),
        })?;

    // Record usage for per-tenant ingest metering (use decompressed size for bytes)
    state.usage_accumulator.record(
        tenant_id,
        "metrics",
        sample_count as u64,
        decompressed_len as u64,
    );

    tracing::info!(
        signal = "metrics",
        tenant_id = %tenant_id,
        series_count = series_count,
        samples = sample_count,
        source = "prometheus",
        "ingested remote write"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Rows and counts from one remote-write body.
struct DecodedRows {
    rows: Vec<GaugeRow>,
    series_count: usize,
    sample_count: usize,
    decompressed_len: usize,
}

/// Decompress, decode, validate and build rows. `None` for a request with no
/// series. If decompression fails and content-encoding isn't snappy, the body
/// is treated as raw protobuf (for flexibility with custom senders).
fn decode_rows(
    limits: &crate::ingest_limits::IngestLimits,
    tenant_id: &str,
    is_snappy: bool,
    body: &[u8],
) -> Result<Option<DecodedRows>, (StatusCode, String)> {
    let (write_req, decompressed_len) = decode_write_request(limits, is_snappy, body)?;
    validate_write_request(limits, &write_req)?;
    if write_req.timeseries.is_empty() {
        return Ok(None);
    }
    Ok(Some(DecodedRows {
        rows: build_rows(limits, tenant_id, &write_req)?,
        series_count: write_req.timeseries.len(),
        sample_count: write_req.timeseries.iter().map(|ts| ts.samples.len()).sum(),
        decompressed_len,
    }))
}

/// Build the `metrics_gauge` rows for a validated write request.
fn build_rows(
    limits: &crate::ingest_limits::IngestLimits,
    tenant_id: &str,
    write_req: &WriteRequest,
) -> Result<Vec<GaugeRow>, (StatusCode, String)> {
    // Build metadata lookup (metric_name → description/unit)
    let mut meta_map = std::collections::HashMap::new();
    for m in &write_req.metadata {
        meta_map.insert(
            m.metric_family_name.clone(),
            (m.help.clone(), m.unit.clone()),
        );
    }

    // Build all gauge rows for metrics_gauge.
    // Arc refactor: tenant_id and the constant scope fields are shared across
    // every sample in the request; allocate each once and Arc-clone per row.
    let tenant_arc: std::sync::Arc<str> = tenant_id.into();
    let empty_str: std::sync::Arc<str> = "".into();
    let scope_prom: std::sync::Arc<str> = "prometheus".into();
    let empty_attrs: std::sync::Arc<Vec<(String, String)>> = std::sync::Arc::new(Vec::new());
    let mut rows: Vec<GaugeRow> = Vec::new();
    let mut fan_out = 0usize;

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

        let (description, unit) = meta_map.get(&metric_name).cloned().unwrap_or_default();
        // The labels are copied into every sample's row.
        limits.check_fan_out(
            "prometheus",
            &mut fan_out,
            crate::ingest_limits::attr_bytes(&attrs),
            ts.samples.len(),
        )?;

        // P1: Build template row once per timeseries, only update time+value per sample
        let template = GaugeRow {
            tenant_id: tenant_arc.clone(),
            resource_attributes: empty_attrs.clone(),
            resource_schema_url: empty_str.clone(),
            scope_name: scope_prom.clone(),
            scope_version: empty_str.clone(),
            scope_attributes: empty_attrs.clone(),
            scope_dropped_attr_count: 0,
            scope_schema_url: empty_str.clone(),
            service_name: service_name.as_str().into(),
            metric_name: metric_name.as_str().into(),
            metric_description: description.as_str().into(),
            metric_unit: unit.as_str().into(),
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
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Series, their labels and samples, and metadata records: the same total
/// `validate_write_request` checks, counted before decoding.
const WRITE_ENTITIES: &[ProtoField] = &[
    counted(1, &[counted(1, &[]), counted(2, &[])]),
    counted(3, &[]),
];

fn decode_write_request(
    limits: &crate::ingest_limits::IngestLimits,
    is_snappy: bool,
    body: &[u8],
) -> Result<(WriteRequest, usize), (StatusCode, String)> {
    let decompressed = match snap::raw::decompress_len(body) {
        Ok(claimed_len) => {
            // Snappy embeds the uncompressed length. Check it before
            // `decompress_vec` allocates the claimed output buffer.
            limits.check_decompressed("prometheus", claimed_len)?;
            snap::raw::Decoder::new()
                .decompress_vec(body)
                .map_err(|_| limits.malformed("prometheus", "invalid Snappy ingest payload"))?
        }
        Err(_) if !is_snappy => {
            limits.check_decompressed("prometheus", body.len())?;
            body.to_vec()
        }
        Err(_) => {
            return Err(limits.malformed("prometheus", "invalid Snappy ingest payload"));
        }
    };
    let len = decompressed.len();
    limits.check_decompressed("prometheus", len)?;
    limits.check_proto_entities(
        "prometheus",
        "invalid Prometheus remote-write payload",
        &decompressed,
        WRITE_ENTITIES,
    )?;
    WriteRequest::decode(decompressed.as_slice())
        .map(|request| (request, len))
        .map_err(|_| limits.malformed("prometheus", "invalid Prometheus remote-write payload"))
}

fn validate_write_request(
    limits: &crate::ingest_limits::IngestLimits,
    request: &WriteRequest,
) -> Result<(), (StatusCode, String)> {
    limits.check_count("prometheus", request.timeseries.len(), limits.max_series)?;
    limits.check_count("prometheus", request.metadata.len(), limits.max_metadata)?;

    let mut samples = 0usize;
    let mut labels = 0usize;
    for series in &request.timeseries {
        limits.check_count(
            "prometheus",
            series.labels.len(),
            limits.max_labels_per_series,
        )?;
        samples = samples
            .checked_add(series.samples.len())
            .ok_or_else(|| limits.check_entities("prometheus", usize::MAX).unwrap_err())?;
        labels = labels
            .checked_add(series.labels.len())
            .ok_or_else(|| limits.check_entities("prometheus", usize::MAX).unwrap_err())?;
        for label in &series.labels {
            limits.check_label("prometheus", &label.name, &label.value)?;
        }
    }
    limits.check_count("prometheus", samples, limits.max_samples)?;
    let total = request
        .timeseries
        .len()
        .checked_add(request.metadata.len())
        .and_then(|count| count.checked_add(samples))
        .and_then(|count| count.checked_add(labels))
        .ok_or_else(|| limits.check_entities("prometheus", usize::MAX).unwrap_err())?;
    limits.check_entities("prometheus", total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn limits() -> crate::ingest_limits::IngestLimits {
        crate::ingest_limits::IngestLimits::for_test(Arc::new(
            crate::self_metrics::SelfMetrics::new(),
        ))
    }

    #[test]
    fn remote_write_rejects_oversized_series_labels_and_samples() {
        let limits = limits();
        let too_many_series = WriteRequest {
            timeseries: (0..11).map(|_| TimeSeries::default()).collect(),
            metadata: Vec::new(),
        };
        assert_eq!(
            validate_write_request(&limits, &too_many_series)
                .unwrap_err()
                .0,
            StatusCode::PAYLOAD_TOO_LARGE
        );

        let long_label = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "x".repeat(33),
                    value: "ok".into(),
                }],
                samples: vec![],
            }],
            metadata: Vec::new(),
        };
        assert_eq!(
            validate_write_request(&limits, &long_label).unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn malformed_snappy_and_protobuf_have_stable_public_errors() {
        let limits = limits();
        let err = snap::raw::decompress_len(&[0xff, 0xff, 0xff])
            .map_err(|_| limits.malformed("prometheus", "invalid Snappy ingest payload"))
            .unwrap_err();
        assert_eq!(
            err,
            (
                StatusCode::BAD_REQUEST,
                "invalid Snappy ingest payload".into()
            )
        );

        let err = WriteRequest::decode([0xff, 0xff, 0xff].as_slice())
            .map_err(|_| limits.malformed("prometheus", "invalid Prometheus remote-write payload"))
            .unwrap_err();
        assert_eq!(
            err,
            (
                StatusCode::BAD_REQUEST,
                "invalid Prometheus remote-write payload".into()
            )
        );
    }

    #[test]
    fn snappy_claimed_output_is_rejected_before_decompression() {
        let limits = limits();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&vec![0_u8; 4097])
            .unwrap();
        assert!(compressed.len() < limits.max_compressed_bytes);
        let error = decode_write_request(&limits, true, &compressed).unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            error.1,
            "decompressed ingest payload exceeds configured limit"
        );
    }

    #[test]
    fn labels_copied_into_every_sample_are_bounded() {
        let limits = limits();
        let request = |value: &str| WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "m".into(),
                    },
                    Label {
                        name: "a".into(),
                        value: value.into(),
                    },
                ],
                samples: vec![Sample::default(); 70],
            }],
            metadata: Vec::new(),
        };
        // 65 label bytes x 70 samples exceeds the 4096-byte test limit while
        // every per-series limit passes.
        let wide = request(&"v".repeat(64));
        validate_write_request(&limits, &wide).unwrap();
        assert_eq!(
            build_rows(&limits, "t", &wide).unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(build_rows(&limits, "t", &request("v")).unwrap().len(), 70);
    }

    #[test]
    fn entity_limit_runs_before_the_protobuf_decode() {
        let limits = limits();
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: Vec::new(),
                samples: vec![Sample::default(); 100],
            }],
            metadata: Vec::new(),
        };
        let body = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        let error = decode_write_request(&limits, true, &body).unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn body_is_decoded_into_rows_in_one_blocking_step() {
        let limits = limits();
        let label = |name: &str, value: &str| Label {
            name: name.into(),
            value: value.into(),
        };
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("__name__", "up"),
                    label("job", "api"),
                    label("a", "b"),
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1_764_000_000_000,
                }],
            }],
            metadata: Vec::new(),
        };
        let raw = request.encode_to_vec();
        let body = snap::raw::Encoder::new().compress_vec(&raw).unwrap();
        let decoded = decode_rows(&limits, "t", true, &body).unwrap().unwrap();
        assert_eq!((decoded.series_count, decoded.sample_count), (1, 1));
        assert_eq!(decoded.decompressed_len, raw.len());
        let row = &decoded.rows[0];
        assert_eq!((&*row.metric_name, &*row.service_name), ("up", "api"));
        assert_eq!(row.attributes, vec![("a".to_string(), "b".to_string())]);
        assert_eq!(row.time_unix, 1_764_000_000_000_000_000);

        let empty = snap::raw::Encoder::new()
            .compress_vec(&WriteRequest::default().encode_to_vec())
            .unwrap();
        assert!(decode_rows(&limits, "t", true, &empty).unwrap().is_none());
    }

    proptest! {
        #[test]
        fn arbitrary_snappy_and_protobuf_never_panic_or_leak_decoder_details(
            body in proptest::collection::vec(any::<u8>(), 0..2048),
            declared_snappy in any::<bool>(),
        ) {
            let limits = limits();
            let result = limits
                .check_compressed("prometheus", body.len())
                .and_then(|_| decode_write_request(&limits, declared_snappy, &body))
                .and_then(|(request, len)| {
                    validate_write_request(&limits, &request).map(|_| (request, len))
                });
            if let Err((status, message)) = result {
                prop_assert!(matches!(status, StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE));
                prop_assert!(!message.contains("prost"));
                prop_assert!(!message.contains("snappy::"));
                prop_assert!(!message.contains("failed to"));
            }
        }
    }
}
