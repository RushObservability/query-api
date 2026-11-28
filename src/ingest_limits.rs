//! Shared resource limits for telemetry ingestion.
//!
//! Keep these limits process-wide and startup validated. Ingest handlers use the
//! same compressed/decompressed byte ceilings, entity budgets, decode semaphore,
//! stable public errors, and low-cardinality rejection metrics.

use axum::http::StatusCode;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::self_metrics::SelfMetrics;

pub type IngestError = (StatusCode, String);

const MIB: usize = 1024 * 1024;

/// How long a request waits for a decode slot before it gets a retryable 429.
const DECODE_WAIT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct IngestLimits {
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_entities: usize,
    pub max_series: usize,
    pub max_samples: usize,
    pub max_metadata: usize,
    pub max_labels_per_series: usize,
    pub max_label_name_bytes: usize,
    pub max_label_value_bytes: usize,
    decode: Arc<Semaphore>,
    decode_wait: Duration,
    metrics: Arc<SelfMetrics>,
}

impl IngestLimits {
    pub fn from_env(metrics: Arc<SelfMetrics>) -> Result<Self, String> {
        let max_compressed_bytes = env_limit(
            "RUSH_INGEST_MAX_COMPRESSED_BYTES",
            8 * MIB,
            64 * 1024,
            256 * MIB,
        )?;
        let max_decompressed_bytes = env_limit(
            "RUSH_INGEST_MAX_DECOMPRESSED_BYTES",
            32 * MIB,
            64 * 1024,
            1024 * MIB,
        )?;
        let max_entities = env_limit("RUSH_INGEST_MAX_ENTITIES", 200_000, 1, 10_000_000)?;
        let max_series = env_limit("RUSH_INGEST_MAX_SERIES", 20_000, 1, 1_000_000)?;
        let max_samples = env_limit("RUSH_INGEST_MAX_SAMPLES", 200_000, 1, 10_000_000)?;
        let max_metadata = env_limit("RUSH_INGEST_MAX_METADATA", 10_000, 1, 1_000_000)?;
        let max_labels_per_series = env_limit("RUSH_INGEST_MAX_LABELS_PER_SERIES", 128, 1, 10_000)?;
        let max_label_name_bytes =
            env_limit("RUSH_INGEST_MAX_LABEL_NAME_BYTES", 256, 1, 64 * 1024)?;
        let max_label_value_bytes =
            env_limit("RUSH_INGEST_MAX_LABEL_VALUE_BYTES", 4096, 1, 1024 * 1024)?;
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        let decode_concurrency =
            env_limit("RUSH_INGEST_DECODE_CONCURRENCY", cores.min(1024), 1, 1024)?;

        Ok(Self {
            max_compressed_bytes,
            max_decompressed_bytes,
            max_entities,
            max_series,
            max_samples,
            max_metadata,
            max_labels_per_series,
            max_label_name_bytes,
            max_label_value_bytes,
            decode: Arc::new(Semaphore::new(decode_concurrency)),
            decode_wait: DECODE_WAIT,
            metrics,
        })
    }

    #[cfg(test)]
    pub fn for_test(metrics: Arc<SelfMetrics>) -> Self {
        Self {
            max_compressed_bytes: 1024,
            max_decompressed_bytes: 4096,
            max_entities: 100,
            max_series: 10,
            max_samples: 100,
            max_metadata: 10,
            max_labels_per_series: 8,
            max_label_name_bytes: 32,
            max_label_value_bytes: 64,
            decode: Arc::new(Semaphore::new(1)),
            decode_wait: Duration::from_millis(50),
            metrics,
        }
    }

    pub fn check_compressed(&self, source: &'static str, bytes: usize) -> Result<(), IngestError> {
        if bytes > self.max_compressed_bytes {
            return Err(self.reject(
                source,
                "compressed_bytes",
                StatusCode::PAYLOAD_TOO_LARGE,
                "compressed ingest payload exceeds configured limit",
            ));
        }
        Ok(())
    }

    pub fn check_body(&self, source: &'static str, body: &Bytes) -> Result<(), IngestError> {
        self.check_compressed(source, body.len())
    }

    pub fn check_decompressed(
        &self,
        source: &'static str,
        bytes: usize,
    ) -> Result<(), IngestError> {
        if bytes > self.max_decompressed_bytes {
            return Err(self.reject(
                source,
                "decompressed_bytes",
                StatusCode::PAYLOAD_TOO_LARGE,
                "decompressed ingest payload exceeds configured limit",
            ));
        }
        Ok(())
    }

    pub fn check_entities(&self, source: &'static str, entities: usize) -> Result<(), IngestError> {
        if entities > self.max_entities {
            return Err(self.reject(
                source,
                "entity_count",
                StatusCode::PAYLOAD_TOO_LARGE,
                "ingest payload contains too many entities",
            ));
        }
        Ok(())
    }

    pub fn check_count(
        &self,
        source: &'static str,
        count: usize,
        max: usize,
    ) -> Result<(), IngestError> {
        if count > max {
            return Err(self.reject(
                source,
                "entity_count",
                StatusCode::PAYLOAD_TOO_LARGE,
                "ingest payload contains too many entities",
            ));
        }
        Ok(())
    }

    pub fn check_label(
        &self,
        source: &'static str,
        name: &str,
        value: &str,
    ) -> Result<(), IngestError> {
        if name.len() > self.max_label_name_bytes || value.len() > self.max_label_value_bytes {
            return Err(self.reject(
                source,
                "entity_count",
                StatusCode::PAYLOAD_TOO_LARGE,
                "ingest label exceeds configured limit",
            ));
        }
        Ok(())
    }

    /// Count `shared_bytes` that row building copies into each of `rows` rows
    /// (a service name, shared tags or labels) against the decompressed-bytes
    /// limit. The body limits see such a value once, so without this a small
    /// request with one long shared value and many empty records expands to
    /// many times the limit in memory.
    pub fn check_fan_out(
        &self,
        source: &'static str,
        total: &mut usize,
        shared_bytes: usize,
        rows: usize,
    ) -> Result<(), IngestError> {
        *total = total.saturating_add(shared_bytes.saturating_mul(rows));
        self.check_decompressed(source, *total)
    }

    /// Run the entity limit on a protobuf body before prost decodes it. An
    /// empty record is 2 bytes on the wire but a full struct in memory, so
    /// checking after the decode let a small body allocate gigabytes first.
    pub fn check_proto_entities(
        &self,
        source: &'static str,
        malformed: &'static str,
        body: &[u8],
        schema: &[ProtoField],
    ) -> Result<(), IngestError> {
        let count =
            count_proto_entities(body, schema).ok_or_else(|| self.malformed(source, malformed))?;
        self.check_entities(source, count)
    }

    /// Run CPU-heavy ingest work (decompress, parse, build rows) on the
    /// blocking pool while holding a decode permit, so it never stalls a tokio
    /// worker and stays within the process-wide decode budget.
    pub async fn spawn_decode<T, F>(&self, source: &'static str, work: F) -> Result<T, IngestError>
    where
        T: Send + 'static,
        F: FnOnce(&IngestLimits) -> Result<T, IngestError> + Send + 'static,
    {
        let permit = self.acquire_decode(source).await?;
        let limits = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&limits)
        })
        .await
        .map_err(|e| crate::api_error::internal_legacy("ingest.decode_task", e))?
    }

    pub fn malformed(&self, source: &'static str, message: &'static str) -> IngestError {
        self.reject(source, "malformed", StatusCode::BAD_REQUEST, message)
    }

    pub async fn acquire_decode(
        &self,
        source: &'static str,
    ) -> Result<OwnedSemaphorePermit, IngestError> {
        match tokio::time::timeout(self.decode_wait, self.decode.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(self.reject(
                source,
                "decode_concurrency",
                StatusCode::TOO_MANY_REQUESTS,
                "ingest decoder is at capacity; retry later",
            )),
        }
    }

    pub fn record_rejection(&self, source: &'static str, reason: &'static str) {
        self.metrics.inc_counter(
            "rush_ingest_limit_rejections_total",
            &[
                ("source", bounded_source(source)),
                ("reason", bounded_reason(reason)),
            ],
            1,
        );
    }

    fn reject(
        &self,
        source: &'static str,
        reason: &'static str,
        status: StatusCode,
        message: &'static str,
    ) -> IngestError {
        self.record_rejection(source, reason);
        (status, message.to_string())
    }
}

/// A message field followed when counting protobuf entities.
pub struct ProtoField {
    pub number: u32,
    /// Whether each occurrence counts toward the entity limit.
    pub counted: bool,
    pub children: &'static [ProtoField],
}

/// A message field whose every occurrence is one entity.
pub const fn counted(number: u32, children: &'static [ProtoField]) -> ProtoField {
    ProtoField {
        number,
        counted: true,
        children,
    }
}

/// A message field that is only walked into.
pub const fn nested(number: u32, children: &'static [ProtoField]) -> ProtoField {
    ProtoField {
        number,
        counted: false,
        children,
    }
}

/// Count occurrences of the `counted` fields reachable through `schema`,
/// reading only tags and lengths (no allocation). `None` when the bytes are
/// not well-formed protobuf; groups are rejected (no OTLP, Prometheus or
/// Datadog message uses them).
pub fn count_proto_entities(mut buf: &[u8], schema: &[ProtoField]) -> Option<usize> {
    let mut count = 0usize;
    while !buf.is_empty() {
        let key = prost::encoding::decode_varint(&mut buf).ok()?;
        match key & 7 {
            0 => {
                prost::encoding::decode_varint(&mut buf).ok()?;
            }
            1 => buf = buf.get(8..)?,
            5 => buf = buf.get(4..)?,
            2 => {
                let len = usize::try_from(prost::encoding::decode_varint(&mut buf).ok()?).ok()?;
                let field = buf.get(..len)?;
                buf = &buf[len..];
                if let Some(f) = schema.iter().find(|f| u64::from(f.number) == key >> 3) {
                    count = count
                        .checked_add(usize::from(f.counted))?
                        .checked_add(count_proto_entities(field, f.children)?)?;
                }
            }
            _ => return None,
        }
    }
    Some(count)
}

/// Bytes of an attribute list's keys and values.
pub fn attr_bytes(attrs: &[(String, String)]) -> usize {
    attrs.iter().map(|(k, v)| k.len() + v.len()).sum()
}

fn env_limit(name: &str, default: usize, min: usize, max: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|value| (*value >= min) && (*value <= max))
            .ok_or_else(|| format!("{name} must be an integer between {min} and {max}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must be valid UTF-8")),
    }
}

fn bounded_source(source: &'static str) -> &'static str {
    match source {
        "prometheus" | "otlp" | "datadog" | "cloudwatch" | "rum" => source,
        _ => "other",
    }
}

fn bounded_reason(reason: &'static str) -> &'static str {
    match reason {
        "compressed_bytes" | "decompressed_bytes" | "entity_count" | "decode_concurrency"
        | "malformed" => reason,
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_and_entity_rejections_are_stable_and_measured() {
        let metrics = Arc::new(SelfMetrics::new());
        let limits = IngestLimits::for_test(metrics.clone());

        assert_eq!(
            limits.check_compressed("prometheus", 1025).unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            limits.check_decompressed("otlp", 4097).unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            limits.check_entities("datadog", 101).unwrap_err().0,
            StatusCode::PAYLOAD_TOO_LARGE
        );

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("reason=\"compressed_bytes\""));
        assert!(rendered.contains("reason=\"decompressed_bytes\""));
        assert!(rendered.contains("reason=\"entity_count\""));
    }

    #[test]
    fn proto_entities_are_counted_without_decoding() {
        // {1: {2: x, 2: y, 3: "s"}, 1: {}, 9: 7} counting field 1 and its field 2.
        const SCHEMA: &[ProtoField] = &[counted(1, &[counted(2, &[])])];
        let body = [
            0x0a, 0x07, 0x12, 0x00, 0x12, 0x00, 0x1a, 0x01, b's', 0x0a, 0x00, 0x48, 0x07,
        ];
        assert_eq!(count_proto_entities(&body, SCHEMA), Some(4));
        assert_eq!(count_proto_entities(&body, &[nested(1, &[])]), Some(0));
        assert_eq!(count_proto_entities(&body[..3], SCHEMA), None);
        assert_eq!(count_proto_entities(&[0x0b], SCHEMA), None);
    }

    #[tokio::test]
    async fn decode_waits_briefly_for_a_free_slot() {
        let limits = IngestLimits::for_test(Arc::new(SelfMetrics::new()));
        let permit = limits.acquire_decode("otlp").await.unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(permit);
        });
        assert!(limits.acquire_decode("datadog").await.is_ok());
    }

    #[tokio::test]
    async fn decode_concurrency_times_out_and_is_measured() {
        let metrics = Arc::new(SelfMetrics::new());
        let limits = IngestLimits::for_test(metrics.clone());
        let _permit = limits.acquire_decode("otlp").await.unwrap();
        let error = limits.acquire_decode("otlp").await.unwrap_err();
        assert_eq!(error.0, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            metrics
                .render_prometheus()
                .contains("reason=\"decode_concurrency\"")
        );
    }
}
