//! Shared resource limits for telemetry ingestion.
//!
//! Keep these limits process-wide and startup validated. Ingest handlers use the
//! same compressed/decompressed byte ceilings, entity budgets, decode semaphore,
//! stable public errors, and low-cardinality rejection metrics.

use axum::http::StatusCode;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::self_metrics::SelfMetrics;

pub type IngestError = (StatusCode, String);

const MIB: usize = 1024 * 1024;

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
        let decode_concurrency = env_limit("RUSH_INGEST_DECODE_CONCURRENCY", 4, 1, 1024)?;

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

    pub fn malformed(&self, source: &'static str, message: &'static str) -> IngestError {
        self.reject(source, "malformed", StatusCode::BAD_REQUEST, message)
    }

    pub async fn acquire_decode(
        &self,
        source: &'static str,
    ) -> Result<OwnedSemaphorePermit, IngestError> {
        self.decode.clone().try_acquire_owned().map_err(|_| {
            self.reject(
                source,
                "decode_concurrency",
                StatusCode::TOO_MANY_REQUESTS,
                "ingest decoder is at capacity; retry later",
            )
        })
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

    #[tokio::test]
    async fn decode_concurrency_fails_fast_and_is_measured() {
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
