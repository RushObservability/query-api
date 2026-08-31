use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;

/// Extract and validate the DD-API-KEY header.
/// Phase 1: accept any non-empty key.
pub fn validate_api_key(headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    let key = headers
        .get("DD-API-KEY")
        .or_else(|| headers.get("dd-api-key"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if key.is_empty() {
        return Err((StatusCode::FORBIDDEN, "missing or empty DD-API-KEY".into()));
    }
    Ok(())
}

/// Decompress body based on Content-Encoding header (gzip, deflate, zstd, or identity).
/// Compressed bodies are inflated on the blocking pool — decompression is
/// synchronous CPU work that would otherwise stall a tokio worker for the
/// duration (tens to hundreds of ms on large agent payloads).
pub async fn decompress_body(
    limits: &crate::ingest_limits::IngestLimits,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, (StatusCode, String)> {
    limits.check_body("datadog", &body)?;
    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if encoding.contains("gzip")
        || encoding.contains("deflate")
        || encoding.contains("zstd")
        || encoding.contains("zstandard")
    {
        let permit = limits.acquire_decode("datadog").await?;
        let limits = limits.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            decompress_body_sync(&limits, &encoding, body)
        })
        .await
        .map_err(|e| crate::api_error::internal_legacy("datadog.decompress_task", e))?
    } else {
        limits.check_decompressed("datadog", body.len())?;
        Ok(body.to_vec())
    }
}

fn decompress_body_sync(
    limits: &crate::ingest_limits::IngestLimits,
    encoding: &str,
    body: Bytes,
) -> Result<Vec<u8>, (StatusCode, String)> {
    if encoding.contains("gzip") {
        let decoder = flate2::read::GzDecoder::new(body.as_ref());
        read_capped(limits, decoder)
    } else if encoding.contains("deflate") {
        let decoder = flate2::read::DeflateDecoder::new(body.as_ref());
        read_capped(limits, decoder)
    } else {
        // zstd / zstandard (only reachable for these encodings via the async wrapper)
        let decoder = zstd::stream::read::Decoder::new(body.as_ref())
            .map_err(|_| limits.malformed("datadog", "invalid compressed Datadog payload"))?;
        read_capped(limits, decoder)
    }
}

fn read_capped<R: std::io::Read>(
    limits: &crate::ingest_limits::IngestLimits,
    reader: R,
) -> Result<Vec<u8>, (StatusCode, String)> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(limits.max_decompressed_bytes as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| limits.malformed("datadog", "invalid compressed Datadog payload"))?;
    limits.check_decompressed("datadog", out.len())?;
    Ok(out)
}

/// Parse Datadog tags ("key:value" strings) into key-value pairs.
/// Tags without a colon are stored as key="" pairs.
pub fn parse_dd_tags(tags: &str) -> Vec<(String, String)> {
    if tags.is_empty() {
        return Vec::new();
    }
    tags.split(',')
        .filter(|t| !t.is_empty())
        .map(|t| {
            if let Some((k, v)) = t.split_once(':') {
                (k.trim().to_string(), v.trim().to_string())
            } else {
                (t.trim().to_string(), String::new())
            }
        })
        .collect()
}

/// Map Datadog severity/status string to OTEL SeverityNumber.
pub fn dd_status_to_severity(status: &str) -> (String, u8) {
    match status.to_lowercase().as_str() {
        "debug" | "trace" => ("DEBUG".into(), 5),
        "info" | "notice" => ("INFO".into(), 9),
        "warn" | "warning" => ("WARN".into(), 13),
        "error" | "err" => ("ERROR".into(), 17),
        "critical" | "fatal" | "emergency" | "alert" => ("FATAL".into(), 21),
        _ => ("INFO".into(), 9),
    }
}

/// POST /datadog/api/v1/validate — API key validation endpoint.
/// The DD agent calls this on startup to verify the key is valid.
pub async fn validate(
    _state: State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_api_key(&headers)?;
    Ok(Json(serde_json::json!({ "valid": true })))
}

/// Catch-all stub for metadata endpoints the agent calls but we don't need.
pub async fn stub_ok() -> impl IntoResponse {
    Json(serde_json::json!({}))
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
    fn datadog_gzip_bomb_is_bounded_and_returns_413() {
        let limits = limits();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&vec![0_u8; 4097]).unwrap();
        let compressed = Bytes::from(encoder.finish().unwrap());
        let error = decompress_body_sync(&limits, "gzip", compressed).unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn malformed_datadog_compression_error_is_stable() {
        let error =
            decompress_body_sync(&limits(), "gzip", Bytes::from_static(b"bad")).unwrap_err();
        assert_eq!(
            error,
            (
                StatusCode::BAD_REQUEST,
                "invalid compressed Datadog payload".into()
            )
        );
    }
}
