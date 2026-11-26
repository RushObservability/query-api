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

/// Decompress the body (gzip, deflate, zstd or identity) and hand it to
/// `parse`, all on the blocking pool under a decode permit. Inflating, JSON or
/// msgpack parsing and row building are all synchronous CPU work that would
/// otherwise stall a tokio worker on large agent payloads.
pub async fn decode_body<T, F>(
    limits: &crate::ingest_limits::IngestLimits,
    headers: &HeaderMap,
    body: Bytes,
    parse: F,
) -> Result<T, (StatusCode, String)>
where
    T: Send + 'static,
    F: FnOnce(&crate::ingest_limits::IngestLimits, Vec<u8>) -> Result<T, (StatusCode, String)>
        + Send
        + 'static,
{
    limits.check_body("datadog", &body)?;
    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    limits
        .spawn_decode("datadog", move |limits| {
            let raw = if is_compressed_encoding(&encoding) {
                decompress_body_sync(
                    limits,
                    "datadog",
                    "invalid compressed Datadog payload",
                    &encoding,
                    &body,
                )?
            } else {
                limits.check_decompressed("datadog", body.len())?;
                body.to_vec()
            };
            parse(limits, raw)
        })
        .await
}

/// Content-Encoding values `decompress_body_sync` can inflate.
pub(crate) fn is_compressed_encoding(encoding: &str) -> bool {
    encoding.contains("gzip")
        || encoding.contains("deflate")
        || encoding.contains("zstd")
        || encoding.contains("zstandard")
}

/// Inflate a gzip, deflate or zstd body, capped at the decompressed-bytes limit.
/// Only call it when `is_compressed_encoding(encoding)` is true.
pub(crate) fn decompress_body_sync(
    limits: &crate::ingest_limits::IngestLimits,
    source: &'static str,
    invalid: &'static str,
    encoding: &str,
    body: &[u8],
) -> Result<Vec<u8>, (StatusCode, String)> {
    if encoding.contains("gzip") {
        let decoder = flate2::read::GzDecoder::new(body);
        read_capped(limits, source, invalid, decoder)
    } else if encoding.contains("deflate") {
        let decoder = flate2::read::DeflateDecoder::new(body);
        read_capped(limits, source, invalid, decoder)
    } else {
        // zstd / zstandard
        let decoder = zstd::stream::read::Decoder::new(body)
            .map_err(|_| limits.malformed(source, invalid))?;
        read_capped(limits, source, invalid, decoder)
    }
}

fn read_capped<R: std::io::Read>(
    limits: &crate::ingest_limits::IngestLimits,
    source: &'static str,
    invalid: &'static str,
    reader: R,
) -> Result<Vec<u8>, (StatusCode, String)> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(limits.max_decompressed_bytes as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| limits.malformed(source, invalid))?;
    limits.check_decompressed(source, out.len())?;
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

    fn dd_sync(
        limits: &crate::ingest_limits::IngestLimits,
        encoding: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, (StatusCode, String)> {
        decompress_body_sync(
            limits,
            "datadog",
            "invalid compressed Datadog payload",
            encoding,
            body,
        )
    }

    #[test]
    fn datadog_gzip_bomb_is_bounded_and_returns_413() {
        let limits = limits();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&vec![0_u8; 4097]).unwrap();
        let compressed = encoder.finish().unwrap();
        let error = dd_sync(&limits, "gzip", &compressed).unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn malformed_datadog_compression_error_is_stable() {
        let error = dd_sync(&limits(), "gzip", b"bad").unwrap_err();
        assert_eq!(
            error,
            (
                StatusCode::BAD_REQUEST,
                "invalid compressed Datadog payload".into()
            )
        );
    }

    #[tokio::test]
    async fn datadog_zstd_encodings_roundtrip() {
        let payload = br#"{"message":"zstd test","service":"api"}"#;
        let compressed = Bytes::from(zstd::encode_all(payload.as_slice(), 1).unwrap());
        for encoding in ["zstd", "zstandard"] {
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", encoding.parse().unwrap());
            let decoded = decode_body(&limits(), &headers, compressed.clone(), |_, raw| Ok(raw))
                .await
                .unwrap();
            assert_eq!(decoded, payload);
        }
    }

    #[tokio::test]
    async fn uncompressed_datadog_body_takes_a_decode_permit() {
        let limits = limits();
        let _held = limits.acquire_decode("datadog").await.unwrap();
        let error = decode_body(
            &limits,
            &HeaderMap::new(),
            Bytes::from_static(b"[]"),
            |_, raw| Ok(raw),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn datadog_zstd_bomb_is_bounded_and_returns_413() {
        let compressed = zstd::encode_all(vec![0_u8; 4097].as_slice(), 1).unwrap();
        let error = dd_sync(&limits(), "zstd", &compressed).unwrap_err();
        assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn malformed_and_truncated_datadog_zstd_errors_are_stable() {
        let mut truncated = zstd::encode_all(b"test payload".as_slice(), 1).unwrap();
        truncated.pop();
        for body in [b"bad".to_vec(), truncated] {
            let error = dd_sync(&limits(), "zstd", &body).unwrap_err();
            assert_eq!(
                error,
                (
                    StatusCode::BAD_REQUEST,
                    "invalid compressed Datadog payload".into()
                )
            );
        }
    }
}
