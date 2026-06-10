use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
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

/// Maximum decompressed body size: 32 MB.  Prevents decompression bombs where a
/// small compressed payload expands to gigabytes, exhausting server memory.
const MAX_DECOMPRESSED_BYTES: u64 = 32 * 1024 * 1024;

/// Decompress body based on Content-Encoding header (gzip, deflate, zstd, or identity).
/// Compressed bodies are inflated on the blocking pool — decompression is
/// synchronous CPU work that would otherwise stall a tokio worker for the
/// duration (tens to hundreds of ms on large agent payloads).
pub async fn decompress_body(headers: &HeaderMap, body: Bytes) -> Result<Vec<u8>, (StatusCode, String)> {
    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if encoding.contains("gzip") || encoding.contains("deflate") || encoding.contains("zstd") || encoding.contains("zstandard") {
        tokio::task::spawn_blocking(move || decompress_body_sync(&encoding, body))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("decompress task failed: {e}")))?
    } else {
        Ok(body.to_vec())
    }
}

fn decompress_body_sync(encoding: &str, body: Bytes) -> Result<Vec<u8>, (StatusCode, String)> {
    if encoding.contains("gzip") {
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(body.as_ref());
        let mut out = Vec::new();
        decoder.take(MAX_DECOMPRESSED_BYTES).read_to_end(&mut out).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("gzip decompression failed: {e}"))
        })?;
        if out.len() as u64 >= MAX_DECOMPRESSED_BYTES {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "decompressed body exceeds 32 MB limit".into()));
        }
        Ok(out)
    } else if encoding.contains("deflate") {
        use std::io::Read;
        let decoder = flate2::read::DeflateDecoder::new(body.as_ref());
        let mut out = Vec::new();
        decoder.take(MAX_DECOMPRESSED_BYTES).read_to_end(&mut out).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("deflate decompression failed: {e}"))
        })?;
        if out.len() as u64 >= MAX_DECOMPRESSED_BYTES {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "decompressed body exceeds 32 MB limit".into()));
        }
        Ok(out)
    } else {
        // zstd / zstandard (only reachable for these encodings via the async wrapper)
        let out = zstd::decode_all(body.as_ref()).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("zstd decompression failed: {e}"))
        })?;
        if out.len() as u64 > MAX_DECOMPRESSED_BYTES {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "decompressed body exceeds 32 MB limit".into()));
        }
        Ok(out)
    }
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
