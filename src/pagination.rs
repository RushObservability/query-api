//! Authenticated, query-bound cursors for telemetry pagination.
//!
//! Cursors are intentionally opaque. Their payload identifies the last emitted
//! row, while the HMAC binds it to the tenant, signal, time range, filters,
//! search text, and projection that produced it. A cursor therefore cannot be
//! edited or replayed against a broader/different query.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::clickhouse_config::ConfigDb;
use crate::models::query::{Filter, TimeRange};

type HmacSha256 = Hmac<Sha256>;
const CURSOR_VERSION: u8 = 1;
const CURSOR_DOMAIN: &[u8] = b"rush-query-cursor-v1\0";
const MAX_TOKEN_BYTES: usize = 4096;
const MAX_TIE_FIELDS: usize = 8;
const MAX_TIE_FIELD_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPosition {
    pub timestamp_ns: i64,
    pub tie: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorPayload {
    version: u8,
    signal: String,
    scope: String,
    position: CursorPosition,
}

#[derive(Debug, Serialize)]
struct CursorScope<'a> {
    tenant_id: &'a str,
    signal: &'a str,
    projection: &'a str,
    time_range: &'a TimeRange,
    filters: &'a [Filter],
    search: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    Invalid,
    ScopeMismatch,
}

/// Stable digest of everything that is allowed to influence the page's row
/// membership. Limit is deliberately excluded so clients may resize pages.
pub fn query_scope(
    tenant_id: &str,
    signal: &str,
    projection: &str,
    time_range: &TimeRange,
    filters: &[Filter],
    search: Option<&str>,
) -> String {
    let encoded = serde_json::to_vec(&CursorScope {
        tenant_id,
        signal,
        projection,
        time_range,
        filters,
        search,
    })
    .expect("cursor scope contains serializable request fields");
    hex::encode(Sha256::digest(encoded))
}

pub fn encode(config_db: &ConfigDb, signal: &str, scope: &str, position: CursorPosition) -> String {
    encode_with_secret(config_db.cursor_hmac_secret(), signal, scope, position)
}

pub fn decode(
    config_db: &ConfigDb,
    token: &str,
    expected_signal: &str,
    expected_scope: &str,
) -> Result<CursorPosition, CursorError> {
    decode_with_secret(
        config_db.cursor_hmac_secret(),
        token,
        expected_signal,
        expected_scope,
    )
}

fn encode_with_secret(
    secret: &[u8],
    signal: &str,
    scope: &str,
    position: CursorPosition,
) -> String {
    let payload = CursorPayload {
        version: CURSOR_VERSION,
        signal: signal.to_string(),
        scope: scope.to_string(),
        position,
    };
    let bytes = serde_json::to_vec(&payload).expect("cursor payload is serializable");
    let signature = signature(secret, &bytes);
    format!(
        "v{CURSOR_VERSION}.{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(signature),
    )
}

fn decode_with_secret(
    secret: &[u8],
    token: &str,
    expected_signal: &str,
    expected_scope: &str,
) -> Result<CursorPosition, CursorError> {
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
        return Err(CursorError::Invalid);
    }
    let mut parts = token.split('.');
    if parts.next() != Some("v1") {
        return Err(CursorError::Invalid);
    }
    let encoded_payload = parts.next().ok_or(CursorError::Invalid)?;
    let encoded_signature = parts.next().ok_or(CursorError::Invalid)?;
    if parts.next().is_some() {
        return Err(CursorError::Invalid);
    }
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| CursorError::Invalid)?;
    let supplied_signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| CursorError::Invalid)?;
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(CURSOR_DOMAIN);
    mac.update(&payload_bytes);
    mac.verify_slice(&supplied_signature)
        .map_err(|_| CursorError::Invalid)?;

    let payload: CursorPayload =
        serde_json::from_slice(&payload_bytes).map_err(|_| CursorError::Invalid)?;
    if payload.version != CURSOR_VERSION
        || payload.position.tie.len() > MAX_TIE_FIELDS
        || payload
            .position
            .tie
            .iter()
            .any(|value| value.len() > MAX_TIE_FIELD_BYTES || value.chars().any(char::is_control))
    {
        return Err(CursorError::Invalid);
    }
    if payload.signal != expected_signal || payload.scope != expected_scope {
        return Err(CursorError::ScopeMismatch);
    }
    Ok(payload.position)
}

fn signature(secret: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(CURSOR_DOMAIN);
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-only-pagination-secret-at-least-32-bytes";

    fn position() -> CursorPosition {
        CursorPosition {
            timestamp_ns: 1_749_600_000_123_456_789,
            tie: vec!["gateway".into(), "deadbeef".into()],
        }
    }

    #[test]
    fn cursor_round_trips_and_preserves_duplicate_timestamp_tie_breakers() {
        let token = encode_with_secret(SECRET, "logs", "scope-a", position());
        let decoded = decode_with_secret(SECRET, &token, "logs", "scope-a").unwrap();
        assert_eq!(decoded, position());
    }

    #[test]
    fn cursor_tampering_is_rejected() {
        let mut token = encode_with_secret(SECRET, "spans", "scope-a", position()).into_bytes();
        let index = token.len() / 2;
        token[index] = if token[index] == b'A' { b'B' } else { b'A' };
        let token = String::from_utf8(token).unwrap();
        assert_eq!(
            decode_with_secret(SECRET, &token, "spans", "scope-a"),
            Err(CursorError::Invalid)
        );
    }

    #[test]
    fn tenant_filter_and_time_scope_changes_are_rejected() {
        let range = TimeRange {
            from: "2026-08-10T00:00:00Z".into(),
            to: "2026-08-10T01:00:00Z".into(),
        };
        let filters = vec![Filter {
            field: "service_name".into(),
            op: crate::models::query::FilterOp::Eq,
            value: serde_json::json!("gateway"),
        }];
        let original = query_scope("tenant-a", "logs", "slim", &range, &filters, Some("POST"));
        let token = encode_with_secret(SECRET, "logs", &original, position());

        let other_tenant = query_scope("tenant-b", "logs", "slim", &range, &filters, Some("POST"));
        let other_filter = query_scope(
            "tenant-a",
            "logs",
            "slim",
            &range,
            &[Filter {
                field: "service_name".into(),
                op: crate::models::query::FilterOp::Eq,
                value: serde_json::json!("payments"),
            }],
            Some("POST"),
        );
        let other_time = query_scope(
            "tenant-a",
            "logs",
            "slim",
            &TimeRange {
                from: range.from.clone(),
                to: "2026-08-10T02:00:00Z".into(),
            },
            &filters,
            Some("POST"),
        );
        for changed_scope in [other_tenant, other_filter, other_time] {
            assert_eq!(
                decode_with_secret(SECRET, &token, "logs", &changed_scope),
                Err(CursorError::ScopeMismatch)
            );
        }
        assert_eq!(
            decode_with_secret(SECRET, &token, "spans", &original),
            Err(CursorError::ScopeMismatch)
        );
    }

    #[test]
    fn oversized_and_malformed_tokens_are_rejected() {
        assert_eq!(
            decode_with_secret(SECRET, &"x".repeat(MAX_TOKEN_BYTES + 1), "logs", "scope"),
            Err(CursorError::Invalid)
        );
        assert_eq!(
            decode_with_secret(SECRET, "v1.not-base64.nope", "logs", "scope"),
            Err(CursorError::Invalid)
        );
    }
}
