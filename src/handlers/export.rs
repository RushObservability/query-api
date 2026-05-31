//! Shared helpers for exporting query results (logs/spans) as CSV or JSON.
//!
//! The interactive query endpoints stay capped at 1000 rows; exports use the
//! admin-configurable `export_max_rows` setting (default 1000) instead.

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;

pub const DEFAULT_EXPORT_MAX_ROWS: u64 = 1000;
pub const EXPORT_MAX_ROWS_CEILING: u64 = 1_000_000;

/// Export output format, parsed from the request body `{ "format": "csv" | "json" }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Csv,
    Json,
}

impl Default for ExportFormat {
    fn default() -> Self {
        ExportFormat::Csv
    }
}

/// Read the configured max export row count (clamped to a sane ceiling).
pub async fn read_export_max_rows(state: &AppState) -> u64 {
    state
        .config_db
        .get_setting("export_max_rows")
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_EXPORT_MAX_ROWS)
        .min(EXPORT_MAX_ROWS_CEILING)
}

/// Resolve the effective row limit for an export request given the configured cap.
/// A missing/zero requested limit means "use the cap".
pub fn effective_limit(requested: u64, cap: u64) -> u64 {
    if requested == 0 { cap } else { requested.min(cap) }
}

/// Escape a single CSV field (RFC 4180): quote if it contains comma/quote/newline.
pub fn csv_field(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Format a ClickHouse DateTime64(9) value (nanoseconds since epoch) as RFC3339.
pub fn ts_rfc3339(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsub = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsub)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

/// Build a file-download response with the right content-type + attachment filename.
pub fn file_response(body: String, content_type: &'static str, filename: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(cd) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, cd);
    }
    (StatusCode::OK, headers, body).into_response()
}

/// Leading `#`-comment lines describing the exported query, for CSV files.
pub fn csv_query_preamble(signal: &str, from: &str, to: &str, search: Option<&str>, query_text: Option<&str>) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Rush export — signal: {signal}\n"));
    out.push_str(&format!("# time range: {from} .. {to}\n"));
    if let Some(q) = query_text {
        if !q.is_empty() {
            out.push_str(&format!("# query: {}\n", q.replace('\n', " ")));
        }
    }
    if let Some(s) = search {
        if !s.is_empty() {
            out.push_str(&format!("# search: {}\n", s.replace('\n', " ")));
        }
    }
    out.push_str(&format!("# exported_at: {}\n", chrono::Utc::now().to_rfc3339()));
    out
}
