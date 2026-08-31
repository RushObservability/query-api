//! Bounded, unauthenticated CSP violation telemetry.
//!
//! Browsers can report violations from login and SSO pages before a Rush
//! session exists. We retain only low-cardinality directive/disposition labels;
//! document URLs, blocked URLs, samples, and user-controlled text are dropped.

use axum::{body::Bytes, extract::State, http::StatusCode};

use crate::AppState;

pub const MAX_CSP_REPORT_BYTES: usize = 16 * 1024;

fn bounded_directive(value: Option<&str>) -> &'static str {
    match value
        .unwrap_or_default()
        .split_whitespace()
        .next()
        .unwrap_or_default()
    {
        "base-uri" => "base-uri",
        "child-src" => "child-src",
        "connect-src" => "connect-src",
        "default-src" => "default-src",
        "font-src" => "font-src",
        "form-action" => "form-action",
        "frame-ancestors" => "frame-ancestors",
        "frame-src" => "frame-src",
        "img-src" => "img-src",
        "object-src" => "object-src",
        "script-src" => "script-src",
        "script-src-attr" => "script-src-attr",
        "script-src-elem" => "script-src-elem",
        "style-src" => "style-src",
        "style-src-attr" => "style-src-attr",
        "style-src-elem" => "style-src-elem",
        "worker-src" => "worker-src",
        _ => "other",
    }
}

fn bounded_disposition(value: Option<&str>) -> &'static str {
    match value.unwrap_or_default() {
        "enforce" => "enforce",
        "report" => "report",
        _ => "unknown",
    }
}

fn report_labels(value: &serde_json::Value) -> Option<(&'static str, &'static str)> {
    let body = value
        .get("csp-report")
        .or_else(|| value.get("body"))
        .unwrap_or(value);
    let directive = body
        .get("effective-directive")
        .or_else(|| body.get("violated-directive"))
        .or_else(|| body.get("effectiveDirective"))
        .and_then(serde_json::Value::as_str);
    let disposition = body.get("disposition").and_then(serde_json::Value::as_str);
    directive.map(|directive| {
        (
            bounded_directive(Some(directive)),
            bounded_disposition(disposition),
        )
    })
}

fn parse_report(body: &[u8]) -> Result<(&'static str, &'static str), ()> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| ())?;
    if let Some(items) = value.as_array() {
        return items.iter().find_map(report_labels).ok_or(());
    }
    report_labels(&value).ok_or(())
}

pub async fn ingest_csp_report(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    if body.len() > MAX_CSP_REPORT_BYTES {
        state.self_metrics.inc_counter(
            "rush_csp_reports_total",
            &[
                ("directive", "unknown"),
                ("disposition", "unknown"),
                ("outcome", "oversized"),
            ],
            1,
        );
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "CSP report is too large"));
    }
    let (directive, disposition) = parse_report(&body).map_err(|_| {
        state.self_metrics.inc_counter(
            "rush_csp_reports_total",
            &[
                ("directive", "unknown"),
                ("disposition", "unknown"),
                ("outcome", "malformed"),
            ],
            1,
        );
        (StatusCode::BAD_REQUEST, "invalid CSP report")
    })?;
    state.self_metrics.inc_counter(
        "rush_csp_reports_total",
        &[
            ("directive", directive),
            ("disposition", disposition),
            ("outcome", "accepted"),
        ],
        1,
    );
    tracing::warn!(directive, disposition, "browser CSP violation reported");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn accepts_legacy_and_reporting_api_shapes_without_retaining_urls() {
        let legacy = br#"{"csp-report":{"document-uri":"https://rush.example/private?q=secret","violated-directive":"script-src-elem","blocked-uri":"https://evil.example/x.js","disposition":"enforce"}}"#;
        assert_eq!(parse_report(legacy), Ok(("script-src-elem", "enforce")));
        let reporting = br#"[{"type":"csp-violation","body":{"effectiveDirective":"style-src-attr","disposition":"report","documentURL":"https://rush.example/private"}}]"#;
        assert_eq!(parse_report(reporting), Ok(("style-src-attr", "report")));
    }

    #[test]
    fn attacker_controlled_labels_collapse_to_bounded_values() {
        let report = br#"{"csp-report":{"violated-directive":"secret-user-value","disposition":"secret-state"}}"#;
        assert_eq!(parse_report(report), Ok(("other", "unknown")));
    }

    proptest! {
        #[test]
        fn arbitrary_webhook_style_payloads_never_panic_or_emit_unbounded_labels(
            body in proptest::collection::vec(any::<u8>(), 0..MAX_CSP_REPORT_BYTES),
        ) {
            if let Ok((directive, disposition)) = parse_report(&body) {
                prop_assert!(directive.len() <= "script-src-attr".len());
                prop_assert!(matches!(disposition, "enforce" | "report" | "unknown"));
            }
        }
    }
}
