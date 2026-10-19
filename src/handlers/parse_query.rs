use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::require_agent_principal;
use crate::llm_gateway::{LlmCaller, LlmOperation, MAX_NATURAL_LANGUAGE_QUERY_BYTES};

#[derive(Debug, Deserialize)]
pub struct ParseQueryRequest {
    pub query: String,
    /// Explore view the filters are for: `"logs"` or `"spans"` (APM). Optional
    /// so older clients keep the previous, mode-agnostic behavior.
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ParsedFilter {
    pub field: String,
    pub op: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParseQueryResponse {
    pub filters: Vec<ParsedFilter>,
    pub search: String,
    pub confidence: f64,
}

/// POST /api/v1/parse-query
///
/// Accepts a natural-language query string and returns structured filters using an LLM.
/// Requires an interactive user session. Returns 501 when the shared LLM
/// gateway is disabled so the frontend can fall back to its rule-based parser.
pub async fn parse_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<ParseQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_agent_principal(&state, &headers).await?;
    if req.query.is_empty() || req.query.len() > MAX_NATURAL_LANGUAGE_QUERY_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("query must be between 1 and {MAX_NATURAL_LANGUAGE_QUERY_BYTES} bytes"),
        ));
    }

    let system_prompt = r#"You are a query parser for an observability platform. Convert the user's natural language query into structured search filters.

Available filter fields:
- service_name: the service or application name (e.g. "payment", "checkout", "frontend", "auth")
- level: log severity level (error, warn, info, debug)
- status_code: HTTP status code as a number (e.g. 200, 404, 500)
- http_method: HTTP method (GET, POST, PUT, DELETE, PATCH)
- environment: deployment environment (production, staging, dev)
- host: hostname or pod name
- span_name: operation or endpoint name

Operators: = (equals), != (not equals), > (greater than), < (less than), >= (gte), <= (lte)

Rules:
- Put recognizable filter expressions into "filters", everything else into "search"
- For status codes, use numeric values (500 not "500")
- Confidence 0.9+ means you're very sure; 0.6-0.8 means you inferred; below 0.6 means mostly text search
- If a word could be a service name (not a common English word), treat it as service_name
- "errors" or "error logs" → level=error; "warnings" → level=warn
- "5xx" or "server errors" → status_code >= 500
- "4xx" or "client errors" → status_code >= 400

Return ONLY valid JSON with NO markdown, NO code blocks, NO explanation:
{"filters":[{"field":"...","op":"=","value":"..."}],"search":"remaining keywords","confidence":0.0}

Examples:
Input: "logs from payment service that have errors"
Output: {"filters":[{"field":"service_name","op":"=","value":"payment"},{"field":"level","op":"=","value":"error"}],"search":"","confidence":0.95}

Input: "500 errors from checkout in production"
Output: {"filters":[{"field":"service_name","op":"=","value":"checkout"},{"field":"status_code","op":"=","value":500},{"field":"environment","op":"=","value":"production"}],"search":"","confidence":0.95}

Input: "slow requests from the auth service"
Output: {"filters":[{"field":"service_name","op":"=","value":"auth"}],"search":"slow requests","confidence":0.85}

Input: "database connection timeout"
Output: {"filters":[],"search":"database connection timeout","confidence":0.6}

Input: "show me warnings from the api-gateway on pod web-1"
Output: {"filters":[{"field":"service_name","op":"=","value":"api-gateway"},{"field":"level","op":"=","value":"warn"},{"field":"host","op":"=","value":"web-1"}],"search":"","confidence":0.9}"#;

    let spans_mode = req.mode.as_deref() == Some("spans");
    let system_prompt = if spans_mode {
        format!("{system_prompt}\n\n{SPANS_MODE_PROMPT}")
    } else {
        system_prompt.to_string()
    };

    let content = state
        .llm_gateway
        .chat(
            LlmOperation::ParseQuery,
            &LlmCaller::new(caller.0, tenant.tenant_id),
            "gpt-4o-mini",
            &system_prompt,
            &req.query,
            400,
            Some(0.1),
        )
        .await?;

    // Strip any markdown code fences the model may have added despite instructions
    let cleaned = content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let parsed: ParseQueryResponse =
        serde_json::from_str(cleaned).unwrap_or_else(|_| ParseQueryResponse {
            filters: vec![],
            search: req.query.clone(),
            confidence: 0.0,
        });

    let parsed = if spans_mode {
        normalize_for_spans(parsed)
    } else {
        parsed
    };

    Ok(Json(parsed))
}

const SPANS_MODE_PROMPT: &str = r#"The user is searching APM spans, not logs. Spans have no log level:
- Never emit a "level" filter.
- "errors", "failures", "5xx" or "server errors" → {"field":"http_status_code","op":">=","value":500}
- "4xx" or "client errors" → {"field":"http_status_code","op":">=","value":400}
- Use "http_status_code" (not "status_code") for HTTP status codes.
- Do not leave words such as "server" or "client" from those phrases in "search"."#;

/// Words that only qualify an error phrase ("server errors", "client errors").
/// When one is all that is left of the search after the phrase became a
/// status-code filter, it carries no meaning and as free text would match
/// almost nothing.
const STRAY_ERROR_QUALIFIERS: &[&str] = &["server", "client"];

/// Rewrite LLM output into filters that exist on spans, in case the model
/// ignored the spans-mode instructions: `level=error` becomes
/// `http_status_code>=500`, other `level` filters are dropped, `status_code`
/// is renamed to the span column `http_status_code`, and a free-text part made
/// only of stray error-phrase qualifiers is dropped.
fn normalize_for_spans(resp: ParseQueryResponse) -> ParseQueryResponse {
    let server_errors = || ParsedFilter {
        field: "http_status_code".to_string(),
        op: ">=".to_string(),
        value: serde_json::json!(500),
    };
    let mut filters: Vec<ParsedFilter> = Vec::with_capacity(resp.filters.len());
    for f in resp.filters {
        let mapped = match f.field.as_str() {
            "level" | "severity" => {
                let is_error = f.op == "="
                    && f.value
                        .as_str()
                        .is_some_and(|v| v.eq_ignore_ascii_case("error"));
                if !is_error {
                    continue;
                }
                server_errors()
            }
            "status_code" => ParsedFilter {
                field: "http_status_code".to_string(),
                ..f
            },
            _ => f,
        };
        let duplicate = filters
            .iter()
            .any(|e| e.field == mapped.field && e.op == mapped.op && e.value == mapped.value);
        if !duplicate {
            filters.push(mapped);
        }
    }
    let has_status = filters.iter().any(|f| f.field == "http_status_code");
    let only_qualifiers = resp
        .search
        .split_whitespace()
        .all(|w| STRAY_ERROR_QUALIFIERS.contains(&w.to_ascii_lowercase().as_str()));
    let search = if has_status && only_qualifiers {
        String::new()
    } else {
        resp.search
    };
    ParseQueryResponse {
        filters,
        search,
        confidence: resp.confidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(field: &str, op: &str, value: serde_json::Value) -> ParsedFilter {
        ParsedFilter {
            field: field.to_string(),
            op: op.to_string(),
            value,
        }
    }

    fn fields(resp: &ParseQueryResponse) -> Vec<(String, String, serde_json::Value)> {
        resp.filters
            .iter()
            .map(|f| (f.field.clone(), f.op.clone(), f.value.clone()))
            .collect()
    }

    #[test]
    fn spans_mode_turns_error_level_into_server_error_status() {
        let resp = normalize_for_spans(ParseQueryResponse {
            filters: vec![
                filter("service_name", "=", serde_json::json!("gateway")),
                filter("level", "=", serde_json::json!("error")),
            ],
            search: "server".to_string(),
            confidence: 0.9,
        });
        assert_eq!(
            fields(&resp),
            vec![
                (
                    "service_name".into(),
                    "=".into(),
                    serde_json::json!("gateway")
                ),
                (
                    "http_status_code".into(),
                    ">=".into(),
                    serde_json::json!(500)
                ),
            ]
        );
        assert_eq!(resp.search, "");
    }

    #[test]
    fn spans_mode_dedupes_and_renames_status_code() {
        let resp = normalize_for_spans(ParseQueryResponse {
            filters: vec![
                filter("level", "=", serde_json::json!("ERROR")),
                filter("status_code", ">=", serde_json::json!(500)),
                filter("level", "=", serde_json::json!("warn")),
            ],
            search: "Server".to_string(),
            confidence: 0.9,
        });
        assert_eq!(
            fields(&resp),
            vec![(
                "http_status_code".into(),
                ">=".into(),
                serde_json::json!(500)
            )]
        );
        assert_eq!(resp.search, "");
    }

    #[test]
    fn spans_mode_keeps_meaningful_search_words() {
        let resp = normalize_for_spans(ParseQueryResponse {
            filters: vec![],
            search: "server".to_string(),
            confidence: 0.6,
        });
        assert!(resp.filters.is_empty());
        assert_eq!(resp.search, "server");

        let resp = normalize_for_spans(ParseQueryResponse {
            filters: vec![filter("level", "=", serde_json::json!("error"))],
            search: "server restart".to_string(),
            confidence: 0.8,
        });
        assert_eq!(resp.search, "server restart");
    }

    #[test]
    fn mode_is_optional_in_request() {
        let req: ParseQueryRequest = serde_json::from_str(r#"{"query":"errors"}"#).unwrap();
        assert!(req.mode.is_none());
        let req: ParseQueryRequest =
            serde_json::from_str(r#"{"query":"errors","mode":"spans"}"#).unwrap();
        assert_eq!(req.mode.as_deref(), Some("spans"));
    }
}
