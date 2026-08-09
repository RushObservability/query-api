use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::require_auth;
use crate::llm_gateway::{
    LlmCaller, LlmOperation, MAX_METRIC_HINT_BYTES, MAX_METRIC_HINT_TOTAL_BYTES, MAX_METRIC_HINTS,
    MAX_NATURAL_LANGUAGE_QUERY_BYTES,
};

#[derive(Debug, Deserialize)]
pub struct ParsePromqlRequest {
    pub query: String,
    /// Known metric names to help the LLM pick the right one
    #[serde(default)]
    pub metric_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParsePromqlResponse {
    pub promql: String,
    pub confidence: f64,
}

/// POST /api/v1/parse-promql
///
/// Accepts a natural-language description and returns a PromQL expression.
/// Requires an interactive user session. Returns 501 when the shared LLM
/// gateway is disabled so the frontend can fall back to its rule-based parser.
pub async fn parse_promql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<ParsePromqlRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    if req.query.is_empty() || req.query.len() > MAX_NATURAL_LANGUAGE_QUERY_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("query must be between 1 and {MAX_NATURAL_LANGUAGE_QUERY_BYTES} bytes"),
        ));
    }
    if req.metric_names.len() > MAX_METRIC_HINTS
        || req
            .metric_names
            .iter()
            .any(|name| name.len() > MAX_METRIC_HINT_BYTES)
        || req.metric_names.iter().map(String::len).sum::<usize>() > MAX_METRIC_HINT_TOTAL_BYTES
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "metric hints exceed the configured count or length limit".to_string(),
        ));
    }

    let metric_hint = if req.metric_names.is_empty() {
        String::new()
    } else {
        let names = req.metric_names.join(", ");
        format!("\n\nKnown metrics in this system: {names}")
    };

    let system_prompt = format!(
        r#"You are a PromQL expert for an observability platform. Convert the user's natural language description into a valid PromQL expression.

PromQL rules:
- Use rate() for counters (metrics ending in _total, _count, _sum, _bucket)
- Use increase() for total increase over a period
- Default window is [5m] unless the user specifies otherwise
- Use histogram_quantile(0.99, rate(metric_bucket[5m])) for p99 latency from histograms
- Use sum by (label) (...) for grouping
- Use topk(N, ...) for top-N
- Label selectors go inside {{}} e.g. metric{{service_name="foo"}}

Return ONLY valid JSON with NO markdown, NO code blocks, NO explanation:
{{"promql":"<expression>","confidence":0.0}}

Confidence: 0.9+ = very sure, 0.7-0.9 = inferred, below 0.7 = best guess

Examples:
Input: "rate over 5min for http_requests_total"
Output: {{"promql":"rate(http_requests_total[5m])","confidence":0.98}}

Input: "p99 latency for request_duration_seconds"
Output: {{"promql":"histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))","confidence":0.95}}

Input: "error rate for checkout service"
Output: {{"promql":"rate(http_requests_total{{service_name=\"checkout\",status_code=~\"5..\"}}[5m])","confidence":0.85}}

Input: "sum of requests by service"
Output: {{"promql":"sum by (service_name) (rate(http_requests_total[5m]))","confidence":0.9}}

Input: "top 5 services by request rate"
Output: {{"promql":"topk(5, sum by (service_name) (rate(http_requests_total[5m])))","confidence":0.85}}

Input: "increase in errors over 1 hour"
Output: {{"promql":"increase(http_requests_total{{status_code=~\"5..\"}}[1h])","confidence":0.85}}

Input: "average cpu usage"
Output: {{"promql":"avg(cpu_usage)","confidence":0.8}}{metric_hint}"#
    );

    let content = state
        .llm_gateway
        .chat(
            LlmOperation::ParsePromql,
            &LlmCaller::new(caller.0, tenant.tenant_id),
            "gpt-4o-mini",
            &system_prompt,
            &req.query,
            200,
            Some(0.1),
        )
        .await?;

    let cleaned = content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let parsed: ParsePromqlResponse =
        serde_json::from_str(cleaned).unwrap_or_else(|_| ParsePromqlResponse {
            promql: req.query.clone(),
            confidence: 0.0,
        });

    Ok(Json(parsed))
}
