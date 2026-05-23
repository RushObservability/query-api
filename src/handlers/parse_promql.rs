use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, Extension};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;

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
/// Returns 501 if LLM_API_KEY is not configured so the frontend can fall back
/// to its rule-based parser gracefully.
pub async fn parse_promql(
    State(_state): State<AppState>,
    Extension(_tenant): Extension<TenantContext>,
    Json(req): Json<ParsePromqlRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let base_url = std::env::var("LLM_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com".to_string());
    let api_key = match std::env::var("LLM_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            return Err((
                StatusCode::NOT_IMPLEMENTED,
                "LLM not configured: LLM_API_KEY not set".to_string(),
            ));
        }
    };
    let model = std::env::var("LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());

    let metric_hint = if req.metric_names.is_empty() {
        String::new()
    } else {
        let names = req.metric_names.iter().take(50).cloned().collect::<Vec<_>>().join(", ");
        format!("\n\nKnown metrics in this system: {names}")
    };

    let system_prompt = format!(r#"You are a PromQL expert for an observability platform. Convert the user's natural language description into a valid PromQL expression.

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
Output: {{"promql":"avg(cpu_usage)","confidence":0.8}}{metric_hint}"#);

    let client = reqwest::Client::new();
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));

    let body = serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": req.query}
        ],
        "temperature": 0.1,
        "max_tokens": 200,
    });

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("LLM request failed: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err((StatusCode::BAD_GATEWAY, format!("LLM error {status}: {text}")));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("LLM response parse failed: {e}")))?;

    let content = json
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("{}");

    let cleaned = content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let parsed: ParsePromqlResponse = serde_json::from_str(cleaned).unwrap_or_else(|_| {
        ParsePromqlResponse {
            promql: req.query.clone(),
            confidence: 0.0,
        }
    });

    Ok(Json(parsed))
}
