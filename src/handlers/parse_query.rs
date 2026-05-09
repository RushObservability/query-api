use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, Extension};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;

#[derive(Debug, Deserialize)]
pub struct ParseQueryRequest {
    pub query: String,
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
/// Requires LLM_API_KEY to be set; returns 501 otherwise so the frontend can fall back
/// to its rule-based parser gracefully.
pub async fn parse_query(
    State(_state): State<AppState>,
    Extension(_tenant): Extension<TenantContext>,
    Json(req): Json<ParseQueryRequest>,
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

    let client = reqwest::Client::new();
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));

    let body = serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": req.query}
        ],
        "temperature": 0.1,
        "max_tokens": 400,
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

    // Strip any markdown code fences the model may have added despite instructions
    let cleaned = content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let parsed: ParseQueryResponse = serde_json::from_str(cleaned).unwrap_or_else(|_| {
        ParseQueryResponse {
            filters: vec![],
            search: req.query.clone(),
            confidence: 0.0,
        }
    });

    Ok(Json(parsed))
}
