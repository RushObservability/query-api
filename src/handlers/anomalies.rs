use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::collections::HashMap;

use crate::AppState;
use crate::models::anomaly::*;

pub async fn analyze_anomaly_event(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    Json(req): Json<AnalyzeAnomalyRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // 1. Look up event
    let event = state
        .config_db
        .get_anomaly_event(&event_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "anomaly event not found".to_string()))?;

    // 2. Look up rule
    let rule = state
        .config_db
        .get_anomaly_rule(&event.rule_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "anomaly rule not found".to_string()))?;

    // 3. Fetch correlations (services + logs)
    let mut corr_services: Vec<(String, u64)> = vec![];
    let mut corr_logs: Vec<CorrelationLog> = vec![];

    let re = regex::Regex::new(r#"status_code="(\d+)""#).unwrap();
    if let Some(caps) = re.captures(&event.metric) {
        if let Ok(status_code) = caps[1].parse::<u16>() {
            let event_ts = chrono::NaiveDateTime::parse_from_str(&event.created_at, "%Y-%m-%dT%H:%M:%SZ")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&event.created_at, "%Y-%m-%dT%H:%M:%S%.fZ"))
                .ok();

            if let Some(ts) = event_ts {
                let from = ts - chrono::Duration::minutes(5);
                let to = ts + chrono::Duration::minutes(5);
                let from_str = from.format("%Y-%m-%d %H:%M:%S").to_string();
                let to_str = to.format("%Y-%m-%d %H:%M:%S").to_string();

                // Service breakdown
                let svc_query = format!(
                    "SELECT service_name, \
                            toString(toStartOfInterval(timestamp, INTERVAL 1 MINUTE)) as bucket, \
                            count() as count \
                     FROM wide_events \
                     WHERE timestamp >= '{}' AND timestamp <= '{}' \
                       AND http_status_code = {} \
                     GROUP BY service_name, bucket \
                     ORDER BY service_name, bucket",
                    from_str, to_str, status_code
                );

                if let Ok(rows) = state.ch.query(&svc_query).fetch_all::<CorrelatedBucket>().await {
                    let mut svc_totals: HashMap<String, u64> = HashMap::new();
                    for row in &rows {
                        *svc_totals.entry(row.service_name.clone()).or_default() += row.count;
                    }
                    let mut sorted: Vec<(String, u64)> = svc_totals.into_iter().collect();
                    sorted.sort_by(|a, b| b.1.cmp(&a.1));
                    sorted.truncate(10);
                    let svc_names: Vec<String> = sorted.iter().map(|(n, _)| format!("'{}'", n.replace('\'', "''"))).collect();
                    corr_services = sorted;

                    // Fetch logs for top services
                    if !svc_names.is_empty() {
                        let log_query = format!(
                            "SELECT toString(Timestamp) as timestamp, \
                                    ServiceName as service_name, \
                                    SeverityText as severity_text, \
                                    Body as body, \
                                    TraceId as trace_id \
                             FROM otel_logs \
                             WHERE Timestamp >= parseDateTimeBestEffort('{}') \
                               AND Timestamp <= parseDateTimeBestEffort('{}') \
                               AND ServiceName IN ({}) \
                             ORDER BY Timestamp DESC \
                             LIMIT 50",
                            from_str, to_str, svc_names.join(", ")
                        );

                        if let Ok(logs) = state.ch.query(&log_query).fetch_all::<CorrelationLog>().await {
                            corr_logs = logs;
                        }
                    }
                }
            }
        }
    }

    // 4. Read LLM config from env
    let base_url = std::env::var("LLM_BASE_URL").unwrap_or_else(|_| "https://api.openai.com".to_string());
    let api_key = std::env::var("LLM_API_KEY")
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "LLM_API_KEY environment variable not set".to_string()))?;
    let model = std::env::var("LLM_MODEL").unwrap_or_else(|_| "gpt-5".to_string());

    // 5. Build system prompt
    let system_prompt = "You are an observability expert analyzing anomaly events from a monitoring system. \
        Given the anomaly event details, the rule configuration, correlated services, and sample logs, \
        provide a clear root-cause analysis. Structure your response with:\n\
        ## Summary\nA brief 1-2 sentence summary of the anomaly.\n\
        ## Root Cause Analysis\nDetailed analysis of what likely caused the anomaly.\n\
        ## Correlated Evidence\nKey evidence from the correlated services and logs.\n\
        ## Recommended Actions\nSpecific steps to investigate or remediate the issue.\n\
        Be concise, specific, and actionable. Reference specific services, metrics, and log entries where relevant.";

    // 6. Build user message
    let split_labels: serde_json::Value = serde_json::from_str(&rule.split_labels).unwrap_or(serde_json::json!([]));

    let mut user_msg = format!(
        "## Anomaly Event\n\
         - **Metric**: {}\n\
         - **Value**: {:.4}\n\
         - **Expected**: {:.4}\n\
         - **Deviation**: {:.1}σ\n\
         - **State**: {}\n\
         - **Timestamp**: {}\n\n\
         ## Rule Configuration\n\
         - **Name**: {}\n\
         - **Pattern**: {}\n\
         - **Source**: {}\n\
         - **Sensitivity**: {:.1}σ\n\
         - **Alpha**: {:.2}\n\
         - **Window**: {}s\n\
         - **Split Labels**: {}\n",
        event.metric, event.value, event.expected, event.deviation,
        event.state, event.created_at,
        rule.name, rule.pattern, rule.source,
        rule.sensitivity, rule.alpha, rule.window_secs, split_labels
    );

    if !corr_services.is_empty() {
        user_msg.push_str("\n## Correlated Services\n");
        for (name, count) in &corr_services {
            user_msg.push_str(&format!("- **{}**: {} requests\n", name, count));
        }
    }

    if !corr_logs.is_empty() {
        user_msg.push_str("\n## Sample Logs (most recent 50)\n");
        for log in &corr_logs {
            user_msg.push_str(&format!(
                "- [{}] [{}] **{}**: {}\n",
                log.timestamp, log.severity_text, log.service_name, log.body
            ));
        }
    }

    if !req.additional_context.is_empty() {
        user_msg.push_str(&format!("\n## Additional Context\n{}\n", req.additional_context));
    }

    // 7. Call LLM
    let client = reqwest::Client::new();
    let llm_url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));

    let llm_body = serde_json::json!({
        "model": model,
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": user_msg }
        ],
        "max_completion_tokens": 16384
    });

    let llm_resp = client
        .post(&llm_url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&llm_body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("LLM request failed: {}", e)))?;

    if !llm_resp.status().is_success() {
        let status = llm_resp.status();
        let body = llm_resp.text().await.unwrap_or_default();
        return Err((StatusCode::BAD_GATEWAY, format!("LLM returned {}: {}", status, body)));
    }

    let resp_text = llm_resp
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to read LLM response: {}", e)))?;

    tracing::debug!("LLM response: {}", &resp_text[..resp_text.len().min(500)]);

    let llm_json: serde_json::Value = serde_json::from_str(&resp_text)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to parse LLM response: {}", e)))?;

    let analysis = llm_json["choices"][0]["message"]["content"]
        .as_str()
        .or_else(|| llm_json["output"].as_str())
        .unwrap_or("No analysis returned from the model")
        .to_string();

    if analysis == "No analysis returned from the model" {
        tracing::warn!("LLM response had no extractable content. Keys: {:?}",
            llm_json.as_object().map(|o| o.keys().collect::<Vec<_>>()));
    }

    Ok(Json(AnalyzeAnomalyResponse { analysis, model }))
}

pub async fn list_anomaly_rules(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rules = state
        .config_db
        .list_anomaly_rules()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<AnomalyRuleResponse> = rules.into_iter().map(AnomalyRuleResponse::from).collect();
    Ok(Json(serde_json::json!({ "rules": responses })))
}

pub async fn create_anomaly_rule(
    State(state): State<AppState>,
    Json(req): Json<CreateAnomalyRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_sources = ["prometheus", "apm"];
    if !valid_sources.contains(&req.source.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid source: {}", req.source)));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let split_labels = serde_json::to_string(&req.split_labels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_anomaly_rule(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.source,
            &req.pattern,
            &req.query,
            &req.service_name,
            &req.apm_metric,
            req.sensitivity,
            req.alpha,
            req.eval_interval_secs,
            req.window_secs,
            &split_labels,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rule = state
        .config_db
        .get_anomaly_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created rule".to_string()))?;

    Ok((StatusCode::CREATED, Json(AnomalyRuleResponse::from(rule))))
}

pub async fn get_anomaly_rule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rule = state
        .config_db
        .get_anomaly_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "anomaly rule not found".to_string()))?;
    let events = state
        .config_db
        .list_anomaly_events(&id, 20)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "rule": AnomalyRuleResponse::from(rule),
        "events": events,
    })))
}

pub async fn update_anomaly_rule(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateAnomalyRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_sources = ["prometheus", "apm"];
    if !valid_sources.contains(&req.source.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid source: {}", req.source)));
    }

    let split_labels = serde_json::to_string(&req.split_labels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_anomaly_rule(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.source,
            &req.pattern,
            &req.query,
            &req.service_name,
            &req.apm_metric,
            req.sensitivity,
            req.alpha,
            req.eval_interval_secs,
            req.window_secs,
            &split_labels,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "anomaly rule not found".to_string()));
    }

    let rule = state
        .config_db
        .get_anomaly_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read rule".to_string()))?;

    Ok(Json(AnomalyRuleResponse::from(rule)))
}

pub async fn delete_anomaly_rule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_anomaly_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "anomaly rule not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_all_anomaly_events(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let events = state
        .config_db
        .list_all_anomaly_events(200)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}

pub async fn get_anomaly_event(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let event = state
        .config_db
        .get_anomaly_event(&event_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "anomaly event not found".to_string()))?;
    Ok(Json(event))
}

pub async fn get_event_correlations(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // 1. Look up the event
    let event = state
        .config_db
        .get_anomaly_event(&event_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "anomaly event not found".to_string()))?;

    // 2. Parse status code from metric string
    let re = regex::Regex::new(r#"status_code="(\d+)""#).unwrap();
    let status_code: u16 = match re.captures(&event.metric) {
        Some(caps) => caps[1]
            .parse()
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid status_code in metric".to_string()))?,
        None => {
            // Not a status-code anomaly — return empty
            return Ok(Json(CorrelationResponse {
                status_code: 0,
                window_from: event.created_at.clone(),
                window_to: event.created_at.clone(),
                services: vec![],
                logs: vec![],
            }));
        }
    };

    // 3. Compute ±5 min window
    let event_ts = chrono::NaiveDateTime::parse_from_str(&event.created_at, "%Y-%m-%dT%H:%M:%SZ")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&event.created_at, "%Y-%m-%dT%H:%M:%S%.fZ"))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("bad event timestamp: {e}")))?;
    let from = event_ts - chrono::Duration::minutes(5);
    let to = event_ts + chrono::Duration::minutes(5);
    let from_str = from.format("%Y-%m-%d %H:%M:%S").to_string();
    let to_str = to.format("%Y-%m-%d %H:%M:%S").to_string();

    // 4. ClickHouse query — service breakdown with 1-minute buckets
    let bucket_query = format!(
        "SELECT service_name, \
                toString(toStartOfInterval(timestamp, INTERVAL 1 MINUTE)) as bucket, \
                count() as count \
         FROM wide_events \
         WHERE timestamp >= '{}' AND timestamp <= '{}' \
           AND http_status_code = {} \
         GROUP BY service_name, bucket \
         ORDER BY service_name, bucket",
        from_str, to_str, status_code
    );

    let bucket_rows: Vec<CorrelatedBucket> = state
        .ch
        .query(&bucket_query)
        .fetch_all()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 5. Group by service, compute totals, take top 10
    let mut svc_map: HashMap<String, Vec<ServiceBucket>> = HashMap::new();
    let mut svc_totals: HashMap<String, u64> = HashMap::new();
    for row in &bucket_rows {
        svc_map
            .entry(row.service_name.clone())
            .or_default()
            .push(ServiceBucket {
                timestamp: row.bucket.clone(),
                count: row.count,
            });
        *svc_totals.entry(row.service_name.clone()).or_default() += row.count;
    }

    let mut services: Vec<CorrelatedService> = svc_map
        .into_iter()
        .map(|(name, buckets)| {
            let total = svc_totals.get(&name).copied().unwrap_or(0);
            CorrelatedService { name, total, buckets }
        })
        .collect();
    services.sort_by(|a, b| b.total.cmp(&a.total));
    services.truncate(10);

    // 6. ClickHouse query — logs for top services
    let logs = if !services.is_empty() {
        let svc_list: Vec<String> = services.iter().map(|s| format!("'{}'", s.name.replace('\'', "''"))).collect();
        let log_query = format!(
            "SELECT toString(Timestamp) as timestamp, \
                    ServiceName as service_name, \
                    SeverityText as severity_text, \
                    Body as body, \
                    TraceId as trace_id \
             FROM otel_logs \
             WHERE Timestamp >= parseDateTimeBestEffort('{}') \
               AND Timestamp <= parseDateTimeBestEffort('{}') \
               AND ServiceName IN ({}) \
             ORDER BY Timestamp DESC \
             LIMIT 200",
            from_str, to_str, svc_list.join(", ")
        );

        state
            .ch
            .query(&log_query)
            .fetch_all::<CorrelationLog>()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    } else {
        vec![]
    };

    // 7. Return response
    Ok(Json(CorrelationResponse {
        status_code,
        window_from: from_str,
        window_to: to_str,
        services,
        logs,
    }))
}
