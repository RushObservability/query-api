use axum::{
    Extension,
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};
use crate::models::monitor::*;
use crate::monitor_engine;

#[derive(Debug, serde::Deserialize)]
pub struct EventsQuery {
    #[serde(default = "default_events_limit")]
    pub limit: i64,
}

fn default_events_limit() -> i64 {
    50
}

/// GET /api/v1/monitors — list monitors (tenant-scoped)
pub async fn list_monitors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let monitors = state
        .config_db
        .list_monitors(&tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<MonitorResponse> = monitors.into_iter().map(MonitorResponse::from).collect();
    Ok(Json(serde_json::json!({ "monitors": responses })))
}

/// POST /api/v1/monitors — create monitor
pub async fn create_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateMonitorRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    // Validate monitor type
    let valid_types = ["metric", "log", "apm", "composite"];
    if !valid_types.contains(&req.monitor_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid monitor type: {}", req.monitor_type),
        ));
    }

    // Validate comparator
    let valid_comparators = ["above", "below"];
    if !valid_comparators.contains(&req.comparator.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid comparator: {}", req.comparator),
        ));
    }

    // Validate no_data_action
    let valid_nda = ["show", "notify", "resolve"];
    if !valid_nda.contains(&req.no_data_action.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid no_data_action: {}", req.no_data_action),
        ));
    }

    // Validate priority range
    if let Some(p) = req.priority {
        if !(1..=5).contains(&p) {
            return Err((
                StatusCode::BAD_REQUEST,
                "priority must be between 1 and 5".to_string(),
            ));
        }
    }

    // Validate query_config based on type
    validate_query_config(&req.monitor_type, &req.query_config)?;

    // Non-composite monitors must have at least a critical threshold
    if req.monitor_type != "composite" && req.critical.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "critical threshold is required for non-composite monitors".to_string(),
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let group_by = serde_json::to_string(&req.group_by)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let notification_channels = serde_json::to_string(&req.notification_channels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let tags = serde_json::to_string(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let composite_monitor_ids = serde_json::to_string(&req.composite_monitor_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_monitor(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.monitor_type,
            &query_config,
            req.critical,
            req.critical_recovery,
            req.warning,
            req.warning_recovery,
            &req.comparator,
            req.eval_window_secs,
            req.eval_interval_secs,
            &group_by,
            &req.no_data_action,
            req.no_data_timeframe,
            req.auto_resolve_hours,
            &req.message,
            &notification_channels,
            req.renotify_interval,
            &tags,
            req.priority,
            req.enabled,
            &req.composite_formula,
            &composite_monitor_ids,
            &req.created_by,
        ).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let monitor = state
        .config_db
        .get_monitor(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read created monitor".to_string(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(MonitorResponse::from(monitor))))
}

/// GET /api/v1/monitors/{id} — get monitor with state
pub async fn get_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let monitor = state
        .config_db
        .get_monitor(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "monitor not found".to_string()))?;

    let events = state
        .config_db
        .list_monitor_events(&id, 20).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "monitor": MonitorResponse::from(monitor),
        "events": events,
    })))
}

/// PUT /api/v1/monitors/{id} — update monitor
pub async fn update_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateMonitorRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    // Validate monitor type
    let valid_types = ["metric", "log", "apm", "composite"];
    if !valid_types.contains(&req.monitor_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid monitor type: {}", req.monitor_type),
        ));
    }

    let valid_comparators = ["above", "below"];
    if !valid_comparators.contains(&req.comparator.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid comparator: {}", req.comparator),
        ));
    }

    let valid_nda = ["show", "notify", "resolve"];
    if !valid_nda.contains(&req.no_data_action.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid no_data_action: {}", req.no_data_action),
        ));
    }

    if let Some(p) = req.priority {
        if !(1..=5).contains(&p) {
            return Err((
                StatusCode::BAD_REQUEST,
                "priority must be between 1 and 5".to_string(),
            ));
        }
    }

    validate_query_config(&req.monitor_type, &req.query_config)?;

    if req.monitor_type != "composite" && req.critical.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "critical threshold is required for non-composite monitors".to_string(),
        ));
    }

    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let group_by = serde_json::to_string(&req.group_by)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let notification_channels = serde_json::to_string(&req.notification_channels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let tags = serde_json::to_string(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let composite_monitor_ids = serde_json::to_string(&req.composite_monitor_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_monitor(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.monitor_type,
            &query_config,
            req.critical,
            req.critical_recovery,
            req.warning,
            req.warning_recovery,
            &req.comparator,
            req.eval_window_secs,
            req.eval_interval_secs,
            &group_by,
            &req.no_data_action,
            req.no_data_timeframe,
            req.auto_resolve_hours,
            &req.message,
            &notification_channels,
            req.renotify_interval,
            &tags,
            req.priority,
            req.enabled,
            &req.composite_formula,
            &composite_monitor_ids,
        ).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "monitor not found".to_string()));
    }

    let monitor = state
        .config_db
        .get_monitor(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read updated monitor".to_string(),
            )
        })?;

    Ok(Json(MonitorResponse::from(monitor)))
}

/// DELETE /api/v1/monitors/{id} — delete monitor
pub async fn delete_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_monitor(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "monitor not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/monitors/{id}/events — list state transition events
pub async fn list_monitor_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<EventsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let events = state
        .config_db
        .list_monitor_events(&id, params.limit).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}

/// POST /api/v1/monitors/preview — preview query results (for the live graph in creation wizard)
pub async fn preview_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<PreviewMonitorRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let group_by: Vec<String> =
        serde_json::from_value(req.group_by.clone()).unwrap_or_default();

    let result = monitor_engine::preview_query(
        &state.ch,
        &tenant.tenant_id,
        &req.monitor_type,
        &req.query_config,
        req.eval_window_secs,
        &group_by,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "current_value": result.current_value,
        "groups": result.groups,
        "timeseries": result.timeseries,
    })))
}

/// POST /api/v1/monitors/{id}/mute — mute (disable) a monitor
pub async fn mute_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let updated = state
        .config_db
        .set_monitor_enabled(&id, &tenant.tenant_id, false).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "monitor not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "ok": true, "enabled": false })))
}

/// POST /api/v1/monitors/{id}/unmute — unmute (enable) a monitor
pub async fn unmute_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let updated = state
        .config_db
        .set_monitor_enabled(&id, &tenant.tenant_id, true).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "monitor not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "ok": true, "enabled": true })))
}

// ── Validation helpers ──

fn validate_query_config(
    monitor_type: &str,
    config: &serde_json::Value,
) -> Result<(), (StatusCode, String)> {
    match monitor_type {
        "metric" => {
            // Allow PromQL-style expression as an alternative to structured config
            let is_promql = config
                .get("type")
                .and_then(|v| v.as_str())
                .map(|t| t == "promql")
                .unwrap_or(false);
            if is_promql {
                if config.get("expr").and_then(|v| v.as_str()).map(|s| !s.is_empty()).unwrap_or(false) {
                    // Valid PromQL config: has a non-empty expression
                } else {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "promql metric monitor query_config requires a non-empty 'expr' field".to_string(),
                    ));
                }
            } else if config.get("metric_name").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "metric monitor query_config requires 'metric_name' or type='promql' with 'expr'".to_string(),
                ));
            }
        }
        "log" => {
            // Log monitors need either search text or filters
            let has_search = config
                .get("search")
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false);
            let has_filters = config
                .get("filters")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if !has_search && !has_filters {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "log monitor query_config requires 'search' or 'filters'".to_string(),
                ));
            }
        }
        "apm" => {
            if config.get("service").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "apm monitor query_config requires 'service'".to_string(),
                ));
            }
            if config.get("metric").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "apm monitor query_config requires 'metric'".to_string(),
                ));
            }
            let valid_metrics = [
                "error_rate",
                "error_count",
                "request_rate",
                "p50_latency",
                "p50",
                "p75_latency",
                "p75",
                "p90_latency",
                "p90",
                "p95_latency",
                "p95",
                "p99_latency",
                "p99",
            ];
            if let Some(metric) = config.get("metric").and_then(|v| v.as_str()) {
                if !valid_metrics.contains(&metric) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "invalid apm metric '{}'; valid options: {}",
                            metric,
                            valid_metrics.join(", ")
                        ),
                    ));
                }
            }
        }
        "composite" => {
            // Composite monitors need a formula and monitor IDs
            // (optional at creation, required for evaluation)
        }
        _ => {}
    }
    Ok(())
}

// ── Autocomplete ──

/// Escape a string value for safe use in a ClickHouse SQL LIKE pattern / literal.
fn escape_ch(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct StringRow {
    value: String,
}

/// GET /api/v1/monitors/autocomplete — return suggestions for metric names, label keys/values,
/// services, endpoints, and log fields.
pub async fn autocomplete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<AutocompleteQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let prefix = escape_ch(&params.prefix);
    let tenant_id = escape_ch(&tenant.tenant_id);

    let suggestions: Vec<String> = match params.ac_type.as_str() {
        "metric" => {
            // Search across gauge, sum, and histogram tables
            let tables = ["otel_metrics_gauge", "otel_metrics_sum", "otel_metrics_histogram"];
            let mut all: Vec<String> = Vec::new();
            for table in &tables {
                let sql = format!(
                    "SELECT DISTINCT MetricName AS value FROM {table} \
                     WHERE tenant_id = '{tenant_id}' AND MetricName LIKE '{prefix}%' \
                     LIMIT 20"
                );
                match state.ch.query(&sql).fetch_all::<StringRow>().await {
                    Ok(rows) => {
                        for r in rows {
                            if !all.contains(&r.value) {
                                all.push(r.value);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(table = table, error = %e, "autocomplete metric query failed");
                    }
                }
            }
            all.truncate(20);
            all
        }
        "label_key" => {
            let metric = escape_ch(&params.metric);
            let tables = ["otel_metrics_gauge", "otel_metrics_sum", "otel_metrics_histogram"];
            let mut all: Vec<String> = Vec::new();
            for table in &tables {
                let sql = format!(
                    "SELECT DISTINCT arrayJoin(mapKeys(Attributes)) AS value \
                     FROM {table} \
                     WHERE tenant_id = '{tenant_id}' AND MetricName = '{metric}' \
                     AND value LIKE '{prefix}%' \
                     LIMIT 20"
                );
                match state.ch.query(&sql).fetch_all::<StringRow>().await {
                    Ok(rows) => {
                        for r in rows {
                            if !all.contains(&r.value) {
                                all.push(r.value);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(table = table, error = %e, "autocomplete label_key query failed");
                    }
                }
            }
            all.truncate(20);
            all
        }
        "label_value" => {
            let metric = escape_ch(&params.metric);
            let key = escape_ch(&params.key);
            let tables = ["otel_metrics_gauge", "otel_metrics_sum", "otel_metrics_histogram"];
            let mut all: Vec<String> = Vec::new();
            for table in &tables {
                let sql = format!(
                    "SELECT DISTINCT Attributes['{key}'] AS value \
                     FROM {table} \
                     WHERE tenant_id = '{tenant_id}' AND MetricName = '{metric}' \
                     AND value LIKE '{prefix}%' \
                     LIMIT 20"
                );
                match state.ch.query(&sql).fetch_all::<StringRow>().await {
                    Ok(rows) => {
                        for r in rows {
                            if !r.value.is_empty() && !all.contains(&r.value) {
                                all.push(r.value);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(table = table, error = %e, "autocomplete label_value query failed");
                    }
                }
            }
            all.truncate(20);
            all
        }
        "service" => {
            let sql = format!(
                "SELECT DISTINCT service_name AS value FROM wide_events \
                 WHERE tenant_id = '{tenant_id}' AND service_name LIKE '{prefix}%' \
                 LIMIT 20"
            );
            state
                .ch
                .query(&sql)
                .fetch_all::<StringRow>()
                .await
                .map(|rows| rows.into_iter().map(|r| r.value).collect())
                .unwrap_or_default()
        }
        "endpoint" => {
            let service = escape_ch(&params.service);
            let sql = format!(
                "SELECT DISTINCT http_path AS value FROM wide_events \
                 WHERE tenant_id = '{tenant_id}' AND service_name = '{service}' \
                 AND http_path LIKE '{prefix}%' \
                 LIMIT 20"
            );
            state
                .ch
                .query(&sql)
                .fetch_all::<StringRow>()
                .await
                .map(|rows| rows.into_iter().map(|r| r.value).collect())
                .unwrap_or_default()
        }
        "log_service" => {
            let sql = format!(
                "SELECT DISTINCT ServiceName AS value FROM otel_logs \
                 WHERE tenant_id = '{tenant_id}' AND ServiceName LIKE '{prefix}%' \
                 LIMIT 20"
            );
            state
                .ch
                .query(&sql)
                .fetch_all::<StringRow>()
                .await
                .map(|rows| rows.into_iter().map(|r| r.value).collect())
                .unwrap_or_default()
        }
        "log_field" => {
            // Return a static list of known log fields, filtered by prefix
            let known_fields = vec![
                "ServiceName",
                "SeverityText",
                "SeverityNumber",
                "Body",
                "TraceId",
                "SpanId",
                "mat_k8s_namespace",
                "mat_k8s_pod",
                "mat_k8s_container",
                "mat_k8s_deployment",
                "mat_k8s_node",
                "mat_level",
                "mat_component",
                "mat_environment",
            ];
            known_fields
                .into_iter()
                .filter(|f| {
                    if params.prefix.is_empty() {
                        true
                    } else {
                        f.to_lowercase().starts_with(&params.prefix.to_lowercase())
                    }
                })
                .map(|s| s.to_string())
                .collect()
        }
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "unknown autocomplete type '{}'; valid types: metric, label_key, label_value, \
                     service, endpoint, log_service, log_field",
                    other
                ),
            ));
        }
    };

    Ok(Json(serde_json::json!({ "suggestions": suggestions })))
}

// ── Suggest ──

/// POST /api/v1/monitors/suggest — given a partial monitor config, suggest improvements.
pub async fn suggest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SuggestRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let mut suggestions: Vec<Suggestion> = Vec::new();

    match req.monitor_type.as_str() {
        "metric" => {
            // Check for counter-like metric names
            if let Some(metric_name) = req.query_config.get("metric_name").and_then(|v| v.as_str())
            {
                let lower = metric_name.to_lowercase();
                if lower.contains("count") || lower.contains("total") {
                    suggestions.push(Suggestion {
                        text: "This looks like a counter. Consider using `rate()` instead \
                               of raw value to alert on the rate of change rather than the \
                               absolute count."
                            .to_string(),
                        severity: "info".to_string(),
                    });
                }
            }

            // Check for avg aggregation on gauge
            if let Some(agg) = req.query_config.get("aggregation").and_then(|v| v.as_str()) {
                if agg == "avg" {
                    suggestions.push(Suggestion {
                        text: "For gauges, consider using `max` aggregation to catch peaks, \
                               not just averages."
                            .to_string(),
                        severity: "info".to_string(),
                    });
                }
            }

            // Check eval_window
            if let Some(window) = req.query_config.get("eval_window_secs").and_then(|v| v.as_i64())
            {
                if window <= 60 {
                    suggestions.push(Suggestion {
                        text: "A 1-minute window can be noisy. Consider 5m or 15m for more \
                               stable alerting."
                            .to_string(),
                        severity: "warning".to_string(),
                    });
                }
            }
        }
        "apm" => {
            if let Some(metric) = req.query_config.get("metric").and_then(|v| v.as_str()) {
                if metric == "error_count" {
                    suggestions.push(Suggestion {
                        text: "Consider using error_rate instead of error_count. Rate-based \
                               alerts are less sensitive to traffic volume changes."
                            .to_string(),
                        severity: "info".to_string(),
                    });
                }

                if metric == "p99_latency" || metric == "p99" {
                    let has_group_by = req
                        .query_config
                        .get("group_by")
                        .and_then(|v| v.as_array())
                        .map(|a| !a.is_empty())
                        .unwrap_or(false);
                    if !has_group_by {
                        suggestions.push(Suggestion {
                            text: "Consider grouping by endpoint to catch per-endpoint \
                                   regressions."
                                .to_string(),
                            severity: "info".to_string(),
                        });
                    }
                }
            }
        }
        "log" => {
            let has_group_by = req
                .query_config
                .get("group_by")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if !has_group_by {
                suggestions.push(Suggestion {
                    text: "Consider grouping by ServiceName to get per-service alerts \
                           instead of one aggregate."
                        .to_string(),
                    severity: "info".to_string(),
                });
            }
        }
        _ => {}
    }

    Ok(Json(SuggestResponse { suggestions }))
}
