use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};
use crate::models::slo::*;

const VALID_WINDOWS: [&str; 4] = ["rolling_1h", "rolling_24h", "rolling_7d", "rolling_30d"];
const VALID_SLO_TYPES: [&str; 2] = ["trace", "metric"];
const VALID_INDICATOR_TYPES: [&str; 3] = ["availability", "latency", "threshold"];
const VALID_THRESHOLD_OPS: [&str; 4] = ["lt", "lte", "gt", "gte"];

fn uses_metric_promql_pair(slo_type: &str, indicator_type: &str) -> bool {
    slo_type == "metric" && matches!(indicator_type, "availability" | "latency")
}

fn validate_metric_promql_pair(
    slo_type: &str,
    indicator_type: &str,
    error_promql: &str,
    total_promql: &str,
) -> Result<(), (StatusCode, String)> {
    if !uses_metric_promql_pair(slo_type, indicator_type) {
        return Ok(());
    }

    for (label, query) in [
        ("total_promql", total_promql),
        ("error_promql", error_promql),
    ] {
        let query = query.trim();
        if query.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{label} is required for metric SLOs"),
            ));
        }
        promql_parser::parser::parse(query)
            .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid {label}: {error}")))?;
    }
    Ok(())
}

fn stored_query_config(query: &str) -> serde_json::Value {
    serde_json::json!({ "promql": query.trim() })
}

pub async fn list_slos(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let slos = state
        .config_db
        .list_slos(&tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;
    let responses: Vec<SloResponse> = slos.into_iter().map(SloResponse::from).collect();
    Ok(Json(serde_json::json!({ "slos": responses })))
}

pub async fn create_slo(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<CreateSloRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    if !VALID_SLO_TYPES.contains(&req.slo_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid slo_type: {}", req.slo_type),
        ));
    }
    if !VALID_INDICATOR_TYPES.contains(&req.indicator_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid indicator_type: {}", req.indicator_type),
        ));
    }
    if !VALID_WINDOWS.contains(&req.window_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid window_type: {}", req.window_type),
        ));
    }
    if req.target_percentage <= 0.0 || req.target_percentage > 100.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "target_percentage must be between 0 and 100".to_string(),
        ));
    }
    validate_metric_promql_pair(
        &req.slo_type,
        &req.indicator_type,
        &req.error_promql,
        &req.total_promql,
    )?;
    // Latency requires threshold_ms > 0
    if req.indicator_type == "latency" {
        match req.threshold_ms {
            Some(ms) if ms > 0.0 => {}
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "latency indicator requires threshold_ms > 0".to_string(),
                ));
            }
        }
    }
    // Threshold requires threshold_value + valid threshold_op and must be metric type
    if req.indicator_type == "threshold" {
        if req.slo_type != "metric" {
            return Err((
                StatusCode::BAD_REQUEST,
                "threshold indicator is only valid for metric slo_type".to_string(),
            ));
        }
        if req.threshold_value.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                "threshold indicator requires threshold_value".to_string(),
            ));
        }
        match &req.threshold_op {
            Some(op) if VALID_THRESHOLD_OPS.contains(&op.as_str()) => {}
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "threshold indicator requires threshold_op (lt, lte, gt, gte)".to_string(),
                ));
            }
        }
    }

    let id = uuid::Uuid::new_v4().to_string();

    // For trace/availability SLOs with no error_filters, default to http_status_code >= 500.
    // Without this, error_filters == total_filters == all requests, causing 100% error rate.
    let effective_error_filters = if uses_metric_promql_pair(&req.slo_type, &req.indicator_type) {
        stored_query_config(&req.error_promql)
    } else if req.slo_type == "trace"
        && req.indicator_type == "availability"
        && req.error_filters.as_array().map_or(true, |a| a.is_empty())
    {
        serde_json::json!([{"field": "http_status_code", "op": ">=", "value": 500}])
    } else {
        req.error_filters.clone()
    };

    let effective_total_filters = if uses_metric_promql_pair(&req.slo_type, &req.indicator_type) {
        stored_query_config(&req.total_promql)
    } else {
        req.total_filters.clone()
    };
    let error_filters = serde_json::to_string(&effective_error_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let total_filters = serde_json::to_string(&effective_total_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_slo(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.description,
            req.enabled,
            &req.slo_type,
            &req.indicator_type,
            &req.service_name,
            &req.metric_name,
            &req.window_type,
            req.target_percentage,
            req.threshold_ms,
            req.threshold_value,
            req.threshold_op.as_deref(),
            &error_filters,
            &total_filters,
            req.eval_interval_secs,
            &channel_ids,
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;

    let slo = state
        .config_db
        .get_slo(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read created slo".to_string(),
            )
        })?;

    // AUDIT: SLO created.
    state.audit.log(
        crate::audit::AuditEvent::new("slo.create", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(tenant.tenant_id.clone())
            .resource("slo", id.clone())
            .changes(serde_json::json!({ "name": req.name, "slo_type": req.slo_type, "indicator_type": req.indicator_type, "enabled": req.enabled }).to_string())
            .description("slo created")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok((StatusCode::CREATED, Json(SloResponse::from(slo))))
}

pub async fn get_slo(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let slo = state
        .config_db
        .get_slo(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "slo not found".to_string()))?;
    let events = state
        .config_db
        .list_slo_events(&id, &tenant.tenant_id, 20)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;

    Ok(Json(serde_json::json!({
        "slo": SloResponse::from(slo),
        "events": events,
    })))
}

pub async fn update_slo(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateSloRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    if !VALID_SLO_TYPES.contains(&req.slo_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid slo_type: {}", req.slo_type),
        ));
    }
    if !VALID_INDICATOR_TYPES.contains(&req.indicator_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid indicator_type: {}", req.indicator_type),
        ));
    }
    if !VALID_WINDOWS.contains(&req.window_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid window_type: {}", req.window_type),
        ));
    }
    if req.target_percentage <= 0.0 || req.target_percentage > 100.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "target_percentage must be between 0 and 100".to_string(),
        ));
    }
    validate_metric_promql_pair(
        &req.slo_type,
        &req.indicator_type,
        &req.error_promql,
        &req.total_promql,
    )?;
    if req.indicator_type == "latency" {
        match req.threshold_ms {
            Some(ms) if ms > 0.0 => {}
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "latency indicator requires threshold_ms > 0".to_string(),
                ));
            }
        }
    }
    if req.indicator_type == "threshold" {
        if req.slo_type != "metric" {
            return Err((
                StatusCode::BAD_REQUEST,
                "threshold indicator is only valid for metric slo_type".to_string(),
            ));
        }
        if req.threshold_value.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                "threshold indicator requires threshold_value".to_string(),
            ));
        }
        match &req.threshold_op {
            Some(op) if VALID_THRESHOLD_OPS.contains(&op.as_str()) => {}
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "threshold indicator requires threshold_op (lt, lte, gt, gte)".to_string(),
                ));
            }
        }
    }

    let effective_error_filters = if uses_metric_promql_pair(&req.slo_type, &req.indicator_type) {
        stored_query_config(&req.error_promql)
    } else if req.slo_type == "trace"
        && req.indicator_type == "availability"
        && req.error_filters.as_array().map_or(true, |a| a.is_empty())
    {
        serde_json::json!([{"field": "http_status_code", "op": ">=", "value": 500}])
    } else {
        req.error_filters.clone()
    };

    let effective_total_filters = if uses_metric_promql_pair(&req.slo_type, &req.indicator_type) {
        stored_query_config(&req.total_promql)
    } else {
        req.total_filters.clone()
    };
    let error_filters = serde_json::to_string(&effective_error_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let total_filters = serde_json::to_string(&effective_total_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_slo(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.description,
            req.enabled,
            &req.slo_type,
            &req.indicator_type,
            &req.service_name,
            &req.metric_name,
            &req.window_type,
            req.target_percentage,
            req.threshold_ms,
            req.threshold_value,
            req.threshold_op.as_deref(),
            &error_filters,
            &total_filters,
            req.eval_interval_secs,
            &channel_ids,
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "slo not found".to_string()));
    }

    let slo = state
        .config_db
        .get_slo(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read slo".to_string(),
            )
        })?;

    // AUDIT: SLO updated.
    state.audit.log(
        crate::audit::AuditEvent::new("slo.update", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(tenant.tenant_id.clone())
            .resource("slo", id.clone())
            .changes(serde_json::json!({ "name": req.name, "slo_type": req.slo_type, "indicator_type": req.indicator_type, "enabled": req.enabled }).to_string())
            .description("slo updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok(Json(SloResponse::from(slo)))
}

pub async fn delete_slo(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_slo(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "slo not found".to_string()));
    }

    // AUDIT: SLO deleted.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("slo.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("slo", id.clone())
                .description("slo deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_slo_events(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let events = state
        .config_db
        .list_slo_events(&id, &tenant.tenant_id, 100)
        .await
        .map_err(|e| crate::api_error::internal_legacy("slos", e))?;
    Ok(Json(serde_json::json!({ "events": events })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_availability_requires_two_valid_promql_expressions() {
        assert!(
            validate_metric_promql_pair(
                "metric",
                "availability",
                "sum(rate(errors_total[5m]))",
                "sum(rate(requests_total[5m]))",
            )
            .is_ok()
        );
        assert!(
            validate_metric_promql_pair(
                "metric",
                "availability",
                "",
                "sum(rate(requests_total[5m]))",
            )
            .is_err()
        );
        assert!(
            validate_metric_promql_pair(
                "metric",
                "availability",
                "not valid PromQL (",
                "sum(rate(requests_total[5m]))",
            )
            .is_err()
        );
    }

    #[test]
    fn trace_slos_do_not_require_promql() {
        assert!(validate_metric_promql_pair("trace", "availability", "", "").is_ok());
    }
}
