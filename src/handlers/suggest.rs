use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

use crate::AppState;
use crate::TenantContext;
use crate::models::query::StringValueRow;

/// 30-second in-memory result cache for suggest queries.
/// Key: "{tenant_id}\0{field}\0{prefix}\0{limit}"
/// Value: (results, cached_at)
pub fn suggest_cache() -> &'static dashmap::DashMap<String, (Vec<String>, std::time::Instant)> {
    static CACHE: std::sync::OnceLock<dashmap::DashMap<String, (Vec<String>, std::time::Instant)>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(dashmap::DashMap::new)
}

#[derive(Debug, Deserialize)]
pub struct SuggestParams {
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_limit")]
    pub limit: u64,
}

fn default_limit() -> u64 {
    20
}

#[derive(Debug, Deserialize)]
pub struct LogSuggestRequest {
    pub field: String,
    #[serde(default)]
    pub prefix: String,
    pub time_range: crate::models::query::TimeRange,
    #[serde(default)]
    pub filters: Vec<crate::models::query::Filter>,
}

fn log_suggest_sql(req: &LogSuggestRequest, tenant: &str) -> Result<String, (StatusCode, String)> {
    super::log_views::validate_field(&req.field)?;
    if req.prefix.len() > 200 || req.filters.len() > 20 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Suggestion prefix or filter count exceeds its limit".into(),
        ));
    }
    let clauses = super::logs::build_log_where(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        None,
        tenant,
    );
    let field = super::logs::resolve_log_field(&req.field);
    let prefix = crate::query_builder::escape_string_literal(&req.prefix);
    // Bound distinct-value work to recent matching rows. The inner predicate
    // applies the same tenant, time range and saved-view filters as Explore.
    Ok(format!(
        "SELECT DISTINCT val FROM (SELECT toString({field}) AS val FROM logs {} \
         ORDER BY Timestamp DESC LIMIT 20000) \
         WHERE val != '' AND length(val) <= 512 AND startsWith(lowerUTF8(val), lowerUTF8('{prefix}')) \
         ORDER BY val LIMIT 20",
        clauses.to_sql()
    ))
}

pub async fn suggest_log_values(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<LogSuggestRequest>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let sql = log_suggest_sql(&req, &tenant.tenant_id)?;
    let rows = crate::tenant_query(&state.ch, &sql, &tenant.tenant_id)
        .fetch_all::<StringValueRow>()
        .await
        .map_err(|error| crate::api_error::internal_legacy("log_suggest.query", error))?;
    Ok(Json(rows.into_iter().map(|row| row.val).collect()))
}

#[cfg(test)]
mod log_tests {
    use super::*;

    #[test]
    fn log_suggestions_scope_custom_fields_to_tenant_time_and_view() {
        let mut req: LogSuggestRequest = serde_json::from_value(serde_json::json!({
            "field": "body.airline", "prefix": "Ex'_%",
            "time_range": {"from": "2026-09-11T00:00:00Z", "to": "2026-09-11T01:00:00Z"},
            "filters": [{"field": "type", "op": "=", "value": "event_data"}]
        }))
        .unwrap();
        let sql = log_suggest_sql(&req, "tenant'one").unwrap();
        assert!(sql.contains("tenant_id = 'tenant''one'"));
        assert!(sql.contains("2026-09-11T00:00:00Z"));
        assert!(sql.contains("2026-09-11T01:00:00Z"));
        assert!(sql.contains("= 'event_data'"));
        assert!(sql.contains("JSON_VALUE(Body, '$.airline')"));
        assert!(sql.contains("lowerUTF8('Ex''_%')"));
        assert!(sql.contains("LIMIT 20000"));
        assert!(sql.ends_with("LIMIT 20"));
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::ClickHouseDialect {}, &sql)
            .unwrap();
        req.field = "airline".into();
        assert!(
            log_suggest_sql(&req, "default")
                .unwrap()
                .contains("LogAttributes['airline']")
        );
        req.prefix = "x".repeat(201);
        assert_eq!(
            log_suggest_sql(&req, "default").unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }
}

pub async fn suggest_values(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(field): Path<String>,
    Query(params): Query<SuggestParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    if params.prefix.len() > 200 {
        return Err((
            StatusCode::BAD_REQUEST,
            "prefix too long (max 200 chars)".into(),
        ));
    }
    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let col_expr = if let Some(attr_path) = field.strip_prefix("attributes.") {
        // OTel attributes use flat dotted keys — try flat key first, nested as fallback.
        // Validate every dot-separated segment to prevent SQL injection via attr_path.
        let parts: Vec<&str> = attr_path.split('.').collect();
        if parts.is_empty()
            || parts
                .iter()
                .any(|p| !crate::query_builder::is_safe_column_name(p))
        {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid attribute path: {attr_path}"),
            ));
        }
        if parts.len() == 1 {
            format!("JSONExtractString(attributes, '{attr_path}')")
        } else {
            let flat = format!("JSONExtractString(attributes, '{attr_path}')");
            let nested_args = parts
                .iter()
                .map(|p| format!("'{p}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let nested = format!("JSONExtractString(attributes, {nested_args})");
            format!("if({flat} != '', {flat}, {nested})")
        }
    } else {
        let allowed = [
            "service_name",
            "span_name",
            "kind",
            "http_method",
            "http_path",
            "status",
        ];
        if !allowed.contains(&field.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("field '{field}' not suggestable; use attributes.* for custom fields"),
            ));
        }
        field.clone()
    };

    let limit = params.limit.min(100);

    // Check 30s result cache before hitting ClickHouse
    let cache_key = format!("{}\0{}\0{}\0{}", tenant_id, field, params.prefix, limit);
    if let Some(entry) = suggest_cache().get(&cache_key) {
        let (cached_values, ts) = entry.value();
        if ts.elapsed() < std::time::Duration::from_secs(30) {
            return Ok(Json(cached_values.clone()));
        }
    }

    // PREWHERE: tenant_id + timestamp (both in primary key of spans) →
    // evaluated at granule level before decompression, avoiding full table scan.
    // WHERE: the LIKE filter on the computed alias (ClickHouse allows alias refs in WHERE).
    let prewhere =
        format!("tenant_id = '{escaped_tenant}' AND timestamp >= now() - INTERVAL 24 HOUR");
    let prefix_filter = if !params.prefix.is_empty() {
        let escaped = crate::query_builder::escape_string_literal(&params.prefix);
        format!("WHERE val LIKE '{escaped}%'")
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT DISTINCT {col_expr} as val \
         FROM spans \
         PREWHERE {prewhere} \
         {prefix_filter} \
         ORDER BY val \
         LIMIT {limit}",
    );

    let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<StringValueRow>()
        .await
        .map_err(|e| {
            tracing::error!("Suggest query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    let values: Vec<String> = rows
        .into_iter()
        .map(|r| r.val)
        .filter(|v| !v.is_empty())
        .collect();
    suggest_cache().insert(cache_key, (values.clone(), std::time::Instant::now()));
    Ok(Json(values))
}
