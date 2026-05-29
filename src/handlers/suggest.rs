use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension,
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

pub async fn suggest_values(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(field): Path<String>,
    Query(params): Query<SuggestParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = tenant_id.replace('\'', "\\'");
    let col_expr = if let Some(attr_path) = field.strip_prefix("attributes.") {
        // OTel attributes use flat dotted keys — try flat key first, nested as fallback.
        // Validate every dot-separated segment to prevent SQL injection via attr_path.
        let parts: Vec<&str> = attr_path.split('.').collect();
        if parts.is_empty() || parts.iter().any(|p| !crate::query_builder::is_safe_column_name(p)) {
            return Err((StatusCode::BAD_REQUEST, format!("invalid attribute path: {attr_path}")));
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

    // PREWHERE: tenant_id + timestamp (both in primary key of wide_events) →
    // evaluated at granule level before decompression, avoiding full table scan.
    // WHERE: the LIKE filter on the computed alias (ClickHouse allows alias refs in WHERE).
    let prewhere = format!(
        "tenant_id = '{escaped_tenant}' AND timestamp >= now() - INTERVAL 24 HOUR"
    );
    let prefix_filter = if !params.prefix.is_empty() {
        let escaped = params.prefix.replace('\'', "\\'");
        format!("WHERE val LIKE '{escaped}%'")
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT DISTINCT {col_expr} as val \
         FROM wide_events \
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
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    let values: Vec<String> = rows.into_iter().map(|r| r.val).filter(|v| !v.is_empty()).collect();
    suggest_cache().insert(cache_key, (values.clone(), std::time::Instant::now()));
    Ok(Json(values))
}
