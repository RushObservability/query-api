use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

use crate::AppState;
use crate::models::query::StringValueRow;

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
    Path(field): Path<String>,
    Query(params): Query<SuggestParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let col_expr = if let Some(attr_path) = field.strip_prefix("attributes.") {
        // OTel attributes use flat dotted keys — try flat key first, nested as fallback
        let parts: Vec<&str> = attr_path.split('.').collect();
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
            "service_version",
            "environment",
            "host_name",
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

    let where_prefix = if params.prefix.is_empty() {
        String::new()
    } else {
        let escaped = params.prefix.replace('\'', "\\'");
        format!("WHERE val LIKE '{escaped}%'")
    };

    let sql = format!(
        "SELECT DISTINCT {col_expr} as val \
         FROM wide_events \
         {where_prefix} \
         ORDER BY val \
         LIMIT {}",
        params.limit.min(100),
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<StringValueRow>()
        .await
        .map_err(|e| {
            tracing::error!("Suggest query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("query failed: {e}"),
            )
        })?;

    let values: Vec<String> = rows.into_iter().map(|r| r.val).filter(|v| !v.is_empty()).collect();
    Ok(Json(values))
}
