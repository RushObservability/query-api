use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::{
    AppState, TenantContext,
    models::query::{Filter, FilterOp},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogViewColumn {
    pub field: String,
    pub label: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogView {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub filters: Vec<Filter>,
    pub columns: Vec<LogViewColumn>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogViews {
    pub views: Vec<LogView>,
}

type ApiError = (StatusCode, String);

fn bad_request(message: &str) -> ApiError {
    (StatusCode::BAD_REQUEST, message.into())
}

pub(crate) fn validate_field(field: &str) -> Result<(), ApiError> {
    if field.is_empty() || field.len() > 128 || field.chars().any(|c| c.is_control()) {
        return Err(bad_request(
            "Column and filter fields must contain 1 to 128 characters",
        ));
    }
    Ok(())
}

pub(crate) fn validate_fields(fields: &[String]) -> Result<(), ApiError> {
    if fields.len() > 20 {
        return Err(bad_request("A log view can show at most 20 columns"));
    }
    for field in fields {
        validate_field(field)?;
    }
    Ok(())
}

fn validate(config: &LogViews) -> Result<(), ApiError> {
    if config.views.len() > 50 {
        return Err(bad_request("At most 50 log views are allowed per tenant"));
    }
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for view in &config.views {
        if uuid::Uuid::parse_str(&view.id).is_err() || !ids.insert(&view.id) {
            return Err(bad_request("Log view IDs must be unique UUIDs"));
        }
        if view.name.trim().is_empty()
            || view.name.len() > 80
            || !names.insert(view.name.trim().to_lowercase())
        {
            return Err(bad_request(
                "Log view names must be unique and contain 1 to 80 characters",
            ));
        }
        if view.columns.is_empty() {
            return Err(bad_request("Choose at least one column"));
        }
        validate_fields(
            &view
                .columns
                .iter()
                .map(|column| column.field.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut columns = HashSet::new();
        for column in &view.columns {
            if column.label.trim().is_empty()
                || column.label.len() > 80
                || !columns.insert(&column.field)
            {
                return Err(bad_request(
                    "Columns need unique fields and labels of 1 to 80 characters",
                ));
            }
        }
        if view.filters.len() > 20 {
            return Err(bad_request("At most 20 base filters are allowed"));
        }
        for filter in &view.filters {
            validate_field(&filter.field)?;
            if !matches!(
                filter.op,
                FilterOp::Eq | FilterOp::Ne | FilterOp::Like | FilterOp::NotLike
            ) || !filter.value.is_string()
                || filter.value.as_str().unwrap_or_default().len() > 512
            {
                return Err(bad_request(
                    "Base filters accept text values with =, !=, LIKE, or NOT LIKE",
                ));
            }
        }
    }
    Ok(())
}

fn setting_key(tenant: &str) -> String {
    format!("log_views:{tenant}")
}

pub async fn get(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<LogViews>, ApiError> {
    // Tenant middleware authorizes this read just like Explore itself, including
    // deliberately public tenants. Saved views are presentation, not access rules.
    let stored = state
        .config_db
        .get_setting(&setting_key(&tenant.tenant_id))
        .await
        .map_err(|error| crate::api_error::internal_legacy("log_views.read", error))?;
    let config = stored
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| crate::api_error::internal_legacy("log_views.decode", error))?
        .unwrap_or_default();
    Ok(Json(config))
}

pub async fn put(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(mut config): Json<LogViews>,
) -> Result<Json<LogViews>, ApiError> {
    let caller = super::users::require_admin(&state, &headers).await?;
    for view in &mut config.views {
        view.name = view.name.trim().into();
        for filter in &mut view.filters {
            filter.field = filter.field.trim().into();
        }
        for column in &mut view.columns {
            column.field = column.field.trim().into();
            column.label = column.label.trim().into();
        }
    }
    validate(&config)?;
    let encoded = serde_json::to_string(&config)
        .map_err(|error| crate::api_error::internal_legacy("log_views.encode", error))?;
    state
        .config_db
        .set_setting(&setting_key(&tenant.tenant_id), &encoded)
        .await
        .map_err(|error| crate::api_error::internal_legacy("log_views.save", error))?;
    state.audit.log(
        crate::audit::AuditEvent::new("log_view.update", "user")
            .actor(caller.0, caller.1)
            .tenant(tenant.tenant_id.clone())
            .resource("log_views", &tenant.tenant_id)
            .outcome("success")
            // Filter values may contain sensitive data. Record only IDs and counts.
            .changes(serde_json::json!({"view_ids": config.views.iter().map(|v| &v.id).collect::<Vec<_>>(), "count": config.views.len()}).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok(Json(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> LogViews {
        serde_json::from_value(serde_json::json!({"views": [{
            "id": "33333333-3333-4333-8333-333333333333", "name": "Flights",
            "filters": [{"field": "type", "op": "=", "value": "event_data"}],
            "columns": [{"field": "timestamp", "label": "Time"}, {"field": "log.airline", "label": "Airline"}]
        }]})).unwrap()
    }
    #[test]
    fn validates_views_and_keeps_tenants_separate() {
        validate(&config()).unwrap();
        assert_ne!(setting_key("one"), setting_key("two"));
        let mut invalid = config();
        invalid.views.push(invalid.views[0].clone());
        assert!(validate(&invalid).is_err());
        invalid = config();
        invalid.views[0].columns.clear();
        assert!(validate(&invalid).is_err());
        invalid = config();
        invalid.views[0].filters[0].value = serde_json::json!(["bad"]);
        assert!(validate(&invalid).is_err());
    }
}
