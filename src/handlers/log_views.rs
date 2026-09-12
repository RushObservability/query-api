use axum::{
    Extension, Json,
    extract::{Query, State},
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
    // Accept response metadata on round-trips, but never persist caller-supplied ownership.
    #[serde(default, skip_serializing)]
    scope: Option<ViewScope>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogViews {
    pub views: Vec<LogView>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ViewScope {
    #[default]
    Tenant,
    Personal,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveScope {
    #[serde(default)]
    scope: ViewScope,
}

#[derive(Debug, Serialize)]
pub struct VisibleLogView {
    #[serde(flatten)]
    view: LogView,
    scope: ViewScope,
}

#[derive(Debug, Serialize)]
pub struct VisibleLogViews {
    views: Vec<VisibleLogView>,
    tenant_id: String,
}

fn visible_views(config: LogViews, scope: ViewScope) -> Vec<VisibleLogView> {
    config
        .views
        .into_iter()
        .map(|view| VisibleLogView { view, scope })
        .collect()
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
        return Err(bad_request(
            "At most 50 log views are allowed per scope in a tenant",
        ));
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

fn scoped_setting_key(
    tenant: &str,
    scope: ViewScope,
    user_id: Option<&str>,
) -> Result<String, ApiError> {
    match scope {
        ViewScope::Tenant => Ok(setting_key(tenant)),
        ViewScope::Personal => {
            let user_id = user_id.filter(|id| !id.is_empty()).ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    "Personal log views require a user session".into(),
                )
            })?;
            // Encoding the pair prevents collisions when tenant names contain separators.
            Ok(format!(
                "personal_log_views:{}",
                serde_json::json!([tenant, user_id])
            ))
        }
    }
}

fn may_save(scope: ViewScope, role: &str) -> bool {
    match scope {
        ViewScope::Tenant => role == "admin",
        ViewScope::Personal => matches!(role, "admin" | "write" | "viewer"),
    }
}

fn ensure_requested_tenant(headers: &HeaderMap, tenant: &str) -> Result<(), ApiError> {
    if let Some(requested) = headers.get("x-rush-tenant") {
        if requested.to_str().ok().map(str::trim) != Some(tenant) {
            // Tenant middleware can fall back to a session default. Never save a
            // view to that fallback when the user explicitly chose another tenant.
            return Err((
                StatusCode::FORBIDDEN,
                "Requested tenant is not available".into(),
            ));
        }
    }
    Ok(())
}

async fn read_views(state: &AppState, key: &str) -> Result<LogViews, ApiError> {
    let stored = state
        .config_db
        .get_setting(key)
        .await
        .map_err(|error| crate::api_error::internal_legacy("log_views.read", error))?;
    stored
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| crate::api_error::internal_legacy("log_views.decode", error))
        .map(Option::unwrap_or_default)
}

pub async fn get(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<Json<VisibleLogViews>, ApiError> {
    // Tenant middleware authorizes this read just like Explore itself, including
    // deliberately public tenants. Saved views are presentation, not access rules.
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let mut views = visible_views(
        read_views(&state, &setting_key(&tenant.tenant_id)).await?,
        ViewScope::Tenant,
    );
    if super::auth::extract_session_cookie(&headers).is_some() {
        let caller = super::users::require_auth(&state, &headers).await?;
        let key = scoped_setting_key(&tenant.tenant_id, ViewScope::Personal, Some(&caller.0))?;
        views.extend(visible_views(
            read_views(&state, &key).await?,
            ViewScope::Personal,
        ));
    }
    Ok(Json(VisibleLogViews {
        views,
        tenant_id: tenant.tenant_id,
    }))
}

pub async fn put(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(options): Query<SaveScope>,
    headers: HeaderMap,
    Json(mut config): Json<LogViews>,
) -> Result<Json<VisibleLogViews>, ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = super::users::require_auth(&state, &headers).await?;
    if !may_save(options.scope, &caller.4) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("log_view.update", "user")
                    .actor(caller.0, caller.1)
                    .tenant(tenant.tenant_id.clone())
                    .resource("log_views", &tenant.tenant_id)
                    .outcome("failure")
                    .changes(
                        serde_json::json!({"scope": options.scope, "reason": "scope_not_allowed"})
                            .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "Only admins can manage shared tenant views".into(),
        ));
    }
    let key = scoped_setting_key(&tenant.tenant_id, options.scope, Some(&caller.0))?;
    for view in &mut config.views {
        if view.scope.is_some_and(|scope| scope != options.scope) {
            return Err(bad_request("Save one visibility scope at a time"));
        }
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
        .set_setting(&key, &encoded)
        .await
        .map_err(|error| crate::api_error::internal_legacy("log_views.save", error))?;
    state.audit.log(
        crate::audit::AuditEvent::new("log_view.update", "user")
            .actor(caller.0.clone(), caller.1)
            .tenant(tenant.tenant_id.clone())
            .resource("log_views", &tenant.tenant_id)
            .outcome("success")
            // Filter values may contain sensitive data. Record only IDs and counts.
            .changes(serde_json::json!({"view_ids": config.views.iter().map(|v| &v.id).collect::<Vec<_>>(), "count": config.views.len(), "scope": options.scope, "owner_id": if options.scope == ViewScope::Personal { Some(&caller.0) } else { None }}).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok(Json(VisibleLogViews {
        views: visible_views(config, options.scope),
        tenant_id: tenant.tenant_id,
    }))
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

    #[test]
    fn personal_views_are_scoped_by_both_user_and_tenant() {
        let key =
            |tenant, user| scoped_setting_key(tenant, ViewScope::Personal, Some(user)).unwrap();
        assert_ne!(key("default", "alice"), key("default", "bob"));
        assert_ne!(key("one", "alice"), key("two", "alice"));
        assert_ne!(key("one:two", "alice"), key("one", "two:alice"));
        assert_ne!(key("default", "alice"), setting_key("default"));
        assert_eq!(
            scoped_setting_key("default", ViewScope::Tenant, None).unwrap(),
            "log_views:default"
        );
        assert_eq!(
            scoped_setting_key("default", ViewScope::Personal, None)
                .unwrap_err()
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert!(scoped_setting_key("default", ViewScope::Personal, Some("")).is_err());
    }

    #[test]
    fn viewers_can_save_personal_views_but_cannot_change_shared_views() {
        for role in ["viewer", "write"] {
            assert!(may_save(ViewScope::Personal, role));
            assert!(!may_save(ViewScope::Tenant, role));
        }
        assert!(may_save(ViewScope::Tenant, "admin"));
        assert!(may_save(ViewScope::Personal, "admin"));
        assert!(!may_save(ViewScope::Personal, "anonymous"));
        assert!(!may_save(ViewScope::Personal, ""));
    }

    #[test]
    fn scope_defaults_to_shared_and_client_cannot_choose_an_owner() {
        assert_eq!(
            serde_json::from_str::<SaveScope>("{}").unwrap().scope,
            ViewScope::Tenant
        );
        assert!(
            serde_json::from_str::<SaveScope>(r#"{"scope":"personal","owner_id":"someone-else"}"#)
                .is_err()
        );
        let response = serde_json::to_value(visible_views(config(), ViewScope::Tenant)).unwrap();
        assert_eq!(response[0]["scope"], "tenant");
        // Existing clients may round-trip the shared scope metadata.
        let roundtrip: LogViews =
            serde_json::from_value(serde_json::json!({"views": response})).unwrap();
        assert_eq!(roundtrip.views[0].scope, Some(ViewScope::Tenant));
        assert!(
            serde_json::to_value(roundtrip).unwrap()["views"][0]
                .get("scope")
                .is_none()
        );
    }

    #[test]
    fn rejected_tenant_selection_never_silently_uses_default() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rush-tenant", "other".parse().unwrap());
        assert_eq!(
            ensure_requested_tenant(&headers, "default").unwrap_err().0,
            StatusCode::FORBIDDEN
        );
        ensure_requested_tenant(&headers, "other").unwrap();
    }
}
