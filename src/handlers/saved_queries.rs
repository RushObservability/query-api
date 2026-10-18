use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};

use crate::models::saved_query::{
    SavedQuery, SavedQueryRequest, VISIBILITY_PRIVATE, VISIBILITY_TENANT,
};
use crate::{AppState, TenantContext};

use super::log_views::{ensure_requested_tenant, validate_field};
use super::users::require_auth;

type ApiError = (StatusCode, String);
/// (user_id, username, display_name, tenant_id, role) from `require_auth`.
type Caller = (String, String, String, String, String);

const MAX_PRIVATE_PER_USER: u64 = 200;
const MAX_SHARED_PER_TENANT: u64 = 500;
const MAX_FILTERS: usize = 30;
const VIEW_MODES: &[&str] = &["", "spans", "logs"];

fn bad_request(message: &str) -> ApiError {
    (StatusCode::BAD_REQUEST, message.into())
}

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, "saved query not found".into())
}

/// Sharing with the whole tenant requires write access; anyone signed in can
/// keep private queries.
fn may_share(role: &str) -> bool {
    matches!(role, "admin" | "write")
}

fn is_visible(caller: &Caller, saved: &SavedQuery) -> bool {
    saved.visibility == VISIBILITY_TENANT || saved.owner_id == caller.0
}

fn may_modify(caller: &Caller, saved: &SavedQuery) -> bool {
    if saved.visibility == VISIBILITY_TENANT {
        caller.4 == "admin" || (saved.owner_id == caller.0 && may_share(&caller.4))
    } else {
        saved.owner_id == caller.0
    }
}

/// Trim and validate a request in place.
fn normalize(req: &mut SavedQueryRequest) -> Result<(), ApiError> {
    req.name = req.name.trim().into();
    req.description = req.description.trim().into();
    req.query.search = req.query.search.trim().into();
    req.query.group_by = req.query.group_by.trim().into();
    if req.name.is_empty() || req.name.chars().count() > 120 {
        return Err(bad_request("Name must contain 1 to 120 characters"));
    }
    if req.description.chars().count() > 1000 {
        return Err(bad_request("Description must not exceed 1000 characters"));
    }
    if !matches!(
        req.visibility.as_str(),
        VISIBILITY_PRIVATE | VISIBILITY_TENANT
    ) {
        return Err(bad_request("Visibility must be private or tenant"));
    }
    let query = &mut req.query;
    if query.filters.len() > MAX_FILTERS {
        return Err(bad_request("A saved query can hold at most 30 filters"));
    }
    for filter in &mut query.filters {
        filter.field = filter.field.trim().into();
        filter.op = filter.op.trim().into();
        validate_field(&filter.field)?;
        if filter.op.is_empty() || filter.op.len() > 16 || filter.op.chars().any(char::is_control) {
            return Err(bad_request(
                "Filter operators must contain 1 to 16 characters",
            ));
        }
        if filter.value.len() > 1024 {
            return Err(bad_request("Filter values must not exceed 1024 characters"));
        }
    }
    if query.search.len() > 4096 {
        return Err(bad_request("Search text must not exceed 4096 characters"));
    }
    if !query.group_by.is_empty() {
        validate_field(&query.group_by)?;
    }
    // Up to one year, in minutes. Zero means "keep the viewer's current range".
    if query.time_preset > 525_600 {
        return Err(bad_request("Time range must not exceed one year"));
    }
    if !VIEW_MODES.contains(&query.view_mode.as_str()) {
        return Err(bad_request("View mode must be spans or logs"));
    }
    Ok(())
}

/// Filter values and search text can hold customer data, so audit entries
/// record only the label, visibility and shape of the query.
fn audit_summary(saved: &SavedQuery) -> serde_json::Value {
    serde_json::json!({
        "name": saved.name,
        "visibility": saved.visibility,
        "owner_id": saved.owner_id,
        "filter_count": saved.query.filters.len(),
        "has_search": !saved.query.search.is_empty(),
    })
}

struct AuditCtx<'a> {
    state: &'a AppState,
    headers: &'a HeaderMap,
    caller: &'a Caller,
    tenant_id: &'a str,
}

impl AuditCtx<'_> {
    async fn log(
        &self,
        action: &str,
        resource_id: &str,
        outcome: &str,
        changes: serde_json::Value,
    ) {
        self.state
            .audit
            .log(
                crate::audit::AuditEvent::new(action, "user")
                    .actor(self.caller.0.clone(), self.caller.1.clone())
                    .tenant(self.tenant_id.to_string())
                    .resource("saved_query", resource_id)
                    .outcome(outcome)
                    .changes(changes.to_string())
                    .context(crate::audit::actor_context_from_headers(self.headers)),
            )
            .await;
    }
}

async fn enforce_cap(
    state: &AppState,
    tenant_id: &str,
    visibility: &str,
    owner_id: &str,
) -> Result<(), ApiError> {
    let (owner, limit, message) = if visibility == VISIBILITY_TENANT {
        (
            None,
            MAX_SHARED_PER_TENANT,
            "This tenant has reached the limit of 500 shared queries",
        )
    } else {
        (
            Some(owner_id),
            MAX_PRIVATE_PER_USER,
            "You have reached the limit of 200 private saved queries",
        )
    };
    let count = state
        .config_db
        .count_saved_queries(tenant_id, visibility, owner)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.count", e))?;
    if count >= limit {
        return Err(bad_request(message));
    }
    Ok(())
}

async fn load_visible(
    state: &AppState,
    caller: &Caller,
    tenant_id: &str,
    id: &str,
) -> Result<SavedQuery, ApiError> {
    let saved = state
        .config_db
        .get_saved_query(tenant_id, id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.get", e))?
        .ok_or_else(not_found)?;
    // Someone else's private query is reported as missing, not forbidden, so
    // IDs do not reveal that it exists.
    if !is_visible(caller, &saved) {
        return Err(not_found());
    }
    Ok(saved)
}

pub async fn list(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = require_auth(&state, &headers).await?;
    let saved = state
        .config_db
        .list_saved_queries(&tenant.tenant_id, &caller.0)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.list", e))?;
    Ok(Json(serde_json::json!({
        "saved_queries": saved,
        "tenant_id": tenant.tenant_id,
    })))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SavedQuery>, ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = require_auth(&state, &headers).await?;
    Ok(Json(
        load_visible(&state, &caller, &tenant.tenant_id, &id).await?,
    ))
}

pub async fn create(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(mut req): Json<SavedQueryRequest>,
) -> Result<(StatusCode, Json<SavedQuery>), ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = require_auth(&state, &headers).await?;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller: &caller,
        tenant_id: &tenant.tenant_id,
    };
    normalize(&mut req)?;
    if req.visibility == VISIBILITY_TENANT && !may_share(&caller.4) {
        audit
            .log(
                "saved_query.create",
                "",
                "failure",
                serde_json::json!({"visibility": req.visibility, "reason": "share_not_allowed"}),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "Write access is required to share queries with the tenant".into(),
        ));
    }
    enforce_cap(&state, &tenant.tenant_id, &req.visibility, &caller.0).await?;

    let now = crate::clickhouse_config::ConfigDb::saved_query_timestamp();
    let saved = SavedQuery {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.tenant_id.clone(),
        name: req.name,
        description: req.description,
        visibility: req.visibility,
        owner_id: caller.0.clone(),
        owner_name: if caller.2.trim().is_empty() {
            caller.1.clone()
        } else {
            caller.2.clone()
        },
        query: req.query,
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .write_saved_query(&saved, false)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.create", e))?;
    audit
        .log(
            "saved_query.create",
            &saved.id,
            "success",
            audit_summary(&saved),
        )
        .await;
    Ok((StatusCode::CREATED, Json(saved)))
}

pub async fn update(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(mut req): Json<SavedQueryRequest>,
) -> Result<Json<SavedQuery>, ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = require_auth(&state, &headers).await?;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller: &caller,
        tenant_id: &tenant.tenant_id,
    };
    normalize(&mut req)?;
    let existing = load_visible(&state, &caller, &tenant.tenant_id, &id).await?;
    let changes_visibility = req.visibility != existing.visibility;
    // Only the owner may move a query between private and shared, so an admin
    // can never make someone else's query vanish from the tenant into a
    // private list they cannot reach.
    let denied = if !may_modify(&caller, &existing) {
        Some("not_owner")
    } else if changes_visibility && existing.owner_id != caller.0 {
        Some("visibility_owner_only")
    } else if req.visibility == VISIBILITY_TENANT && !may_share(&caller.4) {
        Some("share_not_allowed")
    } else {
        None
    };
    if let Some(reason) = denied {
        audit
            .log(
                "saved_query.update",
                &id,
                "failure",
                serde_json::json!({"reason": reason, "visibility": req.visibility}),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "You do not have permission to change this saved query".into(),
        ));
    }
    if changes_visibility {
        enforce_cap(
            &state,
            &tenant.tenant_id,
            &req.visibility,
            &existing.owner_id,
        )
        .await?;
    }

    let saved = SavedQuery {
        name: req.name,
        description: req.description,
        visibility: req.visibility,
        query: req.query,
        updated_at: crate::clickhouse_config::ConfigDb::saved_query_timestamp(),
        ..existing.clone()
    };
    state
        .config_db
        .write_saved_query(&saved, false)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.update", e))?;
    audit
        .log(
            "saved_query.update",
            &id,
            "success",
            serde_json::json!({"before": audit_summary(&existing), "after": audit_summary(&saved)}),
        )
        .await;
    Ok(Json(saved))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let caller = require_auth(&state, &headers).await?;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller: &caller,
        tenant_id: &tenant.tenant_id,
    };
    let existing = load_visible(&state, &caller, &tenant.tenant_id, &id).await?;
    if !may_modify(&caller, &existing) {
        audit
            .log(
                "saved_query.delete",
                &id,
                "failure",
                serde_json::json!({"reason": "not_owner", "visibility": existing.visibility}),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "You do not have permission to delete this saved query".into(),
        ));
    }
    let deleted = SavedQuery {
        updated_at: crate::clickhouse_config::ConfigDb::saved_query_timestamp(),
        ..existing.clone()
    };
    state
        .config_db
        .write_saved_query(&deleted, true)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.delete", e))?;
    audit
        .log(
            "saved_query.delete",
            &id,
            "success",
            audit_summary(&existing),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::saved_query::{SavedQueryBody, SavedQueryFilter};

    fn caller(id: &str, role: &str) -> Caller {
        (
            id.into(),
            id.into(),
            String::new(),
            "default".into(),
            role.into(),
        )
    }

    fn saved(owner: &str, visibility: &str) -> SavedQuery {
        SavedQuery {
            id: "q1".into(),
            tenant_id: "default".into(),
            name: "Errors".into(),
            description: String::new(),
            visibility: visibility.into(),
            owner_id: owner.into(),
            owner_name: owner.into(),
            query: SavedQueryBody::default(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    fn request(json: serde_json::Value) -> SavedQueryRequest {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn private_queries_are_visible_only_to_their_owner() {
        let private = saved("alice", VISIBILITY_PRIVATE);
        assert!(is_visible(&caller("alice", "viewer"), &private));
        assert!(!is_visible(&caller("bob", "admin"), &private));
        assert!(!may_modify(&caller("bob", "admin"), &private));
        assert!(may_modify(&caller("alice", "viewer"), &private));
    }

    #[test]
    fn shared_queries_are_visible_to_all_but_editable_by_writer_owner_or_admin() {
        let shared = saved("alice", VISIBILITY_TENANT);
        assert!(is_visible(&caller("bob", "viewer"), &shared));
        assert!(!may_modify(&caller("bob", "write"), &shared));
        assert!(may_modify(&caller("bob", "admin"), &shared));
        assert!(may_modify(&caller("alice", "write"), &shared));
        // An owner later demoted to viewer can no longer change what the tenant sees.
        assert!(!may_modify(&caller("alice", "viewer"), &shared));
    }

    #[test]
    fn only_writers_may_share() {
        assert!(may_share("admin"));
        assert!(may_share("write"));
        assert!(!may_share("viewer"));
        assert!(!may_share(""));
    }

    #[test]
    fn normalize_trims_and_defaults_to_private() {
        let mut req = request(serde_json::json!({
            "name": "  Checkout errors ",
            "query": {"filters": [{"field": " service.name ", "op": " = ", "value": "checkout"}], "view_mode": "logs"}
        }));
        normalize(&mut req).unwrap();
        assert_eq!(req.name, "Checkout errors");
        assert_eq!(req.visibility, VISIBILITY_PRIVATE);
        assert_eq!(
            req.query.filters[0],
            SavedQueryFilter {
                field: "service.name".into(),
                op: "=".into(),
                value: "checkout".into()
            }
        );
    }

    #[test]
    fn normalize_rejects_invalid_requests() {
        let cases = [
            serde_json::json!({"name": "  ", "query": {}}),
            serde_json::json!({"name": "x", "visibility": "global", "query": {}}),
            serde_json::json!({"name": "x", "query": {"view_mode": "metrics"}}),
            serde_json::json!({"name": "x", "query": {"time_preset": 600_000}}),
            serde_json::json!({"name": "x", "query": {"filters": [{"field": "", "op": "=", "value": "v"}]}}),
            serde_json::json!({"name": "x", "query": {"filters": [{"field": "a", "op": "", "value": "v"}]}}),
            serde_json::json!({"name": "x".repeat(121), "query": {}}),
        ];
        for case in cases {
            assert!(normalize(&mut request(case.clone())).is_err(), "{case}");
        }
        let too_many: Vec<_> = (0..31)
            .map(|i| serde_json::json!({"field": format!("f{i}"), "op": "=", "value": "v"}))
            .collect();
        assert!(
            normalize(&mut request(
                serde_json::json!({"name": "x", "query": {"filters": too_many}})
            ))
            .is_err()
        );
    }

    #[test]
    fn clients_cannot_choose_owner_or_tenant() {
        for field in ["owner_id", "tenant_id", "id"] {
            let mut json = serde_json::json!({"name": "x", "query": {}});
            json[field] = serde_json::json!("someone-else");
            assert!(
                serde_json::from_value::<SavedQueryRequest>(json).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn audit_summary_omits_filter_values_and_search_text() {
        let mut query = saved("alice", VISIBILITY_TENANT);
        query.query.filters.push(SavedQueryFilter {
            field: "user.email".into(),
            op: "=".into(),
            value: "secret@example.com".into(),
        });
        query.query.search = "token=abc123".into();
        let summary = audit_summary(&query).to_string();
        assert!(!summary.contains("secret@example.com"));
        assert!(!summary.contains("abc123"));
        assert!(summary.contains("\"filter_count\":1"));
    }
}
