use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};

use crate::models::saved_query::{
    SavedQuery, SavedQueryRequest, VISIBILITY_GROUP, VISIBILITY_PRIVATE, VISIBILITY_TENANT,
};
use crate::{AppState, TenantContext};

use super::log_views::{ensure_requested_tenant, validate_field};
use super::users::require_principal;

type ApiError = (StatusCode, String);
/// (user_id, username, display_name, tenant_id, role) from `require_auth`.
type Caller = (String, String, String, String, String);

/// The signed-in user plus the groups they belong to.
struct Viewer {
    caller: Caller,
    groups: Vec<String>,
}

impl Viewer {
    fn id(&self) -> &str {
        &self.caller.0
    }

    fn role(&self) -> &str {
        &self.caller.4
    }

    fn in_group(&self, group_id: &str) -> bool {
        self.groups.iter().any(|g| g == group_id)
    }
}

async fn require_viewer(state: &AppState, headers: &HeaderMap) -> Result<Viewer, ApiError> {
    let caller = require_principal(state, headers).await?;
    let groups = state
        .config_db
        .get_user_groups(&caller.0)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.groups", e))?;
    Ok(Viewer { caller, groups })
}

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

/// Sharing requires write access; anyone signed in can keep private queries.
fn may_share(role: &str) -> bool {
    matches!(role, "admin" | "write")
}

/// Whether the viewer may publish a query at this visibility. Group sharing is
/// limited to the viewer's own groups, admins included, so nobody can push
/// queries into a group they cannot see.
fn may_publish(viewer: &Viewer, visibility: &str, group_id: &str) -> bool {
    match visibility {
        VISIBILITY_TENANT => may_share(viewer.role()),
        VISIBILITY_GROUP => may_share(viewer.role()) && viewer.in_group(group_id),
        _ => true,
    }
}

fn is_visible(viewer: &Viewer, saved: &SavedQuery) -> bool {
    saved.owner_id == viewer.id()
        || saved.visibility == VISIBILITY_TENANT
        || (saved.visibility == VISIBILITY_GROUP && viewer.in_group(&saved.group_id))
}

fn may_modify(viewer: &Viewer, saved: &SavedQuery) -> bool {
    if saved.visibility == VISIBILITY_PRIVATE {
        saved.owner_id == viewer.id()
    } else {
        // Group queries reach this check only for members (see `is_visible`).
        viewer.role() == "admin" || (saved.owner_id == viewer.id() && may_share(viewer.role()))
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
    req.group_id = req.group_id.trim().into();
    match req.visibility.as_str() {
        VISIBILITY_GROUP => {
            if req.group_id.is_empty() || req.group_id.len() > 128 {
                return Err(bad_request("Choose a group to share with"));
            }
        }
        VISIBILITY_PRIVATE | VISIBILITY_TENANT => req.group_id.clear(),
        _ => return Err(bad_request("Visibility must be private, group or tenant")),
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
        "group_id": saved.group_id,
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
    let (owner, limit, message) = if visibility != VISIBILITY_PRIVATE {
        (
            None,
            MAX_SHARED_PER_TENANT,
            "This tenant has reached the limit of 500 shared queries at this visibility",
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
    viewer: &Viewer,
    tenant_id: &str,
    id: &str,
) -> Result<SavedQuery, ApiError> {
    let saved = state
        .config_db
        .get_saved_query(tenant_id, id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.get", e))?
        .ok_or_else(not_found)?;
    // A query the viewer cannot see is reported as missing, not forbidden, so
    // IDs do not reveal that it exists.
    if !is_visible(viewer, &saved) {
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
    let caller = require_principal(&state, &headers).await?;
    let groups = state
        .config_db
        .list_user_group_refs(&caller.0)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.groups", e))?;
    let group_ids: Vec<String> = groups.iter().map(|g| g.id.clone()).collect();
    let saved = state
        .config_db
        .list_saved_queries(&tenant.tenant_id, &caller.0, &group_ids)
        .await
        .map_err(|e| crate::api_error::internal_legacy("saved_queries.list", e))?;
    Ok(Json(serde_json::json!({
        "saved_queries": saved,
        // Sharing targets for the save form, and names for group badges.
        "groups": groups,
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
    let viewer = require_viewer(&state, &headers).await?;
    Ok(Json(
        load_visible(&state, &viewer, &tenant.tenant_id, &id).await?,
    ))
}

pub async fn create(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(mut req): Json<SavedQueryRequest>,
) -> Result<(StatusCode, Json<SavedQuery>), ApiError> {
    ensure_requested_tenant(&headers, &tenant.tenant_id)?;
    let viewer = require_viewer(&state, &headers).await?;
    let caller = &viewer.caller;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller,
        tenant_id: &tenant.tenant_id,
    };
    normalize(&mut req)?;
    if !may_publish(&viewer, &req.visibility, &req.group_id) {
        audit
            .log(
                "saved_query.create",
                "",
                "failure",
                serde_json::json!({"visibility": req.visibility, "group_id": req.group_id, "reason": "share_not_allowed"}),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "Sharing needs write access, and you can only share with groups you belong to".into(),
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
        group_id: req.group_id,
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
    let viewer = require_viewer(&state, &headers).await?;
    let caller = &viewer.caller;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller,
        tenant_id: &tenant.tenant_id,
    };
    normalize(&mut req)?;
    let existing = load_visible(&state, &viewer, &tenant.tenant_id, &id).await?;
    let changes_visibility =
        req.visibility != existing.visibility || req.group_id != existing.group_id;
    // Only the owner may change who can see a query, so an admin can never make
    // someone else's query vanish from the people it was shared with.
    let denied = if !may_modify(&viewer, &existing) {
        Some("not_owner")
    } else if changes_visibility && existing.owner_id != caller.0 {
        Some("visibility_owner_only")
    } else if changes_visibility && !may_publish(&viewer, &req.visibility, &req.group_id) {
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
        group_id: req.group_id,
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
    let viewer = require_viewer(&state, &headers).await?;
    let caller = &viewer.caller;
    let audit = AuditCtx {
        state: &state,
        headers: &headers,
        caller,
        tenant_id: &tenant.tenant_id,
    };
    let existing = load_visible(&state, &viewer, &tenant.tenant_id, &id).await?;
    if !may_modify(&viewer, &existing) {
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

    fn viewer(id: &str, role: &str, groups: &[&str]) -> Viewer {
        Viewer {
            caller: (
                id.into(),
                id.into(),
                String::new(),
                "default".into(),
                role.into(),
            ),
            groups: groups.iter().map(|g| g.to_string()).collect(),
        }
    }

    fn saved(owner: &str, visibility: &str) -> SavedQuery {
        SavedQuery {
            id: "q1".into(),
            tenant_id: "default".into(),
            name: "Errors".into(),
            description: String::new(),
            visibility: visibility.into(),
            group_id: if visibility == VISIBILITY_GROUP {
                "sre".into()
            } else {
                String::new()
            },
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
        assert!(is_visible(&viewer("alice", "viewer", &[]), &private));
        assert!(!is_visible(&viewer("bob", "admin", &[]), &private));
        assert!(!may_modify(&viewer("bob", "admin", &[]), &private));
        assert!(may_modify(&viewer("alice", "viewer", &[]), &private));
    }

    #[test]
    fn tenant_queries_are_visible_to_all_but_editable_by_writer_owner_or_admin() {
        let shared = saved("alice", VISIBILITY_TENANT);
        assert!(is_visible(&viewer("bob", "viewer", &[]), &shared));
        assert!(!may_modify(&viewer("bob", "write", &[]), &shared));
        assert!(may_modify(&viewer("bob", "admin", &[]), &shared));
        assert!(may_modify(&viewer("alice", "write", &[]), &shared));
        // An owner later demoted to viewer can no longer change what the tenant sees.
        assert!(!may_modify(&viewer("alice", "viewer", &[]), &shared));
    }

    #[test]
    fn group_queries_are_visible_only_to_members_and_the_owner() {
        let group = saved("alice", VISIBILITY_GROUP);
        assert!(is_visible(&viewer("bob", "viewer", &["sre"]), &group));
        assert!(!is_visible(&viewer("carol", "admin", &["admins"]), &group));
        // The owner keeps access even after leaving the group.
        assert!(is_visible(&viewer("alice", "write", &[]), &group));
        assert!(may_modify(&viewer("alice", "write", &[]), &group));
        assert!(!may_modify(&viewer("bob", "write", &["sre"]), &group));
        assert!(may_modify(&viewer("dave", "admin", &["sre"]), &group));
    }

    #[test]
    fn sharing_needs_write_access_and_group_membership() {
        assert!(may_publish(
            &viewer("a", "viewer", &[]),
            VISIBILITY_PRIVATE,
            ""
        ));
        assert!(!may_publish(
            &viewer("a", "viewer", &[]),
            VISIBILITY_TENANT,
            ""
        ));
        assert!(may_publish(
            &viewer("a", "write", &[]),
            VISIBILITY_TENANT,
            ""
        ));
        assert!(may_publish(
            &viewer("a", "write", &["sre"]),
            VISIBILITY_GROUP,
            "sre"
        ));
        assert!(!may_publish(
            &viewer("a", "viewer", &["sre"]),
            VISIBILITY_GROUP,
            "sre"
        ));
        assert!(!may_publish(
            &viewer("a", "write", &["sre"]),
            VISIBILITY_GROUP,
            "dba"
        ));
        assert!(!may_publish(
            &viewer("a", "admin", &["admins"]),
            VISIBILITY_GROUP,
            "sre"
        ));
        assert!(may_share("admin"));
        assert!(!may_share(""));
    }

    #[test]
    fn normalize_trims_and_defaults_to_private() {
        let mut req = request(serde_json::json!({
            "name": "  Checkout errors ",
            "description": "  Used during the Oct 12 incident ",
            "group_id": "ignored",
            "query": {"filters": [{"field": " service.name ", "op": " = ", "value": "checkout"}], "view_mode": "logs"}
        }));
        normalize(&mut req).unwrap();
        assert_eq!(req.name, "Checkout errors");
        assert_eq!(req.description, "Used during the Oct 12 incident");
        assert_eq!(req.visibility, VISIBILITY_PRIVATE);
        // A group only applies to group visibility.
        assert_eq!(req.group_id, "");
        assert_eq!(
            req.query.filters[0],
            SavedQueryFilter {
                field: "service.name".into(),
                op: "=".into(),
                value: "checkout".into()
            }
        );
        let mut group = request(
            serde_json::json!({"name": "x", "visibility": "group", "group_id": " sre ", "query": {}}),
        );
        normalize(&mut group).unwrap();
        assert_eq!(group.group_id, "sre");
    }

    #[test]
    fn normalize_rejects_invalid_requests() {
        let cases = [
            serde_json::json!({"name": "  ", "query": {}}),
            serde_json::json!({"name": "x", "visibility": "global", "query": {}}),
            serde_json::json!({"name": "x", "visibility": "group", "query": {}}),
            serde_json::json!({"name": "x", "description": "d".repeat(1001), "query": {}}),
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
        let mut query = saved("alice", VISIBILITY_GROUP);
        query.query.filters.push(SavedQueryFilter {
            field: "user.email".into(),
            op: "=".into(),
            value: "secret@example.com".into(),
        });
        query.query.search = "token=abc123".into();
        query.description = "why: incident 42".into();
        let summary = audit_summary(&query).to_string();
        assert!(!summary.contains("secret@example.com"));
        assert!(!summary.contains("abc123"));
        assert!(summary.contains("\"filter_count\":1"));
        assert!(summary.contains("\"group_id\":\"sre\""));
    }
}
