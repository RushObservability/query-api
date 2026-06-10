//! Metric firewall CRUD. Admin-only. Mutations reload the compiled firewall in
//! the live writer immediately (a background task also refreshes periodically).

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use std::sync::Arc;

use crate::AppState;
use crate::clickhouse_config::MetricFirewallRule;
use crate::handlers::users::require_admin;

#[derive(serde::Deserialize)]
pub struct FirewallRuleInput {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// "allow" | "block" | "drop_label"
    pub action: String,
    #[serde(default)]
    pub metric_pattern: String,
    #[serde(default)]
    pub metric_regex: bool,
    #[serde(default)]
    pub match_label_key: String,
    #[serde(default)]
    pub match_label_value: String,
    #[serde(default)]
    pub match_label_value_regex: bool,
    #[serde(default)]
    pub drop_label_pattern: String,
    #[serde(default)]
    pub drop_label_regex: bool,
}

fn default_true() -> bool { true }

fn b(v: bool) -> u8 { if v { 1 } else { 0 } }

/// Validate an input and (on success) return a storage row with the given id/created_at.
fn validate(input: &FirewallRuleInput, id: String, created_at: String) -> Result<MetricFirewallRule, (StatusCode, String)> {
    if input.name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name is required".into()));
    }
    if input.action != "allow" && input.action != "block" && input.action != "drop_label" {
        return Err((StatusCode::BAD_REQUEST, "action must be 'allow', 'block' or 'drop_label'".into()));
    }
    // Validate any regexes so the user gets immediate feedback.
    let check = |pat: &str, is_re: bool, label: &str| -> Result<(), (StatusCode, String)> {
        if is_re && !pat.is_empty() {
            regex::Regex::new(pat).map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid {label} regex: {e}")))?;
        }
        Ok(())
    };
    check(&input.metric_pattern, input.metric_regex, "metric")?;
    check(&input.match_label_value, input.match_label_value_regex, "label value")?;
    check(&input.drop_label_pattern, input.drop_label_regex, "drop label")?;

    if input.action == "drop_label" && input.drop_label_pattern.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "drop_label rules require a drop label pattern".into()));
    }
    // An allow rule with no criteria would exempt every series and silently
    // neuter all block rules — allowing everything is already the default.
    if input.action == "allow" && input.metric_pattern.is_empty() && input.match_label_key.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "allow rules need a metric pattern and/or a label match (allowing everything is the default)".into()));
    }

    Ok(MetricFirewallRule {
        id,
        name: input.name.trim().to_string(),
        enabled: b(input.enabled),
        action: input.action.clone(),
        metric_pattern: input.metric_pattern.clone(),
        metric_regex: b(input.metric_regex),
        match_label_key: input.match_label_key.clone(),
        match_label_value: input.match_label_value.clone(),
        match_label_value_regex: b(input.match_label_value_regex),
        drop_label_pattern: input.drop_label_pattern.clone(),
        drop_label_regex: b(input.drop_label_regex),
        created_at,
    })
}

/// Invariant enforced on every mutation (create/update/delete), evaluated on
/// the post-mutation rule set: an enabled block rule with no match criteria
/// (catch-all) is only permitted while at least one enabled allow rule exists.
/// This is what makes allowlist mode possible without ever letting the
/// firewall silently drop every series — including via deleting or disabling
/// the last allow rule.
fn forbid_block_everything(rules_after: &[MetricFirewallRule]) -> Result<(), (StatusCode, String)> {
    let catch_all_block = rules_after.iter().any(|r| {
        r.enabled == 1
            && r.action != "drop_label"
            && r.action != "allow"
            && r.metric_pattern.is_empty()
            && r.match_label_key.is_empty()
    });
    if catch_all_block && !rules_after.iter().any(|r| r.enabled == 1 && r.action == "allow") {
        return Err((
            StatusCode::BAD_REQUEST,
            "this change would leave the firewall blocking every series: a block rule with no match criteria requires at least one enabled allow rule (allowlist mode)".into(),
        ));
    }
    Ok(())
}

/// Recompile rules and hot-swap them into the live writer's firewall.
async fn reload(state: &AppState) {
    match state.config_db.compiled_metric_firewall().await {
        Ok(fw) => {
            if let Ok(mut g) = state.writer.firewall.write() {
                *g = Arc::new(fw);
            }
        }
        Err(e) => tracing::warn!(error = %e, "failed to reload metric firewall"),
    }
}

/// GET /api/v1/metric-firewall
pub async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let rules = state.config_db.list_metric_firewall().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    Ok(Json(serde_json::json!({ "rules": rules })))
}

/// POST /api/v1/metric-firewall
pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<FirewallRuleInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let rule = validate(&input, id, created_at)?;
    let mut after = state.config_db.list_metric_firewall().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    after.push(rule.clone());
    forbid_block_everything(&after)?;
    state.config_db.upsert_metric_firewall(&rule).await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    reload(&state).await;
    Ok((StatusCode::CREATED, Json(rule)))
}

/// PUT /api/v1/metric-firewall/{id}
pub async fn update(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<FirewallRuleInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let existing = state.config_db.list_metric_firewall().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    // Preserve the original created_at if the rule exists.
    let created_at = existing.iter().find(|r| r.id == id).map(|r| r.created_at.clone())
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string());
    let rule = validate(&input, id.clone(), created_at)?;
    // Check the invariant against the post-update rule set (the old version of
    // this rule replaced by the new one), so e.g. disabling the last allow rule
    // while a catch-all block exists is rejected.
    let mut after: Vec<MetricFirewallRule> = existing.into_iter().filter(|r| r.id != id).collect();
    after.push(rule.clone());
    forbid_block_everything(&after)?;
    state.config_db.upsert_metric_firewall(&rule).await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    reload(&state).await;
    Ok((StatusCode::OK, Json(rule)))
}

/// DELETE /api/v1/metric-firewall/{id}
pub async fn delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    // Deleting the last allow rule while a catch-all block exists would leave
    // the firewall blocking everything — reject it (delete the block first).
    let after: Vec<MetricFirewallRule> = state.config_db.list_metric_firewall().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?
        .into_iter().filter(|r| r.id != id).collect();
    forbid_block_everything(&after)?;
    let deleted = state.config_db.delete_metric_firewall(&id).await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "rule not found".into()));
    }
    reload(&state).await;
    Ok(StatusCode::NO_CONTENT)
}
