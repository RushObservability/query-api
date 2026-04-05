use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use regex::Regex;

use crate::AppState;
use crate::models::custom_skills::{
    CreateCustomSkillRequest, CustomSkill, UpdateCustomSkillRequest,
};

/// Tools that users are allowed to reference in `allowed_tools`.
const KNOWN_TOOLS: &[&str] = &[
    "search_logs",
    "query_traces",
    "get_trace",
    "query_metrics",
    "list_services",
    "service_dependencies",
    "list_deploys",
    "get_anomaly_context",
    "get_argocd_app",
    "kube_describe",
    "kube_events",
    "load_skill",
];

/// Names a user cannot pick (reserved for the platform / providers).
const RESERVED_NAMES: &[&str] = &[
    "rush",
    "system",
    "assistant",
    "anthropic",
    "claude",
    "openai",
];

/// Role-marker tags that must never appear inside skill content, to prevent
/// prompt-injection of conversational boundaries.
const ROLE_MARKERS: &[&str] = &[
    "<system>",
    "</system>",
    "<user>",
    "</user>",
    "<assistant>",
    "</assistant>",
    "<functions>",
    "</functions>",
    "<tool_use>",
    "</tool_use>",
    "<tool_result>",
    "</tool_result>",
];

/// Validate skill fields before writing to the database.
/// Returns a human-readable error string on failure.
fn validate_skill_fields(
    name: &str,
    title: &str,
    description: &str,
    content: &str,
    allowed_tools: &[String],
) -> Result<(), String> {
    // name regex
    let name_re = Regex::new(r"^[a-z0-9_]{1,64}$").unwrap();
    if !name_re.is_match(name) {
        return Err(
            "name must match ^[a-z0-9_]{1,64}$ (lowercase letters, digits, underscore, 1-64 chars)"
                .to_string(),
        );
    }

    // reserved names
    if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(name)) {
        return Err(format!(
            "name '{}' is reserved and cannot be used",
            name
        ));
    }

    // title length
    if title.is_empty() || title.chars().count() > 128 {
        return Err("title must be 1..=128 characters".to_string());
    }

    // description length
    if description.is_empty() || description.chars().count() > 1024 {
        return Err("description must be 1..=1024 characters".to_string());
    }

    // content length
    if content.is_empty() || content.chars().count() > 25000 {
        return Err("content must be 1..=25000 characters".to_string());
    }

    // role markers in content
    let content_lower = content.to_lowercase();
    let offenders: Vec<&str> = ROLE_MARKERS
        .iter()
        .copied()
        .filter(|m| content_lower.contains(&m.to_lowercase()))
        .collect();
    if !offenders.is_empty() {
        return Err(format!(
            "content contains forbidden role-marker tags: {}",
            offenders.join(", ")
        ));
    }

    // allowed_tools must all be in KNOWN_TOOLS
    for tool in allowed_tools {
        if !KNOWN_TOOLS.iter().any(|k| *k == tool.as_str()) {
            return Err(format!(
                "unknown tool '{}' in allowed_tools. Known tools: {}",
                tool,
                KNOWN_TOOLS.join(", ")
            ));
        }
    }

    Ok(())
}

pub async fn list_custom_skills(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let skills = state
        .config_db
        .list_custom_skills()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    Ok(Json(serde_json::json!({ "skills": skills })))
}

pub async fn get_custom_skill(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let skill = state
        .config_db
        .get_custom_skill(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "custom skill not found".to_string()))?;
    Ok(Json(skill))
}

pub async fn create_custom_skill(
    State(state): State<AppState>,
    Json(req): Json<CreateCustomSkillRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_skill_fields(
        &req.name,
        &req.title,
        &req.description,
        &req.content,
        &req.allowed_tools,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // Uniqueness on name (friendlier than the raw SQLite constraint error)
    let existing = state
        .config_db
        .get_custom_skill_by_name(&req.name)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    if existing.is_some() {
        return Err((
            StatusCode::CONFLICT,
            format!("a custom skill named '{}' already exists", req.name),
        ));
    }

    let created = state
        .config_db
        .create_custom_skill(&req, "")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    Ok((StatusCode::CREATED, Json::<CustomSkill>(created)))
}

pub async fn update_custom_skill(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateCustomSkillRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Fetch the existing skill so we can validate against its immutable name.
    let existing = state
        .config_db
        .get_custom_skill(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "custom skill not found".to_string()))?;

    validate_skill_fields(
        &existing.name,
        &req.title,
        &req.description,
        &req.content,
        &req.allowed_tools,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let updated = state
        .config_db
        .update_custom_skill(&id, &req)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "custom skill not found".to_string()))?;

    Ok(Json(updated))
}

pub async fn delete_custom_skill(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_custom_skill(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "custom skill not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}
