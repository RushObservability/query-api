use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::Value;

use crate::handlers::users::require_admin;
use crate::llm_gateway::LlmCaller;
use crate::llm_providers::{
    LlmModel, LlmProviderRouting, LlmProviderSecret, default_base_url, default_model_setting_key,
    key_hint, normalize_reasoning, provider_kind_valid, sre_agent_model_compatible,
    validate_identifier,
};
use crate::{AppState, TenantContext};

#[derive(Debug, Deserialize)]
pub struct ProviderBody {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub routing: LlmProviderRouting,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ModelBody {
    pub provider_id: String,
    pub name: String,
    pub model_id: String,
    #[serde(default)]
    pub reasoning: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

fn invalid(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.into())
}

fn internal(error: impl std::fmt::Display) -> (StatusCode, String) {
    crate::api_error::internal_legacy_with_status(
        StatusCode::INTERNAL_SERVER_ERROR,
        "llm_provider_config",
        error,
    )
}

fn validate_name(name: &str, field: &str) -> Result<String, (StatusCode, String)> {
    let value = name.trim();
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{field} must be between 1 and 128 characters"
        )));
    }
    Ok(value.to_string())
}

fn normalize_provider_body(mut body: ProviderBody) -> Result<ProviderBody, (StatusCode, String)> {
    body.name = validate_name(&body.name, "provider name")?;
    body.kind = body.kind.trim().to_ascii_lowercase();
    if !provider_kind_valid(&body.kind) {
        return Err(invalid("unsupported LLM provider type"));
    }
    body.routing.region = body.routing.region.trim().to_ascii_lowercase();
    if !body.routing.region.is_empty()
        && (body.routing.region.len() > 64
            || !body
                .routing
                .region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'))
    {
        return Err(invalid("AWS region is invalid"));
    }
    if !matches!(body.routing.data_collection.as_str(), "allow" | "deny") {
        return Err(invalid("data_collection must be allow or deny"));
    }
    for values in [
        &mut body.routing.allowed_providers,
        &mut body.routing.provider_order,
    ] {
        if values.len() > 32 {
            return Err(invalid(
                "provider routing lists may contain at most 32 entries",
            ));
        }
        let mut normalized = Vec::with_capacity(values.len());
        for value in values.iter_mut() {
            *value = value.trim().to_ascii_lowercase();
            validate_identifier(value, "provider routing entry").map_err(invalid)?;
            if !normalized.contains(value) {
                normalized.push(value.clone());
            }
        }
        // OpenRouter uses provider_order as an ordered preference list, so
        // preserve the administrator's order while removing duplicates.
        *values = normalized;
    }
    body.base_url = body.base_url.trim().to_string();
    if body.base_url.is_empty() {
        body.base_url = default_base_url(&body.kind, &body.routing.region)
            .ok_or_else(|| invalid("base_url is required for this provider type"))?;
    }
    if body.base_url.len() > 2_048 {
        return Err(invalid("base_url is too long"));
    }
    let parsed = url::Url::parse(&body.base_url).map_err(|_| invalid("base_url is invalid"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(
            "base_url must be an HTTP(S) URL without credentials, query, or fragment",
        ));
    }
    body.api_key = body.api_key.trim().to_string();
    if body.api_key.len() > 16_384 || body.api_key.chars().any(char::is_control) {
        return Err(invalid("provider credential is invalid"));
    }
    Ok(body)
}

fn normalize_model_body(mut body: ModelBody) -> Result<ModelBody, (StatusCode, String)> {
    validate_identifier(body.provider_id.trim(), "provider_id").map_err(invalid)?;
    body.provider_id = body.provider_id.trim().to_string();
    body.name = validate_name(&body.name, "model name")?;
    body.model_id = body.model_id.trim().to_string();
    validate_identifier(&body.model_id, "provider model id").map_err(invalid)?;
    body.reasoning = normalize_reasoning(&body.reasoning).map_err(invalid)?;
    Ok(body)
}

fn require_sre_compatible_model(
    provider_kind: &str,
    model_id: &str,
) -> Result<(), (StatusCode, String)> {
    if sre_agent_model_compatible(provider_kind, model_id) {
        Ok(())
    } else {
        Err(invalid(
            "model is not compatible with SRE investigations; choose a chat model that supports streaming and function tools",
        ))
    }
}

pub async fn list_providers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let providers = state
        .config_db
        .list_llm_providers(&caller.3)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "providers": providers })))
}

pub async fn create_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ProviderBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let body = normalize_provider_body(body)?;
    if body.api_key.is_empty() {
        return Err(invalid("provider credential is required"));
    }
    let now = chrono::Utc::now().to_rfc3339();
    let provider = LlmProviderSecret {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: caller.3.clone(),
        name: body.name,
        kind: body.kind,
        base_url: body.base_url,
        key_hint: key_hint(&body.api_key),
        api_key: body.api_key,
        routing: body.routing,
        enabled: body.enabled,
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .upsert_llm_provider(&provider)
        .await
        .map_err(internal)?;
    audit_provider(
        &state,
        &headers,
        &caller,
        "llm_provider.create",
        &provider,
        true,
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(crate::llm_providers::LlmProviderResponse::from(provider)),
    ))
}

pub async fn update_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<ProviderBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    validate_identifier(&id, "provider id").map_err(invalid)?;
    let current = state
        .config_db
        .get_llm_provider_secret(&caller.3, &id)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "LLM provider not found".into()))?;
    let body = normalize_provider_body(body)?;
    let key_updated = !body.api_key.is_empty();
    let api_key = if key_updated {
        body.api_key
    } else {
        current.api_key
    };
    let provider = LlmProviderSecret {
        id,
        tenant_id: caller.3.clone(),
        name: body.name,
        kind: body.kind,
        base_url: body.base_url,
        key_hint: if key_updated {
            key_hint(&api_key)
        } else {
            current.key_hint
        },
        api_key,
        routing: body.routing,
        enabled: body.enabled,
        created_at: current.created_at,
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    state
        .config_db
        .upsert_llm_provider(&provider)
        .await
        .map_err(internal)?;
    audit_provider(
        &state,
        &headers,
        &caller,
        "llm_provider.update",
        &provider,
        key_updated,
    )
    .await;
    Ok(Json(crate::llm_providers::LlmProviderResponse::from(
        provider,
    )))
}

pub async fn delete_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    validate_identifier(&id, "provider id").map_err(invalid)?;
    let models = state
        .config_db
        .list_llm_models(&caller.3, false)
        .await
        .map_err(internal)?;
    if models.iter().any(|model| model.provider_id == id) {
        return Err((
            StatusCode::CONFLICT,
            "remove models assigned to this provider first".into(),
        ));
    }
    state
        .config_db
        .delete_llm_provider(&caller.3, &id)
        .await
        .map_err(internal)?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("llm_provider.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("llm_provider", &id)
                .outcome("success")
                .changes(serde_json::json!({ "provider_id": id }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn discover_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    validate_identifier(&id, "provider id").map_err(invalid)?;
    let provider = state
        .config_db
        .get_llm_provider_secret(&caller.3, &id)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "LLM provider not found".into()))?;
    let models = state
        .llm_gateway
        .list_configured_provider_models(&LlmCaller::new(&caller.0, &caller.3), &provider)
        .await?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("llm_provider.models_read", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("llm_provider", &id)
                .outcome("success")
                .changes(serde_json::json!({ "model_count": models.len() }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(serde_json::json!({ "models": models })))
}

pub async fn list_models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let models = state
        .config_db
        .list_llm_models(&caller.3, false)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "models": models })))
}

pub async fn create_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ModelBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let body = normalize_model_body(body)?;
    let provider = state
        .config_db
        .get_llm_provider_secret(&caller.3, &body.provider_id)
        .await
        .map_err(internal)?
        .ok_or(invalid("provider_id does not exist"))?;
    require_sre_compatible_model(&provider.kind, &body.model_id)?;
    let now = chrono::Utc::now().to_rfc3339();
    let model = LlmModel {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: caller.3.clone(),
        provider_id: provider.id,
        name: body.name,
        model_id: body.model_id,
        reasoning: body.reasoning,
        enabled: body.enabled,
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .upsert_llm_model(&model)
        .await
        .map_err(internal)?;
    audit_model(&state, &headers, &caller, "llm_model.create", &model).await;
    Ok((StatusCode::CREATED, Json(model)))
}

pub async fn update_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<ModelBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let body = normalize_model_body(body)?;
    let current = state
        .config_db
        .get_llm_model(&caller.3, &id)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "LLM model not found".into()))?;
    let provider = state
        .config_db
        .get_llm_provider_secret(&caller.3, &body.provider_id)
        .await
        .map_err(internal)?
        .ok_or(invalid("provider_id does not exist"))?;
    require_sre_compatible_model(&provider.kind, &body.model_id)?;
    let model = LlmModel {
        id,
        tenant_id: caller.3.clone(),
        provider_id: body.provider_id,
        name: body.name,
        model_id: body.model_id,
        reasoning: body.reasoning,
        enabled: body.enabled,
        created_at: current.created_at,
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    state
        .config_db
        .upsert_llm_model(&model)
        .await
        .map_err(internal)?;
    audit_model(&state, &headers, &caller, "llm_model.update", &model).await;
    Ok(Json(model))
}

pub async fn delete_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    validate_identifier(&id, "model id").map_err(invalid)?;
    let clears_default = state
        .config_db
        .get_setting(&default_model_setting_key(&caller.3))
        .await
        .map_err(internal)?
        .as_deref()
        == Some(id.as_str());
    state
        .config_db
        .delete_llm_model(&caller.3, &id)
        .await
        .map_err(internal)?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("llm_model.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("llm_model", &id)
                .outcome("success")
                .changes(
                    serde_json::json!({ "model_id": id, "clears_default": clears_default })
                        .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    if clears_default {
        state
            .config_db
            .set_setting(&default_model_setting_key(&caller.3), "")
            .await
            .map_err(internal)?;
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("settings.update", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("setting", "sre_agent_model")
                    .outcome("success")
                    .changes(
                        serde_json::json!({ "key": "sre_agent_model", "value": "" }).to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Private data-plane endpoint used by sre-agent. The request uses a Rush model
/// ID. This handler resolves the provider credential and never serializes it.
pub async fn internal_chat(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(mut body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    if !crate::internal_auth::sre_agent_token_matches(&headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".into(),
        ));
    }
    let models = state
        .config_db
        .list_llm_models(&tenant.tenant_id, true)
        .await
        .map_err(internal)?;
    let requested = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let default_model = state
        .config_db
        .get_setting(&default_model_setting_key(&tenant.tenant_id))
        .await
        .map_err(internal)?
        .unwrap_or_default();
    let model = if requested.is_empty() {
        models
            .iter()
            .find(|model| model.id == default_model)
            .or_else(|| models.first())
            .ok_or((
                StatusCode::SERVICE_UNAVAILABLE,
                "No enabled LLM model is configured for this tenant".into(),
            ))?
    } else {
        models
            .iter()
            .find(|model| model.id == requested)
            .ok_or_else(|| invalid("selected model is not available for this tenant"))?
    };
    if let Some(effort) = body.get("reasoning_effort").and_then(Value::as_str) {
        if !effort.is_empty() && !model.reasoning.iter().any(|level| level == effort) {
            return Err(invalid(
                "reasoning effort is not allowed for the selected model",
            ));
        }
    }
    if model.reasoning.is_empty() {
        if let Some(object) = body.as_object_mut() {
            object.remove("reasoning_effort");
        }
    }
    let provider = state
        .config_db
        .get_llm_provider_secret(&tenant.tenant_id, &model.provider_id)
        .await
        .map_err(internal)?
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "The selected LLM provider no longer exists".into(),
        ))?;
    require_sre_compatible_model(&provider.kind, &model.model_id)?;
    state
        .llm_gateway
        .stream_agent_chat(
            &LlmCaller::new("sre-agent", &tenant.tenant_id),
            &provider,
            &model.model_id,
            body,
        )
        .await
}

pub async fn internal_ready(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !crate::internal_auth::sre_agent_token_matches(&headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".into(),
        ));
    }
    let models = state
        .config_db
        .list_llm_models(&tenant.tenant_id, true)
        .await
        .map_err(internal)?;
    let providers = state
        .config_db
        .list_llm_providers(&tenant.tenant_id)
        .await
        .map_err(internal)?;
    let configured = models.iter().any(|model| {
        providers.iter().any(|provider| {
            provider.id == model.provider_id
                && provider.enabled
                && sre_agent_model_compatible(&provider.kind, &model.model_id)
        })
    });
    Ok(Json(serde_json::json!({ "configured": configured })))
}

async fn audit_provider(
    state: &AppState,
    headers: &HeaderMap,
    caller: &(String, String, String, String, String),
    action: &str,
    provider: &LlmProviderSecret,
    key_updated: bool,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(action, "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("llm_provider", &provider.id)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "name": provider.name,
                        "kind": provider.kind,
                        "base_url": provider.base_url,
                        "enabled": provider.enabled,
                        "credential_updated": key_updated,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

async fn audit_model(
    state: &AppState,
    headers: &HeaderMap,
    caller: &(String, String, String, String, String),
    action: &str,
    model: &LlmModel,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(action, "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("llm_model", &model.id)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "provider_id": model.provider_id,
                        "name": model.name,
                        "provider_model_id": model.model_id,
                        "reasoning": model.reasoning,
                        "enabled": model.enabled,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider_body() -> ProviderBody {
        ProviderBody {
            name: "Primary router".into(),
            kind: "openrouter".into(),
            base_url: String::new(),
            api_key: "secret".into(),
            routing: LlmProviderRouting {
                allowed_providers: vec!["Anthropic".into(), "anthropic".into()],
                provider_order: vec![
                    "Amazon-Bedrock".into(),
                    "Anthropic".into(),
                    "amazon-bedrock".into(),
                ],
                allow_fallbacks: true,
                zero_data_retention: true,
                data_collection: "deny".into(),
                region: String::new(),
            },
            enabled: true,
        }
    }

    #[test]
    fn routing_normalization_preserves_preference_order() {
        let body = normalize_provider_body(provider_body()).unwrap();
        assert_eq!(body.routing.allowed_providers, vec!["anthropic"]);
        assert_eq!(
            body.routing.provider_order,
            vec!["amazon-bedrock", "anthropic"]
        );
    }

    #[test]
    fn provider_urls_cannot_embed_credentials() {
        let mut body = provider_body();
        body.base_url = "https://token@example.com/v1".into();
        assert!(normalize_provider_body(body).is_err());
    }

    #[test]
    fn responses_only_openai_pro_models_cannot_be_configured() {
        assert!(require_sre_compatible_model("openai", "gpt-5.4").is_ok());
        let error = require_sre_compatible_model("openai", "gpt-5.5-pro").unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(error.1.contains("streaming and function tools"));
    }
}
