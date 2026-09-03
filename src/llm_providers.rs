use serde::{Deserialize, Serialize};

pub const PROVIDER_OPENAI: &str = "openai";
pub const PROVIDER_ANTHROPIC: &str = "anthropic";
pub const PROVIDER_OPENROUTER: &str = "openrouter";
pub const PROVIDER_BEDROCK: &str = "bedrock";
pub const PROVIDER_OPENAI_COMPATIBLE: &str = "openai_compatible";

pub const REASONING_LEVELS: &[&str] = &["minimal", "low", "medium", "high"];

pub fn default_model_setting_key(tenant_id: &str) -> String {
    format!("sre_agent_model:{tenant_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmProviderRouting {
    #[serde(default)]
    pub allowed_providers: Vec<String>,
    #[serde(default)]
    pub provider_order: Vec<String>,
    #[serde(default = "default_true")]
    pub allow_fallbacks: bool,
    #[serde(default)]
    pub zero_data_retention: bool,
    #[serde(default = "default_data_collection")]
    pub data_collection: String,
    #[serde(default)]
    pub region: String,
}

impl Default for LlmProviderRouting {
    fn default() -> Self {
        Self {
            allowed_providers: Vec::new(),
            provider_order: Vec::new(),
            allow_fallbacks: true,
            zero_data_retention: false,
            data_collection: default_data_collection(),
            region: String::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_data_collection() -> String {
    "allow".to_string()
}

#[derive(Debug, Clone)]
pub struct LlmProviderSecret {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub kind: String,
    pub base_url: String,
    pub api_key: String,
    pub key_hint: String,
    pub routing: LlmProviderRouting,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmProviderResponse {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub base_url: String,
    pub key_configured: bool,
    pub key_hint: String,
    pub routing: LlmProviderRouting,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl From<LlmProviderSecret> for LlmProviderResponse {
    fn from(provider: LlmProviderSecret) -> Self {
        Self {
            id: provider.id,
            name: provider.name,
            kind: provider.kind,
            base_url: provider.base_url,
            key_configured: !provider.api_key.is_empty(),
            key_hint: provider.key_hint,
            routing: provider.routing,
            enabled: provider.enabled,
            created_at: provider.created_at,
            updated_at: provider.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmModel {
    pub id: String,
    pub tenant_id: String,
    pub provider_id: String,
    pub name: String,
    pub model_id: String,
    pub reasoning: Vec<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

pub fn provider_kind_valid(kind: &str) -> bool {
    matches!(
        kind,
        PROVIDER_OPENAI
            | PROVIDER_ANTHROPIC
            | PROVIDER_OPENROUTER
            | PROVIDER_BEDROCK
            | PROVIDER_OPENAI_COMPATIBLE
    )
}

pub fn default_base_url(kind: &str, region: &str) -> Option<String> {
    match kind {
        PROVIDER_OPENAI => Some("https://api.openai.com/".to_string()),
        PROVIDER_ANTHROPIC => Some("https://api.anthropic.com/".to_string()),
        PROVIDER_OPENROUTER => Some("https://openrouter.ai/api/".to_string()),
        PROVIDER_BEDROCK if !region.is_empty() => Some(format!(
            "https://bedrock-runtime.{region}.amazonaws.com/openai/"
        )),
        _ => None,
    }
}

pub fn key_hint(secret: &str) -> String {
    let suffix = secret.chars().rev().take(4).collect::<Vec<_>>();
    suffix.into_iter().rev().collect()
}

pub fn validate_identifier(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(format!("{field} is invalid"));
    }
    Ok(())
}

pub fn normalize_reasoning(levels: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = Vec::new();
    for level in levels {
        let level = level.trim().to_ascii_lowercase();
        if !REASONING_LEVELS.contains(&level.as_str()) {
            return Err("reasoning contains an unsupported effort level".to_string());
        }
        if !normalized.contains(&level) {
            normalized.push(level);
        }
    }
    Ok(normalized)
}

/// Whether a provider model can satisfy the SRE agent's chat contract:
/// text conversations, streaming, and custom function tools.
///
/// OpenAI's model-list response contains IDs but no capability metadata, so
/// keep its allowlist intentionally conservative. Other OpenAI-compatible
/// providers are administrator-managed and validated by their own gateway.
pub fn sre_agent_model_compatible(provider_kind: &str, model_id: &str) -> bool {
    if provider_kind != PROVIDER_OPENAI {
        return true;
    }

    let model = model_id.trim().to_ascii_lowercase();
    let supported_family = model.starts_with("gpt-5")
        || model.starts_with("gpt-4.1")
        || model.starts_with("gpt-4o")
        || model == "o1"
        || model.starts_with("o1-")
        || model == "o3"
        || model.starts_with("o3-")
        || model == "o4"
        || model.starts_with("o4-");
    if !supported_family {
        return false;
    }

    // Pro models are Responses-only and may take several minutes. The SRE
    // agent currently requires the interactive streaming chat contract.
    // The other markers identify modality-specific, ChatGPT, Codex, search,
    // or deep-research models rather than general custom-tool chat models.
    ![
        "-pro",
        "audio",
        "realtime",
        "transcribe",
        "tts",
        "search",
        "image",
        "deep-research",
        "codex",
        "chat-latest",
        "instruct",
        "preview",
    ]
    .iter()
    .any(|marker| model.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bedrock_url_is_derived_from_region() {
        assert_eq!(
            default_base_url(PROVIDER_BEDROCK, "us-west-2").as_deref(),
            Some("https://bedrock-runtime.us-west-2.amazonaws.com/openai/")
        );
    }

    #[test]
    fn reasoning_levels_are_deduplicated_and_validated() {
        let levels = vec!["high".into(), "low".into(), "high".into()];
        assert_eq!(normalize_reasoning(&levels).unwrap(), vec!["high", "low"]);
        assert!(normalize_reasoning(&["extreme".into()]).is_err());
    }

    #[test]
    fn openai_sre_models_are_limited_to_tool_capable_chat_families() {
        for model in [
            "gpt-5.4",
            "gpt-5.4-2026-03-05",
            "gpt-4.1-mini",
            "gpt-4o-2024-11-20",
            "o3",
            "o4-mini",
        ] {
            assert!(
                sre_agent_model_compatible(PROVIDER_OPENAI, model),
                "{model}"
            );
        }

        for model in [
            "gpt-5.5-pro",
            "gpt-5.5-pro-2026-04-23",
            "gpt-5.3-codex",
            "gpt-4o-realtime-preview",
            "gpt-4o-transcribe",
            "text-embedding-3-large",
            "sora-2",
        ] {
            assert!(
                !sre_agent_model_compatible(PROVIDER_OPENAI, model),
                "{model}"
            );
        }

        assert!(sre_agent_model_compatible(
            PROVIDER_OPENROUTER,
            "vendor/custom-model"
        ));
    }
}
