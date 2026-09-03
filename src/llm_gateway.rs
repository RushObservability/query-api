//! Bounded, startup-configured outbound access to OpenAI-compatible LLM APIs.
//!
//! Handlers deliberately do not receive the API key, provider URL, or HTTP
//! client. This keeps authentication, SSRF controls, rate limits, timeouts,
//! response limits, and telemetry in one place.

use crate::api_error;
use crate::outbound::blocked_address;
use crate::self_metrics::SelfMetrics;
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
};
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::StreamExt;
use reqwest::{Client, Response, Url};
use serde_json::Value;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_BASE_URL: &str = "https://api.openai.com";
const DEFAULT_ALLOWED_ORIGIN: &str = "https://api.openai.com";
const DEFAULT_USER_REQUESTS_PER_MINUTE: u32 = 20;
const DEFAULT_TENANT_REQUESTS_PER_MINUTE: u32 = 100;
const DEFAULT_GLOBAL_CONCURRENCY: usize = 8;
const DEFAULT_MAX_INPUT_BYTES: usize = 65_536;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 131_072;
const DEFAULT_MAX_COMPLETION_TOKENS: u32 = 4_096;
const HARD_MAX_COMPLETION_TOKENS: u32 = 16_384;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_TOTAL_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_MAX_LIMITER_KEYS: usize = 10_000;
const QUOTA_WINDOW: Duration = Duration::from_secs(60);

pub const MAX_NATURAL_LANGUAGE_QUERY_BYTES: usize = 4_096;
pub const MAX_METRIC_HINTS: usize = 50;
pub const MAX_METRIC_HINT_BYTES: usize = 128;
pub const MAX_METRIC_HINT_TOTAL_BYTES: usize = 4_096;
pub const MAX_ANOMALY_ADDITIONAL_CONTEXT_BYTES: usize = 8_192;

#[derive(Clone, Copy, Debug)]
pub enum LlmOperation {
    ParseQuery,
    ParsePromql,
    AnalyzeAnomaly,
    ListModels,
    SreInvestigation,
}

impl LlmOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::ParseQuery => "parse_query",
            Self::ParsePromql => "parse_promql",
            Self::AnalyzeAnomaly => "analyze_anomaly",
            Self::ListModels => "list_models",
            Self::SreInvestigation => "sre_investigation",
        }
    }
}

#[derive(Clone, Debug)]
pub struct LlmCaller {
    pub user_id: String,
    pub tenant_id: String,
}

impl LlmCaller {
    pub fn new(user_id: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            tenant_id: tenant_id.into(),
        }
    }
}

#[derive(Clone)]
pub struct LlmGateway {
    inner: Arc<GatewayInner>,
}

struct GatewayInner {
    provider: Option<Provider>,
    limits: Limits,
    concurrency: Arc<Semaphore>,
    user_windows: DashMap<String, QuotaWindow>,
    tenant_windows: DashMap<String, QuotaWindow>,
    metrics: Arc<SelfMetrics>,
}

struct Provider {
    base_url: Url,
    api_key: String,
    client: Client,
}

#[derive(Clone, Copy)]
struct Limits {
    user_requests_per_minute: u32,
    tenant_requests_per_minute: u32,
    max_input_bytes: usize,
    max_response_bytes: usize,
    max_completion_tokens: u32,
    max_limiter_keys: usize,
}

#[derive(Clone, Copy)]
struct QuotaWindow {
    count: u32,
    started: Instant,
}

struct AdmissionGuard {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<SelfMetrics>,
    operation: &'static str,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.metrics.add_gauge(
            "rush_llm_requests_in_flight",
            &[("operation", self.operation)],
            -1.0,
        );
    }
}

impl LlmGateway {
    /// Build the provider exactly once. A configured provider is DNS-resolved
    /// and pinned here so a later DNS rebinding cannot change its destination.
    pub async fn from_env(metrics: Arc<SelfMetrics>) -> anyhow::Result<Self> {
        let limits = Limits {
            user_requests_per_minute: env_u32(
                "RUSH_LLM_USER_REQUESTS_PER_MINUTE",
                DEFAULT_USER_REQUESTS_PER_MINUTE,
                1,
                10_000,
            )?,
            tenant_requests_per_minute: env_u32(
                "RUSH_LLM_TENANT_REQUESTS_PER_MINUTE",
                DEFAULT_TENANT_REQUESTS_PER_MINUTE,
                1,
                100_000,
            )?,
            max_input_bytes: env_usize(
                "RUSH_LLM_MAX_INPUT_BYTES",
                DEFAULT_MAX_INPUT_BYTES,
                1_024,
                1_048_576,
            )?,
            max_response_bytes: env_usize(
                "RUSH_LLM_MAX_RESPONSE_BYTES",
                DEFAULT_MAX_RESPONSE_BYTES,
                1_024,
                1_048_576,
            )?,
            max_completion_tokens: env_u32(
                "RUSH_LLM_MAX_COMPLETION_TOKENS",
                DEFAULT_MAX_COMPLETION_TOKENS,
                1,
                HARD_MAX_COMPLETION_TOKENS,
            )?,
            max_limiter_keys: env_usize(
                "RUSH_LLM_MAX_LIMITER_KEYS",
                DEFAULT_MAX_LIMITER_KEYS,
                100,
                1_000_000,
            )?,
        };
        let concurrency = env_usize(
            "RUSH_LLM_GLOBAL_CONCURRENCY",
            DEFAULT_GLOBAL_CONCURRENCY,
            1,
            1_024,
        )?;
        let connect_timeout_ms = env_u64(
            "RUSH_LLM_CONNECT_TIMEOUT_MS",
            DEFAULT_CONNECT_TIMEOUT_MS,
            100,
            60_000,
        )?;
        let total_timeout_ms = env_u64(
            "RUSH_LLM_TOTAL_TIMEOUT_MS",
            DEFAULT_TOTAL_TIMEOUT_MS,
            connect_timeout_ms,
            300_000,
        )?;

        let api_key = std::env::var("OPENAI_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let provider = match api_key {
            None => None,
            Some(api_key) => {
                let raw_base = std::env::var("OPENAI_BASE_URL")
                    .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
                let allowed = allowed_origins_from_env()?;
                let allow_insecure_private =
                    crate::api_key_auth::env_flag("RUSH_LLM_ALLOW_INSECURE_PRIVATE");
                if allow_insecure_private && crate::api_key_auth::production_mode() {
                    anyhow::bail!("RUSH_LLM_ALLOW_INSECURE_PRIVATE is forbidden in production");
                }
                let (base_url, host, addresses) =
                    validate_and_resolve_base_url(&raw_base, &allowed, allow_insecure_private)
                        .await?;
                let client = Client::builder()
                    // Environment proxy settings could bypass the resolved and
                    // pinned destination below, so this security-sensitive
                    // client always connects directly to the allowed origin.
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .connect_timeout(Duration::from_millis(connect_timeout_ms))
                    .timeout(Duration::from_millis(total_timeout_ms))
                    .resolve_to_addrs(&host, &addresses)
                    .user_agent("rush-query-api/llm-gateway")
                    .build()?;
                Some(Provider {
                    base_url,
                    api_key,
                    client,
                })
            }
        };

        metrics.set_gauge(
            "rush_llm_configured",
            &[],
            if provider.is_some() { 1.0 } else { 0.0 },
        );
        for (limit, value) in [
            (
                "user_requests_per_minute",
                limits.user_requests_per_minute as f64,
            ),
            (
                "tenant_requests_per_minute",
                limits.tenant_requests_per_minute as f64,
            ),
            ("global_concurrency", concurrency as f64),
            ("max_input_bytes", limits.max_input_bytes as f64),
            ("max_response_bytes", limits.max_response_bytes as f64),
            ("max_completion_tokens", limits.max_completion_tokens as f64),
            ("max_limiter_keys", limits.max_limiter_keys as f64),
            ("connect_timeout_ms", connect_timeout_ms as f64),
            ("total_timeout_ms", total_timeout_ms as f64),
        ] {
            metrics.set_gauge("rush_llm_limit", &[("limit", limit)], value);
        }

        Ok(Self {
            inner: Arc::new(GatewayInner {
                provider,
                limits,
                concurrency: Arc::new(Semaphore::new(concurrency)),
                user_windows: DashMap::new(),
                tenant_windows: DashMap::new(),
                metrics,
            }),
        })
    }

    pub fn is_configured(&self) -> bool {
        self.inner.provider.is_some()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn chat(
        &self,
        operation: LlmOperation,
        caller: &LlmCaller,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        requested_completion_tokens: u32,
        temperature: Option<f32>,
    ) -> Result<String, (StatusCode, String)> {
        let provider = self.provider()?;
        validate_model(model)?;
        let total_input = system_prompt.len().saturating_add(user_prompt.len());
        if total_input > self.inner.limits.max_input_bytes {
            self.record(operation, "denied");
            return Err(public_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "llm_input_too_large",
                "LLM input exceeds the configured limit",
            ));
        }

        let guard = self.admit(operation, caller)?;
        let max_tokens = requested_completion_tokens.min(self.inner.limits.max_completion_tokens);
        if max_tokens != requested_completion_tokens {
            self.record(operation, "token_bounded");
        }

        let mut body = serde_json::json!({
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_completion_tokens": max_tokens,
        });
        if let Some(temperature) = temperature {
            body["temperature"] = serde_json::json!(temperature);
        }

        let started = Instant::now();
        let url = provider
            .base_url
            .join("v1/chat/completions")
            .expect("validated origin joins a static path");
        let response = provider
            .client
            .post(url)
            .bearer_auth(&provider.api_key)
            .json(&body)
            .send()
            .await;
        let result = match response {
            Ok(response) => self.read_chat_response(operation, response).await,
            Err(error) if error.is_timeout() => {
                self.record(operation, "timeout");
                Err(api_error::internal_legacy_with_status(
                    StatusCode::GATEWAY_TIMEOUT,
                    "llm_gateway.request_timeout",
                    error,
                ))
            }
            Err(error) => {
                self.record(operation, "failed");
                Err(api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.request",
                    error,
                ))
            }
        };
        self.inner.metrics.observe_histogram(
            "rush_llm_request_duration_ms",
            &[("operation", operation.as_str())],
            started.elapsed().as_secs_f64() * 1000.0,
        );
        drop(guard);
        result
    }

    pub async fn list_models(
        &self,
        caller: &LlmCaller,
    ) -> Result<Vec<String>, (StatusCode, String)> {
        let operation = LlmOperation::ListModels;
        let provider = self.provider()?;
        let guard = self.admit(operation, caller)?;
        let started = Instant::now();
        let url = provider
            .base_url
            .join("v1/models")
            .expect("validated origin joins a static path");
        let response = provider
            .client
            .get(url)
            .bearer_auth(&provider.api_key)
            .send()
            .await;
        let result = match response {
            Ok(response) => match self.read_json_response(operation, response).await {
                Ok(value) => {
                    let models = value["data"]
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item["id"].as_str())
                                .filter(|id| id.len() <= 128)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default();
                    self.record(operation, "completed");
                    Ok(models)
                }
                Err(error) => Err(error),
            },
            Err(error) if error.is_timeout() => {
                self.record(operation, "timeout");
                Err(api_error::internal_legacy_with_status(
                    StatusCode::GATEWAY_TIMEOUT,
                    "llm_gateway.models_timeout",
                    error,
                ))
            }
            Err(error) => {
                self.record(operation, "failed");
                Err(api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.models_request",
                    error,
                ))
            }
        };
        self.inner.metrics.observe_histogram(
            "rush_llm_request_duration_ms",
            &[("operation", operation.as_str())],
            started.elapsed().as_secs_f64() * 1000.0,
        );
        drop(guard);
        result
    }

    /// Send an SRE-agent Chat Completions request through a provider configured
    /// in Rush. The provider key stays inside query-api. The response remains
    /// OpenAI-compatible SSE so the agent loop has one streaming protocol.
    pub async fn stream_agent_chat(
        &self,
        caller: &LlmCaller,
        provider: &crate::llm_providers::LlmProviderSecret,
        upstream_model: &str,
        mut body: Value,
    ) -> Result<AxumResponse, (StatusCode, String)> {
        let operation = LlmOperation::SreInvestigation;
        if !provider.enabled || provider.api_key.trim().is_empty() {
            return Err(public_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "llm_provider_unavailable",
                "The selected LLM provider is disabled or has no credential",
            ));
        }
        validate_model(upstream_model)?;
        let object = body.as_object_mut().ok_or_else(|| {
            public_error(
                StatusCode::BAD_REQUEST,
                "invalid_llm_request",
                "LLM request must be a JSON object",
            )
        })?;
        let messages_valid = object
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| !messages.is_empty() && messages.len() <= 256);
        if !messages_valid {
            return Err(public_error(
                StatusCode::BAD_REQUEST,
                "invalid_llm_messages",
                "LLM request must contain between 1 and 256 messages",
            ));
        }
        object.insert("model".into(), Value::String(upstream_model.to_string()));
        object.insert("stream".into(), Value::Bool(true));
        object.insert(
            "stream_options".into(),
            serde_json::json!({ "include_usage": true }),
        );
        // Routing policy is trusted configuration. Never let the agent inject
        // an arbitrary OpenRouter provider override.
        object.remove("provider");
        if provider.kind == crate::llm_providers::PROVIDER_OPENROUTER {
            let routing = &provider.routing;
            let mut preferences = serde_json::Map::new();
            if !routing.allowed_providers.is_empty() {
                preferences.insert("only".into(), serde_json::json!(routing.allowed_providers));
            }
            if !routing.provider_order.is_empty() {
                preferences.insert("order".into(), serde_json::json!(routing.provider_order));
            }
            preferences.insert(
                "allow_fallbacks".into(),
                Value::Bool(routing.allow_fallbacks),
            );
            preferences.insert("zdr".into(), Value::Bool(routing.zero_data_retention));
            preferences.insert(
                "data_collection".into(),
                Value::String(routing.data_collection.clone()),
            );
            object.insert("provider".into(), Value::Object(preferences));
        }
        if provider.kind == crate::llm_providers::PROVIDER_ANTHROPIC {
            if let Some(effort) = object
                .remove("reasoning_effort")
                .and_then(|value| value.as_str().map(str::to_string))
            {
                let budget_tokens = match effort.as_str() {
                    "minimal" => 1_024,
                    "low" => 2_048,
                    "medium" => 4_096,
                    "high" => 8_192,
                    _ => 0,
                };
                if budget_tokens > 0 {
                    object.insert(
                        "thinking".into(),
                        serde_json::json!({ "type": "enabled", "budget_tokens": budget_tokens }),
                    );
                }
            }
        }
        let use_responses_api = provider.kind == crate::llm_providers::PROVIDER_OPENAI
            && openai_model_uses_responses_api(upstream_model);
        if use_responses_api {
            body = chat_request_to_responses_request(body)?;
        }

        let request_size = serde_json::to_vec(&body).map_err(|error| {
            api_error::internal_legacy_with_status(
                StatusCode::BAD_REQUEST,
                "llm_gateway.serialize",
                error,
            )
        })?;
        // Agent transcripts are larger than the small parsing prompts handled
        // by chat(), but still need a hard process boundary.
        let max_agent_input = self.inner.limits.max_input_bytes.max(1_048_576);
        if request_size.len() > max_agent_input {
            return Err(public_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "llm_input_too_large",
                "LLM input exceeds the configured limit",
            ));
        }

        let guard = self.admit(operation, caller)?;
        let (base_url, client) = dynamic_client(&provider.base_url).await.map_err(|error| {
            api_error::internal_legacy_with_status(
                StatusCode::BAD_REQUEST,
                "llm_gateway.provider_url",
                error,
            )
        })?;
        let endpoint = if use_responses_api {
            "responses"
        } else {
            "chat/completions"
        };
        let url = join_openai_endpoint(&base_url, endpoint).map_err(|error| {
            api_error::internal_legacy_with_status(
                StatusCode::BAD_REQUEST,
                "llm_gateway.provider_url",
                error,
            )
        })?;
        let started = Instant::now();
        let upstream = client
            .post(url)
            .bearer_auth(&provider.api_key)
            .header("content-type", "application/json")
            .body(request_size)
            .send()
            .await
            .map_err(|error| {
                self.record(
                    operation,
                    if error.is_timeout() {
                        "timeout"
                    } else {
                        "failed"
                    },
                );
                api_error::internal_legacy_with_status(
                    if error.is_timeout() {
                        StatusCode::GATEWAY_TIMEOUT
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    "llm_gateway.agent_request",
                    error,
                )
            })?;
        self.inner.metrics.observe_histogram(
            "rush_llm_request_duration_ms",
            &[("operation", operation.as_str())],
            started.elapsed().as_secs_f64() * 1_000.0,
        );
        if !upstream.status().is_success() {
            let status = upstream.status();
            self.record(operation, "failed");
            return Err(api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "llm_gateway.upstream_status",
                format!("provider returned HTTP {status}"),
            ));
        }

        let max_bytes = self.inner.limits.max_response_bytes.max(1_048_576);
        if use_responses_api {
            let value = read_bounded_json(upstream, max_bytes)
                .await
                .map_err(|error| {
                    self.record(operation, "failed");
                    api_error::internal_legacy_with_status(
                        StatusCode::BAD_GATEWAY,
                        "llm_gateway.responses_read",
                        error,
                    )
                })?;
            let sse = responses_response_to_chat_sse(&value, upstream_model).map_err(|error| {
                self.record(operation, "failed");
                api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.responses_translate",
                    error,
                )
            })?;
            self.record(operation, "streaming");
            return Ok(AxumResponse::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                .header(axum::http::header::CACHE_CONTROL, "no-cache")
                .header("X-Accel-Buffering", "no")
                .body(Body::from(sse))
                .map_err(|error| api_error::internal_legacy("llm_gateway.response", error))?
                .into_response());
        }

        let content_type = upstream
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("text/event-stream")
            .to_string();
        let byte_stream = upstream.bytes_stream().scan(
            (0usize, false, guard),
            move |(seen, stopped, _guard), chunk| {
                let next = if *stopped {
                    None
                } else {
                    match chunk {
                        Ok(bytes) => {
                            *seen = seen.saturating_add(bytes.len());
                            if *seen > max_bytes {
                                *stopped = true;
                                Some(Err(std::io::Error::other(
                                    "LLM stream exceeded the response limit",
                                )))
                            } else {
                                Some(Ok::<Bytes, std::io::Error>(bytes))
                            }
                        }
                        Err(error) => {
                            *stopped = true;
                            Some(Err(std::io::Error::other(error)))
                        }
                    }
                };
                std::future::ready(next)
            },
        );
        self.record(operation, "streaming");
        Ok(AxumResponse::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, content_type)
            .header(axum::http::header::CACHE_CONTROL, "no-cache")
            .header("X-Accel-Buffering", "no")
            .body(Body::from_stream(byte_stream))
            .map_err(|error| api_error::internal_legacy("llm_gateway.response", error))?
            .into_response())
    }

    pub async fn list_configured_provider_models(
        &self,
        caller: &LlmCaller,
        provider: &crate::llm_providers::LlmProviderSecret,
    ) -> Result<Vec<String>, (StatusCode, String)> {
        let operation = LlmOperation::ListModels;
        let guard = self.admit(operation, caller)?;
        let (base_url, client) = dynamic_client(&provider.base_url).await.map_err(|error| {
            api_error::internal_legacy_with_status(
                StatusCode::BAD_REQUEST,
                "llm_gateway.provider_url",
                error,
            )
        })?;
        let url = join_openai_endpoint(&base_url, "models").map_err(|error| {
            api_error::internal_legacy_with_status(
                StatusCode::BAD_REQUEST,
                "llm_gateway.provider_url",
                error,
            )
        })?;
        let response = client
            .get(url)
            .bearer_auth(&provider.api_key)
            .send()
            .await
            .map_err(|error| {
                api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.models_request",
                    error,
                )
            })?;
        let result = self.read_json_response(operation, response).await?;
        drop(guard);
        Ok(result["data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| item["id"].as_str())
            .filter(|id| validate_model(id).is_ok())
            .filter(|id| crate::llm_providers::sre_agent_model_compatible(&provider.kind, id))
            .map(str::to_string)
            .collect())
    }

    fn provider(&self) -> Result<&Provider, (StatusCode, String)> {
        self.inner.provider.as_ref().ok_or_else(|| {
            public_error(
                StatusCode::NOT_IMPLEMENTED,
                "llm_not_configured",
                "LLM support is not configured",
            )
        })
    }

    fn admit(
        &self,
        operation: LlmOperation,
        caller: &LlmCaller,
    ) -> Result<AdmissionGuard, (StatusCode, String)> {
        let user_key = format!("{}\0{}", caller.tenant_id, caller.user_id);
        if !consume_quota(
            &self.inner.user_windows,
            user_key,
            self.inner.limits.user_requests_per_minute,
            self.inner.limits.max_limiter_keys,
        ) {
            self.quota_denied(operation, caller, "user");
            return Err(public_error(
                StatusCode::TOO_MANY_REQUESTS,
                "llm_user_quota_exceeded",
                "LLM user request quota exceeded",
            ));
        }
        if !consume_quota(
            &self.inner.tenant_windows,
            caller.tenant_id.clone(),
            self.inner.limits.tenant_requests_per_minute,
            self.inner.limits.max_limiter_keys,
        ) {
            self.quota_denied(operation, caller, "tenant");
            return Err(public_error(
                StatusCode::TOO_MANY_REQUESTS,
                "llm_tenant_quota_exceeded",
                "LLM tenant request quota exceeded",
            ));
        }
        let permit = self
            .inner
            .concurrency
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                self.quota_denied(operation, caller, "global_concurrency");
                public_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "llm_concurrency_exceeded",
                    "LLM service is busy; retry shortly",
                )
            })?;
        self.record(operation, "accepted");
        self.inner.metrics.add_gauge(
            "rush_llm_requests_in_flight",
            &[("operation", operation.as_str())],
            1.0,
        );
        Ok(AdmissionGuard {
            _permit: permit,
            metrics: self.inner.metrics.clone(),
            operation: operation.as_str(),
        })
    }

    fn quota_denied(&self, operation: LlmOperation, caller: &LlmCaller, scope: &'static str) {
        self.record(operation, "denied");
        tracing::warn!(
            event = "llm.quota_denied",
            operation = operation.as_str(),
            quota_scope = scope,
            tenant_id = %caller.tenant_id,
            user_id = %caller.user_id,
            "LLM request denied by resource policy"
        );
    }

    fn record(&self, operation: LlmOperation, outcome: &'static str) {
        self.inner.metrics.inc_counter(
            "rush_llm_requests_total",
            &[("operation", operation.as_str()), ("outcome", outcome)],
            1,
        );
    }

    async fn read_chat_response(
        &self,
        operation: LlmOperation,
        response: Response,
    ) -> Result<String, (StatusCode, String)> {
        let value = self.read_json_response(operation, response).await?;
        let content = value
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .or_else(|| value.get("output").and_then(Value::as_str))
            .ok_or_else(|| {
                self.record(operation, "failed");
                api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.missing_content",
                    "provider response omitted completion content",
                )
            })?;
        self.record(operation, "completed");
        Ok(content.to_string())
    }

    async fn read_json_response(
        &self,
        operation: LlmOperation,
        response: Response,
    ) -> Result<Value, (StatusCode, String)> {
        if !response.status().is_success() {
            let status = response.status();
            self.record(operation, "failed");
            return Err(api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "llm_gateway.upstream_status",
                format_args!("provider returned HTTP {status}"),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.inner.limits.max_response_bytes as u64)
        {
            self.record(operation, "failed");
            return Err(api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "llm_gateway.response_too_large",
                "provider response exceeded the configured byte limit",
            ));
        }
        let mut bytes = Vec::with_capacity(self.inner.limits.max_response_bytes.min(16_384));
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                self.record(
                    operation,
                    if error.is_timeout() {
                        "timeout"
                    } else {
                        "failed"
                    },
                );
                api_error::internal_legacy_with_status(
                    if error.is_timeout() {
                        StatusCode::GATEWAY_TIMEOUT
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    "llm_gateway.response_read",
                    error,
                )
            })?;
            if bytes.len().saturating_add(chunk.len()) > self.inner.limits.max_response_bytes {
                self.record(operation, "failed");
                return Err(api_error::internal_legacy_with_status(
                    StatusCode::BAD_GATEWAY,
                    "llm_gateway.response_too_large",
                    "provider response exceeded the configured byte limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            self.record(operation, "failed");
            api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "llm_gateway.invalid_json",
                error,
            )
        })
    }
}

fn public_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> (StatusCode, String) {
    api_error::ApiError::public(status, code, message).into_legacy()
}

fn validate_model(model: &str) -> Result<(), (StatusCode, String)> {
    if model.is_empty()
        || model.len() > 128
        || !model.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(public_error(
            StatusCode::BAD_REQUEST,
            "invalid_llm_model",
            "LLM model is invalid",
        ));
    }
    Ok(())
}

fn consume_quota(
    windows: &DashMap<String, QuotaWindow>,
    key: String,
    limit: u32,
    max_keys: usize,
) -> bool {
    let now = Instant::now();
    if windows.len() >= max_keys && !windows.contains_key(&key) {
        windows.retain(|_, window| now.duration_since(window.started) < QUOTA_WINDOW);
        if windows.len() >= max_keys {
            return false;
        }
    }
    let mut window = windows.entry(key).or_insert(QuotaWindow {
        count: 0,
        started: now,
    });
    if now.duration_since(window.started) >= QUOTA_WINDOW {
        window.count = 0;
        window.started = now;
    }
    if window.count >= limit {
        return false;
    }
    window.count += 1;
    true
}

fn private_development_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback(),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return private_development_address(IpAddr::V4(mapped));
            }
            ip.is_loopback() || ip.is_unique_local()
        }
    }
}

async fn dynamic_client(raw_base: &str) -> anyhow::Result<(Url, Client)> {
    let mut url = Url::parse(raw_base).map_err(|_| anyhow::anyhow!("LLM base URL is invalid"))?;
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("LLM base URL must not contain credentials");
    }
    if url.host_str().is_none()
        || !matches!(url.scheme(), "http" | "https")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!("LLM base URL must be an HTTP(S) URL without query or fragment");
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    let host = url.host_str().expect("host checked above").to_string();
    let port = url.port_or_known_default().unwrap_or(443);
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|_| anyhow::anyhow!("LLM provider host could not be resolved"))?
            .collect::<Vec<_>>()
    };
    if addresses.is_empty() {
        anyhow::bail!("LLM provider host could not be resolved");
    }
    let all_public = addresses
        .iter()
        .all(|address| !blocked_address(address.ip()));
    let all_development_private = addresses
        .iter()
        .all(|address| private_development_address(address.ip()));
    let allow_private = crate::api_key_auth::env_flag("RUSH_LLM_ALLOW_INSECURE_PRIVATE")
        && !crate::api_key_auth::production_mode();
    if !all_public && !(allow_private && all_development_private) {
        anyhow::bail!("LLM provider must resolve only to public addresses");
    }
    if url.scheme() == "http" && !(allow_private && all_development_private) {
        anyhow::bail!("HTTP LLM providers are allowed only for private development targets");
    }
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
        .timeout(Duration::from_millis(DEFAULT_TOTAL_TIMEOUT_MS.max(300_000)))
        .resolve_to_addrs(&host, &addresses)
        .user_agent("rush-query-api/llm-provider")
        .build()?;
    Ok((url, client))
}

fn join_openai_endpoint(base_url: &Url, endpoint: &str) -> Result<Url, url::ParseError> {
    let path = base_url.path().trim_end_matches('/');
    if path.ends_with("/v1") {
        base_url.join(endpoint)
    } else {
        base_url.join(&format!("v1/{endpoint}"))
    }
}

fn openai_model_uses_responses_api(model: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    model.starts_with("gpt-5") || model.starts_with("o1-pro")
}

fn chat_request_to_responses_request(mut body: Value) -> Result<Value, (StatusCode, String)> {
    let object = body.as_object_mut().ok_or_else(|| {
        public_error(
            StatusCode::BAD_REQUEST,
            "invalid_llm_request",
            "LLM request must be a JSON object",
        )
    })?;
    let messages = object
        .remove("messages")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| {
            public_error(
                StatusCode::BAD_REQUEST,
                "invalid_llm_messages",
                "LLM request must contain messages",
            )
        })?;
    let mut input = Vec::new();
    for message in messages {
        let Some(message) = message.as_object() else {
            continue;
        };
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "tool" {
            let Some(call_id) = message.get("tool_call_id").and_then(Value::as_str) else {
                continue;
            };
            let output = message
                .get("content")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    message
                        .get("content")
                        .cloned()
                        .unwrap_or(Value::Null)
                        .to_string()
                });
            input.push(serde_json::json!({
                "type": "function_call_output",
                "call_id": call_id,
                "output": output,
            }));
            continue;
        }

        if role == "assistant" {
            if let Some(reasoning) = message
                .get("_rush_responses_reasoning")
                .and_then(Value::as_array)
            {
                input.extend(
                    reasoning
                        .iter()
                        .filter(|item| {
                            item.get("type").and_then(Value::as_str) == Some("reasoning")
                        })
                        .cloned(),
                );
            }
            if let Some(content) = message.get("content").and_then(Value::as_str)
                && !content.is_empty()
            {
                input.push(serde_json::json!({ "role": "assistant", "content": content }));
            }
            if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                for tool_call in tool_calls {
                    let function = &tool_call["function"];
                    let Some(name) = function.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(call_id) = tool_call.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    input.push(serde_json::json!({
                        "type": "function_call",
                        "call_id": call_id,
                        "name": name,
                        "arguments": function
                            .get("arguments")
                            .and_then(Value::as_str)
                            .unwrap_or("{}"),
                    }));
                }
            }
            continue;
        }

        if matches!(role, "system" | "developer" | "user") {
            input.push(serde_json::json!({
                "role": role,
                "content": message.get("content").cloned().unwrap_or(Value::String(String::new())),
            }));
        }
    }
    object.insert("input".into(), Value::Array(input));
    object.insert("stream".into(), Value::Bool(false));
    object.insert("store".into(), Value::Bool(false));
    object.insert(
        "include".into(),
        serde_json::json!(["reasoning.encrypted_content"]),
    );
    object.remove("stream_options");

    if let Some(effort) = object.remove("reasoning_effort")
        && effort.as_str().is_some_and(|value| !value.is_empty())
    {
        object.insert("reasoning".into(), serde_json::json!({ "effort": effort }));
    }
    if let Some(tools) = object
        .remove("tools")
        .and_then(|value| value.as_array().cloned())
    {
        let tools = tools
            .into_iter()
            .filter_map(|tool| {
                let function = tool.get("function")?.as_object()?;
                let name = function.get("name")?.clone();
                let parameters = function
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({ "type": "object" }));
                let mut converted = serde_json::json!({
                    "type": "function",
                    "name": name,
                    "parameters": parameters,
                });
                if let Some(description) = function.get("description") {
                    converted["description"] = description.clone();
                }
                if let Some(strict) = function.get("strict") {
                    converted["strict"] = strict.clone();
                }
                Some(converted)
            })
            .collect::<Vec<_>>();
        if !tools.is_empty() {
            object.insert("tools".into(), Value::Array(tools));
        }
    }
    Ok(body)
}

async fn read_bounded_json(response: Response, max_bytes: usize) -> anyhow::Result<Value> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        anyhow::bail!("provider response exceeded the configured byte limit");
    }
    let mut bytes = Vec::with_capacity(max_bytes.min(16_384));
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            anyhow::bail!("provider response exceeded the configured byte limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn responses_response_to_chat_sse(response: &Value, model: &str) -> anyhow::Result<String> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Responses API response omitted output"))?;
    let mut content = String::new();
    let mut tool_calls = Vec::new();
    let mut reasoning = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    for part in parts {
                        if part.get("type").and_then(Value::as_str) == Some("output_text")
                            && let Some(text) = part.get("text").and_then(Value::as_str)
                        {
                            content.push_str(text);
                        }
                    }
                }
            }
            Some("function_call") => {
                let Some(name) = item.get("name").and_then(Value::as_str) else {
                    continue;
                };
                let call_id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if call_id.is_empty() {
                    continue;
                }
                tool_calls.push(serde_json::json!({
                    "index": tool_calls.len(),
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": item.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                    },
                }));
            }
            Some("reasoning") => reasoning.push(item.clone()),
            _ => {}
        }
    }
    if content.is_empty() && tool_calls.is_empty() {
        anyhow::bail!("Responses API response contained no text or function calls");
    }

    let mut delta = serde_json::json!({ "role": "assistant" });
    if !content.is_empty() {
        delta["content"] = Value::String(content);
    }
    if !tool_calls.is_empty() {
        delta["tool_calls"] = Value::Array(tool_calls);
    }
    if !reasoning.is_empty() {
        delta["_rush_responses_reasoning"] = Value::Array(reasoning);
    }
    let prompt_tokens = response
        .pointer("/usage/input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion_tokens = response
        .pointer("/usage/output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let chunk = serde_json::json!({
        "id": response.get("id").and_then(Value::as_str).unwrap_or("rush-responses-adapter"),
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": if delta.get("tool_calls").is_some() { "tool_calls" } else { "stop" },
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens.saturating_add(completion_tokens),
        },
    });
    Ok(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
}

fn allowed_origins_from_env() -> anyhow::Result<Vec<String>> {
    let raw = std::env::var("RUSH_LLM_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| DEFAULT_ALLOWED_ORIGIN.to_string());
    let mut origins = Vec::new();
    for value in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let url = parse_origin(value)?;
        origins.push(url.origin().ascii_serialization());
    }
    if origins.is_empty() {
        anyhow::bail!("RUSH_LLM_ALLOWED_ORIGINS must contain at least one origin");
    }
    origins.sort();
    origins.dedup();
    Ok(origins)
}

fn parse_origin(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("LLM origin is invalid"))?;
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("LLM origin must not contain user credentials");
    }
    if url.host_str().is_none() {
        anyhow::bail!("LLM origin must include a host");
    }
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("LLM origin must use HTTP or HTTPS");
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("LLM origin must not contain a path, query, or fragment");
    }
    Ok(url)
}

async fn validate_and_resolve_base_url(
    raw: &str,
    allowed_origins: &[String],
    allow_insecure_private: bool,
) -> anyhow::Result<(Url, String, Vec<SocketAddr>)> {
    let url = parse_origin(raw)?;
    let origin = url.origin().ascii_serialization();
    if !allowed_origins.iter().any(|allowed| allowed == &origin) {
        anyhow::bail!("configured LLM origin is not in RUSH_LLM_ALLOWED_ORIGINS");
    }
    let host = url
        .host_str()
        .expect("parse_origin checked the host")
        .to_string();
    let port = url.port_or_known_default().unwrap_or(443);
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|_| anyhow::anyhow!("configured LLM host could not be resolved"))?
            .collect::<Vec<_>>()
    };
    if addresses.is_empty() {
        anyhow::bail!("configured LLM host could not be resolved");
    }
    let all_public = addresses
        .iter()
        .all(|address| !blocked_address(address.ip()));
    let all_development_private = addresses
        .iter()
        .all(|address| private_development_address(address.ip()));
    if !all_public && !all_development_private {
        anyhow::bail!("configured LLM origin resolved to a mixed or forbidden address class");
    }
    if all_development_private && !allow_insecure_private {
        anyhow::bail!("configured LLM origin must resolve only to public addresses");
    }
    if url.scheme() == "http" && (!allow_insecure_private || !all_development_private) {
        anyhow::bail!("HTTP LLM origins are allowed only for private development targets");
    }
    Ok((url, host, addresses))
}

fn env_u32(name: &str, default: u32, min: u32, max: u32) -> anyhow::Result<u32> {
    let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be between {min} and {max}");
    }
    Ok(value)
}

fn env_u64(name: &str, default: u64, min: u64, max: u64) -> anyhow::Result<u64> {
    let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be between {min} and {max}");
    }
    Ok(value)
}

fn env_usize(name: &str, default: usize, min: usize, max: usize) -> anyhow::Result<usize> {
    let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be between {min} and {max}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, response::Redirect, routing::post};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn limits() -> Limits {
        Limits {
            user_requests_per_minute: 20,
            tenant_requests_per_minute: 100,
            max_input_bytes: 1_024,
            max_response_bytes: 1_024,
            max_completion_tokens: 100,
            max_limiter_keys: 100,
        }
    }

    fn test_gateway(
        base_url: Url,
        limits: Limits,
        concurrency: usize,
        timeout: Duration,
    ) -> LlmGateway {
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .unwrap();
        LlmGateway {
            inner: Arc::new(GatewayInner {
                provider: Some(Provider {
                    base_url,
                    api_key: "test-key".to_string(),
                    client,
                }),
                limits,
                concurrency: Arc::new(Semaphore::new(concurrency)),
                user_windows: DashMap::new(),
                tenant_windows: DashMap::new(),
                metrics: Arc::new(SelfMetrics::new()),
            }),
        }
    }

    async fn serve(app: Router) -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Url::parse(&format!("http://{address}/")).unwrap()
    }

    fn caller() -> LlmCaller {
        LlmCaller::new("user-1", "tenant-1")
    }

    #[test]
    fn origin_syntax_rejects_ssrf_shaping_components() {
        assert!(parse_origin("https://user:pass@example.com").is_err());
        assert!(parse_origin("https://example.com/proxy").is_err());
        assert!(parse_origin("https://example.com?next=http://127.0.0.1").is_err());
        assert!(parse_origin("https://example.com#fragment").is_err());
        assert!(parse_origin("file:///etc/passwd").is_err());
    }

    #[test]
    fn configured_base_url_accepts_root_or_existing_v1_prefix() {
        let root = Url::parse("https://api.example.com/").unwrap();
        assert_eq!(
            join_openai_endpoint(&root, "chat/completions")
                .unwrap()
                .as_str(),
            "https://api.example.com/v1/chat/completions"
        );
        let versioned = Url::parse("https://api.example.com/openai/v1/").unwrap();
        assert_eq!(
            join_openai_endpoint(&versioned, "models").unwrap().as_str(),
            "https://api.example.com/openai/v1/models"
        );
    }

    #[test]
    fn openai_gpt5_models_use_responses_api() {
        assert!(openai_model_uses_responses_api("gpt-5.5-pro"));
        assert!(openai_model_uses_responses_api("gpt-5.5-pro-2026-04-23"));
        assert!(openai_model_uses_responses_api("o1-pro"));
        assert!(openai_model_uses_responses_api("gpt-5.4"));
        assert!(openai_model_uses_responses_api("gpt-5.5"));
        assert!(openai_model_uses_responses_api("gpt-5.6-sol"));
        assert!(!openai_model_uses_responses_api("gpt-4.1"));
    }

    #[test]
    fn converts_chat_tools_and_history_to_responses_input() {
        let request = chat_request_to_responses_request(json!({
            "model": "gpt-5.5-pro",
            "messages": [
                { "role": "system", "content": "Investigate carefully." },
                { "role": "user", "content": "What failed?" },
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "search_logs", "arguments": "{\"service\":\"api\"}" }
                    }],
                    "_rush_responses_reasoning": [{
                        "type": "reasoning",
                        "id": "rs_1",
                        "encrypted_content": "opaque"
                    }]
                },
                { "role": "tool", "tool_call_id": "call_1", "content": "found an error" }
            ],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "search_logs",
                    "description": "Search logs",
                    "parameters": { "type": "object" }
                }
            }],
            "reasoning_effort": "high",
            "stream": true,
            "stream_options": { "include_usage": true }
        }))
        .unwrap();

        assert_eq!(request["stream"], false);
        assert_eq!(request["store"], false);
        assert_eq!(request["reasoning"]["effort"], "high");
        assert_eq!(request["tools"][0]["name"], "search_logs");
        assert!(request.get("messages").is_none());
        assert!(request.get("stream_options").is_none());
        assert_eq!(request["input"][2]["type"], "reasoning");
        assert_eq!(request["input"][3]["type"], "function_call");
        assert_eq!(request["input"][4]["type"], "function_call_output");
    }

    #[test]
    fn converts_responses_output_to_chat_sse() {
        let sse = responses_response_to_chat_sse(
            &json!({
                "id": "resp_1",
                "output": [
                    { "type": "reasoning", "id": "rs_1", "encrypted_content": "opaque" },
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "search_logs",
                        "arguments": "{\"service\":\"api\"}"
                    }
                ],
                "usage": { "input_tokens": 12, "output_tokens": 8 }
            }),
            "gpt-5.5-pro",
        )
        .unwrap();

        let data = sse.lines().next().unwrap().strip_prefix("data: ").unwrap();
        let chunk: Value = serde_json::from_str(data).unwrap();
        assert_eq!(chunk["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            chunk["choices"][0]["delta"]["tool_calls"][0]["function"]["name"],
            "search_logs"
        );
        assert_eq!(
            chunk["choices"][0]["delta"]["_rush_responses_reasoning"][0]["id"],
            "rs_1"
        );
        assert_eq!(chunk["usage"]["prompt_tokens"], 12);
        assert!(sse.ends_with("data: [DONE]\n\n"));
    }

    #[tokio::test]
    async fn rejects_private_destinations_without_development_override() {
        let allowed = vec!["http://127.0.0.1:8080".to_string()];
        assert!(
            validate_and_resolve_base_url("http://127.0.0.1:8080", &allowed, false)
                .await
                .is_err()
        );
        assert!(
            validate_and_resolve_base_url("http://127.0.0.1:8080", &allowed, true)
                .await
                .is_ok()
        );

        let metadata = vec!["http://169.254.169.254".to_string()];
        assert!(
            validate_and_resolve_base_url("http://169.254.169.254", &metadata, true)
                .await
                .is_err(),
            "the development override must never authorize link-local metadata services"
        );
    }

    #[tokio::test]
    async fn refuses_redirects() {
        static REDIRECT_TARGET_HITS: AtomicUsize = AtomicUsize::new(0);
        async fn redirect() -> Redirect {
            Redirect::temporary("/redirect-target")
        }
        async fn target() -> Json<Value> {
            REDIRECT_TARGET_HITS.fetch_add(1, Ordering::SeqCst);
            Json(json!({"choices":[{"message":{"content":"followed"}}]}))
        }
        REDIRECT_TARGET_HITS.store(0, Ordering::SeqCst);
        let base = serve(
            Router::new()
                .route("/v1/chat/completions", post(redirect))
                .route("/redirect-target", post(target)),
        )
        .await;
        let gateway = test_gateway(base, limits(), 1, Duration::from_secs(1));
        let error = gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "system",
                "user",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_GATEWAY);
        assert_eq!(REDIRECT_TARGET_HITS.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn enforces_total_timeout() {
        async fn slow() -> Json<Value> {
            tokio::time::sleep(Duration::from_millis(150)).await;
            Json(json!({"choices":[{"message":{"content":"late"}}]}))
        }
        let base = serve(Router::new().route("/v1/chat/completions", post(slow))).await;
        let gateway = test_gateway(base, limits(), 1, Duration::from_millis(40));
        let error = gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "system",
                "user",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn rejects_oversized_input_and_response() {
        async fn large() -> Json<Value> {
            Json(json!({"choices":[{"message":{"content":"x".repeat(2_048)}}]}))
        }
        let base = serve(Router::new().route("/v1/chat/completions", post(large))).await;
        let mut small = limits();
        small.max_input_bytes = 16;
        small.max_response_bytes = 128;
        let gateway = test_gateway(base, small, 1, Duration::from_secs(1));
        let input_error = gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "system",
                &"x".repeat(32),
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(input_error.0, StatusCode::PAYLOAD_TOO_LARGE);
        let response_error = gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(response_error.0, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn enforces_user_quota_and_global_concurrency() {
        async fn slow() -> Json<Value> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Json(json!({"choices":[{"message":{"content":"ok"}}]}))
        }
        let base = serve(Router::new().route("/v1/chat/completions", post(slow))).await;
        let mut one_request = limits();
        one_request.user_requests_per_minute = 1;
        let quota_gateway = test_gateway(base.clone(), one_request, 1, Duration::from_secs(1));
        quota_gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap();
        let quota_error = quota_gateway
            .chat(
                LlmOperation::ParseQuery,
                &caller(),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(quota_error.0, StatusCode::TOO_MANY_REQUESTS);

        let concurrency_gateway = test_gateway(base, limits(), 1, Duration::from_secs(1));
        let first_gateway = concurrency_gateway.clone();
        let first = tokio::spawn(async move {
            first_gateway
                .chat(
                    LlmOperation::ParseQuery,
                    &caller(),
                    "gpt-test",
                    "s",
                    "u",
                    10,
                    None,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let busy = concurrency_gateway
            .chat(
                LlmOperation::ParseQuery,
                &LlmCaller::new("user-2", "tenant-2"),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(busy.0, StatusCode::TOO_MANY_REQUESTS);
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn enforces_tenant_quota_across_distinct_users() {
        async fn respond() -> Json<Value> {
            Json(json!({"choices":[{"message":{"content":"ok"}}]}))
        }
        let base = serve(Router::new().route("/v1/chat/completions", post(respond))).await;
        let mut one_tenant_request = limits();
        one_tenant_request.tenant_requests_per_minute = 1;
        let gateway = test_gateway(base, one_tenant_request, 1, Duration::from_secs(1));
        gateway
            .chat(
                LlmOperation::ParseQuery,
                &LlmCaller::new("user-1", "shared-tenant"),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap();
        let error = gateway
            .chat(
                LlmOperation::ParseQuery,
                &LlmCaller::new("user-2", "shared-tenant"),
                "gpt-test",
                "s",
                "u",
                10,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn bounds_completion_tokens_and_emits_low_cardinality_metrics() {
        async fn respond(Json(body): Json<Value>) -> Json<Value> {
            assert_eq!(body["max_completion_tokens"], 100);
            Json(json!({"choices":[{"message":{"content":"ok"}}]}))
        }
        let base = serve(Router::new().route("/v1/chat/completions", post(respond))).await;
        let gateway = test_gateway(base, limits(), 1, Duration::from_secs(1));
        gateway
            .chat(
                LlmOperation::ParsePromql,
                &caller(),
                "gpt-test",
                "s",
                "u",
                500,
                None,
            )
            .await
            .unwrap();
        let metrics = gateway.inner.metrics.render_prometheus();
        assert!(metrics.contains(
            "rush_llm_requests_total{operation=\"parse_promql\",outcome=\"token_bounded\"} 1"
        ));
        assert!(!metrics.contains("tenant-1"));
        assert!(!metrics.contains("user-1"));
    }
}
