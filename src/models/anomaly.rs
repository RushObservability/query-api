use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub source: String,
    pub pattern: String,
    pub query: String,
    pub service_name: String,
    pub apm_metric: String,
    pub sensitivity: f64,
    pub alpha: f64,
    pub eval_interval_secs: i64,
    pub window_secs: i64,
    pub split_labels: String,
    pub notification_channel_ids: String,
    pub state: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyRuleResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub source: String,
    pub pattern: String,
    pub query: String,
    pub service_name: String,
    pub apm_metric: String,
    pub sensitivity: f64,
    pub alpha: f64,
    pub eval_interval_secs: i64,
    pub window_secs: i64,
    pub split_labels: serde_json::Value,
    pub notification_channel_ids: serde_json::Value,
    pub state: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<AnomalyRule> for AnomalyRuleResponse {
    fn from(r: AnomalyRule) -> Self {
        Self {
            id: r.id,
            name: r.name,
            description: r.description,
            enabled: r.enabled,
            source: r.source,
            pattern: r.pattern,
            query: r.query,
            service_name: r.service_name,
            apm_metric: r.apm_metric,
            sensitivity: r.sensitivity,
            alpha: r.alpha,
            eval_interval_secs: r.eval_interval_secs,
            window_secs: r.window_secs,
            split_labels: serde_json::from_str(&r.split_labels)
                .unwrap_or(serde_json::json!([])),
            notification_channel_ids: serde_json::from_str(&r.notification_channel_ids)
                .unwrap_or(serde_json::json!([])),
            state: r.state,
            last_eval_at: r.last_eval_at,
            last_triggered_at: r.last_triggered_at,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyEvent {
    pub id: String,
    pub rule_id: String,
    pub state: String,
    pub metric: String,
    pub value: f64,
    pub expected: f64,
    pub deviation: f64,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyEventWithRule {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub state: String,
    pub metric: String,
    pub value: f64,
    pub expected: f64,
    pub deviation: f64,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAnomalyRuleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub source: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub apm_metric: String,
    #[serde(default = "default_sensitivity")]
    pub sensitivity: f64,
    #[serde(default = "default_alpha")]
    pub alpha: f64,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_window_secs")]
    pub window_secs: i64,
    #[serde(default = "default_empty_array")]
    pub split_labels: serde_json::Value,
    #[serde(default = "default_empty_array")]
    pub notification_channel_ids: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAnomalyRuleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub source: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub apm_metric: String,
    #[serde(default = "default_sensitivity")]
    pub sensitivity: f64,
    #[serde(default = "default_alpha")]
    pub alpha: f64,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_window_secs")]
    pub window_secs: i64,
    #[serde(default = "default_empty_array")]
    pub split_labels: serde_json::Value,
    #[serde(default = "default_empty_array")]
    pub notification_channel_ids: serde_json::Value,
}

fn default_true() -> bool {
    true
}

fn default_sensitivity() -> f64 {
    3.0
}

fn default_alpha() -> f64 {
    0.25
}

fn default_eval_interval() -> i64 {
    300
}

fn default_window_secs() -> i64 {
    3600
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}

// ── AI Analysis types ──

#[derive(Debug, Deserialize)]
pub struct AnalyzeAnomalyRequest {
    #[serde(default)]
    pub additional_context: String,
}

#[derive(Debug, Serialize)]
pub struct AnalyzeAnomalyResponse {
    pub analysis: String,
    pub model: String,
}

// ── Correlation types ──

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct CorrelatedBucket {
    pub service_name: String,
    pub bucket: String,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct CorrelatedService {
    pub name: String,
    pub total: u64,
    pub buckets: Vec<ServiceBucket>,
}

#[derive(Debug, Serialize)]
pub struct ServiceBucket {
    pub timestamp: String,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct CorrelationLog {
    pub timestamp: String,
    pub service_name: String,
    pub severity_text: String,
    pub body: String,
    pub trace_id: String,
}

#[derive(Debug, Serialize)]
pub struct CorrelationResponse {
    pub status_code: u16,
    pub window_from: String,
    pub window_to: String,
    pub services: Vec<CorrelatedService>,
    pub logs: Vec<CorrelationLog>,
}
