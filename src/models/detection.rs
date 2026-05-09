use serde::{Deserialize, Serialize};

// ── Detection Rule (persisted in config_db) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionRule {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub query_sql: String,
    pub interval_secs: i64,
    pub threshold: i64,
    pub severity: String,
    pub window_secs: i64,
    pub enabled: bool,
    pub channels: String,
    pub created_by: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionRuleResponse {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub query_sql: String,
    pub interval_secs: i64,
    pub threshold: i64,
    pub severity: String,
    pub window_secs: i64,
    pub enabled: bool,
    pub channels: serde_json::Value,
    pub created_by: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<DetectionRule> for DetectionRuleResponse {
    fn from(r: DetectionRule) -> Self {
        Self {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            description: r.description,
            query_sql: r.query_sql,
            interval_secs: r.interval_secs,
            threshold: r.threshold,
            severity: r.severity,
            window_secs: r.window_secs,
            enabled: r.enabled,
            channels: serde_json::from_str(&r.channels)
                .unwrap_or(serde_json::json!([])),
            created_by: r.created_by,
            last_eval_at: r.last_eval_at,
            last_triggered_at: r.last_triggered_at,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

// ── Detection Event (persisted in config_db) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionEvent {
    pub id: String,
    pub rule_id: String,
    pub tenant_id: String,
    pub severity: String,
    pub match_count: i64,
    pub sample_data: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionEventWithRule {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub tenant_id: String,
    pub severity: String,
    pub match_count: i64,
    pub sample_data: serde_json::Value,
    pub created_at: String,
}

// ── API request/response types ──

#[derive(Debug, Deserialize)]
pub struct CreateDetectionRuleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub query_sql: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: i64,
    #[serde(default = "default_threshold")]
    pub threshold: i64,
    #[serde(default = "default_severity")]
    pub severity: String,
    #[serde(default = "default_window_secs")]
    pub window_secs: i64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_empty_array")]
    pub channels: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDetectionRuleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub query_sql: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: i64,
    #[serde(default = "default_threshold")]
    pub threshold: i64,
    #[serde(default = "default_severity")]
    pub severity: String,
    #[serde(default = "default_window_secs")]
    pub window_secs: i64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_empty_array")]
    pub channels: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct TestDetectionRuleResponse {
    pub row_count: u64,
    pub would_fire: bool,
    pub sample_data: serde_json::Value,
    pub query_executed: String,
}

fn default_true() -> bool {
    true
}

fn default_interval_secs() -> i64 {
    300
}

fn default_threshold() -> i64 {
    1
}

fn default_severity() -> String {
    "medium".to_string()
}

fn default_window_secs() -> i64 {
    300
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}
