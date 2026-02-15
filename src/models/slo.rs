use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Slo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub service_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub good_filters: String,
    pub total_filters: String,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: String,
    pub state: String,
    pub error_budget_remaining: Option<f64>,
    pub good_count: Option<i64>,
    pub total_count: Option<i64>,
    pub last_eval_at: Option<String>,
    pub last_breached_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub service_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub good_filters: serde_json::Value,
    pub total_filters: serde_json::Value,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: serde_json::Value,
    pub state: String,
    pub error_budget_remaining: Option<f64>,
    pub good_count: Option<i64>,
    pub total_count: Option<i64>,
    pub last_eval_at: Option<String>,
    pub last_breached_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Slo> for SloResponse {
    fn from(s: Slo) -> Self {
        Self {
            id: s.id,
            name: s.name,
            description: s.description,
            enabled: s.enabled,
            service_name: s.service_name,
            window_type: s.window_type,
            target_percentage: s.target_percentage,
            good_filters: serde_json::from_str(&s.good_filters).unwrap_or(serde_json::json!([])),
            total_filters: serde_json::from_str(&s.total_filters).unwrap_or(serde_json::json!([])),
            eval_interval_secs: s.eval_interval_secs,
            notification_channel_ids: serde_json::from_str(&s.notification_channel_ids).unwrap_or(serde_json::json!([])),
            state: s.state,
            error_budget_remaining: s.error_budget_remaining,
            good_count: s.good_count,
            total_count: s.total_count,
            last_eval_at: s.last_eval_at,
            last_breached_at: s.last_breached_at,
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloEvent {
    pub id: String,
    pub slo_id: String,
    pub state: String,
    pub good_count: i64,
    pub total_count: i64,
    pub error_budget_remaining: f64,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateSloRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub service_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub good_filters: serde_json::Value,
    #[serde(default = "default_empty_array")]
    pub total_filters: serde_json::Value,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_empty_array")]
    pub notification_channel_ids: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSloRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub service_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub good_filters: serde_json::Value,
    #[serde(default = "default_empty_array")]
    pub total_filters: serde_json::Value,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_empty_array")]
    pub notification_channel_ids: serde_json::Value,
}

fn default_true() -> bool {
    true
}

fn default_eval_interval() -> i64 {
    60
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}
