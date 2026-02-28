use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: String,
    pub name: String,
    pub channel_type: String,
    pub config: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannelResponse {
    pub id: String,
    pub name: String,
    pub channel_type: String,
    pub config: serde_json::Value,
    pub created_at: String,
}

impl From<NotificationChannel> for NotificationChannelResponse {
    fn from(c: NotificationChannel) -> Self {
        Self {
            id: c.id,
            name: c.name,
            channel_type: c.channel_type,
            config: serde_json::from_str(&c.config).unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: c.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub signal_type: String,
    pub query_config: String,
    pub condition_op: String,
    pub condition_threshold: f64,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: String,
    pub state: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRuleResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub signal_type: String,
    pub query_config: serde_json::Value,
    pub condition_op: String,
    pub condition_threshold: f64,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: serde_json::Value,
    pub state: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<AlertRule> for AlertRuleResponse {
    fn from(r: AlertRule) -> Self {
        Self {
            id: r.id,
            name: r.name,
            description: r.description,
            enabled: r.enabled,
            signal_type: r.signal_type,
            query_config: serde_json::from_str(&r.query_config).unwrap_or(serde_json::Value::Object(Default::default())),
            condition_op: r.condition_op,
            condition_threshold: r.condition_threshold,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: serde_json::from_str(&r.notification_channel_ids).unwrap_or(serde_json::json!([])),
            state: r.state,
            last_eval_at: r.last_eval_at,
            last_triggered_at: r.last_triggered_at,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub id: String,
    pub rule_id: String,
    pub state: String,
    pub value: f64,
    pub threshold: f64,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEventWithRule {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub state: String,
    pub value: f64,
    pub threshold: f64,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    pub channel_type: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateAlertRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_signal_type")]
    pub signal_type: String,
    pub query_config: serde_json::Value,
    pub condition_op: String,
    pub condition_threshold: f64,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_empty_array")]
    pub notification_channel_ids: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAlertRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_signal_type")]
    pub signal_type: String,
    pub query_config: serde_json::Value,
    pub condition_op: String,
    pub condition_threshold: f64,
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

fn default_signal_type() -> String {
    "apm".to_string()
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}
