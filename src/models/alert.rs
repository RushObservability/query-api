use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub channel_type: String,
    pub config: String,
    pub enabled: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannelResponse {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub channel_type: String,
    pub config: serde_json::Value,
    pub enabled: bool,
    pub created_at: String,
}

const SECRET_CONFIG_KEYS: &[&str] = &[
    "url",
    "webhook_url",
    "token",
    "routing_key",
    "api_key",
    "headers",
];

fn redact_config(config: &str) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(config)
        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
    if let Some(object) = value.as_object_mut() {
        for key in SECRET_CONFIG_KEYS {
            if object.remove(*key).is_some() {
                object.insert(format!("{key}_configured"), serde_json::Value::Bool(true));
            }
        }
    }
    value
}

impl From<NotificationChannel> for NotificationChannelResponse {
    fn from(c: NotificationChannel) -> Self {
        Self {
            id: c.id,
            tenant_id: c.tenant_id,
            name: c.name,
            channel_type: c.channel_type,
            config: redact_config(&c.config),
            enabled: c.enabled,
            created_at: c.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationLogEntry {
    pub id: String,
    pub channel_id: String,
    pub tenant_id: String,
    pub alert_type: String,
    pub alert_name: String,
    pub severity: String,
    pub status: String,
    pub error: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChannelRequest {
    pub name: String,
    pub config: serde_json::Value,
    #[serde(default = "default_true")]
    pub enabled: bool,
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
    pub runbook_url: String,
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
    pub runbook_url: String,
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
            query_config: serde_json::from_str(&r.query_config)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            condition_op: r.condition_op,
            condition_threshold: r.condition_threshold,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: serde_json::from_str(&r.notification_channel_ids)
                .unwrap_or(serde_json::json!([])),
            runbook_url: r.runbook_url,
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
    #[serde(default)]
    pub runbook_url: String,
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
    #[serde(default)]
    pub runbook_url: String,
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

#[cfg(test)]
mod tests {
    use super::redact_config;

    #[test]
    fn notification_response_redacts_credentials_and_endpoints() {
        let redacted = redact_config(
            r#"{
            "webhook_url":"https://hooks.example.test/secret",
            "token":"xoxb-secret",
            "channel":"alerts"
        }"#,
        );
        assert_eq!(redacted["webhook_url_configured"], true);
        assert_eq!(redacted["token_configured"], true);
        assert_eq!(redacted["channel"], "alerts");
        assert!(redacted.get("webhook_url").is_none());
        assert!(redacted.get("token").is_none());
    }
}
