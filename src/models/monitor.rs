use serde::{Deserialize, Serialize};

// ── Domain structs (internal, stored as TEXT in SQLite) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Monitor {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub monitor_type: String,
    pub query_config: String,
    pub critical: Option<f64>,
    pub critical_recovery: Option<f64>,
    pub warning: Option<f64>,
    pub warning_recovery: Option<f64>,
    pub comparator: String,
    pub eval_window_secs: i64,
    pub eval_interval_secs: i64,
    pub group_by: String,
    pub state: String,
    pub group_states: String,
    pub no_data_action: String,
    pub no_data_timeframe: i64,
    pub auto_resolve_hours: Option<i64>,
    pub message: String,
    pub notification_channels: String,
    pub renotify_interval: Option<i64>,
    pub tags: String,
    pub priority: Option<i64>,
    pub enabled: bool,
    pub composite_formula: String,
    pub composite_monitor_ids: String,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

// ── API response (JSON fields parsed from TEXT) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorResponse {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub monitor_type: String,
    pub query_config: serde_json::Value,
    pub critical: Option<f64>,
    pub critical_recovery: Option<f64>,
    pub warning: Option<f64>,
    pub warning_recovery: Option<f64>,
    pub comparator: String,
    pub eval_window_secs: i64,
    pub eval_interval_secs: i64,
    pub group_by: serde_json::Value,
    pub state: String,
    pub group_states: serde_json::Value,
    pub no_data_action: String,
    pub no_data_timeframe: i64,
    pub auto_resolve_hours: Option<i64>,
    pub message: String,
    pub notification_channels: serde_json::Value,
    pub renotify_interval: Option<i64>,
    pub tags: serde_json::Value,
    pub priority: Option<i64>,
    pub enabled: bool,
    pub composite_formula: String,
    pub composite_monitor_ids: serde_json::Value,
    pub last_eval_at: Option<String>,
    pub last_triggered_at: Option<String>,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Monitor> for MonitorResponse {
    fn from(m: Monitor) -> Self {
        Self {
            id: m.id,
            tenant_id: m.tenant_id,
            name: m.name,
            monitor_type: m.monitor_type,
            query_config: serde_json::from_str(&m.query_config)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            critical: m.critical,
            critical_recovery: m.critical_recovery,
            warning: m.warning,
            warning_recovery: m.warning_recovery,
            comparator: m.comparator,
            eval_window_secs: m.eval_window_secs,
            eval_interval_secs: m.eval_interval_secs,
            group_by: serde_json::from_str(&m.group_by).unwrap_or(serde_json::json!([])),
            state: m.state,
            group_states: serde_json::from_str(&m.group_states)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            no_data_action: m.no_data_action,
            no_data_timeframe: m.no_data_timeframe,
            auto_resolve_hours: m.auto_resolve_hours,
            message: m.message,
            notification_channels: serde_json::from_str(&m.notification_channels)
                .unwrap_or(serde_json::json!([])),
            renotify_interval: m.renotify_interval,
            tags: serde_json::from_str(&m.tags).unwrap_or(serde_json::json!([])),
            priority: m.priority,
            enabled: m.enabled,
            composite_formula: m.composite_formula,
            composite_monitor_ids: serde_json::from_str(&m.composite_monitor_ids)
                .unwrap_or(serde_json::json!([])),
            last_eval_at: m.last_eval_at,
            last_triggered_at: m.last_triggered_at,
            created_by: m.created_by,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

// ── Monitor events ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorEvent {
    pub id: String,
    pub monitor_id: String,
    pub tenant_id: String,
    pub group_key: String,
    pub prev_state: String,
    pub new_state: String,
    pub value: Option<f64>,
    pub threshold: Option<f64>,
    pub message: String,
    pub created_at: String,
}

// ── API request types ──

#[derive(Debug, Deserialize)]
pub struct CreateMonitorRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub monitor_type: String,
    pub query_config: serde_json::Value,
    pub critical: Option<f64>,
    #[serde(default)]
    pub critical_recovery: Option<f64>,
    #[serde(default)]
    pub warning: Option<f64>,
    #[serde(default)]
    pub warning_recovery: Option<f64>,
    #[serde(default = "default_comparator")]
    pub comparator: String,
    #[serde(default = "default_eval_window")]
    pub eval_window_secs: i64,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_empty_array")]
    pub group_by: serde_json::Value,
    #[serde(default = "default_no_data_action")]
    pub no_data_action: String,
    #[serde(default = "default_no_data_timeframe")]
    pub no_data_timeframe: i64,
    #[serde(default)]
    pub auto_resolve_hours: Option<i64>,
    #[serde(default)]
    pub message: String,
    #[serde(default = "default_empty_array")]
    pub notification_channels: serde_json::Value,
    #[serde(default)]
    pub renotify_interval: Option<i64>,
    #[serde(default = "default_empty_array")]
    pub tags: serde_json::Value,
    #[serde(default)]
    pub priority: Option<i64>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub composite_formula: String,
    #[serde(default = "default_empty_array")]
    pub composite_monitor_ids: serde_json::Value,
    #[serde(default)]
    pub created_by: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMonitorRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub monitor_type: String,
    pub query_config: serde_json::Value,
    pub critical: Option<f64>,
    #[serde(default)]
    pub critical_recovery: Option<f64>,
    #[serde(default)]
    pub warning: Option<f64>,
    #[serde(default)]
    pub warning_recovery: Option<f64>,
    #[serde(default = "default_comparator")]
    pub comparator: String,
    #[serde(default = "default_eval_window")]
    pub eval_window_secs: i64,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_secs: i64,
    #[serde(default = "default_empty_array")]
    pub group_by: serde_json::Value,
    #[serde(default = "default_no_data_action")]
    pub no_data_action: String,
    #[serde(default = "default_no_data_timeframe")]
    pub no_data_timeframe: i64,
    #[serde(default)]
    pub auto_resolve_hours: Option<i64>,
    #[serde(default)]
    pub message: String,
    #[serde(default = "default_empty_array")]
    pub notification_channels: serde_json::Value,
    #[serde(default)]
    pub renotify_interval: Option<i64>,
    #[serde(default = "default_empty_array")]
    pub tags: serde_json::Value,
    #[serde(default)]
    pub priority: Option<i64>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub composite_formula: String,
    #[serde(default = "default_empty_array")]
    pub composite_monitor_ids: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct PreviewMonitorRequest {
    #[serde(rename = "type")]
    pub monitor_type: String,
    pub query_config: serde_json::Value,
    #[serde(default = "default_eval_window")]
    pub eval_window_secs: i64,
    #[serde(default = "default_empty_array")]
    pub group_by: serde_json::Value,
}

// ── Query config structs (deserialized from the JSON blob) ──

#[derive(Debug, Clone, Deserialize)]
pub struct MetricQueryConfig {
    pub metric_name: String,
    #[serde(default = "default_aggregation")]
    pub aggregation: String,
    #[serde(default)]
    pub filters: Vec<MetricFilter>,
    #[serde(default)]
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricFilter {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogQueryConfig {
    #[serde(default)]
    pub search: String,
    #[serde(default)]
    pub filters: Vec<LogFilter>,
    #[serde(default)]
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogFilter {
    pub field: String,
    #[serde(default = "default_eq_op")]
    pub op: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApmQueryConfig {
    pub service: String,
    pub metric: String,
    #[serde(default)]
    pub endpoint_filter: Option<String>,
    #[serde(default)]
    pub group_by: Vec<String>,
}

fn default_true() -> bool {
    true
}

fn default_comparator() -> String {
    "above".to_string()
}

fn default_eval_window() -> i64 {
    300
}

fn default_eval_interval() -> i64 {
    60
}

fn default_no_data_action() -> String {
    "show".to_string()
}

fn default_no_data_timeframe() -> i64 {
    600
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}

fn default_aggregation() -> String {
    "avg".to_string()
}

fn default_eq_op() -> String {
    "=".to_string()
}

// ── Autocomplete request ──

#[derive(Debug, Deserialize)]
pub struct AutocompleteQuery {
    #[serde(rename = "type")]
    pub ac_type: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub metric: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub service: String,
}

// ── Suggest request / response ──

#[derive(Debug, Deserialize)]
pub struct SuggestRequest {
    pub monitor_type: String,
    pub query_config: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct Suggestion {
    pub text: String,
    pub severity: String, // "info" or "warning"
}

#[derive(Debug, Serialize)]
pub struct SuggestResponse {
    pub suggestions: Vec<Suggestion>,
}
