use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Slo {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub slo_type: String,
    pub indicator_type: String,
    pub service_name: String,
    pub metric_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub threshold_ms: Option<f64>,
    pub threshold_value: Option<f64>,
    pub threshold_op: Option<String>,
    pub error_filters: String,
    pub total_filters: String,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: String,
    pub state: String,
    pub error_budget_remaining: Option<f64>,
    pub error_count: Option<f64>,
    pub total_count: Option<f64>,
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
    pub slo_type: String,
    pub indicator_type: String,
    pub service_name: String,
    pub metric_name: String,
    pub error_promql: String,
    pub total_promql: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub threshold_ms: Option<f64>,
    pub threshold_value: Option<f64>,
    pub threshold_op: Option<String>,
    pub error_filters: serde_json::Value,
    pub total_filters: serde_json::Value,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: serde_json::Value,
    pub state: String,
    pub error_budget_remaining: Option<f64>,
    pub error_count: Option<f64>,
    pub total_count: Option<f64>,
    pub last_eval_at: Option<String>,
    pub last_breached_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Slo> for SloResponse {
    fn from(s: Slo) -> Self {
        let error_promql = stored_promql(&s.error_filters).unwrap_or_default();
        let total_promql = stored_promql(&s.total_filters).unwrap_or_default();
        let error_filters = if error_promql.is_empty() {
            serde_json::from_str(&s.error_filters).unwrap_or(serde_json::json!([]))
        } else {
            serde_json::json!([])
        };
        let total_filters = if total_promql.is_empty() {
            serde_json::from_str(&s.total_filters).unwrap_or(serde_json::json!([]))
        } else {
            serde_json::json!([])
        };
        Self {
            id: s.id,
            name: s.name,
            description: s.description,
            enabled: s.enabled,
            slo_type: s.slo_type,
            indicator_type: s.indicator_type,
            service_name: s.service_name,
            metric_name: s.metric_name,
            error_promql,
            total_promql,
            window_type: s.window_type,
            target_percentage: s.target_percentage,
            threshold_ms: s.threshold_ms,
            threshold_value: s.threshold_value,
            threshold_op: s.threshold_op,
            error_filters,
            total_filters,
            eval_interval_secs: s.eval_interval_secs,
            notification_channel_ids: serde_json::from_str(&s.notification_channel_ids)
                .unwrap_or(serde_json::json!([])),
            state: s.state,
            error_budget_remaining: s.error_budget_remaining,
            error_count: s.error_count,
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
    pub tenant_id: String,
    pub state: String,
    pub error_count: f64,
    pub total_count: f64,
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
    #[serde(default = "default_slo_type")]
    pub slo_type: String,
    #[serde(default = "default_indicator_type")]
    pub indicator_type: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub metric_name: String,
    #[serde(default)]
    pub error_promql: String,
    #[serde(default)]
    pub total_promql: String,
    #[serde(default = "default_window_type")]
    pub window_type: String,
    pub target_percentage: f64,
    #[serde(default)]
    pub threshold_ms: Option<f64>,
    #[serde(default)]
    pub threshold_value: Option<f64>,
    #[serde(default)]
    pub threshold_op: Option<String>,
    #[serde(default = "default_empty_array")]
    pub error_filters: serde_json::Value,
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
    #[serde(default = "default_slo_type")]
    pub slo_type: String,
    #[serde(default = "default_indicator_type")]
    pub indicator_type: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub metric_name: String,
    #[serde(default)]
    pub error_promql: String,
    #[serde(default)]
    pub total_promql: String,
    #[serde(default = "default_window_type")]
    pub window_type: String,
    pub target_percentage: f64,
    #[serde(default)]
    pub threshold_ms: Option<f64>,
    #[serde(default)]
    pub threshold_value: Option<f64>,
    #[serde(default)]
    pub threshold_op: Option<String>,
    #[serde(default = "default_empty_array")]
    pub error_filters: serde_json::Value,
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

fn default_slo_type() -> String {
    "trace".to_string()
}

fn default_indicator_type() -> String {
    "availability".to_string()
}

fn default_window_type() -> String {
    "rolling_30d".to_string()
}

fn default_eval_interval() -> i64 {
    60
}

fn default_empty_array() -> serde_json::Value {
    serde_json::json!([])
}

pub fn stored_promql(raw: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()?
        .get("promql")?
        .as_str()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::stored_promql;

    #[test]
    fn reads_promql_from_stored_metric_config() {
        assert_eq!(
            stored_promql(r#"{"promql":" sum(rate(http_requests_total[5m])) "}"#),
            Some("sum(rate(http_requests_total[5m]))".to_string())
        );
    }

    #[test]
    fn ignores_legacy_filter_arrays() {
        assert_eq!(
            stored_promql(r#"[{"field":"service_name","op":"=","value":"api"}]"#),
            None
        );
    }
}
