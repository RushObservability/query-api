use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub name: String,
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Widget {
    pub id: String,
    pub dashboard_id: String,
    pub title: String,
    pub widget_type: String,
    pub query_config: String,
    pub position: String,
    pub display_config: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardWithWidgets {
    #[serde(flatten)]
    pub dashboard: Dashboard,
    pub widgets: Vec<WidgetResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetResponse {
    pub id: String,
    pub dashboard_id: String,
    pub title: String,
    pub widget_type: String,
    pub query_config: serde_json::Value,
    pub position: serde_json::Value,
    pub display_config: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Widget> for WidgetResponse {
    fn from(w: Widget) -> Self {
        Self {
            id: w.id,
            dashboard_id: w.dashboard_id,
            title: w.title,
            widget_type: w.widget_type,
            query_config: serde_json::from_str(&w.query_config).unwrap_or(serde_json::Value::Object(Default::default())),
            position: serde_json::from_str(&w.position).unwrap_or(serde_json::Value::Object(Default::default())),
            display_config: serde_json::from_str(&w.display_config).unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: w.created_at,
            updated_at: w.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateDashboardRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDashboardRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateWidgetRequest {
    pub title: String,
    pub widget_type: String,
    pub query_config: serde_json::Value,
    pub position: serde_json::Value,
    #[serde(default = "default_empty_object")]
    pub display_config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWidgetRequest {
    pub title: String,
    pub widget_type: String,
    pub query_config: serde_json::Value,
    pub position: serde_json::Value,
    #[serde(default = "default_empty_object")]
    pub display_config: serde_json::Value,
}

fn default_empty_object() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}
