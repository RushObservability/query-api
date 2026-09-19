use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DashboardDefaults {
    pub time_range_minutes: u32,
    pub refresh_interval_secs: u32,
}

impl Default for DashboardDefaults {
    fn default() -> Self {
        Self {
            time_range_minutes: 60,
            refresh_interval_secs: 0,
        }
    }
}

impl DashboardDefaults {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=525_600).contains(&self.time_range_minutes) {
            return Err("default time range must be between 1 and 525600 minutes");
        }
        if !matches!(self.refresh_interval_secs, 0 | 30 | 60 | 300) {
            return Err("default refresh interval must be 0, 30, 60, or 300 seconds");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    #[serde(default)]
    pub defaults: DashboardDefaults,
    pub id: String,
    pub name: String,
    pub description: String,
    pub tenant_id: String,
    pub owner_id: String,
    pub visibility: String,
    pub tags: serde_json::Value,
    /// Template variables (Grafana-style) — array of variable definitions.
    #[serde(default = "default_empty_array")]
    pub variables: serde_json::Value,
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
            query_config: serde_json::from_str(&w.query_config)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            position: serde_json::from_str(&w.position)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            display_config: serde_json::from_str(&w.display_config)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: w.created_at,
            updated_at: w.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardTemplate {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub is_builtin: bool,
    pub template_json: serde_json::Value,
    pub tags: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardExport {
    pub format_version: String,
    pub exported_at: String,
    pub dashboard: DashboardExportMeta,
    pub widgets: Vec<WidgetExport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardExportMeta {
    #[serde(default)]
    pub defaults: DashboardDefaults,
    pub name: String,
    pub description: String,
    pub visibility: String,
    pub tags: serde_json::Value,
    #[serde(default = "default_empty_array")]
    pub variables: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetExport {
    pub title: String,
    pub widget_type: String,
    pub query_config: serde_json::Value,
    pub position: serde_json::Value,
    pub display_config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateDashboardRequest {
    #[serde(default)]
    pub defaults: DashboardDefaults,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_visibility")]
    pub visibility: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_empty_array")]
    pub variables: serde_json::Value,
}

fn default_visibility() -> String {
    "tenant".to_string()
}

fn default_empty_array() -> serde_json::Value {
    serde_json::Value::Array(Vec::new())
}

#[derive(Debug, Deserialize)]
pub struct UpdateDashboardRequest {
    /// Omitted by older clients and metadata-only edits; keep the saved defaults.
    pub defaults: Option<DashboardDefaults>,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_visibility")]
    pub visibility: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_empty_array")]
    pub variables: serde_json::Value,
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

#[derive(Debug, Deserialize)]
pub struct ImportDashboardRequest {
    pub format_version: String,
    pub dashboard: DashboardExportMeta,
    pub widgets: Vec<WidgetExport>,
}

#[derive(Debug, Deserialize)]
pub struct CreateFromTemplateRequest {
    pub name: String,
}

fn default_empty_object() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

#[cfg(test)]
mod dashboard_defaults_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_create_and_import_use_one_hour_and_no_refresh() {
        let create: CreateDashboardRequest =
            serde_json::from_value(json!({"name": "Legacy"})).unwrap();
        assert_eq!(create.defaults, DashboardDefaults::default());
        let import: ImportDashboardRequest = serde_json::from_value(json!({
            "format_version": "v1", "dashboard": {"name": "Legacy", "description": "", "visibility": "tenant", "tags": []}, "widgets": []
        })).unwrap();
        assert_eq!(import.dashboard.defaults, DashboardDefaults::default());
        assert_eq!(
            serde_json::from_str::<DashboardDefaults>("{}").unwrap(),
            DashboardDefaults::default()
        );
    }

    #[test]
    fn metadata_edits_can_omit_defaults_and_refresh_can_be_turned_off() {
        let edit: UpdateDashboardRequest =
            serde_json::from_value(json!({"name": "Renamed"})).unwrap();
        assert!(edit.defaults.is_none());
        let edit: UpdateDashboardRequest = serde_json::from_value(json!({"name": "Renamed", "defaults": {"time_range_minutes": 10080, "refresh_interval_secs": 0}})).unwrap();
        assert_eq!(
            edit.defaults.unwrap(),
            DashboardDefaults {
                time_range_minutes: 10080,
                refresh_interval_secs: 0
            }
        );
    }

    #[test]
    fn defaults_survive_export_import() {
        let meta: DashboardExportMeta = serde_json::from_value(json!({
            "name": "Traffic", "description": "", "visibility": "tenant", "tags": [],
            "defaults": {"time_range_minutes": 10080, "refresh_interval_secs": 30}
        }))
        .unwrap();
        let exported = serde_json::to_value(meta).unwrap();
        let imported: DashboardExportMeta = serde_json::from_value(exported).unwrap();
        assert_eq!(
            imported.defaults,
            DashboardDefaults {
                time_range_minutes: 10080,
                refresh_interval_secs: 30
            }
        );
    }

    #[test]
    fn reject_unsafe_ranges_and_refresh_intervals() {
        for minutes in [0, 525_601, u32::MAX] {
            assert!(
                DashboardDefaults {
                    time_range_minutes: minutes,
                    refresh_interval_secs: 0
                }
                .validate()
                .is_err()
            );
        }
        for seconds in [1, 5, 29, 301, u32::MAX] {
            assert!(
                DashboardDefaults {
                    time_range_minutes: 60,
                    refresh_interval_secs: seconds
                }
                .validate()
                .is_err()
            );
        }
        for seconds in [0, 30, 60, 300] {
            assert!(
                DashboardDefaults {
                    time_range_minutes: 525_600,
                    refresh_interval_secs: seconds
                }
                .validate()
                .is_ok()
            );
        }
        assert!(
            serde_json::from_value::<DashboardDefaults>(json!({"time_range_minutes": -1})).is_err()
        );
    }
}
