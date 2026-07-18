use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLink {
    pub tenant_id: String,
    pub service_name: String,
    pub github_repo: String,
    pub github_installation_id: u64,
    pub default_branch: String,
    pub root_path: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateServiceLinkRequest {
    pub service_name: String,
    pub github_repo: String,
    #[serde(default)]
    pub github_installation_id: u64,
    #[serde(default = "default_branch")]
    pub default_branch: String,
    #[serde(default)]
    pub root_path: String,
}

fn default_branch() -> String {
    "main".to_string()
}
