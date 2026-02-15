use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployMarker {
    pub id: String,
    pub service_name: String,
    pub version: String,
    pub commit_sha: String,
    pub description: String,
    pub environment: String,
    pub deployed_by: String,
    pub deployed_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateDeployMarkerRequest {
    pub service_name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub commit_sha: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub deployed_by: String,
}

#[derive(Debug, Deserialize)]
pub struct DeployMarkerQuery {
    pub service_name: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}
