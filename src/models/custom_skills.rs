use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomSkill {
    pub id: String,
    pub name: String,
    pub title: String,
    pub description: String,
    pub content: String,
    pub allowed_tools: Vec<String>,
    pub enabled: bool,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateCustomSkillRequest {
    pub name: String,
    pub title: String,
    pub description: String,
    pub content: String,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCustomSkillRequest {
    pub title: String,
    pub description: String,
    pub content: String,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}
