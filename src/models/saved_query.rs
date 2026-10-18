use serde::{Deserialize, Serialize};

pub const VISIBILITY_PRIVATE: &str = "private";
pub const VISIBILITY_GROUP: &str = "group";
pub const VISIBILITY_TENANT: &str = "tenant";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SavedQueryFilter {
    pub field: String,
    pub op: String,
    pub value: String,
}

/// The Explore state a saved query restores. Stored as JSON so new fields can
/// be added without a schema migration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SavedQueryBody {
    #[serde(default)]
    pub filters: Vec<SavedQueryFilter>,
    #[serde(default)]
    pub search: String,
    #[serde(default)]
    pub group_by: String,
    #[serde(default)]
    pub time_preset: u32,
    #[serde(default)]
    pub view_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SavedQuery {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub visibility: String,
    /// The group the query is shared with when `visibility` is `group`, else empty.
    pub group_id: String,
    pub owner_id: String,
    pub owner_name: String,
    pub query: SavedQueryBody,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedQueryRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_visibility")]
    pub visibility: String,
    #[serde(default)]
    pub group_id: String,
    pub query: SavedQueryBody,
}

/// A group the caller belongs to, offered as a sharing target.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SavedQueryGroup {
    pub id: String,
    pub name: String,
}

fn default_visibility() -> String {
    VISIBILITY_PRIVATE.to_string()
}
