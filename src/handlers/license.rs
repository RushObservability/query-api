use axum::{Json, response::IntoResponse};

/// `GET /api/v1/license` — current license status + entitlements.
/// Non-sensitive (it's the customer's own license), so no auth required; the UI
/// reads it to render the License page and gate add-on UI.
pub async fn get_license() -> impl IntoResponse {
    Json(crate::license::evaluate())
}
