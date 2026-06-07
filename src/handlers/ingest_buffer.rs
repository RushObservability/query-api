//! Ingest-buffer status (durable spool depth) for the Stats/Settings surface.

use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::IntoResponse};

use crate::AppState;
use crate::handlers::users::require_admin;

/// GET /api/v1/ingest/buffer — durable write-buffer depth + backend.
pub async fn buffer_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let b = &state.writer.buffer;
    let pending_bytes = b.total_bytes();
    let max_bytes = b.max_bytes();
    let pct = if max_bytes > 0 { (pending_bytes as f64 / max_bytes as f64 * 100.0).min(100.0) } else { 0.0 };
    let oldest_age_secs = b.oldest_age_secs().await.unwrap_or(0);
    Ok(Json(serde_json::json!({
        "backend": b.backend_name(),
        "pending_bytes": pending_bytes,
        "pending_count": b.segment_count(),
        "max_bytes": max_bytes,
        "used_pct": pct,
        "oldest_age_secs": oldest_age_secs,
        "committed_total": b.committed_total(),
    })))
}
