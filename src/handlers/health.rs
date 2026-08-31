use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::{Value, json};

use crate::AppState;

#[derive(Debug)]
struct AuthenticationPosture {
    production: bool,
    compatibility_override: bool,
    anonymous_query_tenants: Vec<String>,
    anonymous_ingest_tenants: Vec<String>,
    query_failed: bool,
}

impl AuthenticationPosture {
    fn secure(&self) -> bool {
        !self.compatibility_override
            && self.anonymous_query_tenants.is_empty()
            && self.anonymous_ingest_tenants.is_empty()
            && !self.query_failed
    }

    fn ready(&self) -> bool {
        !self.query_failed && (!self.production || !self.compatibility_override)
    }
}

async fn authentication_posture(state: &AppState) -> AuthenticationPosture {
    let production = crate::api_key_auth::production_mode();
    let compatibility_override = crate::api_key_auth::allow_anonymous_default();
    match state.config_db.list_tenants().await {
        Ok(tenants) => {
            let mut anonymous_query_tenants = Vec::new();
            let mut anonymous_ingest_tenants = Vec::new();
            for (id, name, enabled, auth_required, _) in tenants {
                if !enabled {
                    continue;
                }
                if !auth_required {
                    anonymous_query_tenants.push(name.clone());
                }
                match state
                    .config_db
                    .tenant_ingest_auth_required_checked(&id)
                    .await
                {
                    Ok(Some(false)) => anonymous_ingest_tenants.push(name),
                    Ok(Some(true)) => {}
                    Ok(None) => {
                        tracing::error!(tenant_id = %id, "tenant disappeared during health policy check");
                        return AuthenticationPosture {
                            production,
                            compatibility_override,
                            anonymous_query_tenants,
                            anonymous_ingest_tenants,
                            query_failed: true,
                        };
                    }
                    Err(error) => {
                        tracing::error!(tenant_id = %id, %error, "failed to inspect tenant ingest policy");
                        return AuthenticationPosture {
                            production,
                            compatibility_override,
                            anonymous_query_tenants,
                            anonymous_ingest_tenants,
                            query_failed: true,
                        };
                    }
                }
            }
            AuthenticationPosture {
                production,
                compatibility_override,
                anonymous_query_tenants,
                anonymous_ingest_tenants,
                query_failed: false,
            }
        }
        Err(error) => {
            tracing::error!(%error, "failed to inspect tenant authentication posture");
            AuthenticationPosture {
                production,
                compatibility_override,
                anonymous_query_tenants: Vec::new(),
                anonymous_ingest_tenants: Vec::new(),
                query_failed: true,
            }
        }
    }
}

pub async fn healthz(State(state): State<AppState>) -> Json<Value> {
    let auth = authentication_posture(&state).await;
    let audit = state.audit.health();
    let secure = crate::row_policy_supported() && auth.secure() && audit.ready;
    Json(json!({
        "status": "ok",
        "tenant_isolation": crate::tenant_isolation_status(),
        "authentication": {
            "environment": if auth.production { "production" } else { "development" },
            "anonymous_default_compatibility": auth.compatibility_override,
            "anonymous_query_tenants": auth.anonymous_query_tenants,
            "anonymous_ingest_tenants": auth.anonymous_ingest_tenants,
        },
        "audit": {
            "status": if audit.ready { "ok" } else { "degraded" },
            "pending_events": audit.pending_events,
            "pending_bytes": audit.pending_bytes,
            "max_bytes": audit.max_bytes,
            "write_failures": audit.write_failures,
        },
        "secure": secure,
    }))
}

pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let auth = authentication_posture(&state).await;
    let audit = state.audit.health();
    let shutting_down = state.shutdown.is_requested();
    let ready = !shutting_down && crate::tenant_isolation_ready() && auth.ready() && audit.ready;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "status": if ready { "ready" } else if shutting_down { "draining" } else { "not_ready" },
            "shutdown": shutting_down,
            "tenant_isolation": crate::tenant_isolation_status(),
            "authentication": {
                "environment": if auth.production { "production" } else { "development" },
                "anonymous_default_compatibility": auth.compatibility_override,
                "anonymous_query_tenants": auth.anonymous_query_tenants,
                "anonymous_ingest_tenants": auth.anonymous_ingest_tenants,
            },
            "audit": {
                "status": if audit.ready { "ok" } else { "degraded" },
                "pending_events": audit.pending_events,
                "pending_bytes": audit.pending_bytes,
                "max_bytes": audit.max_bytes,
                "write_failures": audit.write_failures,
            },
            "secure": crate::row_policy_supported() && auth.secure() && audit.ready,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::AuthenticationPosture;

    #[test]
    fn production_allows_explicit_tenant_open_modes_but_marks_them_insecure() {
        let open_tenant = AuthenticationPosture {
            production: true,
            compatibility_override: false,
            anonymous_query_tenants: vec!["default".to_string()],
            anonymous_ingest_tenants: vec!["default".to_string()],
            query_failed: false,
        };
        assert!(open_tenant.ready());
        assert!(!open_tenant.secure());

        let override_enabled = AuthenticationPosture {
            production: true,
            compatibility_override: true,
            anonymous_query_tenants: Vec::new(),
            anonymous_ingest_tenants: Vec::new(),
            query_failed: false,
        };
        assert!(!override_enabled.ready());
        assert!(!override_enabled.secure());
    }

    #[test]
    fn development_compatibility_is_ready_but_prominently_insecure() {
        let posture = AuthenticationPosture {
            production: false,
            compatibility_override: true,
            anonymous_query_tenants: vec!["default".to_string()],
            anonymous_ingest_tenants: vec!["default".to_string()],
            query_failed: false,
        };
        assert!(posture.ready());
        assert!(!posture.secure());
    }

    #[test]
    fn locked_production_is_ready_and_secure() {
        let posture = AuthenticationPosture {
            production: true,
            compatibility_override: false,
            anonymous_query_tenants: Vec::new(),
            anonymous_ingest_tenants: Vec::new(),
            query_failed: false,
        };
        assert!(posture.ready());
        assert!(posture.secure());
    }
}
