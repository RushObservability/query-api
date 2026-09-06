//! Build-specific API extensions.
//!
//! The public distribution uses this no-op community edition. The private
//! licensed build replaces this file during composition and adds its handlers,
//! migrations, and runtime checks without changing the core server.

use anyhow::Result;
use axum::Router;
use clickhouse::Client;

use crate::AppState;

pub const BUILD_EDITION: &str = "community";

/// Validate requirements that must hold before the API touches its databases.
pub fn validate_startup() -> Result<()> {
    Ok(())
}

/// Add routes supplied by this build edition.
pub fn routes(router: Router<AppState>) -> Router<AppState> {
    router
}

/// Report whether the licensed Kubernetes access service is available.
pub fn kubernetes_logging_available() -> bool {
    false
}

/// Report whether this build includes paid database integration management.
pub fn managed_integrations_available() -> bool {
    false
}

/// Apply schema owned by this build edition after the community migrations.
pub async fn run_migrations(_client: &Client) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn community_edition_starts_without_a_license() {
        assert!(validate_startup().is_ok());
        assert_eq!(BUILD_EDITION, "community");
        assert!(!kubernetes_logging_available());
        assert!(!managed_integrations_available());
    }

    #[test]
    fn community_edition_contains_no_paid_route_modules() {
        let handlers = include_str!("handlers/mod.rs");
        for paid_handler in [
            "pub mod pg_explain;",
            "pub mod mysql_explain;",
            "pub mod kubernetes_access;",
            "pub mod integrations;",
        ] {
            assert!(
                !handlers.contains(paid_handler),
                "community handler leaked: {paid_handler}"
            );
        }

        let storage = include_str!("clickhouse_config.rs");
        for paid_table in [
            "config_integration_targets",
            "config_pg_explain_jobs",
            "config_mysql_explain_jobs",
            "config_kubernetes_access_events",
        ] {
            assert!(
                !storage.contains(&format!("CREATE TABLE IF NOT EXISTS {paid_table}")),
                "community migration leaked: {paid_table}"
            );
        }
    }
}
