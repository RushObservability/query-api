//! Startup guardrails for durable ingest buffering in a multi-replica deployment.
//!
//! Disk buffering is pod-local. Object-store buffering is shared, but the
//! current queue protocol is intentionally at-least-once and does not claim a
//! batch before reading it. Therefore exactly one process may replay a shared
//! object-store backlog. API replicas can still ingest into the shared prefix;
//! a dedicated drain worker owns replay.

/// Validate the requested/effective buffer topology before serving traffic.
///
/// `expected_replicas` is an operator-provided deployment contract rather than
/// a discovered Kubernetes count. Keeping it explicit makes a bad rollout fail
/// closed instead of silently turning a pod-local spool into an HA data-loss
/// or duplicate-ingest risk.
pub fn validate(
    requested_backend: &str,
    effective_backend: &str,
    expected_replicas: usize,
    drain_only: bool,
    run_replayer: bool,
    require_object_store: bool,
) -> anyhow::Result<()> {
    if !matches!(requested_backend, "disk" | "object_store") {
        anyhow::bail!(
            "unsupported RUSH_BUFFER_BACKEND={requested_backend:?}; expected disk or object_store"
        );
    }
    if expected_replicas == 0 {
        anyhow::bail!("RUSH_EXPECTED_QUERY_API_REPLICAS must be at least 1");
    }
    if require_object_store && effective_backend != "object_store" {
        anyhow::bail!(
            "object_store buffer is required but initialization selected disk; refusing unsafe fallback"
        );
    }
    if expected_replicas <= 1 {
        return Ok(());
    }
    if requested_backend != "object_store" || effective_backend != "object_store" {
        anyhow::bail!(
            "{expected_replicas} query-api replicas require RUSH_BUFFER_BACKEND=object_store; disk buffering is pod-local"
        );
    }
    if !drain_only && run_replayer {
        anyhow::bail!(
            "shared object_store buffering with {expected_replicas} query-api replicas requires RUSH_RUN_REPLAYER=false on API replicas and one RUSH_DRAIN_WORKER_ONLY=true worker"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate;

    #[test]
    fn single_replica_disk_is_the_default_safe_topology() {
        validate("disk", "disk", 1, false, true, false).unwrap();
    }

    #[test]
    fn shared_object_store_allows_api_replica_without_replayer() {
        validate("object_store", "object_store", 3, false, false, true).unwrap();
    }

    #[test]
    fn shared_object_store_allows_dedicated_drain_worker() {
        validate("object_store", "object_store", 3, true, true, true).unwrap();
    }

    #[test]
    fn multi_replica_disk_is_rejected() {
        let error = validate("disk", "disk", 2, false, false, false).unwrap_err();
        assert!(error.to_string().contains("pod-local"));
    }

    #[test]
    fn multi_replica_replayer_on_api_is_rejected() {
        let error = validate("object_store", "object_store", 2, false, true, true).unwrap_err();
        assert!(error.to_string().contains("RUSH_RUN_REPLAYER=false"));
    }

    #[test]
    fn required_object_store_does_not_fallback_to_disk() {
        let error = validate("object_store", "disk", 1, false, true, true).unwrap_err();
        assert!(error.to_string().contains("unsafe fallback"));
    }
}
