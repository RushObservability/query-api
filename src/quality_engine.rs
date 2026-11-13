//! Scheduled tenant-isolated quality scans, bounded request cache and finding history.
use crate::{
    audit::{AuditEvent, AuditLogger},
    clickhouse_config::ConfigDb,
    handlers::telemetry_quality::{QualityFinding, QualityResponse},
};
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

const HISTORY_LIMIT: usize = 500;
const CACHE_LIMIT: usize = 32;
const CACHE_TTL: Duration = Duration::from_secs(60);
const SCAN_SECONDS: u64 = 300;
const RULESET: &str = "rush-v2";

pub const HISTORY_DDL: &str = "CREATE TABLE IF NOT EXISTS config_quality_history (
    tenant_id String, id String, payload String, updated_at UInt64
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (tenant_id, id)
TTL toDateTime(updated_at / 1000) + INTERVAL 90 DAY DELETE";
pub const SNAPSHOT_DDL: &str = "CREATE TABLE IF NOT EXISTS config_quality_snapshots (
    tenant_id String, ruleset String, payload String, updated_at UInt64
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (tenant_id, ruleset)
TTL toDateTime(updated_at / 1000) + INTERVAL 1 DAY DELETE";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryChange {
    pub at: String,
    pub kind: String,
    pub versions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindingHistory {
    pub id: String,
    pub service: String,
    pub rule_id: String,
    pub status: String,
    pub first_detected: String,
    pub last_detected: String,
    pub last_evaluated: String,
    pub resolved_at: Option<String>,
    pub occurrences: u32,
    pub latest: QualityFinding,
    /// Last twenty lifecycle/release observations. Version changes are not causal attribution.
    pub changes: Vec<HistoryChange>,
}

#[derive(Serialize, clickhouse::Row)]
struct HistoryRow<'a> {
    tenant_id: &'a str,
    id: &'a str,
    payload: String,
    updated_at: u64,
}
#[derive(Serialize, clickhouse::Row)]
struct SnapshotRow<'a> {
    tenant_id: &'a str,
    ruleset: &'a str,
    payload: String,
    updated_at: u64,
}
#[derive(Deserialize, clickhouse::Row)]
struct Payload {
    payload: String,
}

type CacheSlot<T> = Arc<Mutex<Option<(Instant, T)>>>;
pub type QualityCache = AnalysisCache<QualityResponse>;
pub struct AnalysisCache<T> {
    entries: Mutex<BTreeMap<(String, u32), CacheSlot<T>>>,
    // Different tenant/window requests cannot multiply seven-source scans indefinitely.
    scans: Semaphore,
}

impl<T: Clone> Default for AnalysisCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> AnalysisCache<T> {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            scans: Semaphore::new(2),
        }
    }

    pub(crate) async fn cached(
        &self,
        tenant: &str,
        minutes: u32,
        compute: impl std::future::Future<Output = T>,
    ) -> Result<(T, bool), (StatusCode, String)> {
        let key = (tenant.to_owned(), minutes);
        let slot = {
            let mut entries = self.entries.lock().await;
            if !entries.contains_key(&key) && entries.len() >= CACHE_LIMIT {
                // Never evict an in-flight slot: identical requests must share its result.
                let removable = entries
                    .iter()
                    .find(|(_, slot)| Arc::strong_count(slot) == 1)
                    .map(|(key, _)| key.clone());
                if let Some(old) = removable {
                    entries.remove(&old);
                } else {
                    return Err((
                        StatusCode::TOO_MANY_REQUESTS,
                        "Telemetry analysis is busy. Try again shortly.".into(),
                    ));
                }
            }
            entries
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut cached = tokio::time::timeout(Duration::from_secs(25), slot.lock())
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Telemetry analysis is busy. Try again shortly.".into(),
                )
            })?;
        if let Some((_, report)) = cached.as_ref().filter(|(at, _)| at.elapsed() < CACHE_TTL) {
            return Ok((report.clone(), true));
        }
        let _permit = tokio::time::timeout(Duration::from_secs(3), self.scans.acquire())
            .await
            .map_err(|_| {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "Telemetry analysis is busy. Try again shortly.".into(),
                )
            })?
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Telemetry analysis unavailable".into(),
                )
            })?;
        let report = compute.await;
        *cached = Some((Instant::now(), report.clone()));
        Ok((report, false))
    }
}

impl AnalysisCache<QualityResponse> {
    pub async fn get(
        &self,
        ch: &clickhouse::Client,
        db: &ConfigDb,
        tenant: &str,
        minutes: u32,
    ) -> Result<QualityResponse, (StatusCode, String)> {
        let (mut report, hit) = self
            .cached(tenant, minutes, async {
                let snapshot = if minutes == 1440 {
                    fresh_snapshot(db, tenant).await
                } else {
                    None
                };
                let mut report = match snapshot {
                    Some(report) => report,
                    None => {
                        crate::handlers::telemetry_quality::analyze(ch, db, tenant, minutes).await
                    }
                };
                report.cache_hit = report.scheduled_at.is_some();
                report
            })
            .await?;
        report.cache_hit |= hit;
        Ok(report)
    }
}

async fn fresh_snapshot(db: &ConfigDb, tenant: &str) -> Option<QualityResponse> {
    let read = db.client.query("SELECT payload FROM config_quality_snapshots FINAL WHERE tenant_id = ? AND ruleset = ? AND updated_at >= ? LIMIT 1")
        .bind(tenant).bind(RULESET).bind((Utc::now().timestamp_millis() - SCAN_SECONDS as i64 * 1000).max(0) as u64)
        .fetch_optional::<Payload>();
    let row = tokio::time::timeout(Duration::from_secs(2), read)
        .await
        .ok()?
        .ok()??;
    serde_json::from_str(&row.payload).ok()
}

async fn load_history(db: &ConfigDb, tenant: &str) -> anyhow::Result<Vec<FindingHistory>> {
    let rows = tokio::time::timeout(Duration::from_secs(3), db.client
        .query("SELECT payload FROM config_quality_history FINAL WHERE tenant_id = ? AND updated_at >= toUnixTimestamp64Milli(now64(3) - INTERVAL 90 DAY) ORDER BY updated_at DESC, id LIMIT 501")
        .bind(tenant).fetch_all::<Payload>()).await??;
    // A malformed row makes history unavailable rather than silently erasing it on the next scan.
    rows.into_iter()
        .map(|r| Ok(serde_json::from_str(&r.payload)?))
        .collect()
}

pub async fn attach_history(db: &ConfigDb, tenant: &str, report: &mut QualityResponse) {
    match load_history(db, tenant).await {
        Ok(mut history) => {
            report.history_capped = history.len() > HISTORY_LIMIT;
            history.truncate(HISTORY_LIMIT);
            report.history = history;
            report.history_available = true;
        }
        Err(error) => {
            tracing::warn!(%error, "quality finding history unavailable");
            report.history_available = false;
        }
    }
    if report.scheduled_at.is_none() {
        report.scheduled_at = fresh_snapshot(db, tenant).await.map(|r| r.evaluated_at);
    }
}

fn versions(finding: &QualityFinding) -> Vec<String> {
    finding
        .versions
        .iter()
        .filter(|v| v.affected > 0 && !v.version.is_empty())
        .map(|v| v.version.clone())
        .collect()
}

/// Only resolve after an exhaustive, successful scan with an evaluated passing
/// check for the SAME service and rule. Capped/partial/no-data scans cannot close findings.
fn may_resolve(report: &QualityResponse, old: &FindingHistory) -> bool {
    !report.partial
        && !report.inventory_capped
        && report.sources.len() == 7
        && report
            .sources
            .iter()
            .all(|s| s.status == "available" && !s.capped)
        && report
            .services
            .iter()
            .find(|s| s.service == old.service)
            .is_some_and(|service| {
                let resource_rule = matches!(
                    old.rule_id.as_str(),
                    "environment"
                        | "instance"
                        | "version"
                        | "namespace"
                        | "sdk_name"
                        | "sdk_language"
                );
                service
                    .checks
                    .iter()
                    .find(|c| c.rule_id == old.rule_id)
                    .is_some_and(|check| {
                        check.status == "pass"
                            && check.sampled > 0
                            && (!resource_rule || check.sampled == service.sampled_records)
                    })
            })
}

fn reconcile(report: &QualityResponse, previous: Vec<FindingHistory>) -> Vec<FindingHistory> {
    let mut previous: BTreeMap<_, _> = previous.into_iter().map(|f| (f.id.clone(), f)).collect();
    let mut updates = Vec::new();
    for finding in &report.findings {
        let mut old = previous.remove(&finding.id);
        let kind = match &old {
            None => Some("opened"),
            Some(h) if h.status == "resolved" => Some("reopened"),
            Some(h) if versions(&h.latest) != versions(finding) => Some("versions_changed"),
            _ => None,
        };
        let mut history = old.take().unwrap_or_else(|| FindingHistory {
            id: finding.id.clone(),
            service: finding.service.clone(),
            rule_id: finding.rule_id.clone(),
            status: "open".into(),
            first_detected: report.evaluated_at.clone(),
            last_detected: report.evaluated_at.clone(),
            last_evaluated: report.evaluated_at.clone(),
            resolved_at: None,
            occurrences: 1,
            latest: finding.clone(),
            changes: vec![],
        });
        if kind == Some("reopened") {
            history.occurrences = history.occurrences.saturating_add(1);
        }
        history.status = "open".into();
        history.resolved_at = None;
        history.last_detected = report.evaluated_at.clone();
        history.last_evaluated = report.evaluated_at.clone();
        history.latest = finding.clone();
        if let Some(kind) = kind {
            history.changes.push(HistoryChange {
                at: report.evaluated_at.clone(),
                kind: kind.into(),
                versions: versions(finding),
            });
        }
        if history.changes.len() > 20 {
            history.changes.remove(0);
        }
        updates.push(history);
    }
    for mut history in previous.into_values() {
        if history.status == "open" && may_resolve(report, &history) {
            history.status = "resolved".into();
            history.resolved_at = Some(report.evaluated_at.clone());
            history.last_evaluated = report.evaluated_at.clone();
            history.changes.push(HistoryChange {
                at: report.evaluated_at.clone(),
                kind: "resolved".into(),
                versions: vec![],
            });
            if history.changes.len() > 20 {
                history.changes.remove(0);
            }
            updates.push(history);
        }
    }
    updates
}

async fn persist(
    db: &ConfigDb,
    audit: &AuditLogger,
    tenant: &str,
    mut report: QualityResponse,
) -> anyhow::Result<()> {
    // Include every current finding even when older history exceeds the UI's
    // 500-row limit. Only the bounded set we actually read may be resolved.
    let ids = report
        .findings
        .iter()
        .map(|f| f.id.clone())
        .collect::<Vec<_>>();
    let rows = tokio::time::timeout(Duration::from_secs(3), db.client
        .query("SELECT payload FROM config_quality_history FINAL WHERE tenant_id = ? AND updated_at >= toUnixTimestamp64Milli(now64(3) - INTERVAL 90 DAY) ORDER BY has(?, id) DESC, updated_at DESC, id LIMIT 701")
        .bind(tenant).bind(&ids).fetch_all::<Payload>()).await??;
    let previous = rows
        .into_iter()
        .map(|r| serde_json::from_str::<FindingHistory>(&r.payload))
        .collect::<Result<Vec<_>, _>>()?;
    let updates = reconcile(&report, previous);
    crate::leader::ensure_leader()?;
    let updated_at = Utc::now().timestamp_millis().max(0) as u64;
    if !updates.is_empty() {
        let mut insert = db
            .client
            .insert::<HistoryRow<'_>>("config_quality_history")
            .await?;
        for history in &updates {
            crate::leader::ensure_leader()?;
            insert
                .write(&HistoryRow {
                    tenant_id: tenant,
                    id: &history.id,
                    payload: serde_json::to_string(history)?,
                    updated_at,
                })
                .await?;
        }
        crate::leader::ensure_leader()?;
        insert.end().await?;
        audit.log(AuditEvent::new("telemetry_quality.history_update", "system").actor("telemetry-quality", "Telemetry quality engine")
            .tenant(tenant).resource("telemetry_quality", tenant).outcome("success")
            .changes(serde_json::json!({"updated": updates.len(), "opened": updates.iter().filter(|h| h.changes.last().is_some_and(|c| c.at == report.evaluated_at && c.kind == "opened")).count(), "resolved": updates.iter().filter(|h| h.status == "resolved").count()}).to_string())).await;
    }
    report.scheduled_at = Some(report.evaluated_at.clone());
    crate::leader::ensure_leader()?;
    let mut insert = db
        .client
        .insert::<SnapshotRow<'_>>("config_quality_snapshots")
        .await?;
    insert
        .write(&SnapshotRow {
            tenant_id: tenant,
            ruleset: RULESET,
            payload: serde_json::to_string(&report)?,
            updated_at,
        })
        .await?;
    insert.end().await?;
    audit.log(AuditEvent::new("telemetry_quality.scan", "system").actor("telemetry-quality", "Telemetry quality engine")
        .tenant(tenant).resource("telemetry_quality", tenant).outcome("success")
        .changes(serde_json::json!({"ruleset": RULESET, "minutes": report.minutes, "findings": report.finding_count, "partial": report.partial}).to_string())).await;
    Ok(())
}

pub fn spawn(state: crate::AppState, lease: Arc<crate::leader::EngineLease>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(SCAN_SECONDS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cursor = String::new();
        loop {
            tokio::select! { _ = state.shutdown.wait_for_request() => break, _ = interval.tick() => {} }
            if !lease.is_leader() {
                continue;
            }
            #[derive(Deserialize, clickhouse::Row)]
            struct Tenant {
                id: String,
            }
            // Page through enabled tenants, excluding the reserved audit tenant. Twenty
            // per tick bounds the work; the cursor makes progress on larger installations.
            let tenants = tokio::time::timeout(Duration::from_secs(3), state.config_db.client
                .query("SELECT id FROM config_tenants FINAL WHERE is_deleted = 0 AND enabled = 1 AND id != '_audit' AND name != '_audit' AND id > ? ORDER BY id LIMIT 20")
                .bind(&cursor).fetch_all::<Tenant>()).await;
            let Ok(Ok(tenants)) = tenants else {
                tracing::warn!("quality scheduler could not load tenants");
                continue;
            };
            let full_page = tenants.len() == 20;
            for tenant in tenants {
                cursor = tenant.id.clone();
                if state.shutdown.is_requested() || !lease.is_leader() {
                    break;
                }
                let start = Instant::now();
                let work = crate::leader::fenced(
                    lease.clone(),
                    crate::query_governor::run_background(&tenant.id, async {
                        if crate::query_governor::active_budget()
                            .is_some_and(|b| b.max_time_range_secs < 86400)
                        {
                            anyhow::bail!("quality scan window exceeds background workload limit");
                        }
                        let report = crate::handlers::telemetry_quality::analyze(
                            &state.ch,
                            &state.config_db,
                            &tenant.id,
                            1440,
                        )
                        .await;
                        persist(&state.config_db, &state.audit, &tenant.id, report).await
                    }),
                );
                let result = tokio::select! { _ = state.shutdown.wait_for_request() => break, result = work => result };
                let ok = matches!(&result, Ok(Ok(())));
                state.self_metrics.record_engine(
                    "telemetry_quality",
                    start.elapsed().as_millis() as u64,
                    ok,
                );
                if !ok {
                    tracing::warn!("scheduled quality scan failed; previous history retained");
                }
            }
            if !full_page {
                cursor.clear();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report(bad: bool) -> QualityResponse {
        let mut report = crate::handlers::telemetry_quality::tests::history_report(bad);
        report.evaluated_at = "2026-11-12T12:00:00Z".into();
        report
    }
    #[tokio::test]
    async fn cache_shares_identical_scans_and_isolates_tenants_and_windows() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = QualityCache::new();
        let calls = AtomicUsize::new(0);
        let work = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            report(true)
        };
        let (first, shared) =
            tokio::join!(cache.cached("a", 60, work()), cache.cached("a", 60, work()));
        assert!(first.is_ok() && shared.unwrap().1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cache.cached("b", 60, work()).await.unwrap();
        cache.cached("a", 360, work()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let slot = cache.entries.lock().await[&("a".into(), 60)].clone();
        slot.lock().await.as_mut().unwrap().0 = Instant::now() - CACHE_TTL;
        cache.cached("a", 60, work()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }
    #[test]
    fn historical_unknown_resource_rows_prevent_resolution() {
        let old = reconcile(&report(true), vec![]);
        let mut clean = report(false);
        clean.services[0].sampled_records += 1;
        assert!(reconcile(&clean, old).is_empty());
    }
    #[test]
    fn lifecycle_preserves_first_detection_and_tracks_reopening_versions() {
        let first = report(true);
        let opened = reconcile(&first, vec![]);
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].changes[0].kind, "opened");
        let mut clean = report(false);
        clean.evaluated_at = "2026-11-13T12:00:00Z".into();
        let resolved = reconcile(&clean, opened.clone());
        assert_eq!(resolved[0].status, "resolved");
        let mut bad = report(true);
        bad.evaluated_at = "2026-11-14T12:00:00Z".into();
        bad.findings[0].versions[0].version = "2.0".into();
        let reopened = reconcile(&bad, resolved);
        assert_eq!(reopened[0].first_detected, opened[0].first_detected);
        assert_eq!(reopened[0].last_detected, bad.evaluated_at);
        assert_eq!(reopened[0].occurrences, 2);
        assert_eq!(reopened[0].changes.last().unwrap().kind, "reopened");
        assert_eq!(reopened[0].changes.last().unwrap().versions, vec!["2.0"]);
        assert!(reopened[0].resolved_at.is_none());
    }
    #[test]
    fn missing_failed_capped_and_unevaluated_data_never_resolve() {
        let old = reconcile(&report(true), vec![]);
        for mode in 0..5 {
            let mut clean = report(false);
            match mode {
                0 => clean.partial = true,
                1 => clean.sources[0].capped = true,
                2 => clean.sources[0].status = "unavailable".into(),
                3 => clean.services.clear(),
                _ => clean.services[0]
                    .checks
                    .iter_mut()
                    .for_each(|c| c.status = "not_evaluated".into()),
            }
            assert!(reconcile(&clean, old.clone()).is_empty(), "mode {mode}");
        }
    }
    #[test]
    fn version_observations_are_bounded_and_do_not_reset_first_detection() {
        let mut bad = report(true);
        let mut history = reconcile(&bad, vec![]);
        let first = history[0].first_detected.clone();
        for i in 0..30 {
            bad.findings[0].versions[0].version = format!("v{i}");
            history = reconcile(&bad, history);
        }
        assert_eq!(history[0].changes.len(), 20);
        assert_eq!(history[0].changes.last().unwrap().kind, "versions_changed");
        assert_eq!(history[0].first_detected, first);
        assert_eq!(history[0].occurrences, 1);
    }
    #[test]
    fn history_ids_do_not_collide_between_services_or_rules() {
        let a = report(true);
        let old = reconcile(&a, vec![]);
        let mut other = report(false);
        other.services[0].service = "other".into();
        assert!(reconcile(&other, old).is_empty());
    }
}
