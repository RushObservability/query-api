//! Query workload admission and ClickHouse resource budgets.
//!
//! The governor deliberately keeps its labels and workload taxonomy fixed. A
//! tenant id is used to select a semaphore, but is never emitted as a metric
//! label, keeping the self-metrics surface low-cardinality.

use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::self_metrics::SelfMetrics;

pub const QUERY_LIMITS_SETTING_KEY: &str = "query_workload_limits";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadClass {
    Interactive,
    Dashboard,
    Background,
    Export,
    Integration,
}

impl WorkloadClass {
    pub const ALL: [Self; 5] = [
        Self::Interactive,
        Self::Dashboard,
        Self::Background,
        Self::Export,
        Self::Integration,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Dashboard => "dashboard",
            Self::Background => "background",
            Self::Export => "export",
            Self::Integration => "integration",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryBudget {
    pub global_concurrency: u32,
    pub per_tenant_concurrency: u32,
    pub queue_timeout_ms: u64,
    pub request_timeout_secs: u64,
    pub max_time_range_secs: u64,
    pub max_execution_time_secs: u64,
    pub max_rows_to_read: u64,
    pub max_bytes_to_read: u64,
    pub max_result_rows: u64,
    pub max_memory_usage: u64,
    pub spill_threshold_bytes: u64,
    pub max_threads: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryGovernorConfig {
    pub interactive: QueryBudget,
    pub dashboard: QueryBudget,
    pub background: QueryBudget,
    pub export: QueryBudget,
    pub integration: QueryBudget,
}

impl QueryGovernorConfig {
    pub fn budget(&self, class: WorkloadClass) -> &QueryBudget {
        match class {
            WorkloadClass::Interactive => &self.interactive,
            WorkloadClass::Dashboard => &self.dashboard,
            WorkloadClass::Background => &self.background,
            WorkloadClass::Export => &self.export,
            WorkloadClass::Integration => &self.integration,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        for class in WorkloadClass::ALL {
            validate_budget(class, self.budget(class))?;
        }
        Ok(())
    }
}

impl Default for QueryGovernorConfig {
    fn default() -> Self {
        const GIB: u64 = 1_073_741_824;
        Self {
            interactive: QueryBudget {
                global_concurrency: 32,
                per_tenant_concurrency: 8,
                queue_timeout_ms: 250,
                request_timeout_secs: 30,
                max_time_range_secs: 7 * 24 * 60 * 60,
                max_execution_time_secs: 30,
                max_rows_to_read: 100_000_000,
                max_bytes_to_read: 5 * GIB,
                max_result_rows: 500_000,
                max_memory_usage: 2 * GIB,
                spill_threshold_bytes: GIB,
                max_threads: 8,
            },
            dashboard: QueryBudget {
                global_concurrency: 16,
                per_tenant_concurrency: 4,
                queue_timeout_ms: 500,
                request_timeout_secs: 45,
                max_time_range_secs: 30 * 24 * 60 * 60,
                max_execution_time_secs: 45,
                max_rows_to_read: 250_000_000,
                max_bytes_to_read: 10 * GIB,
                max_result_rows: 250_000,
                max_memory_usage: 2 * GIB,
                spill_threshold_bytes: GIB,
                max_threads: 6,
            },
            background: QueryBudget {
                global_concurrency: 8,
                per_tenant_concurrency: 2,
                queue_timeout_ms: 1_000,
                request_timeout_secs: 60,
                max_time_range_secs: 30 * 24 * 60 * 60,
                max_execution_time_secs: 60,
                max_rows_to_read: 500_000_000,
                max_bytes_to_read: 20 * GIB,
                max_result_rows: 100_000,
                max_memory_usage: 2 * GIB,
                spill_threshold_bytes: GIB,
                max_threads: 4,
            },
            export: QueryBudget {
                global_concurrency: 4,
                per_tenant_concurrency: 1,
                queue_timeout_ms: 1_000,
                request_timeout_secs: 120,
                max_time_range_secs: 90 * 24 * 60 * 60,
                max_execution_time_secs: 120,
                max_rows_to_read: 1_000_000_000,
                max_bytes_to_read: 50 * GIB,
                max_result_rows: 1_000_000,
                max_memory_usage: 4 * GIB,
                spill_threshold_bytes: 2 * GIB,
                max_threads: 4,
            },
            integration: QueryBudget {
                global_concurrency: 8,
                per_tenant_concurrency: 2,
                queue_timeout_ms: 500,
                request_timeout_secs: 60,
                max_time_range_secs: 30 * 24 * 60 * 60,
                max_execution_time_secs: 60,
                max_rows_to_read: 500_000_000,
                max_bytes_to_read: 20 * GIB,
                max_result_rows: 500_000,
                max_memory_usage: 2 * GIB,
                spill_threshold_bytes: GIB,
                max_threads: 4,
            },
        }
    }
}

fn validate_budget(class: WorkloadClass, budget: &QueryBudget) -> Result<(), String> {
    let name = class.label();
    if !(1..=512).contains(&budget.global_concurrency) {
        return Err(format!(
            "{name}.global_concurrency must be between 1 and 512"
        ));
    }
    if !(1..=budget.global_concurrency).contains(&budget.per_tenant_concurrency) {
        return Err(format!(
            "{name}.per_tenant_concurrency must be between 1 and global_concurrency"
        ));
    }
    if budget.queue_timeout_ms > 30_000 {
        return Err(format!("{name}.queue_timeout_ms cannot exceed 30000"));
    }
    if !(1..=600).contains(&budget.request_timeout_secs) {
        return Err(format!(
            "{name}.request_timeout_secs must be between 1 and 600"
        ));
    }
    if !(60..=365 * 24 * 60 * 60).contains(&budget.max_time_range_secs) {
        return Err(format!(
            "{name}.max_time_range_secs must be between 60 and 31536000"
        ));
    }
    if !(1..=budget.request_timeout_secs).contains(&budget.max_execution_time_secs) {
        return Err(format!(
            "{name}.max_execution_time_secs must be between 1 and request_timeout_secs"
        ));
    }
    if !(1..=10_000_000_000).contains(&budget.max_rows_to_read) {
        return Err(format!(
            "{name}.max_rows_to_read is outside the supported range"
        ));
    }
    if !(1..=1_099_511_627_776).contains(&budget.max_bytes_to_read) {
        return Err(format!(
            "{name}.max_bytes_to_read is outside the supported range"
        ));
    }
    if !(1..=10_000_000).contains(&budget.max_result_rows) {
        return Err(format!(
            "{name}.max_result_rows is outside the supported range"
        ));
    }
    if !(64 * 1024 * 1024..=64 * 1_073_741_824).contains(&budget.max_memory_usage) {
        return Err(format!(
            "{name}.max_memory_usage must be between 64 MiB and 64 GiB"
        ));
    }
    if budget.spill_threshold_bytes == 0 || budget.spill_threshold_bytes > budget.max_memory_usage {
        return Err(format!(
            "{name}.spill_threshold_bytes must be positive and no larger than max_memory_usage"
        ));
    }
    if !(1..=64).contains(&budget.max_threads) {
        return Err(format!("{name}.max_threads must be between 1 and 64"));
    }
    Ok(())
}

#[derive(Clone)]
struct ClassAdmission {
    budget: QueryBudget,
    global: Arc<Semaphore>,
    tenants: Arc<DashMap<String, Arc<Semaphore>>>,
}

struct AdmissionState {
    config: QueryGovernorConfig,
    classes: HashMap<WorkloadClass, ClassAdmission>,
}

impl AdmissionState {
    fn new(config: QueryGovernorConfig) -> Self {
        let classes = WorkloadClass::ALL
            .into_iter()
            .map(|class| {
                let budget = config.budget(class).clone();
                (
                    class,
                    ClassAdmission {
                        global: Arc::new(Semaphore::new(budget.global_concurrency as usize)),
                        tenants: Arc::new(DashMap::new()),
                        budget,
                    },
                )
            })
            .collect();
        Self { config, classes }
    }
}

pub struct QueryGovernor {
    state: RwLock<Arc<AdmissionState>>,
    metrics: Arc<SelfMetrics>,
    admission_count: AtomicU64,
}

static GLOBAL_QUERY_GOVERNOR: OnceLock<Arc<QueryGovernor>> = OnceLock::new();

/// Install the process governor shared by HTTP middleware and in-process
/// background engines. A duplicate install is harmless in test harnesses.
pub fn install_global(governor: Arc<QueryGovernor>) {
    let _ = GLOBAL_QUERY_GOVERNOR.set(governor);
}

pub fn global() -> Option<&'static Arc<QueryGovernor>> {
    GLOBAL_QUERY_GOVERNOR.get()
}

/// Run one detector/monitor job through the same background admission pools
/// used by every other engine. The nested result preserves the job's own error.
pub async fn run_background<T>(
    tenant_id: &str,
    future: impl Future<Output = T>,
) -> Result<T, AdmissionError> {
    let Some(governor) = global() else {
        return Ok(future.await);
    };
    let guard = governor.admit(WorkloadClass::Background, tenant_id).await?;
    let budget = guard.budget().clone();
    let output = with_budget(budget, future).await;
    drop(guard);
    Ok(output)
}

impl QueryGovernor {
    pub fn new(config: QueryGovernorConfig, metrics: Arc<SelfMetrics>) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            state: RwLock::new(Arc::new(AdmissionState::new(config))),
            metrics,
            admission_count: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> QueryGovernorConfig {
        self.state
            .read()
            .expect("query governor lock poisoned")
            .config
            .clone()
    }

    /// Atomically applies new limits to newly admitted work. Existing permits
    /// remain attached to the previous pool until their queries finish.
    pub fn reconfigure(&self, config: QueryGovernorConfig) -> Result<(), String> {
        config.validate()?;
        *self.state.write().expect("query governor lock poisoned") =
            Arc::new(AdmissionState::new(config));
        Ok(())
    }

    pub async fn admit(
        &self,
        class: WorkloadClass,
        tenant_id: &str,
    ) -> Result<QueryAdmissionGuard, AdmissionError> {
        let state = self
            .state
            .read()
            .expect("query governor lock poisoned")
            .clone();
        let admission = state
            .classes
            .get(&class)
            .expect("all workload classes configured")
            .clone();
        // Tenant IDs are dynamic. Periodically discard semaphores that are
        // held only by the map; active and queued requests own another Arc and
        // therefore cannot be evicted underneath an admission.
        if self.admission_count.fetch_add(1, Ordering::Relaxed) % 1_024 == 1_023 {
            admission
                .tenants
                .retain(|_, semaphore| Arc::strong_count(semaphore) > 1);
        }
        let tenant = admission
            .tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| {
                Arc::new(Semaphore::new(
                    admission.budget.per_tenant_concurrency as usize,
                ))
            })
            .clone();
        let label = class.label();
        self.metrics.add_gauge(
            "rush_query_admission_queue_depth",
            &[("workload", label)],
            1.0,
        );
        let queue = QueryQueueGuard {
            metrics: self.metrics.clone(),
            label,
        };
        let timeout = Duration::from_millis(admission.budget.queue_timeout_ms);

        // Acquire the tenant slot first: one noisy tenant cannot reserve every
        // global slot while waiting on its own smaller pool.
        let tenant_permit = acquire(tenant, timeout).await.map_err(|_| {
            self.metrics.inc_counter(
                "rush_query_admission_total",
                &[("workload", label), ("outcome", "tenant_rejected")],
                1,
            );
            AdmissionError::TenantBusy {
                retry_after_secs: retry_after_secs(&admission.budget),
            }
        })?;
        let global_permit = match acquire(admission.global.clone(), timeout).await {
            Ok(permit) => permit,
            Err(()) => {
                drop(tenant_permit);
                self.metrics.inc_counter(
                    "rush_query_admission_total",
                    &[("workload", label), ("outcome", "global_rejected")],
                    1,
                );
                return Err(AdmissionError::GlobalBusy {
                    retry_after_secs: retry_after_secs(&admission.budget),
                });
            }
        };
        drop(queue);
        self.metrics.inc_counter(
            "rush_query_admission_total",
            &[("workload", label), ("outcome", "admitted")],
            1,
        );
        self.metrics
            .add_gauge("rush_query_admission_inflight", &[("workload", label)], 1.0);
        Ok(QueryAdmissionGuard {
            _tenant: tenant_permit,
            _global: global_permit,
            budget: admission.budget,
            metrics: self.metrics.clone(),
            label,
        })
    }
}

async fn acquire(semaphore: Arc<Semaphore>, timeout: Duration) -> Result<OwnedSemaphorePermit, ()> {
    if timeout.is_zero() {
        semaphore.try_acquire_owned().map_err(|_| ())
    } else {
        tokio::time::timeout(timeout, semaphore.acquire_owned())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())
    }
}

fn retry_after_secs(budget: &QueryBudget) -> u64 {
    budget.queue_timeout_ms.div_ceil(1_000).max(1)
}

struct QueryQueueGuard {
    metrics: Arc<SelfMetrics>,
    label: &'static str,
}

impl Drop for QueryQueueGuard {
    fn drop(&mut self) {
        self.metrics.add_gauge(
            "rush_query_admission_queue_depth",
            &[("workload", self.label)],
            -1.0,
        );
    }
}

pub struct QueryAdmissionGuard {
    _tenant: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    budget: QueryBudget,
    metrics: Arc<SelfMetrics>,
    label: &'static str,
}

impl QueryAdmissionGuard {
    pub fn budget(&self) -> &QueryBudget {
        &self.budget
    }
}

impl Drop for QueryAdmissionGuard {
    fn drop(&mut self) {
        self.metrics.add_gauge(
            "rush_query_admission_inflight",
            &[("workload", self.label)],
            -1.0,
        );
    }
}

/// Keep an admission permit attached to a streaming response body.
///
/// Axum considers a handler complete as soon as it returns a `Response`, while
/// export cursors continue doing ClickHouse work as the body is polled. Without
/// this wrapper an export would release its global/per-tenant slots before the
/// first row was sent. Dropping the client body drops this stream state, which
/// in turn drops the permit and the underlying ClickHouse cursor immediately.
pub fn retain_admission_until_body_end(
    response: axum::response::Response,
    guard: QueryAdmissionGuard,
    timeout: Duration,
) -> axum::response::Response {
    let (parts, body) = response.into_parts();
    let deadline = tokio::time::Instant::now() + timeout;
    let state = (
        Box::pin(body.into_data_stream()),
        Some(guard),
        deadline,
        false,
    );
    let stream =
        futures_util::stream::unfold(state, |(mut body, mut guard, deadline, done)| async move {
            if done {
                return None;
            }
            match tokio::time::timeout_at(deadline, body.next()).await {
                Ok(Some(Ok(bytes))) => Some((
                    Ok::<bytes::Bytes, std::io::Error>(bytes),
                    (body, guard, deadline, false),
                )),
                Ok(Some(Err(error))) => {
                    guard.take();
                    Some((
                        Err(std::io::Error::other(error)),
                        (body, guard, deadline, true),
                    ))
                }
                Ok(None) => {
                    guard.take();
                    None
                }
                Err(_) => {
                    guard.take();
                    Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "export response exceeded its workload timeout",
                        )),
                        (body, guard, deadline, true),
                    ))
                }
            }
        });
    axum::response::Response::from_parts(parts, axum::body::Body::from_stream(stream))
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionError {
    TenantBusy { retry_after_secs: u64 },
    GlobalBusy { retry_after_secs: u64 },
}

impl AdmissionError {
    pub fn retry_after_secs(&self) -> u64 {
        match self {
            Self::TenantBusy { retry_after_secs } | Self::GlobalBusy { retry_after_secs } => {
                *retry_after_secs
            }
        }
    }
}

tokio::task_local! {
    static ACTIVE_QUERY_BUDGET: QueryBudget;
}

pub async fn with_budget<T>(budget: QueryBudget, future: impl Future<Output = T>) -> T {
    ACTIVE_QUERY_BUDGET.scope(budget, future).await
}

pub fn active_budget() -> Option<QueryBudget> {
    ACTIVE_QUERY_BUDGET.try_with(Clone::clone).ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedTimeRange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl ValidatedTimeRange {
    pub fn parse(from: &str, to: &str, max_seconds: u64) -> Result<Self, TimeRangeError> {
        let from = parse_time(from).ok_or(TimeRangeError::Malformed)?;
        let to = parse_time(to).ok_or(TimeRangeError::Malformed)?;
        if from > to {
            return Err(TimeRangeError::Reversed);
        }
        let seconds = to.signed_duration_since(from).num_seconds().max(0) as u64;
        if seconds > max_seconds {
            return Err(TimeRangeError::TooLarge { max_seconds });
        }
        Ok(Self { from, to })
    }
}

fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(seconds) = value.parse::<i64>() {
        return DateTime::from_timestamp(seconds, 0);
    }
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
    {
        let whole = seconds.floor() as i64;
        let nanos = ((seconds - whole as f64) * 1_000_000_000.0).round() as u32;
        return DateTime::from_timestamp(whole, nanos.min(999_999_999));
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
        .or_else(|| {
            DateTime::parse_from_rfc3339(&format!("{value}Z"))
                .ok()
                .map(|value| value.with_timezone(&Utc))
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeRangeError {
    Malformed,
    Reversed,
    TooLarge { max_seconds: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn defaults_are_valid_and_round_trip() {
        let config = QueryGovernorConfig::default();
        config.validate().unwrap();
        let raw = serde_json::to_string(&config).unwrap();
        assert_eq!(
            serde_json::from_str::<QueryGovernorConfig>(&raw).unwrap(),
            config
        );
    }

    #[test]
    fn rejects_unsafe_relationships() {
        let mut config = QueryGovernorConfig::default();
        config.interactive.per_tenant_concurrency = 33;
        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("per_tenant_concurrency")
        );
        config = QueryGovernorConfig::default();
        config.interactive.spill_threshold_bytes = config.interactive.max_memory_usage + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("spill_threshold_bytes")
        );
    }

    #[test]
    fn validates_time_ranges_once_with_stable_reasons() {
        assert_eq!(
            ValidatedTimeRange::parse("bad", "2026-08-10T00:00:00Z", 60),
            Err(TimeRangeError::Malformed)
        );
        assert_eq!(
            ValidatedTimeRange::parse("2026-08-10T00:01:00Z", "2026-08-10T00:00:00Z", 60),
            Err(TimeRangeError::Reversed)
        );
        assert_eq!(
            ValidatedTimeRange::parse("2026-08-10T00:00:00Z", "2026-08-10T00:02:00Z", 60),
            Err(TimeRangeError::TooLarge { max_seconds: 60 })
        );
        assert!(ValidatedTimeRange::parse("1786320000", "1786320030", 60).is_ok());
    }

    #[tokio::test]
    async fn per_tenant_and_global_admission_are_bounded() {
        let mut config = QueryGovernorConfig::default();
        config.interactive.global_concurrency = 2;
        config.interactive.per_tenant_concurrency = 1;
        config.interactive.queue_timeout_ms = 0;
        let governor = QueryGovernor::new(config, Arc::new(SelfMetrics::new())).unwrap();
        let _first = governor
            .admit(WorkloadClass::Interactive, "a")
            .await
            .unwrap();
        assert_eq!(
            governor.admit(WorkloadClass::Interactive, "a").await.err(),
            Some(AdmissionError::TenantBusy {
                retry_after_secs: 1
            })
        );
        let _second = governor
            .admit(WorkloadClass::Interactive, "b")
            .await
            .unwrap();
        assert_eq!(
            governor.admit(WorkloadClass::Interactive, "c").await.err(),
            Some(AdmissionError::GlobalBusy {
                retry_after_secs: 1
            })
        );
    }

    #[tokio::test]
    async fn idle_tenant_admission_entries_are_periodically_reclaimed() {
        let governor =
            QueryGovernor::new(QueryGovernorConfig::default(), Arc::new(SelfMetrics::new()))
                .unwrap();
        for index in 0..1_024 {
            let guard = governor
                .admit(WorkloadClass::Interactive, &format!("tenant-{index}"))
                .await
                .unwrap();
            drop(guard);
        }
        let state = governor.state.read().unwrap();
        let tenants = &state.classes[&WorkloadClass::Interactive].tenants;
        assert!(
            tenants.len() < 10,
            "idle tenant map grew to {}",
            tenants.len()
        );
    }

    #[tokio::test]
    async fn timeout_drops_inflight_query_future_for_clickhouse_client_close_cancellation() {
        struct DropMarker(Arc<AtomicBool>);
        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(dropped.clone());
        let work = async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(5), work)
                .await
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn streaming_response_holds_permit_until_body_is_dropped() {
        let mut config = QueryGovernorConfig::default();
        config.export.global_concurrency = 1;
        config.export.per_tenant_concurrency = 1;
        config.export.queue_timeout_ms = 0;
        let governor = QueryGovernor::new(config, Arc::new(SelfMetrics::new())).unwrap();
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-a")
            .await
            .unwrap();
        let body = axum::body::Body::from_stream(futures_util::stream::pending::<
            Result<bytes::Bytes, std::io::Error>,
        >());
        let response = retain_admission_until_body_end(
            axum::response::Response::new(body),
            guard,
            Duration::from_secs(60),
        );

        assert!(matches!(
            governor.admit(WorkloadClass::Export, "tenant-a").await,
            Err(AdmissionError::TenantBusy { .. })
        ));
        drop(response);
        assert!(
            governor
                .admit(WorkloadClass::Export, "tenant-a")
                .await
                .is_ok()
        );
    }
}
