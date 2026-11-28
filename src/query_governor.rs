//! Query workload admission and ClickHouse resource budgets.
//!
//! The governor deliberately keeps its labels and workload taxonomy fixed. A
//! tenant id is used to select a semaphore, but is never emitted as a metric
//! label, keeping the self-metrics surface low-cardinality.

use crate::timestamps::parse_rfc3339;
use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
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

    /// Decodes the stored `QUERY_LIMITS_SETTING_KEY` value. `None` (never
    /// saved) means defaults; a value that fails to decode or validate is an
    /// error, so every caller treats an unusable value the same way.
    pub fn from_stored(raw: Option<&str>) -> Result<Self, String> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let config = serde_json::from_str::<Self>(raw).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }

    /// Process startup: unusable stored limits must not stop the process, so
    /// they fall back to defaults until an admin saves valid ones.
    pub fn at_startup(raw: Option<&str>) -> Self {
        Self::from_stored(raw).unwrap_or_else(|error| {
            tracing::error!(%error, "stored query workload limits are unusable; using safe defaults");
            Self::default()
        })
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
                // No user waits on engine jobs, and the monitor engine sizes
                // itself to the whole pool: a short wait made the SLO, SIEM and
                // anomaly jobs queued behind it skip their cycle instead of
                // taking the next free slot.
                queue_timeout_ms: 30_000,
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
    /// Global slots a tenant's second and later concurrent queries may use.
    /// The rest are kept for tenants with nothing in flight, so a few busy
    /// tenants cannot lock every other tenant out of the class.
    burst: Arc<Semaphore>,
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
                        burst: Arc::new(Semaphore::new(burst_slots(budget.global_concurrency))),
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
    /// Bumped under the `state` write lock whenever the limits change.
    generation: AtomicU64,
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
            generation: AtomicU64::new(0),
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
    /// Unchanged limits keep the live pools, because a rebuild stops counting
    /// queries admitted by the old ones.
    pub fn reconfigure(&self, config: QueryGovernorConfig) -> Result<(), String> {
        config.validate()?;
        let mut state = self.state.write().expect("query governor lock poisoned");
        if state.config != config {
            *state = Arc::new(AdmissionState::new(config));
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Changes each time the live limits change. A reload reads it before
    /// fetching stored limits and passes it to `apply_reloaded`.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Applies limits read from storage unless the live limits changed since
    /// `seen_generation` (a save on this replica landed while the read was in
    /// flight, so the read may be older than it). Returns whether it applied.
    pub fn apply_reloaded(&self, config: QueryGovernorConfig, seen_generation: u64) -> bool {
        if config.validate().is_err() {
            return false;
        }
        let mut state = self.state.write().expect("query governor lock poisoned");
        if self.generation.load(Ordering::SeqCst) != seen_generation || state.config == config {
            return false;
        }
        *state = Arc::new(AdmissionState::new(config));
        self.generation.fetch_add(1, Ordering::SeqCst);
        true
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
        let tenant_permit = acquire(tenant.clone(), timeout).await.map_err(|_| {
            self.metrics.inc_counter(
                "rush_query_admission_total",
                &[("workload", label), ("outcome", "tenant_rejected")],
                1,
            );
            AdmissionError::TenantBusy {
                retry_after_secs: retry_after_secs(&admission.budget),
            }
        })?;
        // Our own permit is counted, so fewer than `per_tenant - 1` free means
        // another query of this tenant is already running.
        let bursting =
            tenant.available_permits() + 1 < admission.budget.per_tenant_concurrency as usize;
        let burst_permit = if bursting {
            acquire(admission.burst.clone(), timeout).await.map(Some)
        } else {
            Ok(None)
        };
        let global_permit = match burst_permit {
            Ok(burst) => acquire(admission.global.clone(), timeout)
                .await
                .map(|global| (burst, global)),
            Err(()) => Err(()),
        };
        let (burst_permit, global_permit) = match global_permit {
            Ok(permits) => permits,
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
            _burst: burst_permit,
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

/// Half of a class's global slots (rounded up) for burst queries: a tenant
/// with nothing in flight is refused only once `global / 2` other tenants are
/// running queries in that class.
fn burst_slots(global_concurrency: u32) -> usize {
    (global_concurrency - global_concurrency / 2) as usize
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

/// How often every API replica and engine process re-reads the stored limits.
pub const QUERY_LIMITS_RELOAD_INTERVAL: Duration = Duration::from_secs(15);

/// `PUT /api/v1/settings/query-limits` reconfigures only the replica that
/// served it; this brings every other process to the stored limits within
/// one interval.
pub fn spawn_limits_reload(
    governor: Arc<QueryGovernor>,
    config_db: Arc<crate::clickhouse_config::ConfigDb>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(QUERY_LIMITS_RELOAD_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        // Logged once per unusable stored value, not on every tick.
        let mut last_rejected: Option<String> = None;
        loop {
            ticker.tick().await;
            let seen_generation = governor.generation();
            let raw = match config_db.get_setting(QUERY_LIMITS_SETTING_KEY).await {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(%error, "failed to reload query workload limits");
                    continue;
                }
            };
            match QueryGovernorConfig::from_stored(raw.as_deref()) {
                Ok(config) => {
                    last_rejected = None;
                    if governor.apply_reloaded(config, seen_generation) {
                        tracing::info!("applied query workload limits saved by another replica");
                    }
                }
                Err(error) => {
                    if last_rejected != raw {
                        tracing::error!(%error, "stored query workload limits are unusable; keeping live limits");
                        last_rejected = raw;
                    }
                }
            }
        }
    });
}

pub struct QueryAdmissionGuard {
    _tenant: OwnedSemaphorePermit,
    _burst: Option<OwnedSemaphorePermit>,
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
    let deadline = tokio::time::Instant::now() + timeout;
    retain_admission_until_body_deadline(response, guard, deadline)
}

const EXPORT_BODY_LATENCY_BUCKETS_MS: &[f64] = &[
    10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 30_000.0, 60_000.0, 120_000.0, 300_000.0,
    600_000.0,
];
const EXPORT_BODY_BYTES_BUCKETS: &[f64] = &[
    0.0,
    1_024.0,
    16_384.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
    4_294_967_296.0,
];

struct ExportBodyObservation {
    metrics: Arc<SelfMetrics>,
    kind: &'static str,
    started: std::time::Instant,
    bytes: u64,
    recorded: bool,
}

impl ExportBodyObservation {
    fn new(metrics: Arc<SelfMetrics>, kind: &'static str) -> Self {
        metrics.add_gauge("rush_export_streams_inflight", &[("kind", kind)], 1.0);
        Self {
            metrics,
            kind,
            started: std::time::Instant::now(),
            bytes: 0,
            recorded: false,
        }
    }
    fn finish(&mut self, outcome: &'static str) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        self.metrics
            .add_gauge("rush_export_streams_inflight", &[("kind", self.kind)], -1.0);
        let labels = [("kind", self.kind), ("outcome", outcome)];
        self.metrics
            .inc_counter("rush_export_streams_total", &labels, 1);
        self.metrics.observe_histogram_with(
            "rush_export_stream_duration_ms",
            &labels,
            self.started.elapsed().as_secs_f64() * 1000.0,
            EXPORT_BODY_LATENCY_BUCKETS_MS,
        );
        self.metrics.observe_histogram_with(
            "rush_export_stream_bytes",
            &labels,
            self.bytes as f64,
            EXPORT_BODY_BYTES_BUCKETS,
        );
    }
}
impl Drop for ExportBodyObservation {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

/// Use the request's original deadline so body upload and handler execution do
/// not grant a fresh timeout when an export starts streaming.
pub fn retain_admission_until_body_deadline(
    response: axum::response::Response,
    guard: QueryAdmissionGuard,
    deadline: tokio::time::Instant,
) -> axum::response::Response {
    observe_export_body(response, guard, deadline, "response")
}

/// Background artifact generation has the same lifecycle but a separate kind,
/// so its bytes are not confused with a later HTTP artifact download.
pub fn retain_artifact_until_body_deadline(
    response: axum::response::Response,
    guard: QueryAdmissionGuard,
    deadline: tokio::time::Instant,
) -> axum::response::Response {
    observe_export_body(response, guard, deadline, "artifact")
}

fn observe_export_body(
    response: axum::response::Response,
    guard: QueryAdmissionGuard,
    deadline: tokio::time::Instant,
    kind: &'static str,
) -> axum::response::Response {
    let observation = ExportBodyObservation::new(guard.metrics.clone(), kind);
    let (parts, body) = response.into_parts();
    let state = (
        Some(Box::pin(body.into_data_stream())),
        Some(guard),
        deadline,
        false,
        observation,
    );
    let stream = futures_util::stream::unfold(
        state,
        |(mut body, mut guard, deadline, done, mut observation)| async move {
            if done {
                return None;
            }
            if tokio::time::Instant::now() >= deadline {
                observation.finish("timeout");
                body.take();
                guard.take();
                return Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "export response exceeded its workload timeout",
                    )),
                    (body, guard, deadline, true, observation),
                ));
            }
            let next =
                tokio::time::timeout_at(deadline, body.as_mut().expect("active body").next()).await;
            match next {
                Ok(Some(Ok(bytes))) => {
                    observation.bytes = observation.bytes.saturating_add(bytes.len() as u64);
                    Some((
                        Ok::<bytes::Bytes, std::io::Error>(bytes),
                        (body, guard, deadline, false, observation),
                    ))
                }
                Ok(Some(Err(error))) => {
                    observation.finish("body_error");
                    body.take();
                    guard.take();
                    Some((
                        Err(std::io::Error::other(error)),
                        (body, guard, deadline, true, observation),
                    ))
                }
                Ok(None) => {
                    observation.finish("complete");
                    body.take();
                    guard.take();
                    None
                }
                Err(_) => {
                    observation.finish("timeout");
                    body.take();
                    guard.take();
                    Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "export response exceeded its workload timeout",
                        )),
                        (body, guard, deadline, true, observation),
                    ))
                }
            }
        },
    );
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
    pub from: OffsetDateTime,
    pub to: OffsetDateTime,
}

impl ValidatedTimeRange {
    pub fn parse(from: &str, to: &str, max_seconds: u64) -> Result<Self, TimeRangeError> {
        let from = parse_time(from).ok_or(TimeRangeError::Malformed)?;
        let to = parse_time(to).ok_or(TimeRangeError::Malformed)?;
        if from > to {
            return Err(TimeRangeError::Reversed);
        }
        let seconds = (to - from).whole_seconds().max(0) as u64;
        if seconds > max_seconds {
            return Err(TimeRangeError::TooLarge {
                max_seconds,
                seconds,
            });
        }
        Ok(Self { from, to })
    }
}

fn parse_time(value: &str) -> Option<OffsetDateTime> {
    if let Ok(seconds) = value.parse::<i64>() {
        return time::OffsetDateTime::from_unix_timestamp(seconds).ok();
    }
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
    {
        let whole = seconds.floor() as i64;
        let nanos = ((seconds - whole as f64) * 1_000_000_000.0).round() as u32;
        let nanos = i128::from(whole) * 1_000_000_000 + i128::from(nanos.min(999_999_999));
        return OffsetDateTime::from_unix_timestamp_nanos(nanos).ok();
    }
    parse_rfc3339(value)
        .or_else(|_| parse_rfc3339(&format!("{value}Z")))
        .ok()
        .map(|value| value.to_offset(time::UtcOffset::UTC))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeRangeError {
    Malformed,
    Reversed,
    /// `seconds` is the requested span.
    TooLarge {
        max_seconds: u64,
        seconds: u64,
    },
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

    #[tokio::test]
    async fn engine_jobs_wait_for_a_background_slot_instead_of_skipping() {
        let governor =
            QueryGovernor::new(QueryGovernorConfig::default(), Arc::new(SelfMetrics::new()))
                .unwrap();
        // The monitor engine fills the pool: 4 tenants x 2 monitors.
        let mut held = Vec::new();
        for tenant in ["t1", "t2", "t3", "t4"] {
            for _ in 0..2 {
                held.push(
                    governor
                        .admit(WorkloadClass::Background, tenant)
                        .await
                        .unwrap(),
                );
            }
        }
        // Monitor queries finish a little after the old 1 s queue timeout.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            drop(held);
        });
        let (same_tenant, other_tenant) = tokio::join!(
            governor.admit(WorkloadClass::Background, "t1"),
            governor.admit(WorkloadClass::Background, "t9"),
        );
        assert!(same_tenant.is_ok() && other_tenant.is_ok());
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
            Err(TimeRangeError::TooLarge {
                max_seconds: 60,
                seconds: 120
            })
        );
        assert!(ValidatedTimeRange::parse("1786320000", "1786320030", 60).is_ok());
    }

    #[test]
    fn stored_limits_decode_to_defaults_or_an_error() {
        assert_eq!(
            QueryGovernorConfig::from_stored(None),
            Ok(QueryGovernorConfig::default())
        );
        let mut changed = QueryGovernorConfig::default();
        changed.dashboard.max_time_range_secs = 60 * 24 * 60 * 60;
        let raw = serde_json::to_string(&changed).unwrap();
        assert_eq!(
            QueryGovernorConfig::from_stored(Some(&raw)),
            Ok(changed.clone())
        );

        let mut invalid = changed;
        invalid.dashboard.per_tenant_concurrency = 0;
        let raw = serde_json::to_string(&invalid).unwrap();
        assert!(QueryGovernorConfig::from_stored(Some(&raw)).is_err());
        assert!(QueryGovernorConfig::from_stored(Some("{not json")).is_err());
        // Startup never fails on what a reload would reject.
        assert_eq!(
            QueryGovernorConfig::at_startup(Some(&raw)),
            QueryGovernorConfig::default()
        );
    }

    #[test]
    fn reload_applies_only_real_changes_and_never_undoes_a_newer_save() {
        let governor =
            QueryGovernor::new(QueryGovernorConfig::default(), Arc::new(SelfMetrics::new()))
                .unwrap();
        let mut changed = QueryGovernorConfig::default();
        changed.dashboard.max_time_range_secs = 60 * 24 * 60 * 60;

        let seen = governor.generation();
        assert!(!governor.apply_reloaded(QueryGovernorConfig::default(), seen));
        assert!(governor.apply_reloaded(changed.clone(), seen));
        assert_eq!(governor.config(), changed);

        // Saving the same limits again keeps the live pools.
        let seen = governor.generation();
        governor.reconfigure(changed.clone()).unwrap();
        assert_eq!(governor.generation(), seen);

        // A save on this replica lands while a reload read is in flight: the
        // (older) read must not replace it.
        let mut saved = changed.clone();
        saved.interactive.per_tenant_concurrency = 3;
        governor.reconfigure(saved.clone()).unwrap();
        assert!(!governor.apply_reloaded(changed, seen));
        assert_eq!(governor.config(), saved);

        // Limits that fail validation are never applied.
        let mut invalid = saved.clone();
        invalid.dashboard.per_tenant_concurrency = 0;
        assert!(!governor.apply_reloaded(invalid, governor.generation()));
        assert_eq!(governor.config(), saved);
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
    async fn busy_tenants_cannot_take_the_slots_an_idle_tenant_needs() {
        let mut config = QueryGovernorConfig::default();
        config.interactive.queue_timeout_ms = 0;
        let governor = QueryGovernor::new(config, Arc::new(SelfMetrics::new())).unwrap();
        // Four tenants each try to run their full per-tenant share (8), which
        // with the old pool was all 32 global slots.
        let mut held = Vec::new();
        for tenant in ["heavy-a", "heavy-b", "heavy-c", "heavy-d"] {
            for _ in 0..8 {
                if let Ok(guard) = governor.admit(WorkloadClass::Interactive, tenant).await {
                    held.push(guard);
                }
            }
        }
        assert!(
            governor
                .admit(WorkloadClass::Interactive, "idle")
                .await
                .is_ok()
        );
        // A tenant on its own still reaches its per-tenant limit.
        drop(held);
        let alone: Vec<_> = futures_util::future::join_all(
            (0..8).map(|_| governor.admit(WorkloadClass::Interactive, "solo")),
        )
        .await;
        assert!(alone.iter().all(Result::is_ok));
        // Burst slots are bounded: half the pool stays for idle tenants.
        let mut busy = Vec::new();
        for tenant in 0..8 {
            for _ in 0..8 {
                if let Ok(guard) = governor
                    .admit(WorkloadClass::Interactive, &format!("busy-{tenant}"))
                    .await
                {
                    busy.push(guard);
                }
            }
        }
        // 8 solo + 8 busy tenants' first queries + 16 burst slots fill the pool.
        assert_eq!(busy.len(), 8 + 16 - 7);
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
    async fn stream_metrics_distinguish_eof_error_timeout_and_artifact_generation() {
        let metrics = Arc::new(SelfMetrics::new());
        let governor = QueryGovernor::new(QueryGovernorConfig::default(), metrics.clone()).unwrap();
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-secret")
            .await
            .unwrap();
        let response = retain_admission_until_body_end(
            axum::response::Response::new(axum::body::Body::from("hello")),
            guard,
            Duration::from_secs(1),
        );
        assert!(
            metrics
                .render_prometheus()
                .contains("rush_export_streams_inflight{kind=\"response\"} 1")
        );
        assert!(
            !metrics
                .render_prometheus()
                .contains("rush_export_streams_total")
        );
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .len(),
            5
        );
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-secret")
            .await
            .unwrap();
        let body = axum::body::Body::from_stream(futures_util::stream::once(async {
            Err::<bytes::Bytes, _>(std::io::Error::other("secret error"))
        }));
        assert!(
            axum::body::to_bytes(
                retain_admission_until_body_end(
                    axum::response::Response::new(body),
                    guard,
                    Duration::from_secs(1)
                )
                .into_body(),
                1024
            )
            .await
            .is_err()
        );
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-secret")
            .await
            .unwrap();
        let response = retain_artifact_until_body_deadline(
            axum::response::Response::new(axum::body::Body::from("artifact")),
            guard,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let text = metrics.render_prometheus();
        assert!(
            text.contains("rush_export_streams_total{kind=\"response\",outcome=\"complete\"} 1")
        );
        assert!(
            text.contains("rush_export_streams_total{kind=\"response\",outcome=\"body_error\"} 1")
        );
        assert!(
            text.contains("rush_export_streams_total{kind=\"artifact\",outcome=\"complete\"} 1")
        );
        assert!(
            text.contains("rush_export_stream_bytes_sum{kind=\"response\",outcome=\"complete\"} 5")
        );
        assert!(!text.contains("secret"));
    }

    #[tokio::test]
    async fn timed_out_stream_drops_cursor_even_if_consumer_keeps_failed_body() {
        struct DropWatch(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropWatch {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watch = DropWatch(dropped.clone());
        let stream = futures_util::stream::pending::<Result<bytes::Bytes, std::io::Error>>().map(
            move |chunk| {
                let _ = &watch;
                chunk
            },
        );
        let metrics = Arc::new(SelfMetrics::new());
        let governor = QueryGovernor::new(QueryGovernorConfig::default(), metrics.clone()).unwrap();
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-a")
            .await
            .unwrap();
        let response = retain_admission_until_body_end(
            axum::response::Response::new(axum::body::Body::from_stream(stream)),
            guard,
            Duration::from_millis(5),
        );
        let mut body = response.into_body().into_data_stream();
        assert!(body.next().await.unwrap().is_err());
        assert!(dropped.load(Ordering::SeqCst));
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_export_streams_inflight{kind=\"response\"} 0"));
        assert!(
            text.contains("rush_export_streams_total{kind=\"response\",outcome=\"timeout\"} 1")
        );
        drop(body);
        assert_eq!(text, metrics.render_prometheus());
    }

    #[tokio::test]
    async fn streaming_response_holds_permit_until_body_is_dropped() {
        let mut config = QueryGovernorConfig::default();
        config.export.global_concurrency = 1;
        config.export.per_tenant_concurrency = 1;
        config.export.queue_timeout_ms = 0;
        let metrics = Arc::new(SelfMetrics::new());
        let governor = QueryGovernor::new(config, metrics.clone()).unwrap();
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
        let text = metrics.render_prometheus();
        assert!(
            text.contains("rush_export_streams_total{kind=\"response\",outcome=\"cancelled\"} 1")
        );
        assert!(text.contains("rush_export_streams_inflight{kind=\"response\"} 0"));
        assert!(
            governor
                .admit(WorkloadClass::Export, "tenant-a")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn streaming_response_keeps_the_original_request_deadline() {
        let mut config = QueryGovernorConfig::default();
        config.export.global_concurrency = 1;
        config.export.per_tenant_concurrency = 1;
        config.export.queue_timeout_ms = 0;
        let governor = QueryGovernor::new(config, Arc::new(SelfMetrics::new())).unwrap();
        let guard = governor
            .admit(WorkloadClass::Export, "tenant-a")
            .await
            .unwrap();
        let response = retain_admission_until_body_deadline(
            axum::response::Response::new(axum::body::Body::from("already ready")),
            guard,
            tokio::time::Instant::now() - Duration::from_millis(1),
        );
        assert!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .is_err()
        );
        assert!(
            governor
                .admit(WorkloadClass::Export, "tenant-a")
                .await
                .is_ok()
        );
    }
}
