//! Leader leases for the background engines.
//!
//! Every query-api replica starts the alert, SLO, anomaly, detection, and
//! retention loops, but only the replica holding an engine's lease runs its
//! cycles. Without this, N replicas evaluate every rule N times and send N
//! copies of every notification.
//!
//! Leases live in a ClickHouse KeeperMap table, the same Keeper that production
//! already requires for SSO replay protection. Acquire and renew are one
//! conditional `ALTER TABLE … UPDATE` under `keeper_map_strict_mode`, so
//! concurrent attempts either apply atomically or fail. Each attempt writes a
//! fresh random token, and a replica only counts itself leader when it reads
//! back its own holder id *and* that token, so a stale read can never make a
//! deposed replica think it still holds the lease.
//!
//! Lease time is measured by ClickHouse (`now64`), so pod clocks don't matter.
//! Locally, a holder acts only until `attempt start + ttl - margin`, measured on
//! the monotonic clock from before the renewal was sent. A replica that stops
//! renewing therefore stops acting before any other replica can take over.
//!
//! Engine cycles run inside [`fenced`], and code with side effects calls
//! [`ensure_leader`] first: outbound notification requests and the engines'
//! state writes. Outside a fenced scope (API handlers, tests) the check passes.
//!
//! Lease changes are reported through logs, `rush_engine_leader*` metrics, and
//! `/healthz`, not the audit log. Every replica changes leases on each start
//! and stop, and the audit chain's sequence is assigned per process, so
//! concurrent audit writes from several replicas collide.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use clickhouse::Client;

use crate::self_metrics::SelfMetrics;
use crate::shutdown::ShutdownController;

const LEASE_TABLE: &str = "config_engine_leases";
const MODE_ENV: &str = "RUSH_LEADER_ELECTION";
const TTL_ENV: &str = "RUSH_LEADER_LEASE_TTL_SECS";
const REPLICAS_ENV: &str = "RUSH_QUERY_API_REPLICAS";
const DEFAULT_TTL_SECS: u64 = 15;
const MIN_TTL_SECS: u64 = 6;
const MAX_TTL_SECS: u64 = 300;

/// How often an engine loop that isn't leader checks whether it has become one.
pub const FOLLOWER_POLL: Duration = Duration::from_secs(5);

/// Engines that take a lease. Names are stable: they are the KeeperMap keys
/// and metric labels.
pub const MONITORS: &str = "monitors";
pub const SLOS: &str = "slos";
pub const ANOMALIES: &str = "anomalies";
pub const DETECTIONS: &str = "detections";
pub const RETENTION: &str = "retention";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// Single development instance: always the leader, nothing stored.
    Local,
    /// Shared KeeperMap lease: exactly one holder across replicas.
    Keeper,
}

impl LeaseMode {
    fn as_str(self) -> &'static str {
        match self {
            LeaseMode::Local => "local",
            LeaseMode::Keeper => "keeper",
        }
    }
}

/// Same rules as the SSO replay store: production and multi-replica installs
/// must coordinate through Keeper; `local` is only for one development instance.
pub fn lease_mode(
    raw: Option<&str>,
    replicas: usize,
    production: bool,
) -> anyhow::Result<LeaseMode> {
    let mode = raw.unwrap_or("auto").trim().to_ascii_lowercase();
    match mode.as_str() {
        "auto" if production || replicas > 1 => Ok(LeaseMode::Keeper),
        "auto" => Ok(LeaseMode::Local),
        "local" if production || replicas > 1 => anyhow::bail!(
            "{MODE_ENV}=local is only allowed for a single development instance; production and multiple replicas require keeper"
        ),
        "local" => Ok(LeaseMode::Local),
        "keeper" => Ok(LeaseMode::Keeper),
        _ => anyhow::bail!("{MODE_ENV} must be one of: auto, local, keeper"),
    }
}

/// Lease timing. Renew three times per TTL; stop acting a third of a TTL
/// before the lease could expire in ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub margin: Duration,
}

impl LeaseTiming {
    pub fn from_ttl_secs(ttl_secs: u64) -> anyhow::Result<Self> {
        if !(MIN_TTL_SECS..=MAX_TTL_SECS).contains(&ttl_secs) {
            anyhow::bail!("{TTL_ENV} must be between {MIN_TTL_SECS} and {MAX_TTL_SECS} seconds");
        }
        let ttl = Duration::from_secs(ttl_secs);
        Ok(Self {
            ttl,
            renew_every: ttl / 3,
            margin: ttl / 3,
        })
    }

    fn from_env() -> anyhow::Result<Self> {
        let secs = match std::env::var(TTL_ENV) {
            Ok(raw) => raw
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("{TTL_ENV} must be a whole number of seconds"))?,
            Err(_) => DEFAULT_TTL_SECS,
        };
        Self::from_ttl_secs(secs)
    }

    /// How long a successful attempt that started at `started` lets us act.
    fn act_until(&self, started: Instant) -> Instant {
        started + self.ttl - self.margin
    }
}

/// Process-wide monotonic origin, so deadlines fit in an atomic.
fn origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

fn to_millis(at: Instant) -> u64 {
    // +1 keeps 0 free to mean "not leader".
    at.saturating_duration_since(origin()).as_millis() as u64 + 1
}

/// One engine's view of its lease.
pub struct EngineLease {
    name: &'static str,
    holder: String,
    mode: LeaseMode,
    /// Monotonic millis (see `to_millis`) until which this replica may act.
    /// 0 means not leader; u64::MAX means always (local mode).
    act_until_ms: AtomicU64,
    epoch: AtomicU64,
    leader: AtomicBool,
    current_holder: RwLock<String>,
}

impl EngineLease {
    fn new(name: &'static str, holder: String, mode: LeaseMode) -> Self {
        let local = mode == LeaseMode::Local;
        Self {
            name,
            current_holder: RwLock::new(if local { holder.clone() } else { String::new() }),
            holder,
            mode,
            act_until_ms: AtomicU64::new(if local { u64::MAX } else { 0 }),
            epoch: AtomicU64::new(0),
            leader: AtomicBool::new(local),
        }
    }

    /// A lease that always holds, for single-instance runs and tests.
    pub fn always(name: &'static str) -> Arc<Self> {
        Arc::new(Self::new(name, "local".to_string(), LeaseMode::Local))
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// True while this replica may act for the engine.
    pub fn is_leader(&self) -> bool {
        let until = self.act_until_ms.load(Ordering::Acquire);
        until == u64::MAX || (until != 0 && to_millis(Instant::now()) < until)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn step_down(&self) {
        self.act_until_ms.store(0, Ordering::Release);
    }
}

tokio::task_local! {
    static CURRENT: Arc<EngineLease>;
}

/// Run one engine cycle with its lease in scope, so [`ensure_leader`] calls
/// anywhere inside the cycle check that lease.
pub async fn fenced<F: std::future::Future>(lease: Arc<EngineLease>, cycle: F) -> F::Output {
    CURRENT.scope(lease, cycle).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeader {
    pub engine: &'static str,
}

impl std::fmt::Display for NotLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} engine lost its leader lease; skipped", self.engine)
    }
}

impl std::error::Error for NotLeader {}

/// Guard for side effects. Inside a [`fenced`] cycle, fails once the lease is
/// lost; outside one it always passes.
pub fn ensure_leader() -> Result<(), NotLeader> {
    match CURRENT.try_with(|lease| (lease.is_leader(), lease.name)) {
        Ok((true, _)) | Err(_) => Ok(()),
        Ok((false, engine)) => Err(NotLeader { engine }),
    }
}

fn renewers() -> &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    static RENEWERS: OnceLock<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> = OnceLock::new();
    RENEWERS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

/// After shutdown is requested, wait up to `limit` for every lease to be
/// released, so the process doesn't exit before telling Keeper.
pub async fn wait_for_release(limit: Duration) {
    let handles: Vec<_> = renewers()
        .lock()
        .map(|mut handles| handles.drain(..).collect())
        .unwrap_or_default();
    if handles.is_empty() {
        return;
    }
    if tokio::time::timeout(limit, futures_util::future::join_all(handles))
        .await
        .is_err()
    {
        tracing::warn!(
            "leader leases were not all released before shutdown; the next leader waits for them to expire"
        );
    }
}

fn registry() -> &'static RwLock<Vec<Arc<EngineLease>>> {
    static LEASES: OnceLock<RwLock<Vec<Arc<EngineLease>>>> = OnceLock::new();
    LEASES.get_or_init(|| RwLock::new(Vec::new()))
}

/// Lease state for `/healthz`.
pub fn status() -> serde_json::Value {
    let leases = registry().read().map(|l| l.clone()).unwrap_or_default();
    let engines: serde_json::Map<String, serde_json::Value> = leases
        .iter()
        .map(|lease| {
            let holder = lease
                .current_holder
                .read()
                .map(|h| h.clone())
                .unwrap_or_default();
            (
                lease.name.to_string(),
                serde_json::json!({
                    "mode": lease.mode.as_str(),
                    "leader": lease.is_leader(),
                    "holder": holder,
                    "epoch": lease.epoch(),
                }),
            )
        })
        .collect();
    serde_json::Value::Object(engines)
}

#[derive(clickhouse::Row, serde::Deserialize, Debug, Clone)]
struct LeaseRow {
    holder: String,
    token: String,
    epoch: u64,
}

/// Creates and renews engine leases for this process.
pub struct LeaderElection {
    mode: LeaseMode,
    client: Client,
    holder: String,
    timing: LeaseTiming,
    metrics: Arc<SelfMetrics>,
    shutdown: Option<ShutdownController>,
}

impl LeaderElection {
    /// Read `RUSH_LEADER_ELECTION`, `RUSH_LEADER_LEASE_TTL_SECS`, and the
    /// replica count, and create the lease table when Keeper is used.
    pub async fn from_env(
        client: Client,
        instance_id: &str,
        metrics: Arc<SelfMetrics>,
        shutdown: Option<ShutdownController>,
    ) -> anyhow::Result<Self> {
        let replicas = match std::env::var(REPLICAS_ENV) {
            Ok(raw) => raw
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|count| *count > 0)
                .ok_or_else(|| anyhow::anyhow!("{REPLICAS_ENV} must be a positive integer"))?,
            Err(_) => 1,
        };
        let mode = lease_mode(
            std::env::var(MODE_ENV).ok().as_deref(),
            replicas,
            crate::api_key_auth::production_mode(),
        )?;
        let election = Self {
            mode,
            client,
            // Restarts reuse pod names; the suffix keeps each process distinct.
            holder: format!(
                "{instance_id}/{}",
                &uuid::Uuid::new_v4().simple().to_string()[..8]
            ),
            timing: LeaseTiming::from_env()?,
            metrics,
            shutdown,
        };
        election.initialize().await?;
        Ok(election)
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        if self.mode == LeaseMode::Local {
            tracing::warn!(
                "background engines run without a leader lease; this is only safe for a single development instance"
            );
            return Ok(());
        }
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {LEASE_TABLE} (\
                 name String,\
                 holder String,\
                 token String,\
                 epoch UInt64,\
                 expires_at DateTime64(3, 'UTC'),\
                 acquired_at DateTime64(3, 'UTC')\
             ) ENGINE = KeeperMap('engine_leases') PRIMARY KEY name"
        );
        self.client.query(&ddl).execute().await.map_err(|error| {
            anyhow::anyhow!(
                "engine leader lease table could not be created; configure ClickHouse Keeper and keeper_map_path_prefix (required for production and multiple replicas): {error}"
            )
        })?;
        tracing::info!(
            holder = %self.holder,
            ttl_secs = self.timing.ttl.as_secs(),
            "background engines coordinate through ClickHouse Keeper leader leases"
        );
        Ok(())
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Create the lease for `name` and start renewing it in the background.
    pub fn lease(&self, name: &'static str) -> Arc<EngineLease> {
        let lease = Arc::new(EngineLease::new(name, self.holder.clone(), self.mode));
        if let Ok(mut leases) = registry().write() {
            leases.retain(|existing| existing.name != name);
            leases.push(lease.clone());
        }
        self.metrics.set_gauge(
            "rush_engine_leader",
            &[("engine", name)],
            lease.is_leader() as u8 as f64,
        );
        if self.mode == LeaseMode::Keeper {
            let renewer = Renewer {
                client: self.client.clone(),
                lease: lease.clone(),
                timing: self.timing,
                metrics: self.metrics.clone(),
            };
            let handle = tokio::spawn(renewer.run(self.shutdown.clone()));
            if let Ok(mut handles) = renewers().lock() {
                handles.push(handle);
            }
        }
        lease
    }
}

struct Renewer {
    client: Client,
    lease: Arc<EngineLease>,
    timing: LeaseTiming,
    metrics: Arc<SelfMetrics>,
}

impl Renewer {
    async fn run(self, shutdown: Option<ShutdownController>) {
        let stop = async {
            match &shutdown {
                Some(shutdown) => shutdown.wait_for_request().await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(stop);
        loop {
            self.attempt().await;
            tokio::select! {
                _ = &mut stop => break,
                _ = tokio::time::sleep(self.timing.renew_every) => {}
            }
        }
        // Hand over right away instead of making the next leader wait out the
        // TTL during a rollout.
        let was_leader = self.lease.is_leader();
        self.lease.step_down();
        if was_leader {
            if let Err(error) = self.release().await {
                tracing::warn!(engine = self.lease.name, %error, "failed to release leader lease on shutdown");
            }
            self.changed(false, None);
        }
    }

    async fn attempt(&self) {
        let started = Instant::now();
        let token = uuid::Uuid::new_v4().simple().to_string();
        match self.acquire(&token).await {
            Ok((row, previous)) => {
                let won = row.holder == self.lease.holder && row.token == token;
                if let Ok(mut current) = self.lease.current_holder.write() {
                    current.clone_from(&row.holder);
                }
                if won {
                    self.lease.epoch.store(row.epoch, Ordering::Release);
                    self.lease
                        .act_until_ms
                        .store(to_millis(self.timing.act_until(started)), Ordering::Release);
                    if !self.lease.leader.swap(true, Ordering::AcqRel) {
                        self.changed(true, previous);
                    }
                } else {
                    self.lease.step_down();
                    if self.lease.leader.swap(false, Ordering::AcqRel) {
                        self.changed(false, Some(row.holder));
                    }
                }
            }
            Err(error) => {
                // Keep the current deadline: it runs out on its own, before any
                // other replica can take the lease over.
                self.metrics.inc_counter(
                    "rush_engine_lease_errors_total",
                    &[("engine", self.lease.name)],
                    1,
                );
                tracing::warn!(engine = self.lease.name, %error, "leader lease renewal failed");
                if !self.lease.is_leader() && self.lease.leader.swap(false, Ordering::AcqRel) {
                    self.changed(false, None);
                }
            }
        }
    }

    /// Returns the row after the attempt and the holder before it.
    async fn acquire(&self, token: &str) -> anyhow::Result<(LeaseRow, Option<String>)> {
        let name = self.lease.name;
        let me = self.lease.holder.as_str();
        let ttl_ms = self.timing.ttl.as_millis() as u64;

        let before = self.read().await?;
        if before.is_none() {
            let insert = format!(
                "INSERT INTO {LEASE_TABLE} (name, holder, token, epoch, expires_at, acquired_at) \
                 SELECT ?, ?, ?, 1, now64(3) + toIntervalMillisecond(?), now64(3)"
            );
            if let Err(error) = self
                .client
                .query(&insert)
                .with_option("keeper_map_strict_mode", "1")
                .with_option("async_insert", "0")
                .bind(name)
                .bind(me)
                .bind(token)
                .bind(ttl_ms)
                .execute()
                .await
            {
                // Another replica created it first; the read below says who.
                tracing::debug!(engine = name, %error, "leader lease row already exists");
            }
        } else {
            let update = format!(
                "ALTER TABLE {LEASE_TABLE} UPDATE \
                     epoch = if(holder = ?, epoch, epoch + 1), \
                     acquired_at = if(holder = ?, acquired_at, now64(3)), \
                     holder = ?, \
                     token = ?, \
                     expires_at = now64(3) + toIntervalMillisecond(?) \
                 WHERE name = ? AND (holder = ? OR expires_at < now64(3))"
            );
            self.client
                .query(&update)
                .with_option("keeper_map_strict_mode", "1")
                .with_option("mutations_sync", "1")
                .bind(me)
                .bind(me)
                .bind(me)
                .bind(token)
                .bind(ttl_ms)
                .bind(name)
                .bind(me)
                .execute()
                .await?;
        }

        let after = self
            .read()
            .await?
            .ok_or_else(|| anyhow::anyhow!("leader lease row for {name} is missing"))?;
        Ok((after, before.map(|row| row.holder)))
    }

    async fn read(&self) -> anyhow::Result<Option<LeaseRow>> {
        let query = format!("SELECT holder, token, epoch FROM {LEASE_TABLE} WHERE name = ?");
        Ok(self
            .client
            .query(&query)
            .bind(self.lease.name)
            .fetch_optional::<LeaseRow>()
            .await?)
    }

    async fn release(&self) -> anyhow::Result<()> {
        let release = format!(
            "ALTER TABLE {LEASE_TABLE} UPDATE expires_at = now64(3) - toIntervalMillisecond(1) \
             WHERE name = ? AND holder = ?"
        );
        self.client
            .query(&release)
            .with_option("keeper_map_strict_mode", "1")
            .with_option("mutations_sync", "1")
            .bind(self.lease.name)
            .bind(&self.lease.holder)
            .execute()
            .await?;
        Ok(())
    }

    fn changed(&self, acquired: bool, other: Option<String>) {
        let name = self.lease.name;
        let epoch = self.lease.epoch();
        self.metrics.set_gauge(
            "rush_engine_leader",
            &[("engine", name)],
            acquired as u8 as f64,
        );
        self.metrics
            .inc_counter("rush_engine_leader_changes_total", &[("engine", name)], 1);
        let other_holder = other.as_deref().unwrap_or("none");
        if acquired {
            tracing::info!(engine = name, holder = %self.lease.holder, epoch, previous = other_holder, "acquired engine leader lease");
        } else {
            tracing::info!(engine = name, holder = %self.lease.holder, new_holder = other_holder, "released engine leader lease");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_matches_sso_store_rules() {
        assert_eq!(lease_mode(None, 1, false).unwrap(), LeaseMode::Local);
        assert_eq!(lease_mode(None, 2, false).unwrap(), LeaseMode::Keeper);
        assert_eq!(lease_mode(None, 1, true).unwrap(), LeaseMode::Keeper);
        assert_eq!(
            lease_mode(Some("keeper"), 1, false).unwrap(),
            LeaseMode::Keeper
        );
        assert_eq!(
            lease_mode(Some(" LOCAL "), 1, false).unwrap(),
            LeaseMode::Local
        );
        assert!(lease_mode(Some("local"), 3, false).is_err());
        assert!(lease_mode(Some("local"), 1, true).is_err());
        assert!(lease_mode(Some("zookeeper"), 1, false).is_err());
    }

    #[test]
    fn timing_acts_well_inside_the_ttl() {
        let timing = LeaseTiming::from_ttl_secs(15).unwrap();
        assert_eq!(timing.renew_every, Duration::from_secs(5));
        assert_eq!(timing.margin, Duration::from_secs(5));
        let started = Instant::now();
        assert_eq!(timing.act_until(started), started + Duration::from_secs(10));
        assert!(LeaseTiming::from_ttl_secs(5).is_err());
        assert!(LeaseTiming::from_ttl_secs(301).is_err());
    }

    #[test]
    fn keeper_lease_starts_as_follower_and_expires() {
        let lease = EngineLease::new(MONITORS, "pod-a/1".into(), LeaseMode::Keeper);
        assert!(!lease.is_leader());
        lease.act_until_ms.store(
            to_millis(Instant::now() + Duration::from_secs(60)),
            Ordering::Release,
        );
        assert!(lease.is_leader());
        lease
            .act_until_ms
            .store(to_millis(Instant::now()), Ordering::Release);
        assert!(
            !lease.is_leader(),
            "a deadline that has arrived no longer holds"
        );
        lease.step_down();
        assert!(!lease.is_leader());
    }

    #[test]
    fn local_lease_always_holds() {
        let lease = EngineLease::always(SLOS);
        assert!(lease.is_leader());
    }

    #[tokio::test]
    async fn ensure_leader_follows_the_scoped_lease() {
        assert!(
            ensure_leader().is_ok(),
            "outside an engine cycle nothing is fenced"
        );

        let lease = Arc::new(EngineLease::new(
            DETECTIONS,
            "pod-a/1".into(),
            LeaseMode::Keeper,
        ));
        let held = lease.clone();
        held.act_until_ms.store(
            to_millis(Instant::now() + Duration::from_secs(60)),
            Ordering::Release,
        );
        let result = fenced(lease.clone(), async {
            let first = ensure_leader();
            held.step_down();
            (first, ensure_leader())
        })
        .await;
        assert_eq!(result.0, Ok(()));
        assert_eq!(result.1, Err(NotLeader { engine: DETECTIONS }));
        assert!(ensure_leader().is_ok());
    }
}
