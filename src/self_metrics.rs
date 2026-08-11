//! In-process self-metrics registry for Rush query-api system health.
//!
//! This is a small, dependency-free metrics registry (std atomics + the already-present
//! `dashmap`). It is the single source of truth for two outputs:
//!
//!   1. **Prometheus text exposition** ([`SelfMetrics::render_prometheus`]) served at the
//!      open `GET /metrics` endpoint (valid 0.0.4 format: `# TYPE`, counters, gauges, and
//!      histograms rendered as `_bucket{le=...}` / `_sum` / `_count`).
//!   2. **Self-ingest snapshot** ([`SelfMetrics::snapshot_series`]) that the stats engine
//!      writes into our own `metrics_gauge` / `metrics_sum` tables every tick, so the same
//!      data is queryable in-product through `/prom/api/v1/*`. Histograms are flattened to
//!      `_count` + `_sum` + `p50` / `p95` / `p99` gauges (computed from buckets) rather than
//!      raw buckets, which is simpler for our PromQL layer.
//!
//! ## Cardinality
//! All series here are **global** (not per-tenant). Label sets MUST be small and finite —
//! never use `tenant_id` or raw request paths as labels. The HTTP middleware labels by the
//! templated `MatchedPath` (a finite set), engine loops by a fixed `engine` name, etc.
//!
//! ## Hot-path cost
//! Counter and gauge updates are a single `DashMap` entry lookup plus a relaxed atomic op.
//! Histogram observations do a bounded linear scan of the fixed bucket list (12 entries) and
//! one atomic increment. No locks are held across `.await`.

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Fixed latency histogram bucket upper bounds, in milliseconds. Chosen to cover sub-ms
/// (effectively the first bucket) through 10s. `+Inf` is implicit (the `_count`).
/// This is the default bucket set used by [`SelfMetrics::observe_histogram`].
pub const LATENCY_BUCKETS_MS: [f64; 11] = [
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

/// Latency bucket bounds (ms) sized for *search* — which can be much slower than the
/// HTTP/engine paths — covering 10ms through 60s.
pub const SEARCH_LATENCY_BUCKETS_MS: [f64; 11] = [
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0,
];

/// Result-row-count bucket bounds (unitless counts), for `rush_search_result_rows`.
pub const RESULT_COUNT_BUCKETS: [f64; 10] = [
    0.0, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0,
];

/// Logical rows returned by bounded authorization/config lookups. Most
/// credential and policy resolutions return zero or one row; role and group
/// permission lookups may return a small bounded set.
pub const AUTH_LOOKUP_ROWS_BUCKETS: [f64; 8] = [0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0];

/// Free-text query-length bucket bounds (characters), for `rush_search_query_length_chars`.
pub const QUERY_LEN_BUCKETS: [f64; 10] =
    [0.0, 1.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0];

/// Logical matched/response byte buckets for coordinated Explore searches.
/// These are deliberately broad because payloads range from tiny needle searches
/// to multi-megabyte result pages. ClickHouse physical bytes-read remain available
/// through its query log; this metric tracks the bounded logical work returned by
/// the coordinator without adding a third instrumentation query.
pub const EXPLORE_BYTES_BUCKETS: [f64; 11] = [
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
];

/// Allowlisted operation names used by operation-level query metrics. Keep this list
/// intentionally small: operation names become Prometheus label values and must never
/// be derived from raw URLs, SQL, tenant IDs, or user input.
pub const QUERY_OPERATIONS: [&str; 7] = [
    "explore_logs",
    "explore_spans",
    "promql_instant",
    "promql_range",
    "promql_series",
    "promql_metadata",
    "other",
];

/// A label set, stored already-sorted by key so identical sets map to one series.
type Labels = Vec<(&'static str, String)>;

/// Kind of a self-ingested point: a gauge (instantaneous) or a sum (monotonic counter).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Sum,
}

/// One flattened series sample for self-ingest into ClickHouse.
#[derive(Clone, Debug)]
pub struct MetricPoint {
    pub name: String,
    pub labels: Labels,
    pub value: f64,
    pub kind: MetricKind,
}

/// f64 gauge stored as its bit pattern in an atomic so reads/writes stay lock-free.
#[derive(Default)]
struct AtomicF64 {
    bits: AtomicU64,
}

impl AtomicF64 {
    fn set(&self, v: f64) {
        self.bits.store(v.to_bits(), Ordering::Relaxed);
    }
    fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
    fn add(&self, delta: f64) {
        // Compare-and-swap loop; gauges that use add/sub (in-flight) are low-frequency
        // relative to counters, so contention is negligible.
        let mut cur = self.bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + delta).to_bits();
            match self
                .bits
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// A fixed-bucket histogram. `buckets[i]` counts observations `<= bounds[i]`
/// (cumulative is computed at render time). `count` is the total (= `+Inf` bucket),
/// `sum` is the running sum of observed values. Each histogram carries its own
/// `&'static` bucket bounds, chosen on first registration — so latency (ms),
/// result-count, and query-length histograms can use different scales.
struct Histogram {
    /// Bucket upper bounds for this histogram (e.g. [`LATENCY_BUCKETS_MS`]).
    bounds: &'static [f64],
    /// One counter per bound; `buckets.len() == bounds.len()`.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum: AtomicF64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Histogram {
            bounds,
            buckets: (0..bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicF64::default(),
        }
    }

    fn observe(&self, value: f64) {
        // Record into the first bucket whose upper bound the value falls under.
        // Bounded linear scan over the (small, fixed) bound list — cheap and
        // branch-predictable. Values above the last finite bound only bump `count`
        // (the implicit `+Inf` bucket).
        for (i, &ub) in self.bounds.iter().enumerate() {
            if value <= ub {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.add(value);
    }

    /// Snapshot the per-bucket (non-cumulative) counts, the total count, and the sum.
    fn snapshot(&self) -> (Vec<u64>, u64, f64) {
        let b: Vec<u64> = self
            .buckets
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .collect();
        (b, self.count.load(Ordering::Relaxed), self.sum.get())
    }
}

/// Compute an approximate quantile (0.0..=1.0) from cumulative bucket counts, using the
/// standard Prometheus `histogram_quantile` linear-interpolation-within-bucket method.
/// `per_bucket` holds the **non-cumulative** counts aligned to `bounds`.
/// Returns the estimated value, or 0.0 when there are no observations.
pub fn quantile_from_buckets(per_bucket: &[u64], bounds: &[f64], total: u64, q: f64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let rank = q * total as f64;
    let mut cumulative = 0u64;
    let mut prev_bound = 0.0f64;
    for (i, &c) in per_bucket.iter().enumerate() {
        let upper = bounds[i];
        let next_cumulative = cumulative + c;
        if (next_cumulative as f64) >= rank {
            // Linear interpolation within this bucket between prev_bound and upper.
            let bucket_count = c as f64;
            if bucket_count == 0.0 {
                return upper;
            }
            let into_bucket = (rank - cumulative as f64) / bucket_count;
            return prev_bound + (upper - prev_bound) * into_bucket;
        }
        cumulative = next_cumulative;
        prev_bound = upper;
    }
    // Everything above the last finite bucket falls in the implicit +Inf bucket; the best
    // bounded estimate we can report is the top finite bound.
    bounds.last().copied().unwrap_or(0.0)
}

/// The registry. Cheap to clone the `Arc` of; cheap to update.
#[derive(Default)]
pub struct SelfMetrics {
    counters: DashMap<(&'static str, Labels), AtomicU64>,
    gauges: DashMap<(&'static str, Labels), AtomicF64>,
    histograms: DashMap<(&'static str, Labels), Histogram>,
}

/// RAII guard for a query-operation concurrency gauge. Keeping the decrement in
/// `Drop` means early returns and handler errors cannot leave a stale in-flight
/// value behind.
pub struct QueryGuard {
    metrics: Arc<SelfMetrics>,
    operation: &'static str,
    signal: &'static str,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.metrics.add_gauge(
            "rush_query_requests_in_flight",
            &[("operation", self.operation), ("signal", self.signal)],
            -1.0,
        );
    }
}

impl SelfMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start tracking one query operation. Operation and signal values are
    /// normalized to fixed allowlists before becoming labels.
    pub fn query_guard(
        self: &Arc<Self>,
        operation: &'static str,
        signal: &'static str,
    ) -> QueryGuard {
        let operation = bounded_query_operation(operation);
        let signal = bounded_query_signal(signal);
        self.add_gauge(
            "rush_query_requests_in_flight",
            &[("operation", operation), ("signal", signal)],
            1.0,
        );
        QueryGuard {
            metrics: self.clone(),
            operation,
            signal,
        }
    }

    /// Sort a label slice into the canonical (key-sorted) order used as the map key.
    fn norm(labels: &[(&'static str, &str)]) -> Labels {
        let mut v: Labels = labels
            .iter()
            .map(|(k, val)| (*k, (*val).to_string()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(b.0));
        v
    }

    /// Increment a counter by `n`.
    pub fn inc_counter(&self, name: &'static str, labels: &[(&'static str, &str)], n: u64) {
        let key = (name, Self::norm(labels));
        if let Some(c) = self.counters.get(&key) {
            c.fetch_add(n, Ordering::Relaxed);
        } else {
            // Insert-or-add: another thread may have raced us; `entry` resolves it.
            self.counters
                .entry(key)
                .or_default()
                .fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Set a gauge to an absolute value.
    pub fn set_gauge(&self, name: &'static str, labels: &[(&'static str, &str)], v: f64) {
        let key = (name, Self::norm(labels));
        if let Some(g) = self.gauges.get(&key) {
            g.set(v);
        } else {
            self.gauges.entry(key).or_default().set(v);
        }
    }

    /// Add a delta (may be negative) to a gauge — used for in-flight tracking.
    pub fn add_gauge(&self, name: &'static str, labels: &[(&'static str, &str)], delta: f64) {
        let key = (name, Self::norm(labels));
        if let Some(g) = self.gauges.get(&key) {
            g.add(delta);
        } else {
            self.gauges.entry(key).or_default().add(delta);
        }
    }

    /// Observe a value (milliseconds) into a histogram using the default
    /// [`LATENCY_BUCKETS_MS`] bucket set. The bucket set is bound to the series on first
    /// use; later calls reuse the existing histogram (and its bounds).
    pub fn observe_histogram(
        &self,
        name: &'static str,
        labels: &[(&'static str, &str)],
        value_ms: f64,
    ) {
        self.observe_histogram_with(name, labels, value_ms, &LATENCY_BUCKETS_MS);
    }

    /// Observe a value into a histogram with an explicit `&'static` bucket set. The first
    /// observation for a (name, labels) series registers the histogram with these `bounds`;
    /// subsequent observations reuse the existing histogram and ignore `bounds` (the bound
    /// set is fixed per series). Use this for non-millisecond histograms (result counts,
    /// query lengths) — see [`RESULT_COUNT_BUCKETS`], [`QUERY_LEN_BUCKETS`].
    pub fn observe_histogram_with(
        &self,
        name: &'static str,
        labels: &[(&'static str, &str)],
        value: f64,
        bounds: &'static [f64],
    ) {
        let key = (name, Self::norm(labels));
        if let Some(h) = self.histograms.get(&key) {
            h.observe(value);
        } else {
            self.histograms
                .entry(key)
                .or_insert_with(|| Histogram::new(bounds))
                .observe(value);
        }
    }

    /// Record a security-sensitive authorization/config lookup. Both label
    /// values are collapsed to fixed allowlists so these hot-path metrics can
    /// never acquire user, tenant, token, route, or query cardinality.
    pub fn record_auth_lookup(
        &self,
        lookup: &'static str,
        duration_ms: f64,
        result_rows: u64,
        outcome: &'static str,
    ) {
        let lookup = bounded_auth_lookup(lookup);
        let outcome = bounded_auth_outcome(outcome);
        self.inc_counter(
            "rush_auth_lookups_total",
            &[("lookup", lookup), ("outcome", outcome)],
            1,
        );
        self.observe_histogram(
            "rush_auth_lookup_duration_ms",
            &[("lookup", lookup)],
            duration_ms,
        );
        self.observe_histogram_with(
            "rush_auth_lookup_result_rows",
            &[("lookup", lookup)],
            result_rows as f64,
            &AUTH_LOOKUP_ROWS_BUCKETS,
        );
    }

    /// Record cache behavior for authorization data. The request-session cache
    /// is scoped to one HTTP request; config caches are bounded and explicitly
    /// invalidated by local mutations.
    pub fn record_auth_cache(&self, lookup: &'static str, outcome: &'static str) {
        self.inc_counter(
            "rush_auth_cache_total",
            &[
                ("lookup", bounded_auth_lookup(lookup)),
                ("outcome", bounded_auth_cache_outcome(outcome)),
            ],
            1,
        );
    }

    /// Convenience: record one engine-loop cycle's outcome.
    ///
    /// Emits `rush_engine_runs_total` (always), `rush_engine_failures_total` (when `!ok`),
    /// the `rush_engine_run_duration_ms` histogram, and the `rush_engine_last_run_timestamp`
    /// gauge (unix seconds). `engine` is a fixed, finite label.
    pub fn record_engine(&self, engine: &'static str, duration_ms: u64, ok: bool) {
        let labels = [("engine", engine)];
        self.inc_counter("rush_engine_runs_total", &labels, 1);
        if !ok {
            self.inc_counter("rush_engine_failures_total", &labels, 1);
        }
        self.observe_histogram("rush_engine_run_duration_ms", &labels, duration_ms as f64);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.set_gauge("rush_engine_last_run_timestamp", &labels, now as f64);
    }

    /// Convenience: record one log/span SEARCH and update all search self-metrics.
    ///
    /// Emits, all labeled only by the fixed `signal` (and `outcome` on the counter):
    ///   - `rush_search_queries_total{signal,outcome}` — always (+1).
    ///   - `rush_search_duration_ms{signal}` — end-to-end latency histogram
    ///     ([`SEARCH_LATENCY_BUCKETS_MS`]); `_sum`/`_count` give the average.
    ///   - `rush_search_result_rows{signal}` — returned-row-count histogram
    ///     ([`RESULT_COUNT_BUCKETS`]); `_sum`/`_count` give the average.
    ///   - `rush_search_query_length_chars{signal}` — free-text length histogram
    ///     ([`QUERY_LEN_BUCKETS`]), **only** when `query_len` is `Some` (a search term was
    ///     present); pure browse (`None`) is still counted as a query but skips this.
    ///   - `rush_search_empty_total{signal}` — +1 when `result_rows == 0` (no-results signal).
    ///
    /// `signal` is a fixed, finite label (e.g. "logs" or "spans"). Never pass tenant,
    /// query text, route, or user as a label here — cardinality must stay tiny.
    pub fn record_search(
        &self,
        signal: &'static str,
        query_len: Option<usize>,
        result_rows: u64,
        duration_ms: u64,
        ok: bool,
    ) {
        let signal_label = [("signal", signal)];
        let outcome = if ok { "ok" } else { "error" };
        self.inc_counter(
            "rush_search_queries_total",
            &[("signal", signal), ("outcome", outcome)],
            1,
        );
        self.observe_histogram_with(
            "rush_search_duration_ms",
            &signal_label,
            duration_ms as f64,
            &SEARCH_LATENCY_BUCKETS_MS,
        );
        self.observe_histogram_with(
            "rush_search_result_rows",
            &signal_label,
            result_rows as f64,
            &RESULT_COUNT_BUCKETS,
        );
        if let Some(len) = query_len {
            self.observe_histogram_with(
                "rush_search_query_length_chars",
                &signal_label,
                len as f64,
                &QUERY_LEN_BUCKETS,
            );
        }
        if result_rows == 0 {
            self.inc_counter("rush_search_empty_total", &signal_label, 1);
        }
    }

    /// Record a query operation with bounded `operation` and `signal` labels.
    ///
    /// These metrics complement the older signal-level `rush_search_*` series with
    /// enough detail to understand which product surface is driving load. Unknown
    /// operation names collapse into `other`, preserving a finite label set even if a
    /// future caller accidentally passes an unbounded value.
    pub fn record_query(
        &self,
        operation: &'static str,
        signal: &'static str,
        result_rows: u64,
        duration_ms: u64,
        ok: bool,
    ) {
        let operation = bounded_query_operation(operation);
        let signal = bounded_query_signal(signal);
        let outcome = if ok { "ok" } else { "error" };
        let labels = [("operation", operation), ("signal", signal)];
        self.inc_counter(
            "rush_query_requests_total",
            &[
                ("operation", operation),
                ("signal", signal),
                ("outcome", outcome),
            ],
            1,
        );
        self.observe_histogram_with(
            "rush_query_duration_ms",
            &labels,
            duration_ms as f64,
            &SEARCH_LATENCY_BUCKETS_MS,
        );
        self.observe_histogram_with(
            "rush_query_result_rows",
            &labels,
            result_rows as f64,
            &RESULT_COUNT_BUCKETS,
        );
        if result_rows == 0 {
            self.inc_counter("rush_query_empty_total", &labels, 1);
        }
    }

    /// Record both the existing signal-level search metrics and the newer operation-level
    /// metrics for a query handler. Keeping this helper preserves the existing dashboards
    /// while making operation attribution consistent across success and error paths.
    pub fn record_query_and_search(
        &self,
        operation: &'static str,
        signal: &'static str,
        query_len: Option<usize>,
        result_rows: u64,
        duration_ms: u64,
        ok: bool,
    ) {
        self.record_search(signal, query_len, result_rows, duration_ms, ok);
        self.record_query(operation, signal, result_rows, duration_ms, ok);
    }

    /// Record one of the two fixed ClickHouse stages used by the Explore
    /// coordinator. Both labels are normalized to finite allowlists.
    pub fn record_explore_stage(
        &self,
        signal: &'static str,
        stage: &'static str,
        result_rows: u64,
        duration_ms: u64,
        ok: bool,
    ) {
        let signal = bounded_query_signal(signal);
        let stage = match stage {
            "rows" | "summary" => stage,
            _ => "other",
        };
        let outcome = if ok { "ok" } else { "error" };
        self.inc_counter(
            "rush_explore_clickhouse_queries_total",
            &[("outcome", outcome), ("signal", signal), ("stage", stage)],
            1,
        );
        self.observe_histogram_with(
            "rush_explore_clickhouse_query_duration_ms",
            &[("signal", signal), ("stage", stage)],
            duration_ms as f64,
            &SEARCH_LATENCY_BUCKETS_MS,
        );
        self.observe_histogram_with(
            "rush_explore_clickhouse_result_rows",
            &[("signal", signal), ("stage", stage)],
            result_rows as f64,
            &RESULT_COUNT_BUCKETS,
        );
    }

    /// Record the request-level outcomes users perceive from the coordinated
    /// Explore endpoint. No tenant, query text, field, or other unbounded label is
    /// admitted. `time_to_first_results_ms` is the row-stage readiness time, not
    /// summary completion. `matched_logical_bytes` is derived during the summary
    /// scan; physical ClickHouse bytes read remain observable in `system.query_log`
    /// by query ID.
    pub fn record_explore_coordinator(
        &self,
        signal: &'static str,
        clickhouse_queries: u64,
        matched_rows: u64,
        matched_logical_bytes: u64,
        time_to_first_results_ms: u64,
    ) {
        let labels = [("signal", bounded_query_signal(signal))];
        self.observe_histogram_with(
            "rush_explore_clickhouse_queries",
            &labels,
            clickhouse_queries as f64,
            &RESULT_COUNT_BUCKETS,
        );
        self.observe_histogram_with(
            "rush_explore_matched_rows",
            &labels,
            matched_rows as f64,
            &RESULT_COUNT_BUCKETS,
        );
        self.observe_histogram_with(
            "rush_explore_matched_logical_bytes",
            &labels,
            matched_logical_bytes as f64,
            &EXPLORE_BYTES_BUCKETS,
        );
        self.observe_histogram_with(
            "rush_explore_time_to_first_results_ms",
            &labels,
            time_to_first_results_ms as f64,
            &SEARCH_LATENCY_BUCKETS_MS,
        );
    }

    pub fn record_explore_response_bytes(&self, signal: &'static str, response_bytes: u64) {
        self.observe_histogram_with(
            "rush_explore_response_bytes",
            &[("signal", bounded_query_signal(signal))],
            response_bytes as f64,
            &EXPLORE_BYTES_BUCKETS,
        );
    }

    // ── Output 1: Prometheus text exposition (0.0.4) ──────────────────────────────

    /// Render the full registry as Prometheus text exposition format (version 0.0.4).
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Counters, grouped by metric name so each gets a single `# TYPE` header.
        let mut counter_names: Vec<&'static str> =
            self.counters.iter().map(|e| e.key().0).collect();
        counter_names.sort_unstable();
        counter_names.dedup();
        for name in counter_names {
            out.push_str(&format!("# TYPE {name} counter\n"));
            let mut rows: Vec<(Labels, u64)> = self
                .counters
                .iter()
                .filter(|e| e.key().0 == name)
                .map(|e| (e.key().1.clone(), e.value().load(Ordering::Relaxed)))
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            for (labels, v) in rows {
                out.push_str(name);
                out.push_str(&render_labels(&labels));
                out.push_str(&format!(" {v}\n"));
            }
        }

        // Gauges.
        let mut gauge_names: Vec<&'static str> = self.gauges.iter().map(|e| e.key().0).collect();
        gauge_names.sort_unstable();
        gauge_names.dedup();
        for name in gauge_names {
            out.push_str(&format!("# TYPE {name} gauge\n"));
            let mut rows: Vec<(Labels, f64)> = self
                .gauges
                .iter()
                .filter(|e| e.key().0 == name)
                .map(|e| (e.key().1.clone(), e.value().get()))
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            for (labels, v) in rows {
                out.push_str(name);
                out.push_str(&render_labels(&labels));
                out.push_str(&format!(" {}\n", fmt_f64(v)));
            }
        }

        // Histograms — cumulative `_bucket{le=...}` series plus `_sum` and `_count`.
        let mut hist_names: Vec<&'static str> = self.histograms.iter().map(|e| e.key().0).collect();
        hist_names.sort_unstable();
        hist_names.dedup();
        for name in hist_names {
            out.push_str(&format!("# TYPE {name} histogram\n"));
            // Each series carries its own bucket bounds, so snapshot the bounds alongside
            // the per-bucket counts rather than assuming a single global bucket set.
            let mut rows: Vec<(Labels, &'static [f64], (Vec<u64>, u64, f64))> = self
                .histograms
                .iter()
                .filter(|e| e.key().0 == name)
                .map(|e| (e.key().1.clone(), e.value().bounds, e.value().snapshot()))
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            for (labels, bounds, (per_bucket, count, sum)) in rows {
                let mut cumulative = 0u64;
                for (i, &ub) in bounds.iter().enumerate() {
                    cumulative += per_bucket[i];
                    out.push_str(name);
                    out.push_str("_bucket");
                    out.push_str(&render_labels_with_le(&labels, &fmt_f64(ub)));
                    out.push_str(&format!(" {cumulative}\n"));
                }
                // +Inf bucket = total count.
                out.push_str(name);
                out.push_str("_bucket");
                out.push_str(&render_labels_with_le(&labels, "+Inf"));
                out.push_str(&format!(" {count}\n"));
                // _sum and _count.
                out.push_str(name);
                out.push_str("_sum");
                out.push_str(&render_labels(&labels));
                out.push_str(&format!(" {}\n", fmt_f64(sum)));
                out.push_str(name);
                out.push_str("_count");
                out.push_str(&render_labels(&labels));
                out.push_str(&format!(" {count}\n"));
            }
        }

        out
    }

    // ── Output 2: self-ingest snapshot ────────────────────────────────────────────

    /// Snapshot every series as flat [`MetricPoint`]s for self-ingest into ClickHouse.
    /// Counters → `Sum`, gauges → `Gauge`. Histograms are flattened to `_count` (sum),
    /// `_sum` (sum), and `p50`/`p95`/`p99` gauges computed from buckets — raw buckets are
    /// intentionally not emitted (simpler for our PromQL layer).
    pub fn snapshot_series(&self) -> Vec<MetricPoint> {
        let mut points = Vec::new();

        for e in self.counters.iter() {
            points.push(MetricPoint {
                name: e.key().0.to_string(),
                labels: e.key().1.clone(),
                value: e.value().load(Ordering::Relaxed) as f64,
                kind: MetricKind::Sum,
            });
        }

        for e in self.gauges.iter() {
            points.push(MetricPoint {
                name: e.key().0.to_string(),
                labels: e.key().1.clone(),
                value: e.value().get(),
                kind: MetricKind::Gauge,
            });
        }

        for e in self.histograms.iter() {
            let name = e.key().0;
            let labels = e.key().1.clone();
            let bounds = e.value().bounds;
            let (per_bucket, count, sum) = e.value().snapshot();
            points.push(MetricPoint {
                name: format!("{name}_count"),
                labels: labels.clone(),
                value: count as f64,
                kind: MetricKind::Sum,
            });
            points.push(MetricPoint {
                name: format!("{name}_sum"),
                labels: labels.clone(),
                value: sum,
                kind: MetricKind::Sum,
            });
            for (suffix, q) in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)] {
                points.push(MetricPoint {
                    name: format!("{name}_{suffix}"),
                    labels: labels.clone(),
                    value: quantile_from_buckets(&per_bucket, bounds, count, q),
                    kind: MetricKind::Gauge,
                });
            }
        }

        points
    }
}

fn bounded_query_operation(operation: &'static str) -> &'static str {
    match operation {
        "explore_logs" | "explore_spans" | "promql_instant" | "promql_range" | "promql_series"
        | "promql_metadata" => operation,
        _ => "other",
    }
}

fn bounded_query_signal(signal: &'static str) -> &'static str {
    match signal {
        "logs" | "spans" | "metrics" => signal,
        _ => "other",
    }
}

fn bounded_auth_lookup(lookup: &'static str) -> &'static str {
    match lookup {
        "session"
        | "user"
        | "role"
        | "tenant_policy"
        | "tenant_ingest_policy"
        | "user_permissions"
        | "api_key_grant" => lookup,
        _ => "other",
    }
}

fn bounded_auth_outcome(outcome: &'static str) -> &'static str {
    match outcome {
        "ok" | "not_found" | "error" => outcome,
        _ => "error",
    }
}

fn bounded_auth_cache_outcome(outcome: &'static str) -> &'static str {
    match outcome {
        "hit" | "miss" | "stale_invalidation" => outcome,
        _ => "miss",
    }
}

/// Render a label set as Prometheus `{k="v",...}`, or empty string when no labels.
fn render_labels(labels: &Labels) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let inner: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    format!("{{{}}}", inner.join(","))
}

/// Render a label set plus an `le` bucket bound for histogram `_bucket` series.
fn render_labels_with_le(labels: &Labels, le: &str) -> String {
    let mut inner: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    inner.push(format!("le=\"{le}\""));
    format!("{{{}}}", inner.join(","))
}

/// Escape a label value per the Prometheus exposition spec: backslash, double-quote, newline.
fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Format an f64 for Prometheus output: integers without a trailing `.0`, otherwise the
/// shortest round-tripping representation. NaN/Inf are emitted per spec.
fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_and_gauge_render() {
        let m = SelfMetrics::new();
        m.inc_counter(
            "rush_http_requests_total",
            &[
                ("route", "/api/v1/query"),
                ("method", "POST"),
                ("status_class", "2xx"),
            ],
            3,
        );
        m.inc_counter(
            "rush_http_requests_total",
            &[
                ("route", "/api/v1/query"),
                ("method", "POST"),
                ("status_class", "2xx"),
            ],
            2,
        );
        m.set_gauge("rush_http_requests_in_flight", &[], 4.0);
        m.add_gauge("rush_http_requests_in_flight", &[], -1.0);

        let text = m.render_prometheus();
        // Counter aggregated to 5.
        assert!(text.contains("# TYPE rush_http_requests_total counter"));
        assert!(
            text.contains("rush_http_requests_total{method=\"POST\",route=\"/api/v1/query\",status_class=\"2xx\"} 5"),
            "counter render wrong:\n{text}"
        );
        // Gauge with no labels, 4 - 1 = 3.
        assert!(text.contains("# TYPE rush_http_requests_in_flight gauge"));
        assert!(
            text.contains("rush_http_requests_in_flight 3\n"),
            "gauge render wrong:\n{text}"
        );
    }

    #[test]
    fn query_guard_balances_operation_in_flight() {
        let metrics = Arc::new(SelfMetrics::new());
        let guard = metrics.query_guard("explore_logs", "logs");
        let during = metrics.render_prometheus();
        assert!(during.contains(
            "rush_query_requests_in_flight{operation=\"explore_logs\",signal=\"logs\"} 1"
        ));
        drop(guard);
        let after = metrics.render_prometheus();
        assert!(after.contains(
            "rush_query_requests_in_flight{operation=\"explore_logs\",signal=\"logs\"} 0"
        ));
    }

    #[test]
    fn histogram_render_is_valid_exposition() {
        let m = SelfMetrics::new();
        let labels = [("route", "/x"), ("method", "GET")];
        // Observations across several buckets.
        m.observe_histogram("rush_http_request_duration_ms", &labels, 3.0); // <=5
        m.observe_histogram("rush_http_request_duration_ms", &labels, 7.0); // <=10
        m.observe_histogram("rush_http_request_duration_ms", &labels, 40.0); // <=50
        m.observe_histogram("rush_http_request_duration_ms", &labels, 99999.0); // +Inf

        let text = m.render_prometheus();
        assert!(text.contains("# TYPE rush_http_request_duration_ms histogram"));
        // le="5" cumulative is 1; le="10" cumulative is 2; le="50" cumulative is 3.
        assert!(text.contains("le=\"5\"} 1"), "le=5 wrong:\n{text}");
        assert!(text.contains("le=\"10\"} 2"), "le=10 wrong:\n{text}");
        assert!(text.contains("le=\"50\"} 3"), "le=50 wrong:\n{text}");
        // +Inf bucket and _count both equal total observations (4).
        assert!(text.contains("le=\"+Inf\"} 4"), "+Inf wrong:\n{text}");
        assert!(
            text.contains("rush_http_request_duration_ms_count{method=\"GET\",route=\"/x\"} 4"),
            "count wrong:\n{text}"
        );
        // _sum = 3 + 7 + 40 + 99999 = 100049.
        assert!(
            text.contains("rush_http_request_duration_ms_sum{method=\"GET\",route=\"/x\"} 100049"),
            "sum wrong:\n{text}"
        );
        // Buckets must be monotonically non-decreasing (cumulative invariant).
        let counts = extract_bucket_counts(&text, "rush_http_request_duration_ms");
        for w in counts.windows(2) {
            assert!(w[1] >= w[0], "buckets not cumulative-monotonic: {counts:?}");
        }
    }

    fn extract_bucket_counts(text: &str, name: &str) -> Vec<u64> {
        let prefix = format!("{name}_bucket");
        text.lines()
            .filter(|l| l.starts_with(&prefix))
            .filter_map(|l| l.rsplit(' ').next().and_then(|n| n.parse::<u64>().ok()))
            .collect()
    }

    #[test]
    fn quantile_math() {
        // 10 observations all <= 5ms → p50/p95/p99 all interpolate within first bucket [0,5].
        let mut per_bucket = [0u64; LATENCY_BUCKETS_MS.len()];
        per_bucket[0] = 10;
        let total = 10u64;
        let p50 = quantile_from_buckets(&per_bucket, &LATENCY_BUCKETS_MS, total, 0.50);
        let p95 = quantile_from_buckets(&per_bucket, &LATENCY_BUCKETS_MS, total, 0.95);
        let p99 = quantile_from_buckets(&per_bucket, &LATENCY_BUCKETS_MS, total, 0.99);
        // rank for p50 = 5 → interpolated 5*(5/10)=2.5; p95 rank=9.5 → 5*(9.5/10)=4.75.
        assert!((p50 - 2.5).abs() < 1e-9, "p50={p50}");
        assert!((p95 - 4.75).abs() < 1e-9, "p95={p95}");
        assert!(p99 <= 5.0 && p99 > 4.75, "p99={p99}");

        // Empty histogram → all quantiles 0.
        let empty = [0u64; LATENCY_BUCKETS_MS.len()];
        assert_eq!(
            quantile_from_buckets(&empty, &LATENCY_BUCKETS_MS, 0, 0.99),
            0.0
        );

        // Spread across buckets: 5 in [0,5], 5 in (5,10]. p50 rank=5 lands at boundary of
        // first bucket → 5.0; p95 rank=9.5 → in second bucket: 5 + (10-5)*((9.5-5)/5)=9.5.
        let mut spread = [0u64; LATENCY_BUCKETS_MS.len()];
        spread[0] = 5;
        spread[1] = 5;
        let p50b = quantile_from_buckets(&spread, &LATENCY_BUCKETS_MS, 10, 0.50);
        let p95b = quantile_from_buckets(&spread, &LATENCY_BUCKETS_MS, 10, 0.95);
        assert!((p50b - 5.0).abs() < 1e-9, "p50b={p50b}");
        assert!((p95b - 9.5).abs() < 1e-9, "p95b={p95b}");
    }

    #[test]
    fn snapshot_series_flattens_histogram() {
        let m = SelfMetrics::new();
        m.inc_counter(
            "rush_ingest_events_total",
            &[("signal", "logs"), ("outcome", "accepted")],
            7,
        );
        m.set_gauge("rush_ingest_spool_bytes", &[], 1234.0);
        m.observe_histogram(
            "rush_engine_run_duration_ms",
            &[("engine", "stats_engine")],
            12.0,
        );

        let points = m.snapshot_series();
        // Counter present as Sum.
        assert!(points.iter().any(|p| p.name == "rush_ingest_events_total"
            && p.kind == MetricKind::Sum
            && (p.value - 7.0).abs() < 1e-9));
        // Gauge present.
        assert!(points.iter().any(|p| p.name == "rush_ingest_spool_bytes"
            && p.kind == MetricKind::Gauge
            && (p.value - 1234.0).abs() < 1e-9));
        // Histogram flattened to _count, _sum, and p-quantile gauges (no raw _bucket).
        assert!(points.iter().any(|p| p.name == "rush_engine_run_duration_ms_count" && p.kind == MetricKind::Sum));
        assert!(
            points
                .iter()
                .any(|p| p.name == "rush_engine_run_duration_ms_sum" && p.kind == MetricKind::Sum)
        );
        assert!(points.iter().any(|p| p.name == "rush_engine_run_duration_ms_p99" && p.kind == MetricKind::Gauge));
        assert!(!points.iter().any(|p| p.name.contains("_bucket")));
    }

    #[test]
    fn custom_bucket_histogram_renders_with_its_own_bounds() {
        let m = SelfMetrics::new();
        let labels = [("signal", "logs")];
        // RESULT_COUNT_BUCKETS = [0,1,5,10,50,100,500,1000,5000,10000].
        m.observe_histogram_with(
            "rush_search_result_rows",
            &labels,
            0.0,
            &RESULT_COUNT_BUCKETS,
        ); // <=0
        m.observe_histogram_with(
            "rush_search_result_rows",
            &labels,
            3.0,
            &RESULT_COUNT_BUCKETS,
        ); // <=5
        m.observe_histogram_with(
            "rush_search_result_rows",
            &labels,
            7.0,
            &RESULT_COUNT_BUCKETS,
        ); // <=10
        m.observe_histogram_with(
            "rush_search_result_rows",
            &labels,
            99999.0,
            &RESULT_COUNT_BUCKETS,
        ); // +Inf

        let text = m.render_prometheus();
        assert!(text.contains("# TYPE rush_search_result_rows histogram"));
        // Bounds come from RESULT_COUNT_BUCKETS, NOT the default ms set: le="0",le="5",le="10".
        assert!(text.contains("le=\"0\"} 1"), "le=0 wrong:\n{text}");
        assert!(text.contains("le=\"5\"} 2"), "le=5 wrong:\n{text}");
        assert!(text.contains("le=\"10\"} 3"), "le=10 wrong:\n{text}");
        // +Inf and _count = 4 total; _sum = 0+3+7+99999 = 100009.
        assert!(text.contains("le=\"+Inf\"} 4"), "+Inf wrong:\n{text}");
        assert!(
            text.contains("rush_search_result_rows_count{signal=\"logs\"} 4"),
            "count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_result_rows_sum{signal=\"logs\"} 100009"),
            "sum wrong:\n{text}"
        );
        // The default ms bucket bound (5000) is NOT emitted as a separate le from this set;
        // confirm the highest finite bound is the custom one (10000), not 10000-from-latency.
        assert!(text.contains("le=\"10000\"} 3"), "le=10000 wrong:\n{text}");

        // Snapshot uses the same custom bounds for quantiles (no panic / no wrong scale).
        let points = m.snapshot_series();
        assert!(
            points
                .iter()
                .any(|p| p.name == "rush_search_result_rows_count" && (p.value - 4.0).abs() < 1e-9)
        );
        let p99 = points
            .iter()
            .find(|p| p.name == "rush_search_result_rows_p99")
            .unwrap();
        // p99 of {0,3,7,large} should land within the bucket bounds (max finite bound 10000).
        assert!(
            p99.value <= 10000.0 && p99.value >= 0.0,
            "p99={}",
            p99.value
        );
    }

    #[test]
    fn record_search_emits_all_series_with_bounded_labels() {
        let m = SelfMetrics::new();
        // A successful logs search with a 4-char term returning 3 rows in 120ms.
        m.record_search("logs", Some(4), 3, 120, true);
        // A successful logs browse (no term) returning 0 rows in 50ms → empty + no length hist.
        m.record_search("logs", None, 0, 50, true);
        // A failed spans search.
        m.record_search("spans", Some(10), 0, 999, false);

        let text = m.render_prometheus();

        // Counter with signal+outcome, low cardinality.
        assert!(
            text.contains("rush_search_queries_total{outcome=\"ok\",signal=\"logs\"} 2"),
            "queries ok wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_queries_total{outcome=\"error\",signal=\"spans\"} 1"),
            "queries err wrong:\n{text}"
        );

        // Empty counter: the 0-row browse (logs) and the 0-row failed spans search.
        assert!(
            text.contains("rush_search_empty_total{signal=\"logs\"} 1"),
            "empty logs wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_empty_total{signal=\"spans\"} 1"),
            "empty spans wrong:\n{text}"
        );

        // Duration histogram uses SEARCH_LATENCY_BUCKETS_MS (le="100" exists, not the ms set's 250-cap).
        assert!(text.contains("# TYPE rush_search_duration_ms histogram"));
        // logs: two observations (120ms, 50ms) → _count 2, _sum 170.
        assert!(
            text.contains("rush_search_duration_ms_count{signal=\"logs\"} 2"),
            "dur count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_duration_ms_sum{signal=\"logs\"} 170"),
            "dur sum wrong:\n{text}"
        );
        // SEARCH_LATENCY_BUCKETS_MS starts at 10 — confirm that bound exists.
        assert!(
            text.contains("rush_search_duration_ms_bucket{signal=\"logs\",le=\"10\"}"),
            "search latency bounds wrong:\n{text}"
        );

        // Result-rows histogram: logs got 3 and 0 → _count 2, _sum 3.
        assert!(
            text.contains("rush_search_result_rows_count{signal=\"logs\"} 2"),
            "rows count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_result_rows_sum{signal=\"logs\"} 3"),
            "rows sum wrong:\n{text}"
        );

        // Query-length histogram: ONLY the two searches with a term (logs len=4) recorded
        // for logs → _count 1; the browse (None) was skipped.
        assert!(
            text.contains("rush_search_query_length_chars_count{signal=\"logs\"} 1"),
            "qlen count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_query_length_chars_sum{signal=\"logs\"} 4"),
            "qlen sum wrong:\n{text}"
        );
        // spans length histogram recorded once (len=10).
        assert!(
            text.contains("rush_search_query_length_chars_count{signal=\"spans\"} 1"),
            "qlen spans count wrong:\n{text}"
        );

        // No high-cardinality labels leaked (no tenant/route/query labels).
        assert!(!text.contains("tenant"), "tenant label leaked:\n{text}");
        assert!(!text.contains("route="), "route label leaked:\n{text}");
    }

    #[test]
    fn record_search_metrics_signal_emits_bounded_series() {
        let m = SelfMetrics::new();
        // PromQL instant query: 8-char expr returning 2 series in 30ms.
        m.record_search("metrics", Some(8), 2, 30, true);
        // PromQL range query that returned 0 series (empty) in 12ms.
        m.record_search("metrics", Some(15), 0, 12, true);
        // A failed PromQL query (parse/eval error) → counted as error, empty.
        m.record_search("metrics", Some(5), 0, 7, false);

        let text = m.render_prometheus();

        // signal="metrics" appears on the counter with the same outcome dimension as logs/spans.
        assert!(
            text.contains("rush_search_queries_total{outcome=\"ok\",signal=\"metrics\"} 2"),
            "metrics ok wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_queries_total{outcome=\"error\",signal=\"metrics\"} 1"),
            "metrics err wrong:\n{text}"
        );
        // Empty counter: the 0-series range query + the failed query.
        assert!(
            text.contains("rush_search_empty_total{signal=\"metrics\"} 2"),
            "metrics empty wrong:\n{text}"
        );
        // Duration histogram: three observations (30+12+7) → _count 3, _sum 49.
        assert!(
            text.contains("rush_search_duration_ms_count{signal=\"metrics\"} 3"),
            "metrics dur count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_duration_ms_sum{signal=\"metrics\"} 49"),
            "metrics dur sum wrong:\n{text}"
        );
        // Result-rows histogram: series counts 2,0,0 → _count 3, _sum 2.
        assert!(
            text.contains("rush_search_result_rows_count{signal=\"metrics\"} 3"),
            "metrics rows count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_result_rows_sum{signal=\"metrics\"} 2"),
            "metrics rows sum wrong:\n{text}"
        );
        // Query-length histogram: all three had a query_len → _count 3, _sum 28 (8+15+5).
        assert!(
            text.contains("rush_search_query_length_chars_count{signal=\"metrics\"} 3"),
            "metrics qlen count wrong:\n{text}"
        );
        assert!(
            text.contains("rush_search_query_length_chars_sum{signal=\"metrics\"} 28"),
            "metrics qlen sum wrong:\n{text}"
        );

        // Cardinality stays bounded: signal is the only added dimension; no tenant/route/query labels.
        assert!(!text.contains("tenant"), "tenant label leaked:\n{text}");
        assert!(!text.contains("route="), "route label leaked:\n{text}");
        assert!(!text.contains("promql"), "query text leaked:\n{text}");
    }

    #[test]
    fn record_query_emits_operation_metrics_with_allowlisted_labels() {
        let m = SelfMetrics::new();
        m.record_query("explore_logs", "logs", 4, 25, true);
        m.record_query("untrusted-operation", "untrusted-signal", 0, 100, false);

        let text = m.render_prometheus();
        assert!(text.contains("# TYPE rush_query_requests_total counter"));
        assert!(text.contains(
            "rush_query_requests_total{operation=\"explore_logs\",outcome=\"ok\",signal=\"logs\"} 1"
        ));
        assert!(text.contains(
            "rush_query_requests_total{operation=\"other\",outcome=\"error\",signal=\"other\"} 1"
        ));
        assert!(text.contains(
            "rush_query_duration_ms_count{operation=\"explore_logs\",signal=\"logs\"} 1"
        ));
        assert!(text.contains("rush_query_empty_total{operation=\"other\",signal=\"other\"} 1"));
        assert!(!text.contains("untrusted-operation"));
        assert!(!text.contains("untrusted-signal"));
    }

    #[test]
    fn explore_coordinator_metrics_have_only_bounded_labels() {
        let metrics = SelfMetrics::new();
        metrics.record_explore_stage("untrusted-signal", "untrusted-stage", 12, 30, false);
        metrics.record_explore_coordinator("logs", 2, 42, 4096, 25);
        metrics.record_explore_response_bytes("logs", 8192);

        let text = metrics.render_prometheus();
        assert!(text.contains("rush_explore_clickhouse_queries_total{outcome=\"error\",signal=\"other\",stage=\"other\"} 1"));
        assert!(text.contains("rush_explore_clickhouse_queries_count{signal=\"logs\"} 1"));
        assert!(text.contains("rush_explore_matched_logical_bytes_count{signal=\"logs\"} 1"));
        assert!(text.contains("rush_explore_time_to_first_results_ms_count{signal=\"logs\"} 1"));
        assert!(text.contains("rush_explore_response_bytes_count{signal=\"logs\"} 1"));
        assert!(!text.contains("untrusted-signal"));
        assert!(!text.contains("untrusted-stage"));
    }

    #[test]
    fn authorization_metrics_emit_quantiles_rows_and_bounded_cache_labels() {
        let metrics = SelfMetrics::new();
        metrics.record_auth_lookup("session", 2.5, 1, "ok");
        metrics.record_auth_lookup("attacker-controlled", 8.0, 0, "unexpected");
        metrics.record_auth_cache("session", "hit");
        metrics.record_auth_cache("attacker-controlled", "unexpected");

        let text = metrics.render_prometheus();
        assert!(text.contains("rush_auth_lookups_total{lookup=\"session\",outcome=\"ok\"} 1"));
        assert!(text.contains("rush_auth_lookup_duration_ms_count{lookup=\"session\"} 1"));
        assert!(text.contains("rush_auth_lookup_result_rows_sum{lookup=\"session\"} 1"));
        assert!(text.contains("rush_auth_cache_total{lookup=\"session\",outcome=\"hit\"} 1"));
        assert!(text.contains("rush_auth_cache_total{lookup=\"other\",outcome=\"miss\"} 1"));
        assert!(!text.contains("attacker-controlled"));
        assert!(!text.contains("unexpected"));

        let snapshot = metrics.snapshot_series();
        for quantile in ["p50", "p95", "p99"] {
            assert!(snapshot.iter().any(|point| {
                point.name == format!("rush_auth_lookup_duration_ms_{quantile}")
                    && point
                        .labels
                        .iter()
                        .any(|(key, value)| *key == "lookup" && value == "session")
            }));
        }
    }
}
