//! RUM summaries stored as aggregate projections on the source table. Complete
//! buckets use mergeable states; partial buckets retain raw bounds.

use crate::timestamps::{TimestampExt, parse_rfc3339};
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use clickhouse::{Client, query::Query};
use sha2::{Digest, Sha256};

use crate::handlers::rum::{
    RumQueryRequest, build_rum_where, resolve_rum_field, rum_filter_condition,
};
use crate::models::query::{Filter, FilterOp};

mod storage;
pub use storage::spawn_maintenance;

#[cfg(test)]
mod tests;

const MIN_ROLLUP_SPAN_SECS: i64 = 6 * 3600;
const PAGE_STATES: &str = "countIfState(EventType = 'pageview') AS views_state, \
    uniqExactState(SessionId) AS sessions_state, \
    avgIfState(DurationMs, DurationMs > 0) AS load_state, \
    countIfState(EventType = 'error') AS errors_state, countState() AS total_state";
const VITAL_STATES: &str = "quantileState(0.75)(VitalValue) AS vital_state, \
    countIfState(VitalRating = 'good') AS good_state, \
    countIfState(VitalRating = 'needs-improvement') AS needs_state, \
    countIfState(VitalRating = 'poor') AS poor_state, countState() AS total_state";

#[derive(Clone, Copy, Debug)]
pub(crate) enum Kind {
    Pages,
    Vitals,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Pages => "agg_rum_pages_5m_g256",
            Self::Vitals => "agg_rum_vitals_1h_g16",
        }
    }

    fn projection_declaration(self) -> String {
        // Aggregate rows can contain large session sets or reservoirs. Small
        // projection granules avoid decoding unrelated apps/tenants/event types.
        let granularity = match self {
            Self::Pages => 256,
            Self::Vitals => 16,
        };
        format!(
            "{} ({}) WITH SETTINGS (index_granularity = {granularity})",
            self.name(),
            self.projection_select()
        )
    }

    fn bucket(self) -> (&'static str, i64) {
        match self {
            Self::Pages => (
                "toStartOfInterval(Timestamp, toIntervalMinute(5), 'UTC')",
                300,
            ),
            Self::Vitals => (
                "toStartOfInterval(Timestamp, toIntervalMinute(60), 'UTC')",
                3600,
            ),
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::Pages => "PagePath",
            Self::Vitals => "VitalName",
        }
    }

    fn states(self) -> &'static str {
        match self {
            Self::Pages => PAGE_STATES,
            Self::Vitals => VITAL_STATES,
        }
    }

    fn projection_select(self) -> String {
        let (bucket, _) = self.bucket();
        let dimensions = self.dimensions();
        format!(
            "SELECT {dimensions}, {bucket} AS rollup_bucket, {} \
             GROUP BY {dimensions}, rollup_bucket",
            self.states()
        )
    }

    fn dimensions(self) -> &'static str {
        match self {
            Self::Pages => "tenant_id, AppName, Environment, PagePath",
            Self::Vitals => "tenant_id, AppName, Environment, EventType, VitalName",
        }
    }

    fn supports_filter(self, filter: &Filter) -> bool {
        let field = resolve_rum_field(&filter.field);
        matches!(field.as_str(), "AppName" | "Environment")
            || match self {
                Self::Pages => field == "PagePath",
                Self::Vitals => matches!(field.as_str(), "EventType" | "VitalName"),
            }
    }
}

/// Readiness is scoped to this API's ClickHouse connection. Until background
/// materialization finishes, handlers retain their original raw queries.
#[derive(Default)]
pub struct RumRollups {
    pages: AtomicBool,
    vitals: AtomicBool,
    // Only an execution hint is cached. Every response still reads current data.
    density_hints: Mutex<HashMap<[u8; 32], (bool, Instant)>>,
}

impl RumRollups {
    pub(crate) async fn fetch_all<T: clickhouse::RowOwned + clickhouse::RowRead>(
        &self,
        kind: Kind,
        req: &RumQueryRequest,
        tenant: &str,
        client: &Client,
    ) -> clickhouse::error::Result<Vec<T>> {
        let plan = self.select(kind, req, tenant, client).await;
        match plan.query(client, tenant).fetch_all::<T>().await {
            Err(error) if plan.uses_rollup => {
                tracing::warn!(%error, projection = kind.name(), "RUM rollup read failed; retrying raw query");
                // Larger aggregate states can hit a query memory limit even
                // when the raw query fits. Suppress this plan for its hint TTL.
                let (from, to) = full_bucket_window(req, kind).expect("validated RUM window");
                let filters = filters(kind, req);
                let sql = density_sql(kind, tenant, &filters, from, to);
                self.remember_hint(Sha256::digest(sql.as_bytes()).into(), false);
                Plan {
                    sql: raw_sql(kind, req, tenant, &filters),
                    uses_rollup: false,
                    projection: kind.name(),
                }
                .query(client, tenant)
                .fetch_all::<T>()
                .await
            }
            result => result,
        }
    }

    fn remember_hint(&self, key: [u8; 32], dense: bool) {
        if let Ok(mut cache) = self.density_hints.lock() {
            if cache.len() >= 256 {
                cache.retain(|_, entry| entry.1.elapsed() < Duration::from_secs(300));
                if cache.len() >= 256 {
                    if let Some(oldest) = cache
                        .iter()
                        .min_by_key(|(_, entry)| entry.1)
                        .map(|(key, _)| *key)
                    {
                        cache.remove(&oldest);
                    }
                }
            }
            cache.insert(key, (dense, Instant::now()));
        }
    }

    pub(crate) async fn select(
        &self,
        kind: Kind,
        req: &RumQueryRequest,
        tenant: &str,
        client: &Client,
    ) -> Plan {
        let candidate = self.plan(kind, req, tenant);
        if !candidate.uses_rollup {
            return candidate;
        }
        let (from, to) = full_bucket_window(req, kind).expect("validated RUM window");
        let filters = filters(kind, req);
        let sql = density_sql(kind, tenant, &filters, from, to);
        let key: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
        let ttl = Duration::from_secs(300);
        let cached = {
            self.density_hints.lock().ok().and_then(|cache| {
                cache
                    .get(&key)
                    .filter(|entry| entry.1.elapsed() < ttl)
                    .map(|entry| entry.0)
            })
        };
        let dense = match cached {
            Some(dense) => dense,
            None => {
                let probe = Plan {
                    sql,
                    uses_rollup: true,
                    projection: kind.name(),
                };
                let dense = match tokio::time::timeout(
                    Duration::from_millis(250),
                    probe.query(client, tenant).fetch_one::<u8>(),
                )
                .await
                {
                    Ok(Ok(dense)) => dense != 0,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "RUM density check failed; retaining raw reads");
                        false
                    }
                    Err(_) => {
                        tracing::warn!("RUM density check timed out; retaining raw reads");
                        false
                    }
                };
                self.remember_hint(key, dense);
                dense
            }
        };
        if dense {
            candidate
        } else {
            Plan {
                sql: raw_sql(kind, req, tenant, &filters),
                uses_rollup: false,
                projection: kind.name(),
            }
        }
    }

    fn flag(&self, kind: Kind) -> &AtomicBool {
        match kind {
            Kind::Pages => &self.pages,
            Kind::Vitals => &self.vitals,
        }
    }

    pub(crate) fn plan(&self, kind: Kind, req: &RumQueryRequest, tenant: &str) -> Plan {
        let filters = filters(kind, req);
        let full_buckets = self
            .flag(kind)
            .load(Ordering::Acquire)
            .then(|| full_bucket_window(req, kind))
            .flatten()
            .filter(|_| filters.iter().all(|filter| kind.supports_filter(filter)));
        match full_buckets {
            Some((from, to)) => Plan {
                sql: rollup_sql(kind, req, tenant, &filters, from, to),
                uses_rollup: true,
                projection: kind.name(),
            },
            None => Plan {
                sql: raw_sql(kind, req, tenant, &filters),
                uses_rollup: false,
                projection: kind.name(),
            },
        }
    }
}

pub(crate) struct Plan {
    pub(crate) sql: String,
    pub(crate) uses_rollup: bool,
    projection: &'static str,
}

impl Plan {
    pub(crate) fn query(&self, client: &Client, tenant: &str) -> Query {
        let query = crate::tenant_query(client, &self.sql, tenant);
        if self.uses_rollup {
            // Aggregate-state rows have a different cost than raw rows. Prefer
            // this measured plan; unlike force_optimize_projection, this hint
            // still permits raw edge reads and fallback for unbuilt parts.
            query
                .with_setting("preferred_optimize_projection_name", self.projection)
                .with_setting("optimize_aggregation_in_order", "1")
        } else {
            // Unsupported raw reads should not pay for projection planning.
            query
                .with_setting("optimize_use_projections", "0")
                .with_setting("optimize_use_projection_filtering", "0")
        }
    }
}

fn filters(kind: Kind, req: &RumQueryRequest) -> Vec<Filter> {
    let mut filters = req.filters.clone();
    if matches!(kind, Kind::Vitals) {
        filters.push(Filter {
            field: "EventType".into(),
            op: FilterOp::Eq,
            value: serde_json::json!("web_vital"),
        });
    }
    filters
}

fn full_bucket_window(req: &RumQueryRequest, kind: Kind) -> Option<(i64, i64)> {
    // Existing RUM predicates use parseDateTimeBestEffort, which has second
    // precision. Match those effective bounds, including the inclusive end.
    let from = parse_rfc3339(&req.time_range.from).ok()?.unix_timestamp();
    let to = parse_rfc3339(&req.time_range.to).ok()?.unix_timestamp();
    let minimum_span = match kind {
        Kind::Pages => MIN_ROLLUP_SPAN_SECS,
        Kind::Vitals => 24 * 3600,
    };
    if from < 0 || to > u32::MAX as i64 || to - from < minimum_span {
        return None;
    }
    let (_, bucket_secs) = kind.bucket();
    let start = (from + bucket_secs - 1) / bucket_secs * bucket_secs;
    let end = to / bucket_secs * bucket_secs;
    (start < end).then_some((start, end))
}

fn raw_sql(kind: Kind, req: &RumQueryRequest, tenant: &str, filters: &[Filter]) -> String {
    let clauses = build_rum_where(filters, &req.time_range.from, &req.time_range.to, tenant);
    match kind {
        Kind::Pages => format!(
            "SELECT PagePath, countIf(EventType = 'pageview') AS views, \
             uniqExact(SessionId) AS unique_sessions, \
             avgIf(DurationMs, DurationMs > 0) AS avg_load_ms, \
             countIf(EventType = 'error') AS error_count FROM rum {} \
             GROUP BY PagePath ORDER BY views DESC LIMIT {}",
            clauses.with_where_extra("PagePath != ''").to_sql(),
            req.limit.min(100),
        ),
        Kind::Vitals => format!(
            "SELECT VitalName, quantile(0.75)(VitalValue) AS p75, \
             countIf(VitalRating = 'good') * 100.0 / count() AS good_pct, \
             countIf(VitalRating = 'needs-improvement') * 100.0 / count() AS needs_improvement_pct, \
             countIf(VitalRating = 'poor') * 100.0 / count() AS poor_pct \
             FROM rum {} GROUP BY VitalName ORDER BY VitalName",
            clauses.to_sql(),
        ),
    }
}

fn rollup_sql(
    kind: Kind,
    req: &RumQueryRequest,
    tenant: &str,
    filters: &[Filter],
    from: i64,
    to: i64,
) -> String {
    let (bucket, _) = kind.bucket();
    let conditions = interior_where(kind, tenant, filters, from, to);
    let key = kind.key();
    let states = kind.states();
    // Keep bucket states separate until the outer merge. Collapsing them here
    // would build whole-window session sets and percentile reservoirs twice.
    let interior = format!(
        "SELECT {key}, {states} FROM rum WHERE {} GROUP BY {}, {bucket}",
        conditions,
        kind.dimensions(),
    );
    let start = time::OffsetDateTime::from_unix_timestamp(from)
        .ok()
        .expect("validated RUM bucket timestamp")
        .to_rfc3339();
    let end = time::OffsetDateTime::from_unix_timestamp(to)
        .ok()
        .expect("validated RUM bucket timestamp")
        .to_rfc3339();
    // Separate PREWHERE ranges are essential: an OR over Timestamp with a
    // whole-window TimestampTime scope would scan the entire raw window.
    let mut head = build_rum_where(filters, &req.time_range.from, &start, tenant)
        .with_where_extra(&format!("Timestamp < toDateTime({from}, 'UTC')"));
    let mut tail = build_rum_where(filters, &end, &req.time_range.to, tenant);
    if matches!(kind, Kind::Pages) {
        head = head.with_where_extra("PagePath != ''");
        tail = tail.with_where_extra("PagePath != ''");
    }
    let head = format!(
        "SELECT {key}, {states} FROM rum {} GROUP BY {key}",
        head.to_sql()
    );
    let tail = format!(
        "SELECT {key}, {states} FROM rum {} GROUP BY {key}",
        tail.to_sql()
    );
    let merged = match kind {
        Kind::Pages => {
            "PagePath, countIfMerge(views_state) AS views, \
            uniqExactMerge(sessions_state) AS unique_sessions, \
            avgIfMerge(load_state) AS avg_load_ms, countIfMerge(errors_state) AS error_count"
        }
        Kind::Vitals => {
            "VitalName, quantileMerge(0.75)(vital_state) AS p75, \
            countIfMerge(good_state) * 100.0 / countMerge(total_state) AS good_pct, \
            countIfMerge(needs_state) * 100.0 / countMerge(total_state) AS needs_improvement_pct, \
            countIfMerge(poor_state) * 100.0 / countMerge(total_state) AS poor_pct"
        }
    };
    let order = match kind {
        Kind::Pages => format!("ORDER BY views DESC LIMIT {}", req.limit.min(100)),
        Kind::Vitals => "ORDER BY VitalName".into(),
    };
    format!(
        "SELECT {merged} FROM ({interior} UNION ALL {head} UNION ALL {tail}) GROUP BY {key} {order}"
    )
}

fn interior_where(kind: Kind, tenant: &str, filters: &[Filter], from: i64, to: i64) -> String {
    let (bucket, _) = kind.bucket();
    let escaped_tenant = crate::query_builder::escape_string_literal(tenant);
    let mut conditions = vec![
        format!("tenant_id = '{escaped_tenant}'"),
        format!("{bucket} >= toDateTime({from}, 'UTC')"),
        format!("{bucket} < toDateTime({to}, 'UTC')"),
    ];
    conditions.extend(filters.iter().map(rum_filter_condition));
    if matches!(kind, Kind::Pages) {
        conditions.push("PagePath != ''".into());
    }
    conditions.join(" AND ")
}

fn density_sql(kind: Kind, tenant: &str, filters: &[Filter], from: i64, to: i64) -> String {
    let (bucket, _) = kind.bucket();
    // quantile() retains up to 8192 samples per state. Pages also need enough
    // aggregation to justify the three branches and smaller projection granules.
    let (minimum_events, per_bucket) = match kind {
        Kind::Pages => (500_000, 64),
        Kind::Vitals => (0, 4 * 8192),
    };
    format!(
        "SELECT toUInt8(count() > 0 AND sum(samples) >= {minimum_events} AND sum(samples) >= {per_bucket} * count()) \
         FROM (SELECT finalizeAggregation(countState()) AS samples FROM rum \
               WHERE {} GROUP BY {}, {bucket})",
        interior_where(kind, tenant, filters, from, to),
        kind.dimensions(),
    )
}
