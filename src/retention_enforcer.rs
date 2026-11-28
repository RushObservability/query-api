use clickhouse::Client;
use std::sync::Arc;
use std::time::Duration;

use crate::clickhouse_config::ConfigDb;
use crate::config::{MetricRetentionRule, RushConfig, TraceRetentionRule};
use crate::query_builder::escape_string_literal as esc;

/// Spawn the retention enforcer as a background task (fire-and-forget).
/// Follows the same pattern as `alert_engine::spawn_alert_engine`.
pub fn spawn_retention_enforcer(
    ch: Client,
    config: RushConfig,
    config_db: Arc<ConfigDb>,
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
    lease: Arc<crate::leader::EngineLease>,
) {
    if !config.retention.enforcer.enabled {
        tracing::info!(
            engine = "retention",
            "retention enforcer disabled by config"
        );
        return;
    }

    let interval_secs = config.retention.enforcer.interval_secs;
    let dry_run = config.retention.enforcer.dry_run;

    tokio::spawn(async move {
        // Wait 60s on startup to let tables settle
        tokio::time::sleep(Duration::from_secs(60)).await;
        tracing::info!(
            engine = "retention",
            interval_secs = interval_secs,
            dry_run = dry_run,
            "retention enforcer started"
        );

        let mut tenant_sql_done: Vec<String> = Vec::new();
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if !lease.is_leader() {
                continue;
            }
            let start = std::time::Instant::now();
            let ok = crate::leader::fenced(lease.clone(), async {
                let mut ok = true;
                // Apply table-level TTLs from the UI-editable global-retention store
                // (the source of truth; rush.toml only seeds it and was applied at boot).
                if !dry_run && crate::leader::ensure_leader().is_ok() {
                    if let Err(e) = apply_global_retention_ttls(&ch, &config_db).await {
                        tracing::error!(error = %e, engine = "retention", "applying global retention TTLs failed");
                        ok = false;
                    }
                }
                if let Err(e) = enforce_retention(&ch, &config).await {
                    tracing::error!(error = %e, engine = "retention", "global retention enforcement failed");
                    ok = false;
                }
                if let Err(e) =
                    enforce_tenant_retention(&ch, &config, &config_db, &mut tenant_sql_done).await
                {
                    tracing::error!(error = %e, engine = "retention", "tenant retention enforcement failed");
                    ok = false;
                }
                ok
            })
            .await;
            self_metrics.record_engine(
                "retention_enforcer",
                start.elapsed().as_millis() as u64,
                ok,
            );
        }
    });
}

async fn enforce_retention(ch: &Client, config: &RushConfig) -> anyhow::Result<()> {
    let table_metrics_ttl = config.effective_metrics_ttl_days();
    let table_traces_ttl = config.effective_traces_ttl_days();
    let dry_run = config.retention.enforcer.dry_run;

    // ── Metric rules ──
    // OR all rules' predicates together so each metric table gets ONE mutation
    // per tick instead of one per rule (mutations are heavyweight in ClickHouse
    // and queue serially).
    let metric_preds: Vec<String> = config
        .retention
        .metrics
        .iter()
        .filter(|rule| rule.retain_days < table_metrics_ttl)
        .filter_map(|rule| {
            let where_clause = build_metric_where(rule);
            if where_clause.is_empty() {
                None
            } else {
                Some(format!(
                    "(toDateTime(TimeUnix) < now() - INTERVAL {} DAY AND {where_clause})",
                    rule.retain_days
                ))
            }
        })
        .collect();
    if !metric_preds.is_empty() {
        let combined = metric_preds.join(" OR ");
        let metric_tables = [
            "metrics_gauge",
            "metrics_sum",
            "metrics_histogram",
            "metrics_exp_histogram",
            "metrics_summary",
        ];
        for table in metric_tables {
            let sql = format!("ALTER TABLE observability.{table} DELETE WHERE {combined}");
            execute_or_log(ch, &sql, dry_run).await;
        }
    }

    // ── Trace rules ── (same combining, per span table)
    // spans_raw is gone — spans are stored only in the wide `spans` table below.
    let wide_preds: Vec<String> = config
        .retention
        .traces
        .iter()
        .filter(|rule| rule.retain_days < table_traces_ttl)
        .filter_map(|rule| {
            build_trace_where_wide(rule).map(|clause| {
                format!(
                    "(toDateTime(timestamp) < now() - INTERVAL {} DAY AND {clause})",
                    rule.retain_days
                )
            })
        })
        .collect();
    if !wide_preds.is_empty() {
        let sql = format!(
            "ALTER TABLE observability.spans DELETE WHERE {}",
            wide_preds.join(" OR ")
        );
        execute_or_log(ch, &sql, dry_run).await;
    }

    Ok(())
}

fn build_metric_where(rule: &MetricRetentionRule) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref name) = rule.name {
        // Glob → SQL LIKE: `*` → `%`, `?` → `_`
        let like = esc(&name.replace('*', "%").replace('?', "_"));
        parts.push(format!("MetricName LIKE '{like}'"));
    }
    if let Some(ref re) = rule.name_regex {
        parts.push(format!("match(MetricName, '{}')", esc(re)));
    }
    for (k, v) in &rule.labels {
        parts.push(format!("Attributes['{}'] = '{}'", esc(k), esc(v)));
    }

    parts.join(" AND ")
}

fn build_trace_where_wide(rule: &TraceRetentionRule) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref svc) = rule.service_name {
        parts.push(format!("service_name = '{}'", esc(svc)));
    }
    if let Some(ref attr) = rule.attribute {
        // spans has `environment` as a first-class column for the common case
        if attr.key == "deployment.environment" {
            parts.push(format!("environment = '{}'", esc(&attr.value)));
        } else {
            // Fall back to the JSON attributes column
            parts.push(format!(
                "JSONExtractString(attributes, '{}') = '{}'",
                esc(&attr.key),
                esc(&attr.value)
            ));
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

/// Returns false when the delete was attempted and failed or was not run.
async fn execute_or_log(ch: &Client, sql: &str, dry_run: bool) -> bool {
    if dry_run {
        tracing::info!(
            engine = "retention",
            dry_run = true,
            "would execute retention delete"
        );
        return true;
    }
    if let Err(error) = crate::leader::ensure_leader() {
        tracing::info!(engine = "retention", %error, "retention delete not executed");
        return false;
    }
    tracing::debug!(engine = "retention", "executing retention delete");
    if let Err(e) = ch.query(sql).execute().await {
        tracing::warn!(error = %e, engine = "retention", "retention delete failed");
        return false;
    }
    true
}

/// Enforce per-tenant retention overrides via active DELETE mutations.
///
/// ClickHouse TTLs are table-level and cannot vary per tenant_id. For tenants
/// with SHORTER retention than the global TTL, we issue
/// `ALTER TABLE ... DELETE WHERE toDate(ts) < cutoff AND tenant_id IN (...)`.
///
/// Tenants wanting LONGER retention than global are not supported (the global
/// TTL will have already removed the data).
/// Read the global-retention store and apply the effective per-signal caps as
/// table-level TTLs. This makes the UI-editable caps the source of truth.
async fn apply_global_retention_ttls(ch: &Client, config_db: &ConfigDb) -> anyhow::Result<()> {
    let g = match config_db.get_global_retention().await? {
        Some(g) => g,
        None => return Ok(()), // not seeded yet; boot-time rush.toml TTLs stand
    };
    crate::migrations::apply_retention_ttls(
        ch,
        g.effective_logs() as u32,
        g.effective_metrics() as u32,
        g.effective_apm() as u32,
    )
    .await
}

/// AUDIT EXEMPTION: this enforcer only ever touches the telemetry tables
/// (logs/spans/metrics*/rum*) listed explicitly in `enforce_retention` and
/// below, driven by per-tenant override rows from `list_all_tenant_retention()`.
/// `observability.audit_events` is never in any of those lists and the reserved
/// `_audit` tenant has no retention overrides, so audit data is governed solely
/// by the table's own long TTL (RUSH_AUDIT_RETENTION_DAYS, default 730d) and is
/// never deleted by tenant retention.
///
/// All tenants' overrides for a table go into one mutation (ClickHouse refuses
/// new mutations past `number_of_mutations_to_throw`, 1000 by default). The
/// cutoff is a day, so the same statements are only re-sent after they
/// change: the next UTC day or an edited override. `done` holds the last
/// statements that all succeeded.
async fn enforce_tenant_retention(
    ch: &Client,
    config: &RushConfig,
    config_db: &ConfigDb,
    done: &mut Vec<String>,
) -> anyhow::Result<()> {
    let dry_run = config.retention.enforcer.dry_run;

    let overrides = config_db.list_all_tenant_retention().await?;
    if overrides.is_empty() {
        return Ok(());
    }

    tracing::debug!(
        engine = "retention",
        tenant_overrides = overrides.len(),
        "processing tenant retention overrides"
    );

    // Caps come from the global-retention store (falling back to rush.toml only
    // if the store hasn't been seeded yet).
    let store = config_db.get_global_retention().await?;
    let caps = GlobalCaps {
        metrics: store
            .map(|g| g.effective_metrics())
            .unwrap_or(config.effective_metrics_ttl_days() as i32),
        traces: store
            .map(|g| g.effective_apm())
            .unwrap_or(config.effective_traces_ttl_days() as i32),
        logs: store
            .map(|g| g.effective_logs())
            .unwrap_or(config.effective_logs_ttl_days() as i32),
    };

    let statements =
        tenant_retention_statements(&overrides, caps, time::OffsetDateTime::now_utc().date());
    if statements == *done {
        tracing::debug!(
            engine = "retention",
            "tenant retention unchanged today, skipping"
        );
        return Ok(());
    }
    let mut all_ok = true;
    for sql in &statements {
        all_ok &= execute_or_log(ch, sql, dry_run).await;
    }
    if all_ok {
        *done = statements;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct GlobalCaps {
    metrics: i32,
    traces: i32,
    logs: i32,
}

/// Keeps each mutation well under ClickHouse's 256 KiB `max_query_size`.
const MAX_TENANT_DELETE_SQL: usize = 100_000;

/// One DELETE per table for every override shorter than its signal's global
/// cap (more only when the predicate would pass `MAX_TENANT_DELETE_SQL`).
/// Tenants with the same retention share one `IN` list.
fn tenant_retention_statements(
    overrides: &[(String, String, i32)],
    caps: GlobalCaps,
    today: time::Date,
) -> Vec<String> {
    use std::collections::BTreeMap;
    // signal -> retain_days -> escaped tenant ids
    let mut grouped: BTreeMap<&str, BTreeMap<i32, Vec<String>>> = BTreeMap::new();
    for (tenant_id, signal, retain_days) in overrides {
        let global_days = match signal.as_str() {
            "metrics" => caps.metrics,
            "traces" => caps.traces,
            "logs" => caps.logs,
            other => {
                tracing::warn!(
                    engine = "retention",
                    tenant_id = %tenant_id,
                    signal = %other,
                    "unknown signal type, skipping"
                );
                continue;
            }
        };

        // Only enforce if the tenant wants SHORTER retention than global.
        // Longer retention is not possible — the global TTL already dropped the data.
        if *retain_days >= global_days {
            tracing::debug!(
                engine = "retention",
                tenant_id = %tenant_id,
                signal = %signal,
                retain_days = retain_days,
                global_days = global_days,
                "tenant retention >= global, skipping"
            );
            continue;
        }
        // Escape tenant_id for a quoted SQL literal (quotes, backslashes, and `?`).
        grouped
            .entry(signal.as_str())
            .or_default()
            .entry(*retain_days)
            .or_default()
            .push(format!("'{}'", esc(tenant_id)));
    }

    let mut statements = Vec::new();
    for (signal, by_days) in grouped {
        for (table, ts) in tenant_tables(signal) {
            let prefix = format!("ALTER TABLE observability.{table} DELETE WHERE ");
            let mut predicates = Vec::new();
            for (days, tenants) in &by_days {
                let cutoff = today - time::Duration::days(i64::from(*days));
                // Chunks keep one predicate from passing the size limit on its own.
                for chunk in tenants.chunks(1000) {
                    predicates.push(format!(
                        "(toDate({ts}) < toDate('{cutoff}') AND tenant_id IN ({}))",
                        chunk.join(", ")
                    ));
                }
            }
            let mut sql = String::new();
            for predicate in predicates {
                if !sql.is_empty() && sql.len() + predicate.len() + 4 > MAX_TENANT_DELETE_SQL {
                    statements.push(std::mem::take(&mut sql));
                }
                sql.push_str(if sql.is_empty() { &prefix } else { " OR " });
                sql.push_str(&predicate);
            }
            if !sql.is_empty() {
                statements.push(sql);
            }
        }
    }
    statements
}

/// The tables a tenant's retention override deletes from, with each table's
/// timestamp column.
fn tenant_tables(signal: &str) -> &'static [(&'static str, &'static str)] {
    match signal {
        // Rollups too: reads over long ranges are served from them, and they
        // keep Attributes and values as long as raw or longer.
        "metrics" => &[
            ("metrics_gauge", "TimeUnix"),
            ("metrics_sum", "TimeUnix"),
            ("metrics_histogram", "TimeUnix"),
            ("metrics_exp_histogram", "TimeUnix"),
            ("metrics_summary", "TimeUnix"),
            ("metrics_gauge_1m", "bucket"),
            ("metrics_sum_1m", "bucket"),
            ("metrics_gauge_1h", "bucket"),
            ("metrics_sum_1h", "bucket"),
        ],
        // spans is the single wide table (spans_raw was removed). RUM is
        // governed by the apm cap too (per product decision).
        "traces" => &[
            ("spans", "timestamp"),
            ("rum", "Timestamp"),
            ("rum_replay", "chunk_ts"),
        ],
        "logs" => &[("logs", "Timestamp")],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AttributeMatch;

    #[test]
    fn tenant_metric_retention_covers_the_rollups() {
        let tables = tenant_tables("metrics");
        for rollup in [
            "metrics_gauge_1m",
            "metrics_sum_1m",
            "metrics_gauge_1h",
            "metrics_sum_1h",
        ] {
            assert!(tables.contains(&(rollup, "bucket")), "{rollup} missing");
        }
        assert!(tables.contains(&("metrics_gauge", "TimeUnix")));
        // Every metric table with a TTL is covered.
        assert_eq!(tables.len(), 9);
        assert_eq!(tenant_tables("traces")[0], ("spans", "timestamp"));
        assert_eq!(tenant_tables("logs"), &[("logs", "Timestamp")]);
    }

    #[test]
    fn tenant_overrides_share_one_mutation_per_table() {
        let caps = GlobalCaps {
            metrics: 30,
            traces: 30,
            logs: 30,
        };
        let today = time::macros::date!(2026 - 11 - 27);
        // 1000 tenants used to mean 1000 mutations on each metric table every hour.
        let mut overrides: Vec<(String, String, i32)> = (0..1000)
            .map(|i| (format!("t{i}"), "metrics".to_string(), 7 + i % 2))
            .collect();
        overrides.push(("o'k".into(), "logs".into(), 3));
        overrides.push(("long".into(), "logs".into(), 90));
        overrides.push(("x".into(), "bogus".into(), 1));
        let sql = tenant_retention_statements(&overrides, caps, today);
        assert_eq!(sql.len(), 10, "logs + 9 metric tables");
        let gauge = &sql[1];
        assert!(gauge.starts_with("ALTER TABLE observability.metrics_gauge DELETE WHERE "));
        assert!(
            gauge.contains("(toDate(TimeUnix) < toDate('2026-11-20') AND tenant_id IN ('t0', ")
        );
        assert!(
            gauge.contains(" OR (toDate(TimeUnix) < toDate('2026-11-19') AND tenant_id IN ('t1', ")
        );
        assert!(sql.iter().any(|s| {
            s.starts_with("ALTER TABLE observability.metrics_sum_1h DELETE WHERE (toDate(bucket)")
        }));
        assert_eq!(
            sql[0],
            "ALTER TABLE observability.logs DELETE WHERE (toDate(Timestamp) < toDate('2026-11-24') AND tenant_id IN ('o''k'))"
        );
        // Same input, same statements: the next hourly run sends nothing.
        assert_eq!(tenant_retention_statements(&overrides, caps, today), sql);
        assert_ne!(
            tenant_retention_statements(&overrides, caps, today.next_day().unwrap()),
            sql
        );
    }

    #[test]
    fn tenant_deletes_split_before_the_query_size_limit() {
        let caps = GlobalCaps {
            metrics: 30,
            traces: 30,
            logs: 30,
        };
        let overrides: Vec<(String, String, i32)> = (0..20_000)
            .map(|i| (format!("tenant-{i:08}"), "logs".to_string(), 1 + i % 3))
            .collect();
        let sql =
            tenant_retention_statements(&overrides, caps, time::macros::date!(2026 - 11 - 27));
        assert!(sql.len() > 1);
        assert!(sql.iter().all(|s| s.len() <= MAX_TENANT_DELETE_SQL));
        let tenants: usize = sql.iter().map(|s| s.matches("'tenant-").count()).sum();
        assert_eq!(tenants, 20_000);
    }

    #[test]
    fn rule_values_are_escaped_as_sql_literals() {
        let metric = MetricRetentionRule {
            name: Some("o'k*".into()),
            name_regex: Some(r"^http_\d+?$".into()),
            labels: [("en'v".to_string(), "pr?d".to_string())].into(),
            retain_days: 1,
        };
        assert_eq!(
            build_metric_where(&metric),
            r"MetricName LIKE 'o''k%' AND match(MetricName, '^http_\\d+\x3F$') AND Attributes['en''v'] = 'pr\x3Fd'"
        );
        let trace = TraceRetentionRule {
            service_name: Some("o'brien".into()),
            attribute: Some(AttributeMatch {
                key: "team?".into(),
                value: "a'b".into(),
            }),
            retain_days: 1,
        };
        assert_eq!(
            build_trace_where_wide(&trace).unwrap(),
            r"service_name = 'o''brien' AND JSONExtractString(attributes, 'team\x3F') = 'a''b'"
        );
    }
}
