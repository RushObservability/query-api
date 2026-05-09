use clickhouse::Client;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{MetricRetentionRule, TraceRetentionRule, RushConfig};
use crate::config_db::ConfigDb;

/// Spawn the retention enforcer as a background task (fire-and-forget).
/// Follows the same pattern as `alert_engine::spawn_alert_engine`.
pub fn spawn_retention_enforcer(ch: Client, config: RushConfig, config_db: Arc<ConfigDb>) {
    if !config.retention.enforcer.enabled {
        tracing::info!(engine = "retention", "retention enforcer disabled by config");
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

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = enforce_retention(&ch, &config).await {
                tracing::error!(error = %e, engine = "retention", "global retention enforcement failed");
            }
            if let Err(e) = enforce_tenant_retention(&ch, &config, &config_db).await {
                tracing::error!(error = %e, engine = "retention", "tenant retention enforcement failed");
            }
        }
    });
}

async fn enforce_retention(ch: &Client, config: &RushConfig) -> anyhow::Result<()> {
    let table_metrics_ttl = config.effective_metrics_ttl_days();
    let table_traces_ttl = config.effective_traces_ttl_days();
    let dry_run = config.retention.enforcer.dry_run;

    // ── Metric rules ──
    for rule in &config.retention.metrics {
        // Only enforce rules that are shorter than the table TTL
        if rule.retain_days >= table_metrics_ttl {
            continue;
        }
        let where_clause = build_metric_where(rule);
        if where_clause.is_empty() {
            continue;
        }
        let metric_tables = [
            "otel_metrics_gauge",
            "otel_metrics_sum",
            "otel_metrics_histogram",
            "otel_metrics_exponential_histogram",
            "otel_metrics_summary",
        ];
        for table in metric_tables {
            let sql = format!(
                "ALTER TABLE observability.{table} DELETE WHERE \
                 toDateTime(TimeUnix) < now() - INTERVAL {} DAY AND {where_clause}",
                rule.retain_days
            );
            execute_or_log(ch, &sql, dry_run).await;
        }
    }

    // ── Trace rules ──
    for rule in &config.retention.traces {
        if rule.retain_days >= table_traces_ttl {
            continue;
        }
        // otel_traces
        if let Some(clause) = build_trace_where_otel(rule) {
            let sql = format!(
                "ALTER TABLE observability.otel_traces DELETE WHERE \
                 toDateTime(Timestamp) < now() - INTERVAL {} DAY AND {clause}",
                rule.retain_days
            );
            execute_or_log(ch, &sql, dry_run).await;
        }
        // wide_events
        if let Some(clause) = build_trace_where_wide(rule) {
            let sql = format!(
                "ALTER TABLE observability.wide_events DELETE WHERE \
                 toDateTime(timestamp) < now() - INTERVAL {} DAY AND {clause}",
                rule.retain_days
            );
            execute_or_log(ch, &sql, dry_run).await;
        }
    }

    Ok(())
}

fn build_metric_where(rule: &MetricRetentionRule) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref name) = rule.name {
        // Glob → SQL LIKE: `*` → `%`, `?` → `_`
        let like = name.replace('*', "%").replace('?', "_");
        parts.push(format!("MetricName LIKE '{like}'"));
    }
    if let Some(ref re) = rule.name_regex {
        parts.push(format!("match(MetricName, '{re}')"));
    }
    for (k, v) in &rule.labels {
        parts.push(format!("Attributes['{k}'] = '{v}'"));
    }

    parts.join(" AND ")
}

fn build_trace_where_otel(rule: &TraceRetentionRule) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref svc) = rule.service_name {
        parts.push(format!("ServiceName = '{svc}'"));
    }
    if let Some(ref attr) = rule.attribute {
        parts.push(format!(
            "ResourceAttributes['{}'] = '{}'",
            attr.key, attr.value
        ));
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

fn build_trace_where_wide(rule: &TraceRetentionRule) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref svc) = rule.service_name {
        parts.push(format!("service_name = '{svc}'"));
    }
    if let Some(ref attr) = rule.attribute {
        // wide_events has `environment` as a first-class column for the common case
        if attr.key == "deployment.environment" {
            parts.push(format!("environment = '{}'", attr.value));
        } else {
            // Fall back to the JSON attributes column
            parts.push(format!(
                "JSONExtractString(attributes, '{}') = '{}'",
                attr.key, attr.value
            ));
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

async fn execute_or_log(ch: &Client, sql: &str, dry_run: bool) {
    if dry_run {
        tracing::info!(engine = "retention", dry_run = true, "would execute retention delete");
        return;
    }
    tracing::debug!(engine = "retention", "executing retention delete");
    if let Err(e) = ch.query(sql).execute().await {
        tracing::warn!(error = %e, engine = "retention", "retention delete failed");
    }
}

/// Enforce per-tenant retention overrides via active DELETE mutations.
///
/// ClickHouse TTLs are table-level and cannot vary per tenant_id. For tenants
/// with SHORTER retention than the global TTL, we issue
/// `ALTER TABLE ... DELETE WHERE tenant_id = '...' AND toDate(ts) < today() - N`.
///
/// Tenants wanting LONGER retention than global are not supported (the global
/// TTL will have already removed the data).
async fn enforce_tenant_retention(
    ch: &Client,
    config: &RushConfig,
    config_db: &ConfigDb,
) -> anyhow::Result<()> {
    let dry_run = config.retention.enforcer.dry_run;

    let overrides = config_db.list_all_tenant_retention()?;
    if overrides.is_empty() {
        return Ok(());
    }

    tracing::debug!(
        engine = "retention",
        tenant_overrides = overrides.len(),
        "processing tenant retention overrides"
    );

    let global_metrics = config.effective_metrics_ttl_days() as i32;
    let global_traces = config.effective_traces_ttl_days() as i32;
    let global_logs = config.effective_logs_ttl_days() as i32;

    for (tenant_id, signal, retain_days) in &overrides {
        let global_days = match signal.as_str() {
            "metrics" => global_metrics,
            "traces" => global_traces,
            "logs" => global_logs,
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

        // Escape single quotes in tenant_id to prevent SQL injection
        let safe_tenant_id = tenant_id.replace('\'', "''");

        match signal.as_str() {
            "metrics" => {
                let metric_tables = [
                    "otel_metrics_gauge",
                    "otel_metrics_sum",
                    "otel_metrics_histogram",
                    "otel_metrics_exponential_histogram",
                    "otel_metrics_summary",
                ];
                for table in metric_tables {
                    let sql = format!(
                        "ALTER TABLE observability.{table} DELETE \
                         WHERE tenant_id = '{safe_tenant_id}' \
                         AND toDate(TimeUnix) < today() - {retain_days}"
                    );
                    execute_or_log(ch, &sql, dry_run).await;
                }
            }
            "traces" => {
                // otel_traces
                let sql = format!(
                    "ALTER TABLE observability.otel_traces DELETE \
                     WHERE tenant_id = '{safe_tenant_id}' \
                     AND toDate(Timestamp) < today() - {retain_days}"
                );
                execute_or_log(ch, &sql, dry_run).await;

                // wide_events
                let sql = format!(
                    "ALTER TABLE observability.wide_events DELETE \
                     WHERE tenant_id = '{safe_tenant_id}' \
                     AND toDate(timestamp) < today() - {retain_days}"
                );
                execute_or_log(ch, &sql, dry_run).await;
            }
            "logs" => {
                let sql = format!(
                    "ALTER TABLE observability.otel_logs DELETE \
                     WHERE tenant_id = '{safe_tenant_id}' \
                     AND toDate(Timestamp) < today() - {retain_days}"
                );
                execute_or_log(ch, &sql, dry_run).await;
            }
            _ => {} // already handled above
        }
    }

    Ok(())
}
