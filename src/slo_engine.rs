use std::sync::Arc;
use crate::config_db::ConfigDb;
use crate::models::query::Filter;
use crate::query_builder::{build_where_clause, build_metrics_where_clause};
use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct SumRow {
    total: f64,
}

/// trace + availability: COUNT errors / COUNT total on wide_events
async fn eval_trace_availability(
    ch: &Client,
    error_filters: &[Filter],
    total_filters: &[Filter],
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let error_where = build_where_clause(error_filters, from, to);
    let error_sql = format!("SELECT count() as count FROM wide_events WHERE {error_where}");
    let error_count = ch.query(&error_sql).fetch_one::<CountRow>().await?.count as i64;

    let total_where = build_where_clause(total_filters, from, to);
    let total_sql = format!("SELECT count() as count FROM wide_events WHERE {total_where}");
    let total_count = ch.query(&total_sql).fetch_one::<CountRow>().await?.count as i64;

    Ok((error_count, total_count))
}

/// trace + latency: COUNT(duration_ns > threshold) / COUNT total on wide_events
/// consumed = 1.0 - (fast_count / total_count), so error_count = slow_count
async fn eval_trace_latency(
    ch: &Client,
    total_filters: &[Filter],
    threshold_ns: i64,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let total_where = build_where_clause(total_filters, from, to);
    let total_sql = format!("SELECT count() as count FROM wide_events WHERE {total_where}");
    let total_count = ch.query(&total_sql).fetch_one::<CountRow>().await?.count as i64;

    // Count requests that exceeded the threshold (slow = errors for budget)
    let slow_sql = format!(
        "SELECT count() as count FROM wide_events WHERE {total_where} AND Duration > {threshold_ns}"
    );
    let slow_count = ch.query(&slow_sql).fetch_one::<CountRow>().await?.count as i64;

    Ok((slow_count, total_count))
}

/// metric + availability: SUM(Value) error / SUM(Value) total on otel_metrics_sum
async fn eval_metric_availability(
    ch: &Client,
    error_filters: &[Filter],
    total_filters: &[Filter],
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let error_where = build_metrics_where_clause(error_filters, from, to);
    let error_sql = format!("SELECT sum(Value) as total FROM otel_metrics_sum WHERE {error_where}");
    let error_count = ch.query(&error_sql).fetch_one::<SumRow>().await?.total as i64;

    let total_where = build_metrics_where_clause(total_filters, from, to);
    let total_sql = format!("SELECT sum(Value) as total FROM otel_metrics_sum WHERE {total_where}");
    let total_count = ch.query(&total_sql).fetch_one::<SumRow>().await?.total as i64;

    Ok((error_count, total_count))
}

/// metric + latency: histogram bucket query on otel_metrics_histogram
/// Count samples in bucket <= threshold / total count
async fn eval_metric_latency(
    ch: &Client,
    total_filters: &[Filter],
    threshold_ms: f64,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let where_clause = build_metrics_where_clause(total_filters, from, to);

    // Total count from histogram
    let total_sql = format!(
        "SELECT sum(Count) as total FROM otel_metrics_histogram WHERE {where_clause}"
    );
    let total_count = ch.query(&total_sql).fetch_one::<SumRow>().await?.total as i64;

    // Fast count: samples in buckets <= threshold
    // Histogram ExplicitBounds are in the metric's unit; we pass threshold_ms directly
    let fast_sql = format!(
        "SELECT sum(BucketCounts[indexOf(ExplicitBounds, eb)]) as total \
         FROM otel_metrics_histogram \
         ARRAY JOIN ExplicitBounds AS eb \
         WHERE {where_clause} AND eb <= {threshold_ms}"
    );
    let fast_count = ch.query(&fast_sql).fetch_one::<SumRow>().await.unwrap_or(SumRow { total: 0.0 }).total as i64;

    // slow_count = total - fast (error count for budget)
    let slow_count = total_count - fast_count;
    Ok((slow_count.max(0), total_count))
}

/// metric + threshold: COUNT violating / COUNT total on otel_metrics_gauge
/// threshold_op defines what "good" means, so violating = NOT good
async fn eval_metric_threshold(
    ch: &Client,
    total_filters: &[Filter],
    threshold_value: f64,
    threshold_op: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let where_clause = build_metrics_where_clause(total_filters, from, to);

    let total_sql = format!(
        "SELECT count() as count FROM otel_metrics_gauge WHERE {where_clause}"
    );
    let total_count = ch.query(&total_sql).fetch_one::<CountRow>().await?.count as i64;

    // "good" condition based on threshold_op (what good means)
    // Violating = NOT good
    let violating_op = match threshold_op {
        "lt" => format!("Value >= {threshold_value}"),   // good = Value < threshold
        "lte" => format!("Value > {threshold_value}"),   // good = Value <= threshold
        "gt" => format!("Value <= {threshold_value}"),   // good = Value > threshold
        "gte" => format!("Value < {threshold_value}"),   // good = Value >= threshold
        _ => format!("Value >= {threshold_value}"),
    };

    let violating_sql = format!(
        "SELECT count() as count FROM otel_metrics_gauge WHERE {where_clause} AND {violating_op}"
    );
    let violating_count = ch.query(&violating_sql).fetch_one::<CountRow>().await?.count as i64;

    Ok((violating_count, total_count))
}

/// Write SLO gauge metrics to ClickHouse so they can be graphed over time.
/// Emits: rush_slo_current, rush_slo_error_budget_remaining, rush_slo_error_count, rush_slo_total_count
async fn write_slo_metrics(
    ch: &Client,
    slo_id: &str,
    slo_name: &str,
    current_pct: f64,
    error_budget_remaining: f64,
    error_count: i64,
    total_count: i64,
    now_nanos: i64,
) {
    let escaped_name = slo_name.replace('\'', "\\'");
    let attrs = format!(
        "{{'slo.id': '{slo_id}', 'slo.name': '{escaped_name}'}}"
    );
    let metrics = [
        ("rush_slo_current", current_pct),
        ("rush_slo_error_budget_remaining", error_budget_remaining * 100.0),
        ("rush_slo_error_count", error_count as f64),
        ("rush_slo_total_count", total_count as f64),
    ];
    let values: Vec<String> = metrics.iter().map(|(name, val)| {
        format!(
            "({{}}, '', '', '', {{}}, 0, '', 'wide-slo-engine', '{name}', '', '', {attrs}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])"
        )
    }).collect();
    let sql = format!(
        "INSERT INTO otel_metrics_gauge \
         (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
          ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
          MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
          Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
          Exemplars.SpanId, Exemplars.TraceId) VALUES {}",
        values.join(", ")
    );
    if let Err(e) = ch.query(&sql).execute().await {
        tracing::warn!("slo metrics write failed: {e}");
    }
}

pub fn spawn_slo_engine(config_db: Arc<ConfigDb>, ch: Client) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if let Err(e) = eval_slos(&config_db, &ch, &http_client).await {
                tracing::error!("slo engine error: {e}");
            }
        }
    });
}

fn window_minutes(window_type: &str) -> i64 {
    match window_type {
        "rolling_1h" => 60,
        "rolling_24h" => 1440,
        "rolling_7d" => 10080,
        "rolling_30d" => 43200,
        _ => 60,
    }
}

async fn eval_slos(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let due_slos = config_db.get_due_slos(&now_str)?;

    for slo in due_slos {
        let minutes = window_minutes(&slo.window_type);
        let from = (now - chrono::Duration::minutes(minutes))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        // Parse filters
        let error_filters: Vec<Filter> = match serde_json::from_str(&slo.error_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad error_filters: {e}", slo.id);
                continue;
            }
        };
        let total_filters: Vec<Filter> = match serde_json::from_str(&slo.total_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad total_filters: {e}", slo.id);
                continue;
            }
        };

        // Evaluate based on (slo_type, indicator_type)
        let eval_result = match (slo.slo_type.as_str(), slo.indicator_type.as_str()) {
            ("trace", "availability") => {
                eval_trace_availability(ch, &error_filters, &total_filters, &from, &now_str).await
            }
            ("trace", "latency") => {
                let threshold_ns = (slo.threshold_ms.unwrap_or(0.0) * 1_000_000.0) as i64;
                eval_trace_latency(ch, &total_filters, threshold_ns, &from, &now_str).await
            }
            ("metric", "availability") => {
                eval_metric_availability(ch, &error_filters, &total_filters, &from, &now_str).await
            }
            ("metric", "latency") => {
                let threshold_ms = slo.threshold_ms.unwrap_or(0.0);
                eval_metric_latency(ch, &total_filters, threshold_ms, &from, &now_str).await
            }
            ("metric", "threshold") => {
                let threshold_value = slo.threshold_value.unwrap_or(0.0);
                let threshold_op = slo.threshold_op.as_deref().unwrap_or("lt");
                eval_metric_threshold(ch, &total_filters, threshold_value, threshold_op, &from, &now_str).await
            }
            _ => {
                tracing::warn!("slo {}: unsupported type/indicator: {}/{}", slo.id, slo.slo_type, slo.indicator_type);
                config_db.update_slo_state(&slo.id, "no_data", 0.0, 0, 0, &now_str, None)?;
                continue;
            }
        };

        let (error_count, total_count) = match eval_result {
            Ok(counts) => counts,
            Err(e) => {
                tracing::warn!("slo {}: evaluation failed: {e}", slo.id);
                config_db.update_slo_state(&slo.id, "no_data", 0.0, 0, 0, &now_str, None)?;
                continue;
            }
        };

        // Calculate error budget
        // error_budget = 1 - target/100 (allowed error rate)
        // consumed = error_count / total_count (actual error rate)
        // remaining = error_budget - consumed
        let (new_state, error_budget_remaining) = if total_count == 0 {
            ("no_data", 0.0_f64)
        } else {
            let error_budget = 1.0 - slo.target_percentage / 100.0;
            let consumed = error_count as f64 / total_count as f64;
            let remaining = error_budget - consumed;
            let state = if remaining > 0.0 { "compliant" } else { "breaching" };
            (state, remaining)
        };

        let old_state = slo.state.as_str();

        if new_state != old_state {
            // State changed — record event and notify
            let event_id = uuid::Uuid::new_v4().to_string();
            let message = format!(
                "SLO '{}': {} (errors={}, total={}, budget_remaining={:.4}%)",
                slo.name,
                match new_state {
                    "breaching" => "BREACHING",
                    "compliant" => "COMPLIANT",
                    _ => "NO_DATA",
                },
                error_count,
                total_count,
                error_budget_remaining * 100.0,
            );

            config_db.create_slo_event(
                &event_id,
                &slo.id,
                new_state,
                error_count,
                total_count,
                error_budget_remaining,
                &message,
            )?;

            let breached_at = if new_state == "breaching" { Some(now_str.as_str()) } else { None };
            config_db.update_slo_state(
                &slo.id, new_state, error_budget_remaining,
                error_count, total_count, &now_str, breached_at,
            )?;

            // Send notifications
            let channel_ids: Vec<String> = serde_json::from_str(&slo.notification_channel_ids)
                .unwrap_or_default();
            for channel_id in &channel_ids {
                if let Ok(Some(channel)) = config_db.get_channel(channel_id) {
                    let config: serde_json::Value = serde_json::from_str(&channel.config)
                        .unwrap_or(serde_json::json!({}));
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = match channel.channel_type.as_str() {
                            "slack" => serde_json::json!({ "text": message }),
                            _ => serde_json::json!({
                                "slo": slo.name,
                                "state": new_state,
                                "error_count": error_count,
                                "total_count": total_count,
                                "error_budget_remaining": error_budget_remaining,
                                "message": message,
                            }),
                        };
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!("slo {}: notification to {} failed: {e}", slo.id, channel.name);
                        }
                    }
                }
            }

            tracing::info!("slo '{}' state: {} -> {}", slo.name, old_state, new_state);
        } else {
            config_db.update_slo_state(
                &slo.id, new_state, error_budget_remaining,
                error_count, total_count, &now_str, None,
            )?;
        }

        // Write SLO gauge metrics for graphing
        let current_pct = if total_count > 0 {
            ((total_count - error_count) as f64 / total_count as f64) * 100.0
        } else {
            0.0
        };
        let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
        write_slo_metrics(
            ch, &slo.id, &slo.name,
            current_pct, error_budget_remaining,
            error_count, total_count, now_nanos,
        ).await;
    }

    Ok(())
}
