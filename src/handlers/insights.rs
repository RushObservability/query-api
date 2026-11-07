//! One cheap "what's wrong" read: firing anomalies, repeated WARN/ERROR log
//! templates, and a 0–100 health score. No LLM involved, so agents can call it
//! before spending tokens on a deeper investigation. Defaults to the last few
//! minutes; `from`/`to` describe an earlier incident window instead.

use axum::{
    Json,
    extract::{Extension, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::services::LOG_BODY_TEMPLATE;
use crate::handlers::users::require_principal;
use crate::models::anomaly::{AnomalyEventWithRule, AnomalyRule};

const DEFAULT_MINUTES: u64 = 15;
const MAX_MINUTES: u64 = 1440;
/// A template needs this many occurrences in the window to count as repeated.
const MIN_PATTERN_COUNT: u64 = 3;
const MAX_PATTERNS: u64 = 50;

#[derive(Debug, Deserialize)]
pub struct InsightsParams {
    /// Look back from now. Ignored when `from`/`to` are set.
    pub minutes: Option<u64>,
    /// Window start and end (RFC3339 or Unix seconds). Both or neither; at most 24h.
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FiringAnomaly {
    pub rule_id: String,
    pub name: String,
    pub source: String,
    pub service: String,
    pub metric: String,
    pub value: f64,
    pub expected: f64,
    /// Standard deviations from the rule's baseline when it fired.
    pub deviation: f64,
    pub since: String,
    /// Still anomalous at the end of the window (always true for "now").
    pub open_at_end: bool,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct LogPattern {
    pub service: String,
    pub severity: String,
    pub template: String,
    pub example: String,
    pub count: u64,
    pub last_seen: String,
}

#[derive(Debug, Serialize)]
pub struct Insights {
    pub generated_at: String,
    pub from: String,
    pub to: String,
    pub window_minutes: u64,
    pub health_score: u8,
    pub summary: String,
    pub anomalies: Vec<FiringAnomaly>,
    pub log_patterns: Vec<LogPattern>,
}

/// Start at 100 and deduct per finding; floor at 0. This summarizes findings,
/// it is not an uptime figure.
fn health_score(anomalies: &[FiringAnomaly], patterns: &[LogPattern]) -> u8 {
    let anomaly_cost: u32 = anomalies
        .iter()
        .map(|a| match a.deviation.abs() {
            d if d >= 3.0 => 15,
            d if d >= 2.5 => 10,
            // A firing rule always costs something, even with a low sensitivity.
            _ => 5,
        })
        .sum();
    let pattern_cost: u32 = patterns
        .iter()
        .map(|p| match p.severity.as_str() {
            "ERROR" | "FATAL" if p.count >= 10 => 5,
            "WARN" if p.count >= 20 => 2,
            _ => 0,
        })
        .sum();
    100u32.saturating_sub(anomaly_cost + pattern_cost) as u8
}

/// Rules anomalous at any point in a window starting at `from`, from the
/// events of [`ConfigDb::anomaly_events_for_window`] (newest first): an
/// anomalous event inside the window, or a last pre-window event that was
/// anomalous and so still open when the window began.
///
/// [`ConfigDb::anomaly_events_for_window`]: crate::clickhouse_config::ConfigDb::anomaly_events_for_window
fn anomalies_in_window(
    events: &[AnomalyEventWithRule],
    rules: &[AnomalyRule],
    from: &str,
) -> Vec<FiringAnomaly> {
    let mut seen = std::collections::HashSet::new();
    events
        .iter()
        .filter(|e| seen.insert(e.rule_id.as_str()))
        .filter_map(|newest| {
            let mine = || events.iter().filter(|e| e.rule_id == newest.rule_id);
            let in_window = mine().any(|e| e.state == "anomalous" && e.created_at.as_str() >= from);
            let open_at_start = mine()
                .find(|e| e.created_at.as_str() < from)
                .is_some_and(|e| e.state == "anomalous");
            if !in_window && !open_at_start {
                return None;
            }
            let reading = mine().find(|e| e.state == "anomalous")?;
            let rule = rules.iter().find(|r| r.id == newest.rule_id);
            Some(FiringAnomaly {
                rule_id: newest.rule_id.clone(),
                name: newest.rule_name.clone(),
                source: rule.map(|r| r.source.clone()).unwrap_or_default(),
                service: rule.map(|r| r.service_name.clone()).unwrap_or_default(),
                metric: reading.metric.clone(),
                value: reading.value,
                expected: reading.expected,
                deviation: reading.deviation,
                since: reading.created_at.clone(),
                open_at_end: newest.state == "anomalous",
            })
        })
        .collect()
}

/// `YYYY-MM-DD HH:MM:SS`, the format of config `created_at` columns.
fn config_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn log_patterns_sql(escaped_tenant: &str, from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    // Formatted from parsed times, so safe to inline.
    let (from, to) = (
        from.format("%Y-%m-%dT%H:%M:%S%.9fZ"),
        to.format("%Y-%m-%dT%H:%M:%S%.9fZ"),
    );
    format!(
        "SELECT ServiceName AS service, SeverityText AS severity, \
            {LOG_BODY_TEMPLATE} AS template, \
            argMax(Body, Timestamp) AS example, \
            count() AS count, \
            toString(max(Timestamp)) AS last_seen \
         FROM logs \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND TimestampDate >= toDate(parseDateTime64BestEffort('{from}', 9)) \
            AND TimestampDate <= toDate(parseDateTime64BestEffort('{to}', 9)) \
            AND Timestamp >= parseDateTime64BestEffort('{from}', 9) \
            AND Timestamp <= parseDateTime64BestEffort('{to}', 9) \
         WHERE SeverityText IN ('ERROR', 'WARN', 'FATAL') \
         GROUP BY service, severity, template \
         HAVING count >= {MIN_PATTERN_COUNT} \
         ORDER BY count DESC \
         LIMIT {MAX_PATTERNS}"
    )
}

pub async fn get_insights(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<InsightsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_principal(&state, &headers).await?;
    let tenant_id = &tenant.tenant_id;
    let now = Utc::now();
    let (from, to, explicit_window) = match (params.from.as_deref(), params.to.as_deref()) {
        (None, None) => {
            let minutes = params
                .minutes
                .unwrap_or(DEFAULT_MINUTES)
                .clamp(1, MAX_MINUTES);
            (now - chrono::Duration::minutes(minutes as i64), now, false)
        }
        (Some(from), Some(to)) => {
            let range =
                crate::query_governor::ValidatedTimeRange::parse(from, to, MAX_MINUTES * 60)
                    .map_err(|e| {
                        use crate::query_governor::TimeRangeError;
                        let message = match e {
                            TimeRangeError::Malformed => {
                                "from and to must be RFC3339 or Unix seconds"
                            }
                            TimeRangeError::Reversed => "from must be before to",
                            TimeRangeError::TooLarge { .. } => {
                                "from and to must be at most 24h apart"
                            }
                        };
                        (StatusCode::BAD_REQUEST, message.to_string())
                    })?;
            (range.from, range.to, true)
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "pass both from and to, or neither".to_string(),
            ));
        }
    };
    let minutes = (to - from).num_minutes().max(0) as u64;

    let rules = state
        .config_db
        .list_anomaly_rules(tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("insights", e))?;
    let anomalies: Vec<FiringAnomaly> = if explicit_window {
        let events = state
            .config_db
            .anomaly_events_for_window(tenant_id, &config_time(from), &config_time(to))
            .await
            .map_err(|e| crate::api_error::internal_legacy("insights", e))?;
        anomalies_in_window(&events, &rules, &config_time(from))
    } else {
        let events = state
            .config_db
            .list_all_anomaly_events(tenant_id, 200)
            .await
            .map_err(|e| crate::api_error::internal_legacy("insights", e))?;
        rules
            .into_iter()
            .filter(|r| r.enabled && r.state == "anomalous")
            .map(|r| {
                // Events are newest first; the latest anomalous one carries the reading.
                let event = events
                    .iter()
                    .find(|e| e.rule_id == r.id && e.state == "anomalous");
                FiringAnomaly {
                    metric: event.map(|e| e.metric.clone()).unwrap_or_default(),
                    value: event.map_or(0.0, |e| e.value),
                    expected: event.map_or(0.0, |e| e.expected),
                    deviation: event.map_or(0.0, |e| e.deviation),
                    since: r.last_triggered_at.unwrap_or_default(),
                    open_at_end: true,
                    rule_id: r.id,
                    name: r.name,
                    source: r.source,
                    service: r.service_name,
                }
            })
            .collect()
    };

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let log_patterns = crate::tenant_query(
        &state.ch,
        &log_patterns_sql(&escaped_tenant, from, to),
        tenant_id,
    )
    .fetch_all::<LogPattern>()
    .await
    .map_err(|e| {
        tracing::error!(error = %e, handler = "get_insights", "query failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "query failed".to_string(),
        )
    })?;

    let health_score = health_score(&anomalies, &log_patterns);
    let error_patterns = log_patterns.iter().filter(|p| p.severity != "WARN").count();
    let window = if explicit_window {
        format!("{} to {}", from.to_rfc3339(), to.to_rfc3339())
    } else {
        format!("the last {minutes}m")
    };
    let summary = format!(
        "health {health_score}/100 over {window}; firing anomalies: {}; repeated error patterns: {error_patterns}; repeated warning patterns: {}",
        anomalies.len(),
        log_patterns.len() - error_patterns,
    );

    Ok(Json(Insights {
        generated_at: now.to_rfc3339(),
        from: from.to_rfc3339(),
        to: to.to_rfc3339(),
        window_minutes: minutes,
        health_score,
        summary,
        anomalies,
        log_patterns,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anomaly(deviation: f64) -> FiringAnomaly {
        FiringAnomaly {
            rule_id: String::new(),
            name: String::new(),
            source: String::new(),
            service: String::new(),
            metric: String::new(),
            value: 0.0,
            expected: 0.0,
            deviation,
            since: String::new(),
            open_at_end: true,
        }
    }

    fn pattern(severity: &str, count: u64) -> LogPattern {
        LogPattern {
            service: String::new(),
            severity: severity.to_string(),
            template: String::new(),
            example: String::new(),
            count,
            last_seen: String::new(),
        }
    }

    #[test]
    fn health_score_deducts_per_finding_and_floors_at_zero() {
        assert_eq!(health_score(&[], &[]), 100);
        // 15 + 10 + 5 (negative deviations count by magnitude) + 5 (below 2σ still firing)
        let anomalies = [anomaly(4.0), anomaly(2.7), anomaly(-2.2), anomaly(1.0)];
        assert_eq!(health_score(&anomalies, &[]), 65);
        // ERROR/FATAL from 10 → 5 each, WARN from 20 → 2, smaller ones free.
        let patterns = [
            pattern("ERROR", 10),
            pattern("FATAL", 50),
            pattern("ERROR", 9),
            pattern("WARN", 20),
            pattern("WARN", 19),
        ];
        assert_eq!(health_score(&[], &patterns), 88);
        let many: Vec<_> = (0..10).map(|_| anomaly(5.0)).collect();
        assert_eq!(health_score(&many, &patterns), 0);
    }

    #[test]
    fn log_patterns_sql_is_tenant_scoped_and_has_no_bind_placeholders() {
        let to = DateTime::parse_from_rfc3339("2026-11-07T04:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = log_patterns_sql("acme", to - chrono::Duration::minutes(15), to);
        assert!(sql.contains("PREWHERE tenant_id = 'acme'"));
        assert!(sql.contains(
            "Timestamp >= parseDateTime64BestEffort('2026-11-07T04:15:00.000000000Z', 9)"
        ));
        assert!(sql.contains(
            "Timestamp <= parseDateTime64BestEffort('2026-11-07T04:30:00.000000000Z', 9)"
        ));
        // The clickhouse crate treats `?` as a bind placeholder.
        assert!(!sql.contains('?'));
    }

    fn event(rule: &str, state: &str, at: &str, deviation: f64) -> AnomalyEventWithRule {
        AnomalyEventWithRule {
            id: format!("{rule}-{at}"),
            rule_id: rule.into(),
            tenant_id: "default".into(),
            rule_name: format!("rule {rule}"),
            state: state.into(),
            metric: "p95_ms".into(),
            value: 900.0,
            expected: 200.0,
            deviation,
            message: String::new(),
            created_at: at.into(),
        }
    }

    #[test]
    fn window_counts_rules_anomalous_inside_it_or_still_open_when_it_began() {
        let from = "2026-11-07 04:00:00";
        // Newest first, as the query returns them.
        let events = [
            event("resolved-inside", "normal", "2026-11-07 04:20:00", 0.0),
            event("opened-inside", "anomalous", "2026-11-07 04:10:00", 3.5),
            event("resolved-inside", "anomalous", "2026-11-07 04:05:00", 2.6),
            event("carried-over", "anomalous", "2026-11-07 03:00:00", 4.0),
            event("closed-before", "normal", "2026-11-07 03:30:00", 0.0),
        ];
        let rules: Vec<AnomalyRule> = serde_json::from_value(serde_json::json!([{
            "id": "opened-inside", "tenant_id": "default", "name": "rule opened-inside",
            "description": "", "enabled": true, "source": "apm", "pattern": "", "query": "",
            "service_name": "checkout", "apm_metric": "", "sensitivity": 3.0, "alpha": 0.25,
            "eval_interval_secs": 60, "window_secs": 3600, "split_labels": "[]",
            "notification_channel_ids": "[]", "state": "anomalous", "last_eval_at": null,
            "last_triggered_at": null, "created_at": "", "updated_at": ""
        }]))
        .unwrap();
        let found = anomalies_in_window(&events, &rules, from);
        let summary: Vec<_> = found
            .iter()
            .map(|a| (a.rule_id.as_str(), a.open_at_end, a.deviation))
            .collect();
        assert_eq!(
            summary,
            [
                ("resolved-inside", false, 2.6),
                ("opened-inside", true, 3.5),
                ("carried-over", true, 4.0),
            ]
        );
        assert_eq!(found[1].service, "checkout");
        assert_eq!(found[2].since, "2026-11-07 03:00:00");
    }
}
