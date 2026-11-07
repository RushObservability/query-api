//! One cheap "what's wrong right now" read: firing anomalies, repeated
//! WARN/ERROR log templates, and a 0–100 health score. No LLM involved, so
//! agents can call it before spending tokens on a deeper investigation.

use axum::{
    Json,
    extract::{Extension, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::services::LOG_BODY_TEMPLATE;
use crate::handlers::users::require_principal;

const DEFAULT_MINUTES: u64 = 15;
const MAX_MINUTES: u64 = 1440;
/// A template needs this many occurrences in the window to count as repeated.
const MIN_PATTERN_COUNT: u64 = 3;
const MAX_PATTERNS: u64 = 50;

#[derive(Debug, Deserialize)]
pub struct InsightsParams {
    pub minutes: Option<u64>,
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

fn log_patterns_sql(escaped_tenant: &str, minutes: u64) -> String {
    format!(
        "SELECT ServiceName AS service, SeverityText AS severity, \
            {LOG_BODY_TEMPLATE} AS template, \
            argMax(Body, Timestamp) AS example, \
            count() AS count, \
            toString(max(Timestamp)) AS last_seen \
         FROM logs \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND TimestampDate >= toDate(now() - INTERVAL {minutes} MINUTE) \
            AND Timestamp >= now() - INTERVAL {minutes} MINUTE \
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
    let minutes = params
        .minutes
        .unwrap_or(DEFAULT_MINUTES)
        .clamp(1, MAX_MINUTES);

    let rules = state
        .config_db
        .list_anomaly_rules(tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("insights", e))?;
    let events = state
        .config_db
        .list_all_anomaly_events(tenant_id, 200)
        .await
        .map_err(|e| crate::api_error::internal_legacy("insights", e))?;
    let anomalies: Vec<FiringAnomaly> = rules
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
                rule_id: r.id,
                name: r.name,
                source: r.source,
                service: r.service_name,
            }
        })
        .collect();

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let log_patterns = crate::tenant_query(
        &state.ch,
        &log_patterns_sql(&escaped_tenant, minutes),
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
    let summary = format!(
        "health {health_score}/100 over the last {minutes}m; firing anomalies: {}; repeated error patterns: {error_patterns}; repeated warning patterns: {}",
        anomalies.len(),
        log_patterns.len() - error_patterns,
    );

    Ok(Json(Insights {
        generated_at: chrono::Utc::now().to_rfc3339(),
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
        let sql = log_patterns_sql("acme", 15);
        assert!(sql.contains("PREWHERE tenant_id = 'acme'"));
        assert!(sql.contains("INTERVAL 15 MINUTE"));
        // The clickhouse crate treats `?` as a bind placeholder.
        assert!(!sql.contains('?'));
    }
}
