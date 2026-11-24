use crate::timestamps::{DATE, TimestampExt};
use axum::{
    Extension, Json,
    extract::{Query, State},
    http::StatusCode,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use time::{Date, Duration, OffsetDateTime};

use crate::{AppState, TenantContext, api_error::internal_legacy, tenant_query};

const COHORT_WEEKS: i64 = 6;
const FOLLOW_UP_WEEKS: usize = 5;
const MAX_ACTION_BYTES: usize = 256;

// Each inner row is one user. The earliest retained event assigns the cohort,
// even when the action filter is set. The small array only tracks report weeks.
const COHORT_SQL: &str = "
WITH fromUnixTimestamp64Milli(?, 'UTC') AS history_start,
     toDate(?) AS report_start,
     toDate(?) AS report_end,
     ? AS requested_action
SELECT toString(cohort_week) AS week_start,
       countIf(matches_action) AS new_users,
       sumForEach(arrayMap(offset -> toUInt64(matches_action AND has(active_weeks, addWeeks(cohort_week, offset))),
                           range(1, 1 + ?))) AS returned,
       topKIf(50)(first_action, notEmpty(first_action) AND length(first_action) <= ?) AS actions
FROM (
    SELECT cohort_week, active_weeks, first_action,
           (requested_action = '' OR requested_action = first_action) AS matches_action
    FROM (
        SELECT toMonday(min(Timestamp), 'UTC') AS cohort_week,
               groupUniqArrayIf(toMonday(Timestamp, 'UTC'), Timestamp >= toDateTime(report_start, 'UTC')) AS active_weeks,
               argMinIf(tuple(Timestamp, EventName), tuple(Timestamp, EventName),
                        EventType = 'custom' AND notEmpty(trimBoth(EventName))) AS earliest_action,
               if(toMonday(earliest_action.1, 'UTC') = cohort_week, earliest_action.2, '') AS first_action
        FROM rum
        PREWHERE tenant_id = ? AND AppName = ?
            AND TimestampTime >= toDateTime(history_start) AND TimestampTime < toDateTime(report_end, 'UTC')
        WHERE Timestamp >= history_start AND notEmpty(trimBoth(UserId))
        GROUP BY UserId
        HAVING min(Timestamp) >= toDateTime(report_start, 'UTC')
    )
)
GROUP BY cohort_week
ORDER BY cohort_week";

const COVERAGE_SQL: &str = "
WITH fromUnixTimestamp64Milli(?, 'UTC') AS coverage_start,
     fromUnixTimestamp64Milli(?, 'UTC') AS coverage_end
SELECT count() AS total_sessions,
       countIf(NOT identified) AS missing_user_id_sessions
FROM (
    SELECT max(notEmpty(trimBoth(UserId))) AS identified
    FROM rum
    PREWHERE tenant_id = ? AND AppName = ?
        AND TimestampTime >= toDateTime(coverage_start) AND TimestampTime <= toDateTime(coverage_end)
    WHERE Timestamp >= coverage_start AND Timestamp < coverage_end AND notEmpty(trimBoth(SessionId))
    GROUP BY SessionId
)";

#[derive(Deserialize)]
pub struct CohortRequest {
    app_name: String,
    #[serde(default)]
    first_action: String,
}

#[derive(Clone, Copy)]
struct ReportWindow {
    now: OffsetDateTime,
    history_start: OffsetDateTime,
    coverage_start: OffsetDateTime,
    cohort_start: Date,
    cohort_end: Date,
}

fn week_start(day: Date) -> Date {
    day - Duration::days(i64::from(day.weekday().number_days_from_monday()))
}

impl ReportWindow {
    fn new(now: OffsetDateTime, retention_days: u32) -> Self {
        let history_start = now - Duration::days(i64::from(retention_days));
        let cohort_end = week_start(now.date());
        let earliest_week = cohort_end - Duration::weeks(COHORT_WEEKS);
        let earliest_start = earliest_week.midnight().assume_utc();
        let coverage_start = history_start.max(earliest_start);
        let retained_week = week_start(coverage_start.date());
        let retained_week_start = retained_week.midnight().assume_utc();
        let first_complete_week = if retained_week_start == coverage_start {
            retained_week
        } else {
            retained_week + Duration::WEEK
        };
        Self {
            now,
            history_start,
            coverage_start,
            cohort_start: first_complete_week.min(cohort_end),
            cohort_end,
        }
    }
}

#[derive(Debug, Deserialize, Row)]
struct CohortRow {
    week_start: String,
    new_users: u64,
    returned: Vec<u64>,
    actions: Vec<String>,
}

#[derive(Debug, Deserialize, Row, Serialize)]
struct IdentityCoverage {
    total_sessions: u64,
    missing_user_id_sessions: u64,
}

time::serde::format_description!(serde_date, Date, DATE);

#[derive(Debug, Serialize)]
struct Cohort {
    #[serde(with = "serde_date")]
    week_start: Date,
    new_users: u64,
    returned: Vec<Option<u64>>,
}

#[derive(Debug, Serialize)]
pub struct CohortReport {
    #[serde(with = "time::serde::rfc3339")]
    as_of: OffsetDateTime,
    #[serde(with = "serde_date")]
    cohort_start: Date,
    #[serde(with = "serde_date")]
    cohort_end: Date,
    #[serde(with = "time::serde::rfc3339")]
    coverage_start: OffsetDateTime,
    coverage: IdentityCoverage,
    first_actions: BTreeSet<String>,
    cohorts: Vec<Cohort>,
}

/// GET /api/v1/rum/cohorts — weekly retention for an app in the active tenant.
pub async fn returning_users(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(req): Query<CohortRequest>,
) -> Result<Json<CohortReport>, (StatusCode, String)> {
    if req.app_name.trim().is_empty() || req.first_action.len() > MAX_ACTION_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid app name or first action".into(),
        ));
    }
    let tenant_id = &tenant.tenant_id;
    let (global, overrides) = tokio::try_join!(
        state.config_db.get_global_retention(),
        state.config_db.get_tenant_retention(tenant_id),
    )
    .map_err(|error| internal_legacy("rum.cohorts.retention", error))?;
    let global_days = global
        .map(|retention| retention.effective_apm() as u32)
        .unwrap_or_else(|| state.config.effective_traces_ttl_days());
    let retention_days = overrides
        .iter()
        .find(|(signal, days)| signal == "traces" && *days > 0)
        .map_or(global_days, |(_, days)| global_days.min(*days as u32))
        .max(1);
    let window = ReportWindow::new(OffsetDateTime::now_utc(), retention_days);
    let query = |sql| tenant_query(&state.ch, sql, tenant_id);
    fetch_report(
        query(COHORT_SQL),
        query(COVERAGE_SQL),
        tenant_id,
        &req,
        window,
    )
    .await
    .map(Json)
    .map_err(|error| internal_legacy("rum.cohorts", error))
}

async fn fetch_report(
    cohort_query: clickhouse::query::Query,
    coverage_query: clickhouse::query::Query,
    tenant: &str,
    req: &CohortRequest,
    window: ReportWindow,
) -> anyhow::Result<CohortReport> {
    let rows = async {
        if window.cohort_start == window.cohort_end {
            return Ok(Vec::new());
        }
        cohort_query
            .bind(window.history_start.unix_millis())
            .bind(window.cohort_start.to_string())
            .bind(window.cohort_end.to_string())
            .bind(&req.first_action)
            .bind(FOLLOW_UP_WEEKS)
            .bind(MAX_ACTION_BYTES)
            .bind(tenant)
            .bind(&req.app_name)
            .fetch_all::<CohortRow>()
            .await
    };
    let coverage = coverage_query
        .bind(window.coverage_start.unix_millis())
        .bind(window.now.unix_millis())
        .bind(tenant)
        .bind(&req.app_name)
        .fetch_one::<IdentityCoverage>();
    let (rows, coverage) = tokio::try_join!(rows, coverage)?;

    let mut first_actions = BTreeSet::new();
    let mut cohorts = Vec::new();
    for row in rows {
        first_actions.extend(row.actions);
        if row.new_users == 0 {
            continue;
        }
        let week_start = Date::parse(&row.week_start, DATE)?;
        let returned = row
            .returned
            .into_iter()
            .zip(1..=FOLLOW_UP_WEEKS as i64)
            .map(|(count, week)| {
                (week_start + Duration::weeks(week) < window.cohort_end).then_some(count)
            })
            .collect();
        cohorts.push(Cohort {
            week_start,
            new_users: row.new_users,
            returned,
        });
    }
    Ok(CohortReport {
        as_of: window.now,
        cohort_start: window.cohort_start,
        cohort_end: window.cohort_end,
        coverage_start: window.coverage_start,
        coverage,
        first_actions,
        cohorts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamps::parse_rfc3339;
    use clickhouse::Client;

    fn report_time() -> OffsetDateTime {
        parse_rfc3339("2026-11-23T12:00:00Z").unwrap()
    }

    #[test]
    fn window_uses_complete_utc_weeks_and_respects_retention() {
        let long = ReportWindow::new(report_time(), 90);
        assert_eq!(long.cohort_start.to_string(), "2026-10-12");
        assert_eq!(long.cohort_end.to_string(), "2026-11-23");
        let standard = ReportWindow::new(report_time(), 30);
        assert_eq!(
            standard.coverage_start.to_rfc3339(),
            "2026-10-24T12:00:00+00:00"
        );
        assert_eq!(standard.cohort_start.to_string(), "2026-10-26");
        let short = ReportWindow::new(report_time(), 3);
        assert_eq!(short.cohort_start, short.cohort_end);
        let monday = ReportWindow::new(parse_rfc3339("2026-11-23T00:00:00Z").unwrap(), 28);
        assert_eq!(monday.cohort_start.to_string(), "2026-10-26");
    }

    #[tokio::test]
    #[ignore = "requires a disposable ClickHouse at RUSH_TEST_CLICKHOUSE_URL"]
    async fn report_counts_users_and_sessions_within_the_app_and_tenant() -> anyhow::Result<()> {
        let db = Client::default().with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?);
        db.query(
            "CREATE TABLE rum (
            tenant_id LowCardinality(String),
            AppName LowCardinality(String),
            Timestamp DateTime64(9),
            TimestampTime DateTime DEFAULT toDateTime(Timestamp),
            EventType LowCardinality(String),
            EventName String,
            SessionId String,
            UserId String
        ) ENGINE = MergeTree ORDER BY (tenant_id, AppName, EventType, TimestampTime)",
        )
        .execute()
        .await?;
        // s1 becomes identified after login. Duplicate events and sessions for
        // u1 in a return week count once. Its later custom events don't change
        // its first action. u3 first appeared before the display window and
        // u4's apparent return falls in the current, incomplete week.
        db.query(
            "
INSERT INTO rum (tenant_id, AppName, Timestamp, SessionId, UserId, EventType, EventName) VALUES
('a', 'Storefront', '2026-10-13 08:00:00', 's1', '',   'pageview', ''),
('a', 'Storefront', '2026-10-13 08:02:00', 's1', 'u1', 'custom', 'Sign up'),
('a', 'Storefront', '2026-10-13 08:05:00', 's1', 'u1', 'custom', 'Start checkout'),
('a', 'Storefront', '2026-10-19 00:00:00', 's2', 'u1', 'pageview', ''),
('a', 'Storefront', '2026-10-19 00:00:00', 's2', 'u1', 'pageview', ''),
('a', 'Storefront', '2026-10-25 23:59:59.999999999', 's3', 'u1', 'custom', 'Start checkout'),
('a', 'Storefront', '2026-11-02 00:00:00', 's4', 'u1', 'pageview', ''),
('a', 'Storefront', '2026-10-12 00:00:00', 's5', 'u2', 'custom', 'Start checkout'),
('a', 'Storefront', '2026-11-02 00:00:00', 's6', 'u2', 'pageview', ''),
('a', 'Storefront', '2026-10-07 08:00:00', 'old', 'u3', 'pageview', ''),
('a', 'Storefront', '2026-10-14 08:00:00', 's7', 'u3', 'custom', 'Sign up'),
('a', 'Storefront', '2026-11-16 08:00:00', 's8', 'u4', 'pageview', ''),
('a', 'Storefront', '2026-11-23 08:00:00', 's9', 'u4', 'custom', 'Only late'),
('a', 'Storefront', '2026-11-12 08:00:00', 's10', '', 'pageview', ''),
('a', 'Storefront', '2026-11-12 08:05:00', 's10', ' ', 'custom', 'Anonymous'),
('a', 'Storefront', '2026-11-12 08:10:00', '',    '', 'pageview', ''),
('a', 'Storefront', '2026-11-14 08:00:00', 's11', '', 'pageview', ''),
('a', 'Other',      '2026-11-14 08:00:00', 's11', 'u9', 'pageview', ''),
('b', 'Storefront', '2026-11-14 08:00:00', 's10', 'u5', 'custom', 'Sign up'),
('b', 'Storefront', '2026-10-26 08:00:00', 'foreign', 'u1', 'pageview', '')
",
        )
        .execute()
        .await?;

        let get_report = |action: &str| {
            let req = CohortRequest {
                app_name: "Storefront".into(),
                first_action: action.into(),
            };
            let db = db.clone();
            async move {
                fetch_report(
                    db.query(COHORT_SQL),
                    db.query(COVERAGE_SQL),
                    "a",
                    &req,
                    ReportWindow::new(report_time(), 90),
                )
                .await
            }
        };
        let all = get_report("").await?;
        assert_eq!(
            all.first_actions.into_iter().collect::<Vec<_>>(),
            ["Sign up", "Start checkout"]
        );
        assert_eq!(all.cohorts.len(), 2);
        assert_eq!(all.cohorts[0].week_start.to_string(), "2026-10-12");
        assert_eq!(all.cohorts[0].new_users, 2);
        assert_eq!(
            all.cohorts[0].returned,
            [Some(1), Some(0), Some(2), Some(0), Some(0)]
        );
        assert_eq!(all.cohorts[1].returned, [None; FOLLOW_UP_WEEKS]);
        assert_eq!(all.coverage.total_sessions, 11);
        assert_eq!(all.coverage.missing_user_id_sessions, 2);

        let filtered = get_report("Start checkout").await?;
        assert_eq!(filtered.cohorts.len(), 1);
        assert_eq!(filtered.cohorts[0].new_users, 1);
        assert_eq!(
            filtered.cohorts[0].returned,
            [Some(0), Some(0), Some(1), Some(0), Some(0)]
        );
        assert_eq!(filtered.coverage.total_sessions, 11);
        assert_eq!(filtered.coverage.missing_user_id_sessions, 2);
        let unmatched = get_report("x'\\y").await?;
        assert!(unmatched.cohorts.is_empty());
        assert_eq!(unmatched.first_actions.len(), 2);
        Ok(())
    }
}
