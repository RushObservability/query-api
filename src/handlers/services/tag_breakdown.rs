use axum::{
    Extension, Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::query_builder::{ERROR_SPAN, escape_string_literal};
use crate::service_tags::{REQUEST_SPAN, ServiceTag, TOP_TAGS};
use crate::{AppState, TenantContext};

#[derive(Deserialize)]
pub struct TagBreakdownParams {
    service: String,
    dimension: ServiceTag,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Row)]
struct Aggregate {
    is_other: u8,
    tag: String,
    is_total: u64,
    bucket_ms: i64,
    requests: u64,
    errors: u64,
    p95_ms: f64,
    other_values: u64,
    untagged_requests: u64,
}

#[derive(Default, Serialize)]
struct Group {
    value: Option<String>,
    requests: u64,
    errors: u64,
    p95_ms: f64,
    other_values: u64,
    untagged_requests: u64,
    buckets: Vec<Bucket>,
}

#[derive(Serialize)]
struct Bucket {
    timestamp_ms: i64,
    requests: u64,
    errors: u64,
    p95_ms: f64,
}

#[derive(Serialize)]
struct TagBreakdown {
    bucket_seconds: i64,
    total_requests: u64,
    groups: Vec<Group>,
}

const MAX_RANGE_MS: i64 = 366 * 86_400_000;

pub async fn service_tag_breakdown(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<TagBreakdownParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let from = params.from.timestamp_millis();
    let to = params.to.timestamp_millis();
    if from < 0
        || to <= from
        || to - from > MAX_RANGE_MS
        || params.to.timestamp_nanos_opt().is_none()
        || params.service.is_empty()
        || params.service.len() > 1024
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Specify a service and a positive time range of at most 366 days.".into(),
        ));
    }

    let interval = [60, 300, 900, 3600, 21_600, 86_400, 345_600]
        .into_iter()
        .find(|interval| to - from <= 120_000 * interval)
        .unwrap_or(345_600);
    let sql = breakdown_sql(
        &tenant.tenant_id,
        &params.service,
        params.dimension,
        from,
        to,
        interval,
    );
    let rows = super::time_breakdown_query(&state.ch, &sql, &tenant.tenant_id)
        .fetch_all::<Aggregate>()
        .await
        .map_err(|error| {
            tracing::error!(%error, handler = "service_tag_breakdown", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    Ok(Json(group_rows(interval, rows)))
}

fn breakdown_sql(
    tenant: &str,
    service: &str,
    dimension: ServiceTag,
    from: i64,
    to: i64,
    interval: i64,
) -> String {
    let tenant = escape_string_literal(tenant);
    let service = escape_string_literal(service);
    let value = dimension.expression();
    let bucket_ms = interval * 1000;
    let last_bucket = (to - from - 1) / bucket_ms;
    let scope = format!(
        "FROM spans PREWHERE tenant_id = '{tenant}' AND service_name = '{service}'          AND timestamp >= fromUnixTimestamp64Milli({from})          AND timestamp <= fromUnixTimestamp64Milli({to}) WHERE {REQUEST_SPAN}"
    );
    // Rank across the whole window, then aggregate the selected series and
    // their remainder from raw request spans. GROUPING SETS gives a window
    // percentile and a percentile for each bucket from those same spans.
    format!(
        "WITH (
            SELECT groupArray(value) FROM (
                SELECT {value} AS value {scope} AND value != ''
                GROUP BY value ORDER BY count() DESC, value ASC LIMIT {TOP_TAGS}
            )
         ) AS top_values
         SELECT is_other, tag, GROUPING(bucket_ms) AS is_total, bucket_ms,
                count() AS requests, countIf(failed) AS errors,
                quantile(0.95)(duration_ns) / 1000000.0 AS p95_ms,
                uniqExactIf(value, value != '') AS other_values,
                countIf(value = '') AS untagged_requests
         FROM (
            SELECT {value} AS value, NOT has(top_values, value) AS is_other,
                if(is_other, '', value) AS tag,
                {from} + least(intDiv(toUnixTimestamp64Milli(timestamp) - {from}, {bucket_ms}), {last_bucket}) * {bucket_ms} AS bucket_ms,
                duration_ns, {ERROR_SPAN} AS failed
            {scope}
         )
         GROUP BY GROUPING SETS ((is_other, tag, bucket_ms), (is_other, tag))
         ORDER BY bucket_ms"
    )
}

fn group_rows(interval: i64, rows: Vec<Aggregate>) -> TagBreakdown {
    let mut groups = BTreeMap::<(bool, String), Group>::new();
    for row in rows {
        let is_other = row.is_other != 0;
        let group = groups.entry((is_other, row.tag)).or_default();
        if row.is_total == 1 {
            group.requests = row.requests;
            group.errors = row.errors;
            group.p95_ms = row.p95_ms;
            if is_other {
                group.other_values = row.other_values;
                group.untagged_requests = row.untagged_requests;
            }
        } else {
            group.buckets.push(Bucket {
                timestamp_ms: row.bucket_ms,
                requests: row.requests,
                errors: row.errors,
                p95_ms: row.p95_ms,
            });
        }
    }
    let mut groups: Vec<_> = groups
        .into_iter()
        .map(|((is_other, tag), mut group)| {
            group.value = (!is_other).then_some(tag);
            group
        })
        .collect();
    groups.sort_by(|a, b| {
        a.value
            .is_none()
            .cmp(&b.value.is_none())
            .then_with(|| b.requests.cmp(&a.requests))
            .then_with(|| a.value.cmp(&b.value))
    });
    TagBreakdown {
        bucket_seconds: interval,
        total_requests: groups.iter().map(|group| group.requests).sum(),
        groups,
    }
}

#[cfg(test)]
mod tests;
