use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, Extension};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;

#[derive(Debug, Deserialize)]
pub struct StatsRequest {
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Deserialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct UsageSignalStats {
    pub events_count: u64,
    pub bytes_count: u64,
}

#[derive(Debug, Serialize)]
pub struct StatsUsage {
    pub traces: UsageSignalStats,
    pub logs: UsageSignalStats,
    pub metrics: UsageSignalStats,
    pub rum: UsageSignalStats,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub spans: SignalStats,
    pub logs: SignalStats,
    pub metrics: MetricStats,
    pub storage: Vec<TableStorage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<StatsUsage>,
}

#[derive(Debug, Serialize)]
pub struct SignalStats {
    pub total_events: u64,
    pub events_per_sec: f64,
    pub events_today: u64,
}

#[derive(Debug, Serialize)]
pub struct MetricStats {
    pub total_datapoints: u64,
    pub datapoints_per_sec: f64,
    pub datapoints_today: u64,
    pub unique_series: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TableStorage {
    #[serde(rename = "table_name")]
    pub table_name: String,
    #[serde(rename = "total_rows")]
    pub total_rows: u64,
    #[serde(rename = "bytes_on_disk")]
    pub bytes_on_disk: u64,
    #[serde(rename = "compressed_bytes")]
    pub compressed_bytes: u64,
    #[serde(rename = "uncompressed_bytes")]
    pub uncompressed_bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct CountResult {
    count: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct RateResult {
    rate: f64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct UsageRow {
    signal: String,
    events: u64,
    bytes: u64,
}

pub async fn get_stats(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<StatsRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = tenant_id.replace('\'', "\\'");
    let (from, to) = if let Some(tr) = &req.time_range {
        (tr.from.clone(), tr.to.clone())
    } else {
        let to = chrono::Utc::now();
        let from = to - chrono::Duration::hours(1);
        (from.to_rfc3339(), to.to_rfc3339())
    };

    let today_start = chrono::Utc::now().date_naive().to_string();

    let err = |e: clickhouse::error::Error| {
        tracing::error!("stats query failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
    };

    // ── Span stats ──
    let span_total: u64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() as count FROM otel_traces WHERE tenant_id = '{escaped_tenant}' AND Timestamp >= parseDateTimeBestEffort('{from}') AND Timestamp <= parseDateTimeBestEffort('{to}')"
        ), tenant_id)
        .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);

    let span_rate: f64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() / greatest(1, dateDiff('second', parseDateTimeBestEffort('{from}'), parseDateTimeBestEffort('{to}'))) as rate FROM otel_traces WHERE tenant_id = '{escaped_tenant}' AND Timestamp >= parseDateTimeBestEffort('{from}') AND Timestamp <= parseDateTimeBestEffort('{to}')"
        ), tenant_id)
        .fetch_one::<RateResult>().await.map(|r| r.rate).unwrap_or(0.0);

    let span_today: u64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() as count FROM otel_traces WHERE tenant_id = '{escaped_tenant}' AND toDate(Timestamp) = '{today_start}'"
        ), tenant_id)
        .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);

    // ── Log stats ──
    let log_total: u64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() as count FROM otel_logs WHERE tenant_id = '{escaped_tenant}' AND Timestamp >= parseDateTimeBestEffort('{from}') AND Timestamp <= parseDateTimeBestEffort('{to}')"
        ), tenant_id)
        .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);

    let log_rate: f64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() / greatest(1, dateDiff('second', parseDateTimeBestEffort('{from}'), parseDateTimeBestEffort('{to}'))) as rate FROM otel_logs WHERE tenant_id = '{escaped_tenant}' AND Timestamp >= parseDateTimeBestEffort('{from}') AND Timestamp <= parseDateTimeBestEffort('{to}')"
        ), tenant_id)
        .fetch_one::<RateResult>().await.map(|r| r.rate).unwrap_or(0.0);

    let log_today: u64 = crate::tenant_query(&state.ch, &format!(
            "SELECT count() as count FROM otel_logs WHERE tenant_id = '{escaped_tenant}' AND toDate(Timestamp) = '{today_start}'"
        ), tenant_id)
        .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);

    // ── Metric stats ──
    let metric_total: u64 = {
        let g: u64 = crate::tenant_query(&state.ch, &format!(
                "SELECT count() as count FROM otel_metrics_gauge WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
            ), tenant_id)
            .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);
        let s: u64 = crate::tenant_query(&state.ch, &format!(
                "SELECT count() as count FROM otel_metrics_sum WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
            ), tenant_id)
            .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);
        let h: u64 = crate::tenant_query(&state.ch, &format!(
                "SELECT count() as count FROM otel_metrics_histogram WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
            ), tenant_id)
            .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);
        g + s + h
    };

    let metric_rate: f64 = {
        let range_secs = format!(
            "greatest(1, dateDiff('second', parseDateTimeBestEffort('{from}'), parseDateTimeBestEffort('{to}')))"
        );
        let g: f64 = crate::tenant_query(&state.ch, &format!(
                "SELECT count() / {range_secs} as rate FROM otel_metrics_gauge WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
            ), tenant_id)
            .fetch_one::<RateResult>().await.map(|r| r.rate).unwrap_or(0.0);
        let s: f64 = crate::tenant_query(&state.ch, &format!(
                "SELECT count() / {range_secs} as rate FROM otel_metrics_sum WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
            ), tenant_id)
            .fetch_one::<RateResult>().await.map(|r| r.rate).unwrap_or(0.0);
        g + s
    };

    let metric_today: u64 = {
        let g: u64 = crate::tenant_query(&state.ch, &format!("SELECT count() as count FROM otel_metrics_gauge WHERE tenant_id = '{escaped_tenant}' AND toDate(TimeUnix) = '{today_start}'"), tenant_id)
            .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);
        let s: u64 = crate::tenant_query(&state.ch, &format!("SELECT count() as count FROM otel_metrics_sum WHERE tenant_id = '{escaped_tenant}' AND toDate(TimeUnix) = '{today_start}'"), tenant_id)
            .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);
        g + s
    };

    let unique_series: u64 = crate::tenant_query(&state.ch, &format!("SELECT uniq(MetricName, Attributes) as count FROM otel_metrics_gauge WHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= now() - INTERVAL 1 HOUR"), tenant_id)
        .fetch_one::<CountResult>().await.map(|r| r.count).unwrap_or(0);

    // ── Storage stats from system.parts ──
    let storage: Vec<TableStorage> = state.ch
        .query(
            "SELECT \
                 table as table_name, \
                 sum(rows) as total_rows, \
                 sum(bytes_on_disk) as bytes_on_disk, \
                 sum(data_compressed_bytes) as compressed_bytes, \
                 sum(data_uncompressed_bytes) as uncompressed_bytes \
             FROM system.parts \
             WHERE database = 'observability' AND active \
             GROUP BY table \
             ORDER BY bytes_on_disk DESC"
        )
        .fetch_all::<TableStorage>()
        .await
        .unwrap_or_default();

    // ── Per-tenant ingest usage from tenant_usage table ──
    let usage_rows: Vec<UsageRow> = state.ch
        .query(&format!(
            "SELECT signal, sum(events_count) AS events, sum(bytes_count) AS bytes \
             FROM observability.tenant_usage \
             WHERE tenant_id = '{escaped_tenant}' AND bucket >= toStartOfDay(now()) \
             GROUP BY signal"
        ))
        .fetch_all::<UsageRow>()
        .await
        .unwrap_or_default();

    let mut usage_traces = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_logs = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_metrics = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_rum = UsageSignalStats { events_count: 0, bytes_count: 0 };

    for row in &usage_rows {
        match row.signal.as_str() {
            "traces" => { usage_traces.events_count = row.events; usage_traces.bytes_count = row.bytes; }
            "logs" => { usage_logs.events_count = row.events; usage_logs.bytes_count = row.bytes; }
            "metrics" => { usage_metrics.events_count = row.events; usage_metrics.bytes_count = row.bytes; }
            "rum" => { usage_rum.events_count = row.events; usage_rum.bytes_count = row.bytes; }
            _ => {}
        }
    }

    let stats_usage = if usage_rows.is_empty() {
        None
    } else {
        Some(StatsUsage {
            traces: usage_traces,
            logs: usage_logs,
            metrics: usage_metrics,
            rum: usage_rum,
        })
    };

    Ok(Json(StatsResponse {
        spans: SignalStats {
            total_events: span_total,
            events_per_sec: span_rate,
            events_today: span_today,
        },
        logs: SignalStats {
            total_events: log_total,
            events_per_sec: log_rate,
            events_today: log_today,
        },
        metrics: MetricStats {
            total_datapoints: metric_total,
            datapoints_per_sec: metric_rate,
            datapoints_today: metric_today,
            unique_series,
        },
        storage,
        usage: stats_usage,
    }))
}
