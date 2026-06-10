use clickhouse::Client;
use std::sync::Arc;
use crate::spool::IngestBuffer;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct BytesRow {
    total: u64,
}

pub fn spawn_stats_engine(ch: Client, buffer: Arc<IngestBuffer>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = collect_and_write(&ch, &buffer).await {
                tracing::error!("stats engine error: {e}");
            }
        }
    });
}

async fn collect_and_write(ch: &Client, buffer: &IngestBuffer) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
    let one_hour_ago = (now - chrono::Duration::hours(1))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // All of these queries are independent — run them concurrently so the tick's
    // wall time is the slowest query, not the sum of all of them (~14 round trips).
    let q_spans = format!(
        "SELECT count() as count FROM spans_raw WHERE Timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND Timestamp <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_logs = format!(
        "SELECT count() as count FROM logs WHERE Timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND Timestamp <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_gauge = format!(
        "SELECT count() as count FROM metrics_gauge WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_sum = format!(
        "SELECT count() as count FROM metrics_sum WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_hist = format!(
        "SELECT count() as count FROM metrics_histogram WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let (
        span_total,
        span_bytes,
        log_total,
        metric_gauge,
        metric_sum,
        metric_hist,
        unique_series,
        storage_bytes,
        storage_rows,
        storage_local_bytes,
        storage_object_store_bytes,
        disk_local_free_bytes,
        disk_local_total_bytes,
        buf_oldest,
    ) = tokio::join!(
        query_count(ch, &q_spans),
        query_bytes(ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND table = 'spans_raw' AND active"
        ),
        query_count(ch, &q_logs),
        query_count(ch, &q_gauge),
        query_count(ch, &q_sum),
        query_count(ch, &q_hist),
        query_count(ch,
            "SELECT uniq(MetricName, Attributes) as count FROM metrics_gauge WHERE TimeUnix >= now() - INTERVAL 1 HOUR"
        ),
        query_bytes(ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND active"
        ),
        query_count(ch,
            "SELECT sum(rows) as count FROM system.parts WHERE database = 'observability' AND active"
        ),
        // Tiered storage breakdown: data bytes on local disk vs object store.
        // Classified by joining each part's disk to system.disks.type, matching
        // the on-demand /stats endpoint. Object store = any non-Local disk.
        query_bytes(ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type = 'Local'"
        ),
        query_bytes(ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type != 'Local'"
        ),
        // Local disk capacity (headroom) from system.disks.
        query_bytes(ch,
            "SELECT sum(free_space) as total FROM system.disks WHERE type = 'Local'"
        ),
        query_bytes(ch,
            "SELECT sum(total_space) as total FROM system.disks WHERE type = 'Local'"
        ),
        // Ingest buffer (durable spool) replay lag.
        buffer.oldest_age_secs(),
    );
    let metric_total = metric_gauge + metric_sum + metric_hist;
    let buf_oldest = buf_oldest.unwrap_or(0);

    // ── Write all metrics ──
    let metrics: Vec<(&str, f64)> = vec![
        ("rush_stats_ingest_buffer_pending_bytes", buffer.total_bytes() as f64),
        ("rush_stats_ingest_buffer_pending_count", buffer.segment_count() as f64),
        ("rush_stats_ingest_buffer_oldest_age_secs", buf_oldest as f64),
        // Cumulative counter — drain rate = rate(rush_stats_ingest_buffer_committed_total).
        ("rush_stats_ingest_buffer_committed_total", buffer.committed_total() as f64),
        ("rush_stats_span_events_total", span_total as f64),
        ("rush_stats_span_events_bytes", span_bytes as f64),
        ("rush_stats_logs_total", log_total as f64),
        ("rush_stats_metrics_total", metric_total as f64),
        ("rush_stats_unique_series", unique_series as f64),
        ("rush_stats_storage_bytes", storage_bytes as f64),
        ("rush_stats_storage_rows", storage_rows as f64),
        // Tiered storage: where the data physically lives.
        ("rush_stats_storage_local_bytes", storage_local_bytes as f64),
        ("rush_stats_storage_object_store_bytes", storage_object_store_bytes as f64),
        // Local disk capacity, for headroom / move-pressure monitoring.
        ("rush_stats_disk_local_free_bytes", disk_local_free_bytes as f64),
        ("rush_stats_disk_local_total_bytes", disk_local_total_bytes as f64),
    ];

    let values: Vec<String> = metrics.iter().map(|(name, val)| {
        format!(
            "({{}}, '', '', '', {{}}, 0, '', 'wide-stats-engine', '{name}', '', '', {{}}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])"
        )
    }).collect();

    let sql = format!(
        "INSERT INTO metrics_gauge \
         (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
          ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
          MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
          Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
          Exemplars.SpanId, Exemplars.TraceId) VALUES {}",
        values.join(", ")
    );

    if let Err(e) = ch.query(&sql).execute().await {
        tracing::warn!("stats engine: metric write failed: {e}");
    }

    tracing::debug!("stats engine: wrote {} metrics", metrics.len());
    Ok(())
}

async fn query_count(ch: &Client, sql: &str) -> u64 {
    ch.query(sql).fetch_one::<CountRow>().await.map(|r| r.count).unwrap_or(0)
}

async fn query_bytes(ch: &Client, sql: &str) -> u64 {
    ch.query(sql).fetch_one::<BytesRow>().await.map(|r| r.total).unwrap_or(0)
}
