use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct BytesRow {
    total: u64,
}

pub fn spawn_stats_engine(ch: Client) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = collect_and_write(&ch).await {
                tracing::error!("stats engine error: {e}");
            }
        }
    });
}

async fn collect_and_write(ch: &Client) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
    let one_hour_ago = (now - chrono::Duration::hours(1))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // ── Span stats ──
    let span_total = query_count(ch, &format!(
        "SELECT count() as count FROM spans_raw WHERE Timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND Timestamp <= parseDateTimeBestEffort('{now_str}')"
    )).await;

    let span_bytes = query_bytes(ch,
        "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND table = 'spans_raw' AND active"
    ).await;

    // ── Log stats ──
    let log_total = query_count(ch, &format!(
        "SELECT count() as count FROM logs WHERE Timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND Timestamp <= parseDateTimeBestEffort('{now_str}')"
    )).await;

    // ── Metric stats ──
    let metric_gauge = query_count(ch, &format!(
        "SELECT count() as count FROM metrics_gauge WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    )).await;
    let metric_sum = query_count(ch, &format!(
        "SELECT count() as count FROM metrics_sum WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    )).await;
    let metric_hist = query_count(ch, &format!(
        "SELECT count() as count FROM metrics_histogram WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    )).await;
    let metric_total = metric_gauge + metric_sum + metric_hist;

    let unique_series = query_count(ch,
        "SELECT uniq(MetricName, Attributes) as count FROM metrics_gauge WHERE TimeUnix >= now() - INTERVAL 1 HOUR"
    ).await;

    // ── Storage ──
    let storage_bytes: u64 = ch.query(
        "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND active"
    ).fetch_one::<BytesRow>().await.map(|r| r.total).unwrap_or(0);

    let storage_rows = query_count(ch,
        "SELECT sum(rows) as count FROM system.parts WHERE database = 'observability' AND active"
    ).await;

    // ── Tiered storage breakdown: data bytes on local disk vs object store ──
    // Classified by joining each part's disk to system.disks.type, matching the
    // on-demand /stats endpoint. Object store = any non-Local disk (S3/MinIO).
    let storage_local_bytes = query_bytes(ch,
        "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
         LEFT JOIN system.disks d ON p.disk_name = d.name \
         WHERE p.database = 'observability' AND p.active AND d.type = 'Local'"
    ).await;
    let storage_object_store_bytes = query_bytes(ch,
        "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
         LEFT JOIN system.disks d ON p.disk_name = d.name \
         WHERE p.database = 'observability' AND p.active AND d.type != 'Local'"
    ).await;

    // ── Local disk capacity (headroom) from system.disks ──
    let disk_local_free_bytes = query_bytes(ch,
        "SELECT sum(free_space) as total FROM system.disks WHERE type = 'Local'"
    ).await;
    let disk_local_total_bytes = query_bytes(ch,
        "SELECT sum(total_space) as total FROM system.disks WHERE type = 'Local'"
    ).await;

    // ── Write all metrics ──
    let metrics: Vec<(&str, f64)> = vec![
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
