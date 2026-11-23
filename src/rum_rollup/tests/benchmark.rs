use std::time::Instant;

use super::*;

#[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
struct QueryStats {
    read_rows: u64,
    read_bytes: u64,
    peak_memory_bytes: u64,
    projections: Vec<String>,
}

async fn insert(
    client: &Client,
    database: &str,
    table: &str,
    rows: u64,
    offset: u64,
    anchor: i64,
) -> anyhow::Result<f64> {
    let density = std::env::var("RUSH_RUM_BENCH_SESSIONS_PER_SECOND")
        .unwrap_or("1".into())
        .parse::<u64>()?;
    anyhow::ensure!(density > 0, "sessions per second must be positive");
    let started = Instant::now();
    client.query(&format!(r#"INSERT INTO {database}.{table}
        (tenant_id, Timestamp, AppName, Environment, SessionId, PagePath, EventType,
         VitalName, VitalValue, VitalRating, DurationMs, BrowserName, Attributes)
        SELECT if(number % 2 = 0, 'a', 'b'),
            fromUnixTimestamp64Second({anchor} - toInt64(intDiv(session, {density}) % 172800)),
            if(session % 7 = 0, 'other', 'shop'), if(session % 13 = 0, 'stage', 'prod'),
            hex(MD5(toString(session))), concat('/route/', toString(session % 20)),
            multiIf(event = 0, 'pageview', event = 1, 'error', event <= 6, 'web_vital', 'interaction'),
            if(event >= 2 AND event <= 6, ['LCP','INP','FCP','CLS','TTFB'][toInt64(event) - 1], ''),
            if(event >= 2 AND event <= 6, toFloat64(100 + cityHash64(session, event) % 5000), 0),
            if(event >= 2 AND event <= 6, ['good','needs-improvement','poor'][session % 3 + 1], ''),
            if(event = 0, toFloat64(100 + session % 500), 0),
            if(session % 3 = 0, 'Firefox', 'Chrome'),
            concat('{{"request.id":"', toString(number), '","payload":"', repeat(hex(cityHash64(number)), 8), '"}}')
        FROM (SELECT number, intDiv(number, 20) AS session, intDiv(number, 2) % 10 AS event FROM numbers({offset}, {rows}))"#))
        .execute().await?;
    Ok(started.elapsed().as_secs_f64() * 1000.0)
}

async fn execute(read: &Client, kind: Kind, plan: &Plan, tag: &str) -> anyhow::Result<f64> {
    let query = plan
        .query(read, "a")
        .with_setting("rush_tenant_id", "a")
        .with_setting("use_query_condition_cache", "0")
        .with_setting("use_query_cache", "0")
        .with_setting("max_threads", "2")
        .with_setting("log_queries", "1")
        .with_setting("log_comment", tag);
    let started = Instant::now();
    match kind {
        Kind::Pages => {
            query.fetch_all::<RumPageRow>().await?;
        }
        Kind::Vitals => {
            query.fetch_all::<RumVitalRow>().await?;
        }
    }
    Ok(started.elapsed().as_secs_f64() * 1000.0)
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

#[tokio::test]
#[ignore = "disposable ClickHouse benchmark; set RUSH_TEST_CLICKHOUSE_* and optionally RUSH_RUM_BENCH_ROWS/RUNS"]
async fn benchmark_rollups() -> anyhow::Result<()> {
    let admin = admin()?;
    let rows = std::env::var("RUSH_RUM_BENCH_ROWS")
        .unwrap_or("2000000".into())
        .parse::<u64>()?;
    let runs = std::env::var("RUSH_RUM_BENCH_RUNS")
        .unwrap_or("11".into())
        .parse::<usize>()?;
    anyhow::ensure!(
        rows >= 100000 && runs >= 3,
        "use at least 100000 rows and three runs"
    );
    let id = uuid::Uuid::new_v4().simple().to_string();
    let database = format!("test_rum_benchmark_{id}");
    let user = format!("test_rum_bench_reader_{id}");
    admin
        .query(&format!("CREATE DATABASE {database}"))
        .execute()
        .await?;
    let result: anyhow::Result<serde_json::Value> = async {
        let anchor = chrono::Utc::now().timestamp() / 300 * 300;
        admin.query(&raw_ddl(&database, "rum")?).execute().await?;
        let populate_ms = insert(&admin, &database, "rum", rows, 0, anchor).await?;
        let base_bytes = admin.query("SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = ? AND table = 'rum'")
            .bind(&database).fetch_one::<u64>().await?;
        let started = Instant::now();
        for kind in [Kind::Pages, Kind::Vitals] {
            storage::prepare(&admin, &database, kind).await?;
        }
        let backfill_ms = started.elapsed().as_secs_f64() * 1000.0;
        let projection_bytes = admin.query("SELECT sum(bytes_on_disk) FROM system.projection_parts WHERE active AND database = ? AND table = 'rum'")
            .bind(&database).fetch_one::<u64>().await?;
        let read = reader(&admin, &database, &user).await?;
        let normal = || request(anchor - 86400 + 17, anchor + 23,
            vec![filter("AppName", FilterOp::Eq, serde_json::json!("shop"))]);
        let mut unsupported = normal();
        unsupported.filters.push(filter("BrowserName", FilterOp::Eq, serde_json::json!("Chrome")));
        let cases = [
            ("pages", Kind::Pages, normal()),
            ("vitals", Kind::Vitals, normal()),
            ("six_hour_vitals", Kind::Vitals, request(anchor - 21600, anchor + 23,
                vec![filter("AppName", FilterOp::Eq, serde_json::json!("shop"))])),
            ("seven_day_vitals", Kind::Vitals, request(anchor - 7 * 86400, anchor + 23,
                vec![filter("AppName", FilterOp::Eq, serde_json::json!("shop"))])),
            ("unsupported_pages", Kind::Pages, unsupported),
            ("short_pages", Kind::Pages, request(anchor - 3600, anchor + 23,
                vec![filter("AppName", FilterOp::Eq, serde_json::json!("shop"))])),
        ];
        let mut measurements = serde_json::Map::new();
        for (name, kind, req) in cases {
            let raw = RumRollups::default().plan(kind, &req, "a");
            let selector = ready();
            let select_started = Instant::now();
            let optimized = selector.select(kind, &req, "a", &read).await;
            let selection_cold_ms = select_started.elapsed().as_secs_f64() * 1000.0;
            let mut samples: [Vec<f64>; 2] = [vec![], vec![]];
            for iteration in 0..=runs {
                let order = if iteration % 2 == 0 { [0, 1] } else { [1, 0] };
                for mode in order {
                    let tag = if iteration == 0 { format!("{id}-warmup") } else { format!("{id}-{name}-{mode}") };
                    let elapsed = execute(&read, kind, if mode == 0 { &raw } else { &optimized }, &tag).await?;
                    if iteration > 0 { samples[mode].push(elapsed); }
                }
            }
            let mut modes = serde_json::Map::new();
            for (mode, label) in [(0, "raw"), (1, "optimized")] {
                let mut sorted = samples[mode].clone();
                sorted.sort_by(f64::total_cmp);
                modes.insert(label.into(), serde_json::json!({
                    "median_ms": median(&sorted),
                    "p95_ms": sorted[(runs as f64 * 0.95).ceil() as usize - 1],
                    "uses_rollup_plan": mode == 1 && optimized.uses_rollup,
                    "selection_cold_ms": if mode == 1 { selection_cold_ms } else { 0.0 },
                }));
            }
            if let Some((from, to)) = full_bucket_window(&req, kind)
                .filter(|_| filters(kind, &req).iter().all(|filter| kind.supports_filter(filter))) {
                let dense = read.query(&density_sql(kind, "a", &filters(kind, &req), from, to))
                    .with_setting("rush_tenant_id", "a")
                    .with_setting("preferred_optimize_projection_name", kind.name())
                    .fetch_one::<u8>().await?;
                anyhow::ensure!(optimized.uses_rollup == (dense != 0), "density selector disagrees with stored counts");
                modes.get_mut("optimized").unwrap().as_object_mut().unwrap()
                    .insert("dense_buckets".into(), serde_json::json!(dense != 0));
            }
            if matches!(name, "pages" | "vitals") {
                let diagnostic = Plan {
                    sql: format!("EXPLAIN projections = 1, indexes = 1 {}", optimized.sql),
                    uses_rollup: optimized.uses_rollup,
                    projection: optimized.projection,
                };
                let explain = diagnostic.query(&read, "a").with_setting("rush_tenant_id", "a")
                    .fetch_all::<String>().await?;
                let relevant: Vec<_> = explain.into_iter().filter(|line|
                    ["Projection", "ReadFrom", "Granules", "Parts", "Condition", "Description", "Filter"].iter().any(|word| line.contains(word))
                ).collect();
                modes.get_mut("optimized").unwrap().as_object_mut().unwrap()
                    .insert("explain".into(), serde_json::json!(relevant));
            }
            measurements.insert(name.into(), serde_json::Value::Object(modes));
        }
        // Compare ingestion on otherwise identical tables; alternate order and
        // discard the first pair to reduce warm-up/order effects.
        for table in ["insert_raw", "insert_rollup"] {
            admin.query(&raw_ddl(&database, table)?).execute().await?;
        }
        for kind in [Kind::Pages, Kind::Vitals] {
            admin.query(&format!("ALTER TABLE {database}.insert_rollup ADD PROJECTION {}", kind.projection_declaration())).execute().await?;
        }
        let mut inserts: [Vec<f64>; 2] = [vec![], vec![]];
        for iteration in 0..4 {
            let order = if iteration % 2 == 0 { [0, 1] } else { [1, 0] };
            for mode in order {
                let elapsed = insert(&admin, &database, if mode == 0 { "insert_raw" } else { "insert_rollup" }, 100000, iteration * 100000, anchor).await?;
                if iteration > 0 { inserts[mode].push(elapsed); }
            }
        }
        admin.query("SYSTEM FLUSH LOGS").execute().await?;
        for (name, modes) in &mut measurements {
            for (mode, label) in [(0, "raw"), (1, "optimized")] {
                let stats = admin.query(
                    "SELECT toUInt64(avg(read_rows)) AS read_rows, toUInt64(avg(read_bytes)) AS read_bytes, \
                     max(memory_usage) AS peak_memory_bytes, arrayDistinct(arrayFlatten(groupArray(projections))) AS projections \
                     FROM system.query_log WHERE type = 'QueryFinish' AND is_initial_query = 1 AND log_comment = ?"
                ).bind(format!("{id}-{name}-{mode}")).fetch_one::<QueryStats>().await?;
                modes[label].as_object_mut().unwrap().extend(serde_json::to_value(stats)?.as_object().unwrap().clone());
            }
        }
        Ok(serde_json::json!({
            "clickhouse_version": admin.query("SELECT version()").fetch_one::<String>().await?,
            "rows": rows, "runs": runs, "threads": 2, "cache": "warm", "tenant_row_policy": true,
            "sessions_per_second": std::env::var("RUSH_RUM_BENCH_SESSIONS_PER_SECOND").unwrap_or("1".into()).parse::<u64>()?,
            "base_bytes": base_bytes, "projection_bytes": projection_bytes,
            "storage_overhead_pct": projection_bytes as f64 / base_bytes as f64 * 100.0,
            "populate_ms": populate_ms, "backfill_ms": backfill_ms,
            "insert_rows_per_batch": 100000,
            "insert_median_ms": { "raw": median(&inserts[0]), "optimized": median(&inserts[1]) },
            "queries": measurements,
        }))
    }.await;
    let drop_db = admin
        .query(&format!("DROP DATABASE {database} SYNC"))
        .execute()
        .await;
    let drop_user = admin
        .query(&format!("DROP USER IF EXISTS {user}"))
        .execute()
        .await;
    let report = result?;
    drop_db?;
    drop_user?;
    println!("RUM_BENCHMARK={report}");
    for name in ["pages", "vitals"] {
        anyhow::ensure!(
            !report["queries"][name]["optimized"]["uses_rollup_plan"]
                .as_bool()
                .unwrap()
                || !report["queries"][name]["optimized"]["projections"]
                    .as_array()
                    .unwrap()
                    .is_empty(),
            "{name} did not use a projection"
        );
    }
    Ok(())
}
