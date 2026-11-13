//! Read-only review candidates from bounded telemetry samples. Never return log
//! bodies, URLs, trace IDs, metric attribute values, or fingerprints to clients.
use crate::{
    AppState, TenantContext,
    handlers::{
        telemetry_quality::{QualityParams, QualitySource, read_bounded, validated_minutes},
        users::require_principal,
    },
    query_builder::escape_string_literal,
};
use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const LIMIT: usize = 1_000;
const MAX_FINDINGS: usize = 200;
// Leave a 10% margin above a normal 15-second reporting interval.
const HIGH_CADENCE_PER_MINUTE: f64 = 4.4;
const SOURCES: [&str; 7] = [
    "logs",
    "spans",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteRule {
    pub id: String,
    pub title: String,
    pub signal: String,
    pub threshold: String,
    pub recommendation: String,
    pub safeguard: String,
}
fn rules() -> Vec<WasteRule> {
    [
        ("verbose_logs", "Verbose log volume", "logs", "At least 100 logs with numeric or recognized text severity; TRACE/DEBUG is at least 25%.", "Review the SDK or application log level for this service and environment. Use temporary scoped debug logging during investigations.", "Keep warnings, errors, audit logs and incident diagnostics. Debug output may be intentional outside production."),
        ("repeated_logs", "Repeated low-severity messages", "logs", "At least 20 identical non-empty log bodies of severity INFO or below in one UTC minute.", "Review the repeating message at its source. Log state changes or summarize repeated events, and check for retry loops before adding a rate limit.", "The candidate count excludes the first message per burst. Matching bodies can be legitimate independent events; preserve errors and required request/audit records."),
        ("large_logs", "Large low-severity log bodies", "logs", "At least three logs of severity INFO or below with bodies of 16 KiB or more.", "Review whether these records contain repeated payload dumps. Prefer concise summaries and retain detailed payloads only where troubleshooting requires them.", "Body bytes are uncompressed UTF-8 bytes in this sample. They are not stored, compressed, billable or guaranteed removable bytes."),
        ("health_spans", "Successful health-check spans", "traces", "At least 50 spans; successful GET/HEAD health or metrics endpoints are at least 20 spans and 20%.", "Review successful probe spans separately from application traffic. Consider scoped sampling of routine successes at the instrumentation or Collector layer.", "Keep failed probes, latency anomalies and useful trace relationships. Verify the endpoint is operational traffic before changing collection."),
        ("static_spans", "Successful static-asset spans", "traces", "At least 50 spans; successful GET/HEAD static assets are at least 20 spans and 20%.", "Review whether routine static-asset requests need full tracing. Consider scoped sampling for successful asset downloads.", "Keep failures, slow downloads and browser-to-server investigations. File extensions are a heuristic; confirm route semantics."),
        ("short_spans", "Very short internal spans", "traces", "At least 100 spans; successful child INTERNAL spans shorter than 1 ms are at least 80%.", "Review very fine-grained instrumentation. Keep spans that explain the critical path and consider profiling for frequent local operations.", "Short duration alone does not prove low value. Preserve important operations and trace structure; do not drop all short spans."),
        ("duplicate_spans", "Repeated identical spans", "traces", "At least five extra copies of identical stored spans with non-empty trace and span identities.", "Check for redundant exporters, duplicate Collector receivers or retry behavior that exports the same span more than once.", "Only identical stored span fingerprints count. Different content with the same span ID does not count; inspect the export path before removing a destination."),
        ("metric_cardinality", "High-cardinality metric attributes", "metrics", "Per metric/source: at least 200 datapoints, at least 100 distinct label values, and more than 80% distinct. First 64 attributes inspected.", "Review whether this attribute contains request, session or other unbounded identifiers. Prefer bounded dimensions or an appropriate histogram.", "Preserve required series identity, instance/tenant boundaries and meaningful dimensions. Distinct hashed values are sampled evidence, not an exact population series count."),
        ("metric_cadence", "Frequent metric reporting", "metrics", "Per series: at least 10 distinct timestamps spanning at least 60 seconds, above four intervals/minute by more than 10% to allow scheduling jitter.", "Review the SDK export or collection interval for this series. A longer interval may reduce volume if alerting and diagnostic requirements allow it.", "Distinct timestamps prevent duplicate points inflating cadence. Preserve counter semantics, resets and required alert resolution. This is observed cadence, not a window-wide rate."),
    ].into_iter().map(|(id,title,signal,threshold,recommendation,safeguard)| WasteRule { id:id.into(),title:title.into(),signal:signal.into(),threshold:threshold.into(),recommendation:recommendation.into(),safeguard:safeguard.into() }).collect()
}

#[derive(Debug, Clone, Default, Deserialize, clickhouse::Row)]
struct Sample {
    service: String,
    metric: String,
    observed_at: i64,
    severity: u8,
    body_hash: u64,
    body_bytes: u64,
    health: u8,
    static_asset: u8,
    short_internal: u8,
    span_identity: u8,
    record_hash: u64,
    series_hash: u64,
    label_keys: Vec<String>,
    label_hashes: Vec<u64>,
    labels_capped: u8,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteEvidence {
    pub label: String,
    pub value: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteFinding {
    pub id: String,
    pub rule_id: String,
    pub service: String,
    pub signal: String,
    pub subject: String,
    pub source: String,
    pub confidence: String,
    pub candidate_records: u64,
    pub sampled_records: u64,
    pub candidate_body_bytes: Option<u64>,
    pub first_observed: String,
    pub last_observed: String,
    pub evidence: Vec<WasteEvidence>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteCheck {
    pub rule_id: String,
    pub status: String,
    pub evaluated_groups: usize,
    pub insufficient_groups: usize,
    pub findings: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteResponse {
    pub evaluated_at: String,
    pub from: String,
    pub to: String,
    pub minutes: u32,
    pub sample_limit_per_source: usize,
    pub cache_hit: bool,
    pub cache_ttl_seconds: u64,
    pub partial: bool,
    pub sources: Vec<QualitySource>,
    pub rules: Vec<WasteRule>,
    pub checks: Vec<WasteCheck>,
    pub findings: Vec<WasteFinding>,
    pub finding_count: usize,
    pub omitted_findings: usize,
    pub services: Vec<String>,
    pub omitted_services: usize,
    pub sampled_records: usize,
    pub sampled_log_body_bytes: u64,
    pub methodology: String,
}
fn signal(table: &str) -> &'static str {
    match table {
        "logs" => "logs",
        "spans" => "traces",
        _ => "metrics",
    }
}

fn sample_sql(table: &str, tenant: &str, from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    assert!(SOURCES.contains(&table));
    let (fields, time, order) = match table {
        "logs" => ("ServiceName AS service, '' AS metric, toUnixTimestamp64Milli(Timestamp) AS observed_at, toUInt8(if(SeverityNumber BETWEEN 1 AND 24, SeverityNumber, transform(upperUTF8(trimBoth(SeverityText)), ['TRACE','DEBUG','INFO','INFORMATION','NOTICE','WARN','WARNING','ERROR','ERR','FATAL','CRITICAL'], [1,5,9,9,9,13,13,17,17,21,21], 0))) AS severity, cityHash64(Body) AS body_hash, toUInt64(length(Body)) AS body_bytes, toUInt8(0) AS health, toUInt8(0) AS static_asset, toUInt8(0) AS short_internal, toUInt8(0) AS span_identity, toUInt64(0) AS record_hash, toUInt64(0) AS series_hash, CAST([], 'Array(String)') AS label_keys, CAST([], 'Array(UInt64)') AS label_hashes, toUInt8(0) AS labels_capped".to_string(), "Timestamp", ""),
        "spans" => {
            let method = "coalesce(nullIf(http_method, ''), JSONExtractString(attributes, 'http.request.method'))";
            let success = format!("http_status_code BETWEEN 200 AND 299 AND status NOT IN ('ERROR', 'STATUS_CODE_ERROR') AND {method} IN ('GET','HEAD')");
            let path = "lowerUTF8(splitByChar(char(63), http_path)[1])";
            let health = format!("{success} AND trimRight({path}, '/') IN ('/health','/healthz','/ready','/readyz','/live','/livez','/liveness','/readiness','/ping','/metrics')");
            let assets = format!("{success} AND match({path}, '[.](js|css|map|woff|woff2|ttf|ico|png|jpg|jpeg|gif|svg|webp|avif)$')");
            (format!("service_name AS service, '' AS metric, toUnixTimestamp64Milli(timestamp) AS observed_at, toUInt8(0) AS severity, toUInt64(0) AS body_hash, toUInt64(0) AS body_bytes, toUInt8({health}) AS health, toUInt8({assets}) AS static_asset, toUInt8(kind IN ('INTERNAL','SPAN_KIND_INTERNAL') AND notEmpty(parent_span_id) AND duration_ns > 0 AND duration_ns < 1000000 AND status NOT IN ('ERROR','STATUS_CODE_ERROR')) AS short_internal, toUInt8(notEmpty(trace_id) AND notEmpty(span_id)) AS span_identity, cityHash64(tuple(tenant_id,timestamp,trace_id,span_id,parent_span_id,service_name,span_name,kind,status,duration_ns,http_method,http_path,http_status_code,attributes,event_names,event_timestamps,event_attributes,link_trace_ids,link_span_ids,resource_attributes)) AS record_hash, toUInt64(0) AS series_hash, CAST([], 'Array(String)') AS label_keys, CAST([], 'Array(UInt64)') AS label_hashes, toUInt8(0) AS labels_capped"), "timestamp", " ORDER BY timestamp DESC")
        },
        _ => {
            let monotonic = if table == "metrics_sum" { "IsMonotonic" } else { "0" };
            let temporality = if matches!(table,"metrics_sum"|"metrics_histogram"|"metrics_exp_histogram") { "AggregationTemporality" } else { "0" };
            (format!("ServiceName AS service, MetricName AS metric, toUnixTimestamp64Milli(TimeUnix) AS observed_at, toUInt8(0) AS severity, toUInt64(0) AS body_hash, toUInt64(0) AS body_bytes, toUInt8(0) AS health, toUInt8(0) AS static_asset, toUInt8(0) AS short_internal, toUInt8(0) AS span_identity, toUInt64(0) AS record_hash, cityHash64(tuple(MetricName,MetricUnit,ScopeName,ScopeVersion,ScopeSchemaUrl,ResourceSchemaUrl,arraySort(mapKeys(ResourceAttributes)),arrayMap(k -> ResourceAttributes[k], arraySort(mapKeys(ResourceAttributes))),arraySort(mapKeys(ScopeAttributes)),arrayMap(k -> ScopeAttributes[k], arraySort(mapKeys(ScopeAttributes))),arraySort(mapKeys(Attributes)),arrayMap(k -> Attributes[k], arraySort(mapKeys(Attributes))),{temporality},{monotonic})) AS series_hash, arraySlice(mapKeys(Attributes),1,64) AS label_keys, arrayMap(v -> cityHash64(v), arraySlice(mapValues(Attributes),1,64)) AS label_hashes, toUInt8(length(Attributes) > 64) AS labels_capped"), "TimeUnix", " ORDER BY MetricName, ServiceName, TimeUnix")
        }
    };
    let tenant = escape_string_literal(tenant);
    let from = from.format("%Y-%m-%d %H:%M:%S%.9f");
    let to = to.format("%Y-%m-%d %H:%M:%S%.9f");
    format!(
        "SELECT {fields} FROM {table} PREWHERE tenant_id = '{tenant}' AND {time} >= toDateTime64('{from}',9,'UTC') AND {time} <= toDateTime64('{to}',9,'UTC'){order} LIMIT {}",
        LIMIT + 1
    )
}

pub async fn get_advisor(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<QualityParams>,
) -> Result<Json<WasteResponse>, (StatusCode, String)> {
    require_principal(&state, &headers).await?;
    let minutes = validated_minutes(&params)?;
    if crate::query_governor::active_budget()
        .is_some_and(|b| u64::from(minutes) * 60 > b.max_time_range_secs)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "time range exceeds the configured workload limit".into(),
        ));
    }
    let (mut report, hit) = state
        .waste_cache
        .cached(
            &tenant.tenant_id,
            minutes,
            analyze(&state.ch, &tenant.tenant_id, minutes),
        )
        .await?;
    report.cache_hit = hit;
    Ok(Json(report))
}
async fn analyze(ch: &clickhouse::Client, tenant: &str, minutes: u32) -> WasteResponse {
    let to = Utc::now();
    let from = to - chrono::Duration::minutes(i64::from(minutes));
    let mut records = Vec::new();
    let mut sources = Vec::new();
    for table in SOURCES {
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            read_bounded::<Sample>(ch, &sample_sql(table, tenant, from, to), tenant),
        )
        .await;
        match result {
            Ok(Ok(mut rows)) => {
                let capped = rows.len() > LIMIT;
                rows.truncate(LIMIT);
                sources.push(QualitySource {
                    table: table.into(),
                    signal: signal(table).into(),
                    status: "available".into(),
                    sampled_records: rows.len(),
                    capped,
                });
                records.extend(rows.into_iter().map(|row| (table, row)));
            }
            _ => sources.push(QualitySource {
                table: table.into(),
                signal: signal(table).into(),
                status: "unavailable".into(),
                sampled_records: 0,
                capped: false,
            }),
        }
    }
    evaluate(records, sources, from, to, minutes)
}

fn timestamp(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|v| v.to_rfc3339())
        .unwrap_or_default()
}
fn evidence(label: &str, value: impl ToString) -> WasteEvidence {
    WasteEvidence {
        label: label.into(),
        value: value.to_string(),
    }
}
fn finding(
    rule: &str,
    source: &str,
    service: &str,
    subject: &str,
    rows: &[&Sample],
    candidates: &[&Sample],
    body_bytes: Option<u64>,
    facts: Vec<WasteEvidence>,
) -> WasteFinding {
    let mut hash = Sha256::new();
    for value in [rule, source, service, subject] {
        hash.update((value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    WasteFinding {
        id: hex::encode(hash.finalize()),
        rule_id: rule.into(),
        source: source.into(),
        service: service.into(),
        subject: subject.into(),
        signal: signal(source).into(),
        confidence: if rule == "duplicate_spans" {
            "strong"
        } else {
            "review"
        }
        .into(),
        candidate_records: candidates.len() as u64,
        sampled_records: rows.len() as u64,
        candidate_body_bytes: body_bytes,
        first_observed: timestamp(
            candidates
                .iter()
                .map(|r| r.observed_at)
                .min()
                .unwrap_or_default(),
        ),
        last_observed: timestamp(
            candidates
                .iter()
                .map(|r| r.observed_at)
                .max()
                .unwrap_or_default(),
        ),
        evidence: facts,
    }
}
fn check(checks: &mut BTreeMap<String, WasteCheck>, rule: &str, enough: bool, found: bool) {
    let entry = checks.entry(rule.into()).or_insert(WasteCheck {
        rule_id: rule.into(),
        status: "not_evaluated".into(),
        evaluated_groups: 0,
        insufficient_groups: 0,
        findings: 0,
    });
    if enough {
        entry.evaluated_groups += 1;
    } else {
        entry.insufficient_groups += 1;
    }
    if found {
        entry.findings += 1;
    }
    entry.status = if entry.findings > 0 {
        "finding"
    } else if entry.evaluated_groups > 0 {
        "no_candidates"
    } else {
        "not_evaluated"
    }
    .into();
}

fn evaluate(
    records: Vec<(&str, Sample)>,
    sources: Vec<QualitySource>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    minutes: u32,
) -> WasteResponse {
    let mut groups: BTreeMap<(&str, &str), Vec<&Sample>> = BTreeMap::new();
    for (table, row) in &records {
        groups
            .entry((table, row.service.as_str()))
            .or_default()
            .push(row);
    }
    let mut findings = Vec::new();
    let mut checks = BTreeMap::new();
    for ((table, service), rows) in &groups {
        if *table == "logs" {
            let known: Vec<_> = rows
                .iter()
                .copied()
                .filter(|r| (1..=24).contains(&r.severity))
                .collect();
            let verbose: Vec<_> = known.iter().copied().filter(|r| r.severity <= 8).collect();
            let found = known.len() >= 100 && verbose.len() * 4 >= known.len();
            check(&mut checks, "verbose_logs", known.len() >= 100, found);
            if found {
                findings.push(finding(
                    "verbose_logs",
                    table,
                    service,
                    "",
                    &known,
                    &verbose,
                    Some(verbose.iter().map(|r| r.body_bytes).sum()),
                    vec![
                        evidence("TRACE/DEBUG records", verbose.len()),
                        evidence("Known-severity logs", known.len()),
                    ],
                ));
            }
            let low: Vec<_> = known.iter().copied().filter(|r| r.severity <= 12).collect();
            let large: Vec<_> = low
                .iter()
                .copied()
                .filter(|r| r.body_bytes >= 16 * 1024)
                .collect();
            check(&mut checks, "large_logs", low.len() >= 3, large.len() >= 3);
            if large.len() >= 3 {
                findings.push(finding(
                    "large_logs",
                    table,
                    service,
                    "",
                    &low,
                    &large,
                    Some(large.iter().map(|r| r.body_bytes).sum()),
                    vec![
                        evidence("Bodies of at least 16 KiB", large.len()),
                        evidence("Eligible low-severity logs", low.len()),
                    ],
                ));
            }
            let mut bursts: BTreeMap<(i64, u64), Vec<&Sample>> = BTreeMap::new();
            for row in &low {
                if row.body_bytes > 0 {
                    bursts
                        .entry((row.observed_at.div_euclid(60_000), row.body_hash))
                        .or_default()
                        .push(row);
                }
            }
            for burst in bursts.values_mut() {
                burst.sort_by_key(|row| row.observed_at);
            }
            let burst_count = bursts.values().filter(|b| b.len() >= 20).count();
            let repeats: Vec<_> = bursts
                .values()
                .filter(|b| b.len() >= 20)
                .flat_map(|b| b.iter().skip(1).copied())
                .collect();
            check(
                &mut checks,
                "repeated_logs",
                low.len() >= 20,
                !repeats.is_empty(),
            );
            if !repeats.is_empty() {
                findings.push(finding(
                    "repeated_logs",
                    table,
                    service,
                    "",
                    &low,
                    &repeats,
                    Some(repeats.iter().map(|r| r.body_bytes).sum()),
                    vec![
                        evidence("Sampled minute/message bursts", burst_count),
                        evidence("Copies after first in each burst", repeats.len()),
                    ],
                ));
            }
        } else if *table == "spans" {
            for (rule, min_count, min_affected, ratio) in [
                ("health_spans", 50, 20, 20),
                ("static_spans", 50, 20, 20),
                ("short_spans", 100, 50, 80),
            ] {
                let candidates: Vec<_> = rows
                    .iter()
                    .copied()
                    .filter(|r| match rule {
                        "health_spans" => r.health != 0,
                        "static_spans" => r.static_asset != 0,
                        _ => r.short_internal != 0,
                    })
                    .collect();
                let found = rows.len() >= min_count
                    && candidates.len() >= min_affected
                    && candidates.len() * 100 >= rows.len() * ratio;
                check(&mut checks, rule, rows.len() >= min_count, found);
                if found {
                    findings.push(finding(
                        rule,
                        table,
                        service,
                        "",
                        rows,
                        &candidates,
                        None,
                        vec![
                            evidence("Candidate spans", candidates.len()),
                            evidence("Sampled service spans", rows.len()),
                        ],
                    ));
                }
            }
            let valid: Vec<_> = rows
                .iter()
                .copied()
                .filter(|r| r.span_identity != 0)
                .collect();
            let mut copies: BTreeMap<u64, Vec<&Sample>> = BTreeMap::new();
            for row in &valid {
                copies.entry(row.record_hash).or_default().push(row);
            }
            let duplicates: Vec<_> = copies
                .values()
                .flat_map(|r| r.iter().skip(1).copied())
                .collect();
            check(
                &mut checks,
                "duplicate_spans",
                valid.len() >= 6,
                duplicates.len() >= 5,
            );
            if duplicates.len() >= 5 {
                findings.push(finding(
                    "duplicate_spans",
                    table,
                    service,
                    "",
                    &valid,
                    &duplicates,
                    None,
                    vec![
                        evidence("Extra identical stored span copies", duplicates.len()),
                        evidence(
                            "Fingerprints repeated",
                            copies.values().filter(|r| r.len() > 1).count(),
                        ),
                    ],
                ));
            }
        } else {
            let mut metrics: BTreeMap<&str, Vec<&Sample>> = BTreeMap::new();
            for row in rows {
                metrics.entry(&row.metric).or_default().push(row);
            }
            for (metric, points) in metrics {
                let mut labels: BTreeMap<&str, (Vec<&Sample>, BTreeSet<u64>)> = BTreeMap::new();
                for row in &points {
                    for (key, value) in row.label_keys.iter().zip(&row.label_hashes) {
                        let entry = labels.entry(key).or_default();
                        entry.0.push(row);
                        entry.1.insert(*value);
                    }
                }
                if labels.is_empty() {
                    check(&mut checks, "metric_cardinality", false, false);
                }
                for (key, (label_rows, values)) in labels {
                    let found = label_rows.len() >= 200
                        && values.len() >= 100
                        && values.len() * 100 > label_rows.len() * 80;
                    check(
                        &mut checks,
                        "metric_cardinality",
                        label_rows.len() >= 200,
                        found,
                    );
                    if found {
                        findings.push(finding(
                            "metric_cardinality",
                            table,
                            service,
                            &format!("{metric} · {key}"),
                            &label_rows,
                            &label_rows,
                            None,
                            vec![
                                evidence("Metric", metric),
                                evidence("Attribute key", key),
                                evidence("Distinct sampled hashed values", values.len()),
                                evidence("Datapoints with attribute", label_rows.len()),
                                evidence(
                                    "Datapoints with additional uninspected attributes",
                                    points.iter().filter(|r| r.labels_capped != 0).count(),
                                ),
                            ],
                        ));
                    }
                }
                let mut series: BTreeMap<u64, Vec<&Sample>> = BTreeMap::new();
                for row in &points {
                    series.entry(row.series_hash).or_default().push(row);
                }
                let mut fast = Vec::new();
                let mut max_rate = 0.0_f64;
                let mut evaluated = Vec::new();
                let mut enough_series = 0;
                for series_rows in series.values() {
                    let mut times: BTreeMap<i64, &Sample> = BTreeMap::new();
                    for row in series_rows {
                        times.entry(row.observed_at).or_insert(row);
                    }
                    let span = times
                        .last_key_value()
                        .unwrap()
                        .0
                        .saturating_sub(*times.first_key_value().unwrap().0);
                    if times.len() < 10 || span < 60_000 {
                        continue;
                    }
                    enough_series += 1;
                    evaluated.extend(times.values().copied());
                    let rate = (times.len() - 1) as f64 * 60_000.0 / span as f64;
                    if rate > HIGH_CADENCE_PER_MINUTE {
                        max_rate = max_rate.max(rate);
                        fast.extend(times.into_values());
                    }
                }
                check(
                    &mut checks,
                    "metric_cadence",
                    enough_series > 0,
                    !fast.is_empty(),
                );
                if !fast.is_empty() {
                    findings.push(finding(
                        "metric_cadence",
                        table,
                        service,
                        metric,
                        &evaluated,
                        &fast,
                        None,
                        vec![
                            evidence("Metric", metric),
                            evidence("Highest observed intervals/min", format!("{max_rate:.1}")),
                            evidence("Series with sufficient observations", enough_series),
                            evidence("Distinct evaluated datapoints", evaluated.len()),
                        ],
                    ));
                }
            }
        }
    }
    findings.sort_by(|a, b| {
        b.candidate_records
            .cmp(&a.candidate_records)
            .then(a.id.cmp(&b.id))
    });
    let finding_count = findings.len();
    let omitted_findings = finding_count.saturating_sub(MAX_FINDINGS);
    findings.truncate(MAX_FINDINGS);
    let mut services = records
        .iter()
        .map(|(_, r)| r.service.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let omitted_services = services.len().saturating_sub(500);
    services.truncate(500);
    let rules = rules();
    for rule in &rules {
        checks.entry(rule.id.clone()).or_insert(WasteCheck {
            rule_id: rule.id.clone(),
            status: "not_evaluated".into(),
            evaluated_groups: 0,
            insufficient_groups: 0,
            findings: 0,
        });
    }
    WasteResponse { evaluated_at:Utc::now().to_rfc3339(), from:from.to_rfc3339(), to:to.to_rfc3339(),minutes,sample_limit_per_source:LIMIT,cache_hit:false,cache_ttl_seconds:60,
        partial:sources.iter().any(|s|s.status!="available") || omitted_findings>0 || omitted_services>0,
        sources,rules,checks:checks.into_values().collect(),findings,finding_count,omitted_findings,services,omitted_services,
        sampled_records:records.len(),sampled_log_body_bytes:records.iter().filter(|(s,_)|*s=="logs").map(|(_,r)|r.body_bytes).sum(),
        methodology:"Bounded storage-order samples, not random or exhaustive. Counts and log body bytes describe only sampled records. Rules can overlap: candidate counts and bytes must not be added as savings. No storage, billing or population-wide savings are estimated. Missing sources and insufficient observations remain not evaluated. Recognized severity text is used when numeric severity is missing or invalid; other unknown severities are excluded. No collection settings are changed. Message/series fingerprints, raw messages, URLs, trace/span IDs and attribute values are not returned.".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(i: u64) -> Sample {
        Sample {
            service: "checkout".into(),
            metric: "requests".into(),
            observed_at: 1_800_000_000_000 + i as i64 * 1_000,
            severity: 12,
            body_hash: i,
            body_bytes: 100,
            span_identity: 1,
            record_hash: i,
            series_hash: 1,
            ..Default::default()
        }
    }
    fn run(table: &'static str, rows: Vec<Sample>) -> WasteResponse {
        let now = Utc::now();
        evaluate(
            rows.into_iter().map(|r| (table, r)).collect(),
            SOURCES
                .iter()
                .map(|s| QualitySource {
                    table: (*s).into(),
                    signal: signal(s).into(),
                    status: "available".into(),
                    sampled_records: 0,
                    capped: false,
                })
                .collect(),
            now - chrono::Duration::hours(1),
            now,
            60,
        )
    }
    fn has(report: &WasteResponse, rule: &str) -> bool {
        report.findings.iter().any(|f| f.rule_id == rule)
    }
    #[test]
    fn verbose_ratio_excludes_unknown_severity_and_requires_enough_data() {
        let rows = (0..100)
            .map(|i| {
                let mut r = row(i);
                if i < 25 {
                    r.severity = 5;
                }
                r
            })
            .collect();
        let report = run("logs", rows);
        let finding = report
            .findings
            .iter()
            .find(|f| f.rule_id == "verbose_logs")
            .unwrap();
        assert_eq!(finding.candidate_records, 25);
        assert_eq!(finding.sampled_records, 100);
        let rows = (0..110)
            .map(|i| {
                let mut r = row(i);
                r.severity = if i < 99 { 5 } else { 0 };
                r
            })
            .collect();
        let report = run("logs", rows);
        assert!(!has(&report, "verbose_logs"));
        assert_eq!(
            report
                .checks
                .iter()
                .find(|c| c.rule_id == "verbose_logs")
                .unwrap()
                .status,
            "not_evaluated"
        );
    }
    #[test]
    fn repeated_logs_count_extra_copies_within_a_minute_without_exposing_fingerprints() {
        let rows = (0..40)
            .rev()
            .map(|i| {
                let mut r = row(i);
                r.body_hash = 9_932_198_771_244;
                r
            })
            .collect();
        let report = run("logs", rows);
        let f = report
            .findings
            .iter()
            .find(|f| f.rule_id == "repeated_logs")
            .unwrap();
        assert_eq!(f.candidate_records, 39);
        assert_eq!(f.first_observed, timestamp(row(1).observed_at));
        assert_eq!(f.candidate_body_bytes, Some(3900));
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("9932198771244"));
        assert!(!json.contains("body_hash"));
        let rows = (0..30)
            .map(|i| {
                let mut r = row(i);
                r.body_hash = 1;
                r.observed_at += i as i64 * 60_000;
                r
            })
            .collect();
        assert!(!has(&run("logs", rows), "repeated_logs"));
        let rows = (0..30)
            .map(|i| {
                let mut r = row(i);
                r.body_hash = 1;
                if i >= 15 {
                    r.service = "other".into();
                }
                r
            })
            .collect();
        assert!(!has(&run("logs", rows), "repeated_logs"));
    }
    #[test]
    fn large_body_bytes_are_sampled_content_and_exclude_warning_error_records() {
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.body_bytes = 16 * 1024;
                r.severity = if i < 3 { 12 } else { 17 };
                r
            })
            .collect();
        let report = run("logs", rows);
        let f = report
            .findings
            .iter()
            .find(|f| f.rule_id == "large_logs")
            .unwrap();
        assert_eq!(f.candidate_records, 3);
        assert_eq!(f.candidate_body_bytes, Some(3 * 16 * 1024));
        assert_eq!(f.sampled_records, 3);
    }
    #[test]
    fn routine_and_short_spans_require_ratios_and_minimum_samples() {
        let rows = (0..100)
            .map(|i| {
                let mut r = row(i);
                r.health = u8::from(i < 20);
                r.static_asset = u8::from(i < 20);
                r.short_internal = u8::from(i < 80);
                r
            })
            .collect();
        let report = run("spans", rows);
        for rule in ["health_spans", "static_spans", "short_spans"] {
            assert!(has(&report, rule));
        }
        let rows = (0..49)
            .map(|i| {
                let mut r = row(i);
                r.health = 1;
                r.short_internal = 1;
                r
            })
            .collect();
        let report = run("spans", rows);
        assert!(!has(&report, "health_spans"));
        assert!(!has(&report, "short_spans"));
    }
    #[test]
    fn duplicate_spans_require_identical_content_and_a_span_identity() {
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                if i < 6 {
                    r.record_hash = 1000;
                }
                r
            })
            .collect();
        let report = run("spans", rows);
        let f = report
            .findings
            .iter()
            .find(|f| f.rule_id == "duplicate_spans")
            .unwrap();
        assert_eq!(f.candidate_records, 5);
        assert!(!has(
            &run("spans", (0..10).map(row).collect()),
            "duplicate_spans"
        ));
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.span_identity = 0;
                r.record_hash = 1;
                r
            })
            .collect();
        assert!(!has(&run("spans", rows), "duplicate_spans"));
    }
    #[test]
    fn cardinality_is_scoped_to_service_metric_and_source() {
        let rows = (0..220)
            .map(|i| {
                let mut r = row(i);
                r.label_keys = vec!["request_id".into()];
                r.label_hashes = vec![i];
                r
            })
            .collect();
        assert!(has(&run("metrics_gauge", rows), "metric_cardinality"));
        let rows = (0..220)
            .map(|i| {
                let mut r = row(i);
                if i >= 110 {
                    r.metric = "other".into();
                }
                r.label_keys = vec!["request_id".into()];
                r.label_hashes = vec![i];
                r
            })
            .collect();
        assert!(!has(&run("metrics_gauge", rows), "metric_cardinality"));
    }
    #[test]
    fn cadence_uses_distinct_timestamps_and_observed_intervals() {
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.observed_at += i as i64 * 9_000;
                r
            })
            .collect();
        assert!(has(&run("metrics_gauge", rows), "metric_cadence"));
        let rows = (0..50)
            .map(|i| {
                let mut r = row(i);
                r.observed_at = 1_800_000_000_000;
                r
            })
            .collect();
        assert!(!has(&run("metrics_gauge", rows), "metric_cadence"));
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.observed_at += i as i64 * 29_000;
                r
            })
            .collect();
        assert!(!has(&run("metrics_gauge", rows), "metric_cadence"));
    }
    #[test]
    fn scheduling_jitter_near_fifteen_seconds_is_not_a_cadence_finding() {
        let rows = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.observed_at += i as i64 * 13_900;
                r
            })
            .collect();
        assert!(!has(&run("metrics_gauge", rows), "metric_cadence"));
    }
    #[test]
    fn cadence_denominator_excludes_sparse_series_and_duplicate_timestamps() {
        let mut rows: Vec<_> = (0..10)
            .map(|i| {
                let mut r = row(i);
                r.observed_at += i as i64 * 9_000;
                r
            })
            .collect();
        rows.push(rows[0].clone());
        let mut sparse = row(99);
        sparse.series_hash = 2;
        rows.push(sparse);
        let report = run("metrics_gauge", rows);
        let finding = report
            .findings
            .iter()
            .find(|f| f.rule_id == "metric_cadence")
            .unwrap();
        assert_eq!(finding.candidate_records, 10);
        assert_eq!(finding.sampled_records, 10);
    }
    #[test]
    fn unavailable_sources_and_sparse_data_are_never_a_clean_assessment() {
        let now = Utc::now();
        let report = evaluate(
            vec![],
            vec![QualitySource {
                table: "logs".into(),
                signal: "logs".into(),
                status: "unavailable".into(),
                sampled_records: 0,
                capped: false,
            }],
            now,
            now,
            60,
        );
        assert!(report.partial);
        assert!(report.checks.iter().all(|c| c.status == "not_evaluated"));
        assert_eq!(report.sampled_records, 0);
    }
    #[test]
    fn queries_are_bounded_scoped_and_return_metadata_instead_of_messages() {
        let now = Utc::now();
        for table in SOURCES {
            let sql = sample_sql(table, "tenant' OR 1", now, now);
            assert!(sql.contains("tenant_id = 'tenant'' OR 1'"));
            assert!(sql.contains("LIMIT 1001"));
            assert!(
                !sql.contains('?'),
                "literal question marks must not become driver parameters"
            );
            assert!(!sql.contains("Body AS"));
            assert!(!sql.contains("http_path AS"));
        }
        let spans = sample_sql("spans", "tenant", now, now);
        assert!(spans.contains("http_status_code BETWEEN 200 AND 299"));
        assert!(spans.contains("'STATUS_CODE_ERROR'"));
        let metric = sample_sql("metrics_gauge", "tenant", now, now);
        assert!(metric.contains("arraySort(mapKeys(Attributes))"));
        assert!(metric.contains("ResourceSchemaUrl"));
    }
    #[tokio::test]
    #[ignore = "read-only check against RUSH_TEST_CLICKHOUSE_URL"]
    async fn live_sample_queries_match_schema_and_budget() {
        let client = clickhouse::Client::default()
            .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL").unwrap())
            .with_database(
                std::env::var("RUSH_TEST_CLICKHOUSE_DATABASE").unwrap_or("observability".into()),
            )
            .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or("default".into()))
            .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
        let tenant = std::env::var("RUSH_TEST_TENANT").unwrap_or("default".into());
        let to = Utc::now();
        for table in SOURCES {
            let rows = read_bounded::<Sample>(
                &client,
                &sample_sql(table, &tenant, to - chrono::Duration::hours(24), to),
                &tenant,
            )
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"));
            assert!(rows.len() <= LIMIT + 1);
            println!("{table}: {} sampled records", rows.len());
        }
    }
}
