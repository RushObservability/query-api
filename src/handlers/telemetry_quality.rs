//! Bounded telemetry-quality checks. Findings describe the sample,
//! not the population. Never fetch log bodies or return metric label values.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::handlers::users::require_principal;
use crate::query_builder::escape_string_literal;
use crate::{AppState, TenantContext};

const SAMPLE_LIMIT: usize = 1_000;
const MAX_FINDINGS: usize = 200;
const MAX_SERVICES: usize = 500;
const SOURCES: [&str; 7] = [
    "spans",
    "logs",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
];

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityParams {
    pub minutes: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRule {
    pub id: String,
    pub title: String,
    pub impact: String,
    pub signals: String,
    pub description: String,
    pub remediation: String,
}

pub fn rules() -> Vec<QualityRule> {
    vec![
        QualityRule {
            id: "service_name".into(),
            title: "Missing service name".into(),
            impact: "high".into(),
            signals: "Logs, metrics, traces".into(),
            description: "The service name is empty or uses the SDK unknown_service default.".into(),
            remediation: "Set service.name on the SDK resource so telemetry can be attributed to a service.".into(),
        },
        QualityRule {
            id: "environment".into(),
            title: "Missing deployment environment".into(),
            impact: "high".into(),
            signals: "Logs, metrics, traces".into(),
            description: "Neither deployment.environment.name nor its legacy deployment.environment attribute is present.".into(),
            remediation: "Set deployment.environment.name on the SDK resource or through the Collector resource processor.".into(),
        },
        QualityRule {
            id: "instance".into(),
            title: "Missing service instance ID".into(),
            impact: "medium".into(),
            signals: "Logs, metrics, traces".into(),
            description: "The resource has no service.instance.id. Historical spans without retained resource metadata are not evaluated.".into(),
            remediation: "Set a unique service.instance.id for each running instance, such as a Kubernetes pod UID.".into(),
        },
        QualityRule {
            id: "severity".into(),
            title: "Missing or invalid log severity".into(),
            impact: "medium".into(),
            signals: "Logs".into(),
            description: "severityNumber is outside the OpenTelemetry range 1–24.".into(),
            remediation: "Use an OpenTelemetry logging bridge and map the application log level to severityNumber.".into(),
        },
        QualityRule {
            id: "metric_unit".into(),
            title: "Missing metric unit".into(),
            impact: "low".into(),
            signals: "Metrics".into(),
            description: "The metric unit is empty. Use 1 for dimensionless measurements where appropriate.".into(),
            remediation: "Declare the unit when creating the instrument and check that recorded values use that unit.".into(),
        },
        QualityRule {
            id: "unit_conflict".into(),
            title: "Conflicting metric units".into(),
            impact: "high".into(),
            signals: "Metrics".into(),
            description: "The same service and metric name have multiple non-empty units in the sample.".into(),
            remediation: "Use one unit per metric name. Convert values and check dashboards and alert thresholds when changing units.".into(),
        },
        QualityRule {
            id: "label_cardinality".into(),
            title: "High-cardinality metric attribute".into(),
            impact: "medium".into(),
            signals: "Metrics".into(),
            description: "Among the first 64 attributes per datapoint, a label has at least 100 distinct sampled values across at least 200 datapoints, with more than 80% distinct. This is a review candidate, not proof of waste.".into(),
            remediation: "Check whether this label carries request, session, user or URL identifiers. Prefer bounded dimensions; preserve required series identity.".into(),
        },
        QualityRule {
            id: "span_names".into(),
            title: "Likely dynamic span names".into(),
            impact: "medium".into(),
            signals: "Traces".into(),
            description: "At least 100 sampled span names are distinct, with more than half the sample distinct, and at least 10 names contain long numeric IDs or UUIDs.".into(),
            remediation: "Use operation or matched route templates for span names, keeping individual identifiers out of the name.".into(),
        },
        QualityRule { id: "version".into(), title: "Missing service version".into(), impact: "medium".into(), signals: "Logs, metrics, traces".into(), description: "The resource has no service.version, limiting release comparisons.".into(), remediation: "Set service.version to your deployed release or build identifier.".into() },
        QualityRule { id: "namespace".into(), title: "Missing service namespace".into(), impact: "low".into(), signals: "Logs, metrics, traces".into(), description: "The resource has no service.namespace to distinguish related service groups.".into(), remediation: "Set service.namespace to a stable application or team namespace.".into() },
        QualityRule { id: "sdk_name".into(), title: "Missing telemetry SDK name".into(), impact: "low".into(), signals: "Logs, metrics, traces".into(), description: "The resource has no telemetry.sdk.name. Non-SDK sources may omit this intentionally.".into(), remediation: "For SDK sources, preserve telemetry.sdk.name through the Collector resource processors.".into() },
        QualityRule { id: "sdk_language".into(), title: "Missing telemetry SDK language".into(), impact: "low".into(), signals: "Logs, metrics, traces".into(), description: "The resource has no telemetry.sdk.language. Non-SDK sources may omit this intentionally.".into(), remediation: "For SDK sources, preserve telemetry.sdk.language through the Collector resource processors.".into() },
        QualityRule { id: "metric_description".into(), title: "Missing metric description".into(), impact: "low".into(), signals: "Metrics".into(), description: "The metric description is empty, leaving its meaning undocumented in telemetry.".into(), remediation: "Describe the quantity measured when creating the instrument.".into() },
        QualityRule { id: "span_kind".into(), title: "Unspecified or invalid span kind".into(), impact: "medium".into(), signals: "Traces".into(), description: "The span kind is not INTERNAL, SERVER, CLIENT, PRODUCER or CONSUMER.".into(), remediation: "Set the span kind for the operation so service topology can distinguish local work, requests and messaging.".into() },
    ]
}

#[derive(Debug, Clone, Default, Deserialize, clickhouse::Row)]
pub struct Sample {
    pub service: String,
    pub version: String,
    pub has_environment: u8,
    pub has_instance: u8,
    pub has_severity: u8,
    pub metric: String,
    pub unit: String,
    pub label_keys: Vec<String>,
    pub label_hashes: Vec<u64>,
    pub name_hash: u64,
    pub dynamic_name: u8,
    pub observed_at: i64,
    pub resource_known: u8,
    pub has_namespace: u8,
    pub has_sdk_name: u8,
    pub has_sdk_language: u8,
    pub has_description: u8,
    pub valid_kind: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualitySource {
    pub table: String,
    pub signal: String,
    pub status: String,
    pub sampled_records: usize,
    pub capped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEvidence {
    pub version: String,
    pub sampled: u64,
    pub affected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityFinding {
    pub id: String,
    pub rule_id: String,
    pub service: String,
    pub subject: String,
    pub affected: u64,
    pub sampled: u64,
    pub first_observed: String,
    pub last_observed: String,
    pub observed: String,
    pub expected: String,
    pub versions: Vec<VersionEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleEvaluation {
    pub rule_id: String,
    pub status: String,
    pub sampled: u64,
    pub affected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityService {
    pub service: String,
    pub status: String,
    pub sampled_records: u64,
    pub finding_count: usize,
    pub checks: Vec<RuleEvaluation>,
    pub score: QualityScore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityScore {
    pub value: Option<u32>,
    pub passed_weight: u32,
    pub evaluated_weight: u32,
    pub evaluated_checks: usize,
    pub not_evaluated_checks: usize,
}

/// Rush sample score v1. Each evaluated service/rule pair counts once.
/// Findings fail the check; unknown checks never contribute passing weight.
fn score(checks: &[RuleEvaluation], definitions: &[QualityRule]) -> QualityScore {
    let mut result = QualityScore {
        value: None,
        passed_weight: 0,
        evaluated_weight: 0,
        evaluated_checks: 0,
        not_evaluated_checks: 0,
    };
    for check in checks {
        if check.status == "not_evaluated" {
            result.not_evaluated_checks += 1;
            continue;
        }
        let weight = definitions
            .iter()
            .find(|r| r.id == check.rule_id)
            .map_or(1, |r| match r.impact.as_str() {
                "high" => 3,
                "medium" => 2,
                _ => 1,
            });
        result.evaluated_checks += 1;
        result.evaluated_weight += weight;
        if check.status == "pass" {
            result.passed_weight += weight;
        }
    }
    if result.evaluated_weight > 0 {
        result.value = Some(
            (100 * result.passed_weight + result.evaluated_weight / 2) / result.evaluated_weight,
        );
    }
    result
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityResponse {
    pub evaluated_at: String,
    pub from: String,
    pub to: String,
    pub minutes: u32,
    pub sample_limit_per_source: usize,
    pub sampling: String,
    pub sources: Vec<QualitySource>,
    pub inventory_available: bool,
    pub inventory_capped: bool,
    pub partial: bool,
    pub finding_count: usize,
    pub high_impact_findings: usize,
    pub assessed_services: usize,
    pub unknown_services: usize,
    pub omitted_findings: usize,
    pub omitted_services: usize,
    pub rules: Vec<QualityRule>,
    pub findings: Vec<QualityFinding>,
    pub services: Vec<QualityService>,
    pub score: QualityScore,
    #[serde(default)]
    pub history: Vec<crate::quality_engine::FindingHistory>,
    pub history_available: bool,
    pub history_capped: bool,
    pub scheduled_at: Option<String>,
    pub cache_hit: bool,
}

fn signal(table: &str) -> &'static str {
    match table {
        "spans" => "traces",
        "logs" => "logs",
        _ => "metrics",
    }
}

fn validated_minutes(params: &QualityParams) -> Result<u32, (StatusCode, String)> {
    let value = params.minutes.unwrap_or(1440);
    if !(5..=10_080).contains(&value) {
        return Err((
            StatusCode::BAD_REQUEST,
            "minutes must be between 5 and 10080".into(),
        ));
    }
    Ok(value)
}

/// Table names come only from SOURCES. Tenant predicates and row policies both
/// apply; clients cannot select a global scope or a different tenant here.
fn sample_sql(table: &str, tenant: &str, from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    assert!(SOURCES.contains(&table));
    let tenant = escape_string_literal(tenant);
    let from = from.format("%Y-%m-%d %H:%M:%S%.9f");
    let to = to.format("%Y-%m-%d %H:%M:%S%.9f");
    let (fields, time) = if table == "spans" {
        // Empty legacy resource metadata remains unknown, not a failed check.
        ("service_name AS service, if(notEmpty(resource_attributes), JSONExtractString(resource_attributes, 'service.version'), JSONExtractString(attributes, 'service.version')) AS version, \
          toUInt8(notEmpty(trimBoth(JSONExtractString(resource_attributes, 'deployment.environment.name'))) OR notEmpty(trimBoth(JSONExtractString(resource_attributes, 'deployment.environment')))) AS has_environment, toUInt8(notEmpty(trimBoth(JSONExtractString(resource_attributes, 'service.instance.id')))) AS has_instance, toUInt8(0) AS has_severity, \
          '' AS metric, '' AS unit, CAST([], 'Array(String)') AS label_keys, CAST([], 'Array(UInt64)') AS label_hashes, \
          cityHash64(span_name) AS name_hash, \
          toUInt8(match(span_name, '[0-9]{6,}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')) AS dynamic_name".to_string(), "timestamp")
    } else {
        let common = "ServiceName AS service, ResourceAttributes['service.version'] AS version, \
            toUInt8(notEmpty(trimBoth(ResourceAttributes['deployment.environment.name'])) OR notEmpty(trimBoth(ResourceAttributes['deployment.environment']))) AS has_environment, \
            toUInt8(notEmpty(trimBoth(ResourceAttributes['service.instance.id']))) AS has_instance";
        if table == "logs" {
            (
                format!(
                    "{common}, toUInt8(SeverityNumber BETWEEN 1 AND 24) AS has_severity, \
                '' AS metric, '' AS unit, CAST([], 'Array(String)') AS label_keys, CAST([], 'Array(UInt64)') AS label_hashes, \
                toUInt64(0) AS name_hash, toUInt8(0) AS dynamic_name"
                ),
                "Timestamp",
            )
        } else {
            (
                format!(
                    "{common}, toUInt8(0) AS has_severity, MetricName AS metric, MetricUnit AS unit, \
                arraySlice(mapKeys(Attributes), 1, 64) AS label_keys, \
                arrayMap(v -> cityHash64(v), arraySlice(mapValues(Attributes), 1, 64)) AS label_hashes, \
                toUInt64(0) AS name_hash, toUInt8(0) AS dynamic_name"
                ),
                "TimeUnix",
            )
        }
    };
    // Read in primary-key order where possible so LIMIT stops the read instead
    // of starting large parallel ranges before enough sample rows are found.
    let order = match table {
        "spans" => " ORDER BY timestamp DESC",
        "logs" => "",
        _ => " ORDER BY MetricName, ServiceName, TimeUnix",
    };
    let extra = if table == "spans" {
        "toUInt8(notEmpty(resource_attributes)) AS resource_known, toUInt8(notEmpty(trimBoth(JSONExtractString(resource_attributes, 'service.namespace')))) AS has_namespace, toUInt8(notEmpty(trimBoth(JSONExtractString(resource_attributes, 'telemetry.sdk.name')))) AS has_sdk_name, toUInt8(notEmpty(trimBoth(JSONExtractString(resource_attributes, 'telemetry.sdk.language')))) AS has_sdk_language, toUInt8(0) AS has_description, toUInt8(replaceOne(kind, 'SPAN_KIND_', '') IN ('INTERNAL','SERVER','CLIENT','PRODUCER','CONSUMER')) AS valid_kind".to_string()
    } else {
        format!(
            "toUInt8(1) AS resource_known, toUInt8(notEmpty(trimBoth(ResourceAttributes['service.namespace']))) AS has_namespace, toUInt8(notEmpty(trimBoth(ResourceAttributes['telemetry.sdk.name']))) AS has_sdk_name, toUInt8(notEmpty(trimBoth(ResourceAttributes['telemetry.sdk.language']))) AS has_sdk_language, {} AS has_description, toUInt8(0) AS valid_kind",
            if table == "logs" {
                "toUInt8(0)"
            } else {
                "toUInt8(notEmpty(trimBoth(MetricDescription)))"
            }
        )
    };
    format!(
        "SELECT {fields}, toUnixTimestamp64Milli({time}) AS observed_at, {extra} FROM {table} \
        PREWHERE tenant_id = '{tenant}' AND {time} >= toDateTime64('{from}', 9, 'UTC') \
        AND {time} <= toDateTime64('{to}', 9, 'UTC'){order} LIMIT {}",
        SAMPLE_LIMIT + 1
    )
}

async fn read_sample(
    client: &clickhouse::Client,
    sql: &str,
    tenant: &str,
) -> Result<Vec<Sample>, clickhouse::error::Error> {
    let budget = crate::query_governor::active_budget();
    let max_memory = budget
        .as_ref()
        .map_or(64 << 20, |b| b.max_memory_usage.min(64 << 20))
        .to_string();
    let max_rows = budget
        .as_ref()
        .map_or(5_000_000, |b| b.max_rows_to_read.min(5_000_000))
        .to_string();
    let max_bytes = budget
        .as_ref()
        .map_or(128 << 20, |b| b.max_bytes_to_read.min(128 << 20))
        .to_string();
    let max_execution = budget
        .as_ref()
        .map_or(2, |b| b.max_execution_time_secs.min(2))
        .to_string();
    let query = crate::tenant_query(client, sql, tenant)
        .with_setting("max_memory_usage", max_memory.as_str())
        .with_setting("max_rows_to_read", max_rows.as_str())
        .with_setting("max_bytes_to_read", max_bytes.as_str())
        .with_setting("max_execution_time", max_execution.as_str())
        .with_setting("max_threads", "1")
        .with_setting("max_block_size", "1024")
        .with_setting("preferred_block_size_bytes", "1048576")
        .with_setting("read_overflow_mode", "throw")
        .with_setting("timeout_overflow_mode", "throw");
    query.fetch_all::<Sample>().await
}

pub async fn get_quality(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<QualityParams>,
) -> Result<Json<QualityResponse>, (StatusCode, String)> {
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
    let mut report = state
        .quality_cache
        .get(&state.ch, &state.config_db, &tenant.tenant_id, minutes)
        .await?;
    crate::quality_engine::attach_history(&state.config_db, &tenant.tenant_id, &mut report).await;
    Ok(Json(report))
}

pub(crate) async fn analyze(
    ch: &clickhouse::Client,
    config_db: &crate::clickhouse_config::ConfigDb,
    tenant: &str,
    minutes: u32,
) -> QualityResponse {
    let to = Utc::now();
    let from = to - chrono::Duration::minutes(i64::from(minutes));
    let mut records = Vec::new();
    let mut sources = Vec::new();
    // Sequential small reads avoid multiplying a single page's CH concurrency.
    for table in SOURCES {
        let sql = sample_sql(table, tenant, from, to);
        let result =
            tokio::time::timeout(Duration::from_secs(3), read_sample(ch, &sql, tenant)).await;
        match result {
            Ok(Ok(mut rows)) => {
                let capped = rows.len() > SAMPLE_LIMIT;
                rows.truncate(SAMPLE_LIMIT);
                sources.push(QualitySource {
                    table: table.into(),
                    signal: signal(table).into(),
                    status: "available".into(),
                    sampled_records: rows.len(),
                    capped,
                });
                records.extend(rows.into_iter().map(|r| (signal(table), r)));
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
    // Linked services provide an explicit inventory for "no recent data".
    // A failed inventory read is visible, never silently treated as complete.
    #[derive(Deserialize, clickhouse::Row)]
    struct LinkedService {
        service_name: String,
    }
    let inventory = tokio::time::timeout(
        Duration::from_secs(2),
        config_db.client.query("SELECT service_name FROM config_service_links_v2 FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY service_name LIMIT 501")
            .bind(tenant).fetch_all::<LinkedService>(),
    )
    .await;
    let (known, inventory_available) = match inventory {
        Ok(Ok(links)) => (links.into_iter().map(|s| s.service_name).collect(), true),
        _ => (Vec::new(), false),
    };
    evaluate(
        records,
        sources,
        known,
        inventory_available,
        from,
        to,
        minutes,
    )
}

#[derive(Default)]
struct Tally {
    sampled: u64,
    affected: u64,
    first: i64,
    last: i64,
    versions: BTreeMap<String, (u64, u64)>,
}

impl Tally {
    fn add(&mut self, sample: &Sample, bad: bool) {
        self.sampled += 1;
        let version = self.versions.entry(sample.version.clone()).or_default();
        version.0 += 1;
        if bad {
            self.affected += 1;
            version.1 += 1;
            self.first = if self.first == 0 {
                sample.observed_at
            } else {
                self.first.min(sample.observed_at)
            };
            self.last = self.last.max(sample.observed_at);
        }
    }
}

fn timestamp(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

fn finding(
    service: &str,
    rule: &str,
    subject: &str,
    tally: Tally,
    observed: String,
    expected: &str,
) -> QualityFinding {
    let mut hash = Sha256::new();
    for value in [service, rule, subject] {
        hash.update((value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    QualityFinding {
        id: hex::encode(hash.finalize()),
        rule_id: rule.into(),
        service: service.into(),
        subject: subject.into(),
        affected: tally.affected,
        sampled: tally.sampled,
        first_observed: timestamp(tally.first),
        last_observed: timestamp(tally.last),
        observed,
        expected: expected.into(),
        versions: tally
            .versions
            .into_iter()
            .take(20)
            .map(|(version, (sampled, affected))| VersionEvidence {
                version,
                sampled,
                affected,
            })
            .collect(),
    }
}

fn evaluate(
    records: Vec<(&str, Sample)>,
    sources: Vec<QualitySource>,
    known: Vec<String>,
    inventory_available: bool,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    minutes: u32,
) -> QualityResponse {
    let definitions = rules();
    let inventory_capped = known.len() > MAX_SERVICES;
    let mut tallies: BTreeMap<(String, String), Tally> = BTreeMap::new();
    let mut service_records: BTreeMap<String, u64> = known.into_iter().map(|s| (s, 0)).collect();
    let mut units: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    let mut labels: BTreeMap<(String, String, String), (usize, BTreeSet<u64>)> = BTreeMap::new();
    let mut names: BTreeMap<String, (usize, usize, BTreeSet<u64>)> = BTreeMap::new();
    for (signal, sample) in &records {
        *service_records.entry(sample.service.clone()).or_default() += 1;
        if *signal == "metrics" {
            if !sample.unit.trim().is_empty() {
                units
                    .entry((sample.service.clone(), sample.metric.clone()))
                    .or_default()
                    .insert(sample.unit.trim().into());
            }
            for (key, value) in sample.label_keys.iter().zip(&sample.label_hashes) {
                let (count, values) = labels
                    .entry((sample.service.clone(), sample.metric.clone(), key.clone()))
                    .or_default();
                *count += 1;
                values.insert(*value);
            }
        }
        if *signal == "traces" {
            let (count, dynamic, values) = names.entry(sample.service.clone()).or_default();
            *count += 1;
            *dynamic += usize::from(sample.dynamic_name != 0);
            values.insert(sample.name_hash);
        }
    }
    let bad_labels: BTreeSet<_> = labels
        .iter()
        .filter(|(_, (count, values))| {
            *count >= 200 && values.len() >= 100 && values.len() as f64 / *count as f64 > 0.8
        })
        .map(|(key, _)| key.clone())
        .collect();
    for (signal, sample) in &records {
        let service = sample.service.trim();
        let mut checks = vec![(
            "service_name",
            service.is_empty() || service.starts_with("unknown_service"),
        )];
        if *signal != "traces" || sample.resource_known != 0 {
            checks.extend([
                ("environment", sample.has_environment == 0),
                ("instance", sample.has_instance == 0),
                ("version", sample.version.trim().is_empty()),
                ("namespace", sample.has_namespace == 0),
                ("sdk_name", sample.has_sdk_name == 0),
                ("sdk_language", sample.has_sdk_language == 0),
            ]);
        }
        if *signal == "logs" {
            checks.push(("severity", sample.has_severity == 0));
        }
        if *signal == "metrics" {
            checks.push(("metric_unit", sample.unit.trim().is_empty()));
            checks.push(("metric_description", sample.has_description == 0));
            checks.push((
                "unit_conflict",
                units
                    .get(&(sample.service.clone(), sample.metric.clone()))
                    .is_some_and(|v| v.len() > 1),
            ));
            if sample.label_keys.iter().any(|key| {
                labels
                    .get(&(sample.service.clone(), sample.metric.clone(), key.clone()))
                    .is_some_and(|(count, _)| *count >= 200)
            }) {
                checks.push((
                    "label_cardinality",
                    sample.label_keys.iter().any(|key| {
                        bad_labels.contains(&(
                            sample.service.clone(),
                            sample.metric.clone(),
                            key.clone(),
                        ))
                    }),
                ));
            }
        }
        if *signal == "traces" {
            checks.push(("span_kind", sample.valid_kind == 0));
            let bad = names
                .get(&sample.service)
                .is_some_and(|(count, dynamic, values)| {
                    values.len() >= 100
                        && values.len() as f64 / *count as f64 > 0.5
                        && *dynamic >= 10
                });
            if names
                .get(&sample.service)
                .is_some_and(|(count, _, _)| *count >= 100)
            {
                checks.push(("span_names", bad && sample.dynamic_name != 0));
            }
        }
        for (rule, bad) in checks {
            tallies
                .entry((sample.service.clone(), rule.into()))
                .or_default()
                .add(sample, bad);
        }
    }
    let mut findings = Vec::new();
    let mut evaluations: BTreeMap<String, Vec<RuleEvaluation>> = BTreeMap::new();
    for ((service, rule), tally) in tallies {
        evaluations
            .entry(service.clone())
            .or_default()
            .push(RuleEvaluation {
                rule_id: rule.clone(),
                status: if tally.affected > 0 {
                    "finding"
                } else {
                    "pass"
                }
                .into(),
                sampled: tally.sampled,
                affected: tally.affected,
            });
        if tally.affected == 0 {
            continue;
        }
        let (observed, expected) = match rule.as_str() {
            "version" => (
                "service.version is empty".into(),
                "An explicit release or build identifier",
            ),
            "namespace" => (
                "service.namespace is empty".into(),
                "A stable service group namespace",
            ),
            "sdk_name" => (
                "telemetry.sdk.name is empty".into(),
                "SDK identity for SDK-generated telemetry",
            ),
            "sdk_language" => (
                "telemetry.sdk.language is empty".into(),
                "SDK language for SDK-generated telemetry",
            ),
            "metric_description" => (
                "Metric description is empty".into(),
                "A description of the measured quantity",
            ),
            "span_kind" => (
                "Span kind is unspecified or invalid".into(),
                "INTERNAL, SERVER, CLIENT, PRODUCER or CONSUMER",
            ),
            "service_name" => (
                "Service name is missing or uses unknown_service".into(),
                "An explicit service.name",
            ),
            "environment" => (
                "Deployment environment is missing on sampled resources".into(),
                "deployment.environment.name on the resource",
            ),
            "instance" => (
                "Service instance ID is missing on sampled resources".into(),
                "A unique service.instance.id",
            ),
            "severity" => (
                "Log severityNumber is unset or outside 1–24".into(),
                "A severityNumber between 1 and 24",
            ),
            "metric_unit" => (
                "One or more metric instruments have no unit".into(),
                "An explicit metric unit, including 1 for dimensionless values",
            ),
            "unit_conflict" => {
                let examples: Vec<_> = units
                    .iter()
                    .filter(|((s, _), v)| s == &service && v.len() > 1)
                    .take(5)
                    .map(|((_, metric), values)| {
                        format!(
                            "{}: {}",
                            metric,
                            values.iter().cloned().collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect();
                (
                    examples.join("; "),
                    "One non-empty unit for each metric name",
                )
            }
            "label_cardinality" => {
                let examples: Vec<_> = labels
                    .iter()
                    .filter(|((s, metric, key), _)| {
                        s == &service
                            && bad_labels.contains(&(s.clone(), metric.clone(), key.clone()))
                    })
                    .take(5)
                    .map(|((_, metric, key), (count, values))| {
                        format!(
                            "{metric} / {key}: {} distinct values in {count} sampled points",
                            values.len()
                        )
                    })
                    .collect();
                (
                    examples.join("; "),
                    "Bounded metric attribute values; review required",
                )
            }
            _ => {
                let (count, dynamic, values) = &names[&service];
                (
                    format!(
                        "{} distinct names in {count} sampled spans; {dynamic} names contain likely identifiers",
                        values.len()
                    ),
                    "Stable operation or route-template span names",
                )
            }
        };
        findings.push(finding(&service, &rule, "", tally, observed, expected));
    }
    let impact = |f: &QualityFinding| {
        definitions
            .iter()
            .find(|r| r.id == f.rule_id)
            .map_or(2, |r| match r.impact.as_str() {
                "high" => 0,
                "medium" => 1,
                _ => 2,
            })
    };
    findings.sort_by(|a, b| {
        impact(a)
            .cmp(&impact(b))
            .then(b.affected.cmp(&a.affected))
            .then(a.id.cmp(&b.id))
    });
    let finding_count = findings.len();
    let high_impact_findings = findings.iter().filter(|f| impact(f) == 0).count();
    let mut services: Vec<_> = service_records
        .into_iter()
        .map(|(service, sampled_records)| {
            let checks = definitions
                .iter()
                .map(|rule| {
                    evaluations
                        .get(&service)
                        .and_then(|v| v.iter().find(|v| v.rule_id == rule.id))
                        .cloned()
                        .unwrap_or(RuleEvaluation {
                            rule_id: rule.id.clone(),
                            status: "not_evaluated".into(),
                            sampled: 0,
                            affected: 0,
                        })
                })
                .collect::<Vec<_>>();
            let finding_count = checks.iter().filter(|c| c.status == "finding").count();
            QualityService {
                service,
                status: if sampled_records == 0 {
                    "unknown"
                } else if finding_count > 0 {
                    "findings"
                } else {
                    "no_findings_in_sample"
                }
                .into(),
                sampled_records,
                finding_count,
                score: score(&checks, &definitions),
                checks,
            }
        })
        .collect();
    services.sort_by(|a, b| {
        b.finding_count
            .cmp(&a.finding_count)
            .then(a.service.cmp(&b.service))
    });
    let assessed_services = services.iter().filter(|s| s.sampled_records > 0).count();
    let unknown_services = services.len() - assessed_services;
    let omitted_findings = findings.len().saturating_sub(MAX_FINDINGS);
    let omitted_services = services.len().saturating_sub(MAX_SERVICES);
    findings.truncate(MAX_FINDINGS);
    services.truncate(MAX_SERVICES);
    let combined = services
        .iter()
        .flat_map(|s| s.checks.clone())
        .collect::<Vec<_>>();
    QualityResponse {
        score: score(&combined, &definitions),
        history: vec![], history_available: false, history_capped: false, scheduled_at: None, cache_hit: false,
        evaluated_at: Utc::now().to_rfc3339(),
        from: from.to_rfc3339(),
        to: to.to_rfc3339(),
        minutes,
        sample_limit_per_source: SAMPLE_LIMIT,
        sampling: "Bounded storage-order samples, not random or exhaustive. Percentages describe sampled records only. Historical spans without retained resource attributes are not evaluated for resource checks. Rush sample score v1 uses high=3, medium=2, low=1: 100 × passing weight / evaluated weight, rounded to nearest integer. Unknown checks are excluded. This is a partial Rush assessment, not the Instrumentation Score standard.".into(),
        partial: !inventory_available
            || sources.iter().any(|s| s.status == "unavailable")
            || omitted_findings > 0
            || omitted_services > 0,
        sources,
        inventory_available,
        inventory_capped,
        finding_count,
        high_impact_findings,
        assessed_services,
        unknown_services,
        omitted_findings,
        omitted_services,
        rules: definitions,
        findings,
        services,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    fn sample(service: &str) -> Sample {
        Sample {
            service: service.into(),
            version: "1.0".into(),
            observed_at: 1_800_000_000_000,
            has_environment: 1,
            has_instance: 1,
            has_severity: 1,
            has_namespace: 1,
            has_sdk_name: 1,
            has_sdk_language: 1,
            has_description: 1,
            valid_kind: 1,
            metric: "duration".into(),
            unit: "s".into(),
            ..Default::default()
        }
    }
    fn run(records: Vec<(&str, Sample)>, known: Vec<String>) -> QualityResponse {
        let now = Utc::now();
        evaluate(
            records,
            vec![],
            known,
            true,
            now - chrono::Duration::hours(1),
            now,
            60,
        )
    }
    pub(crate) fn history_report(bad: bool) -> QualityResponse {
        let mut row = sample("checkout");
        row.has_environment = u8::from(!bad);
        let mut report = run(vec![("logs", row)], vec![]);
        report.sources = SOURCES
            .iter()
            .map(|table| QualitySource {
                table: (*table).into(),
                signal: signal(table).into(),
                status: "available".into(),
                sampled_records: 1,
                capped: false,
            })
            .collect();
        report
    }
    #[test]
    fn retained_trace_resources_are_evaluated_and_old_rows_stay_unknown() {
        let mut retained = sample("checkout");
        retained.resource_known = 1;
        retained.has_environment = 0;
        let mut old = retained.clone();
        old.resource_known = 0;
        let report = run(vec![("traces", retained), ("traces", old)], vec![]);
        let env = report.services[0]
            .checks
            .iter()
            .find(|c| c.rule_id == "environment")
            .unwrap();
        assert_eq!(env.sampled, 1);
        assert_eq!(env.affected, 1);
    }
    #[test]
    fn score_excludes_unknowns_and_uses_disclosed_weights() {
        let rules = rules();
        let checks = vec![
            RuleEvaluation {
                rule_id: "service_name".into(),
                status: "pass".into(),
                sampled: 1,
                affected: 0,
            },
            RuleEvaluation {
                rule_id: "instance".into(),
                status: "finding".into(),
                sampled: 1,
                affected: 1,
            },
            RuleEvaluation {
                rule_id: "namespace".into(),
                status: "not_evaluated".into(),
                sampled: 0,
                affected: 0,
            },
        ];
        let result = score(&checks, &rules);
        assert_eq!(result.value, Some(60));
        assert_eq!(result.passed_weight, 3);
        assert_eq!(result.evaluated_weight, 5);
        assert_eq!(result.not_evaluated_checks, 1);
        assert_eq!(score(&checks[2..], &rules).value, None);
    }
    #[test]
    fn missing_metadata_is_evidence_and_absent_services_are_unknown() {
        let mut s = sample("checkout");
        s.has_environment = 0;
        let r = run(vec![("logs", s)], vec!["quiet".into()]);
        assert_eq!(r.finding_count, 1);
        assert_eq!(r.findings[0].rule_id, "environment");
        let quiet = r.services.iter().find(|s| s.service == "quiet").unwrap();
        assert_eq!(quiet.status, "unknown");
        assert!(quiet.checks.iter().all(|c| c.status == "not_evaluated"));
        assert_eq!(r.findings[0].versions[0].affected, 1);
    }
    #[test]
    fn trace_resource_loss_is_not_an_instrumentation_failure() {
        let mut s = sample("checkout");
        s.has_environment = 0;
        s.has_instance = 0;
        let r = run(vec![("traces", s)], vec![]);
        assert_eq!(r.finding_count, 0);
        assert_eq!(
            r.services[0]
                .checks
                .iter()
                .find(|c| c.rule_id == "environment")
                .unwrap()
                .status,
            "not_evaluated"
        );
    }
    #[test]
    fn unit_conflicts_cross_versions_but_not_services() {
        let a = sample("payments");
        let mut b = a.clone();
        b.unit = "ms".into();
        b.version = "2.0".into();
        let mut other = sample("other");
        other.unit = "ms".into();
        let r = run(
            vec![("metrics", a), ("metrics", b), ("metrics", other)],
            vec![],
        );
        let f = r
            .findings
            .iter()
            .find(|f| f.rule_id == "unit_conflict")
            .unwrap();
        assert_eq!(f.service, "payments");
        assert_eq!(f.affected, 2);
        assert_eq!(f.versions.len(), 2);
    }
    #[test]
    fn cardinality_is_bounded_sample_evidence_without_label_values() {
        let rows = (0..220)
            .map(|i| {
                let mut s = sample("payments");
                s.label_keys = vec!["request_id".into()];
                s.label_hashes = vec![i];
                ("metrics", s)
            })
            .collect();
        let r = run(rows, vec![]);
        assert!(r.findings.iter().any(|f| f.rule_id == "label_cardinality"));
        let json = serde_json::to_string(&r).unwrap();
        assert!(!json.contains("label_hashes"));
        assert!(json.contains("220 distinct values"));
    }
    #[test]
    fn unavailable_sources_are_partial_not_healthy() {
        let now = Utc::now();
        let r = evaluate(
            vec![],
            vec![QualitySource {
                table: "logs".into(),
                signal: "logs".into(),
                status: "unavailable".into(),
                sampled_records: 0,
                capped: false,
            }],
            vec!["checkout".into()],
            false,
            now,
            now,
            60,
        );
        assert!(r.partial);
        assert_eq!(r.services[0].status, "unknown");
    }
    #[test]
    fn queries_are_scoped_bounded_and_do_not_read_log_bodies() {
        let now = Utc::now();
        for source in SOURCES {
            let sql = sample_sql(source, "a'b", now, now);
            assert!(sql.contains("tenant_id = 'a''b'"));
            assert!(sql.contains("LIMIT 1001"));
            assert!(!sql.contains("Body"));
        }
        assert!(validated_minutes(&QualityParams { minutes: Some(0) }).is_err());
        assert!(
            validated_minutes(&QualityParams {
                minutes: Some(10081)
            })
            .is_err()
        );
    }

    #[test]
    fn low_volume_cardinality_checks_are_not_evaluated() {
        let r = run(
            vec![("metrics", sample("small")), ("traces", sample("small"))],
            vec![],
        );
        for rule in ["label_cardinality", "span_names"] {
            assert_eq!(
                r.services[0]
                    .checks
                    .iter()
                    .find(|c| c.rule_id == rule)
                    .unwrap()
                    .status,
                "not_evaluated"
            );
        }
    }

    #[test]
    fn dynamic_names_require_cardinality_evidence() {
        let rows = (0..200)
            .map(|i| {
                let mut s = sample("checkout");
                s.name_hash = i;
                s.dynamic_name = 1;
                ("traces", s)
            })
            .collect();
        let r = run(rows, vec![]);
        assert!(r.findings.iter().any(|f| f.rule_id == "span_names"));
        let r = run(vec![("traces", sample("checkout")); 200], vec![]);
        assert!(!r.findings.iter().any(|f| f.rule_id == "span_names"));
    }

    #[tokio::test]
    #[ignore = "read-only schema check against RUSH_TEST_CLICKHOUSE_URL and its configured tenant"]
    async fn live_sample_queries_match_the_schema() {
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
            let rows = read_sample(
                &client,
                &sample_sql(table, &tenant, to - chrono::Duration::hours(24), to),
                &tenant,
            )
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"));
            println!("{table}: {} sampled rows", rows.len());
            assert!(rows.len() <= SAMPLE_LIMIT + 1);
        }
    }
}
