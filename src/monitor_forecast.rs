use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

use crate::promql::compute::linear_regression_slope;

pub const HISTORY_SECS: i64 = 24 * 3600;
const BUCKET_SECS: i64 = 3600;
const MIN_SAMPLES: usize = 12;
const MIN_COVERAGE_SECS: i64 = 12 * 3600;
const SERIES_LIMIT: usize = 100;
const MAX_LABELS_PER_MAP: usize = 128;
const RESOURCE_ERROR: &str = "Choose a gauge metric and a valid resource series.";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastConfig {
    pub metric_name: String,
    pub resource: ForecastResource,
    pub input_unit: InputUnit,
    pub horizon_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastResource {
    pub service: String,
    pub attributes: BTreeMap<String, String>,
    pub resource_attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputUnit {
    Ratio,
    Percent,
}

pub(crate) fn valid_metric(name: &str) -> bool {
    !name.trim().is_empty() && name.len() <= 256 && !name.chars().any(char::is_control)
}

impl ForecastConfig {
    pub(crate) fn from_query(query: &serde_json::Value) -> Result<Self, String> {
        let metric_name = query["metric_name"]
            .as_str()
            .filter(|name| valid_metric(name))
            .ok_or(RESOURCE_ERROR)?;
        let resource = &query["resource"];
        let service = resource["service"]
            .as_str()
            .filter(|service| service.len() <= 1024)
            .ok_or(RESOURCE_ERROR)?;
        let resource = ForecastResource {
            service: service.to_owned(),
            attributes: query_labels(&resource["attributes"]).ok_or(RESOURCE_ERROR)?,
            resource_attributes: query_labels(&resource["resource_attributes"])
                .ok_or(RESOURCE_ERROR)?,
        };
        if !valid_resource(&resource) {
            return Err(RESOURCE_ERROR.into());
        }
        let input_unit = match query["input_unit"].as_str() {
            Some("ratio") => InputUnit::Ratio,
            Some("percent") => InputUnit::Percent,
            _ => return Err("Sample unit must be ratio or percent.".into()),
        };
        let horizon_secs = query["horizon_secs"]
            .as_i64()
            .filter(|seconds| [21_600, 43_200, 86_400, 172_800].contains(seconds))
            .ok_or("Forecast window must be 6, 12, 24, or 48 hours.")?;
        Ok(Self {
            metric_name: metric_name.to_owned(),
            resource,
            input_unit,
            horizon_secs,
        })
    }
}

fn query_labels(value: &serde_json::Value) -> Option<BTreeMap<String, String>> {
    let labels = value
        .as_object()
        .filter(|labels| labels.len() <= MAX_LABELS_PER_MAP)?;
    labels
        .iter()
        .map(|(key, value)| {
            let value = value.as_str()?;
            valid_label(key, value).then(|| (key.clone(), value.to_owned()))
        })
        .collect()
}

fn valid_label(key: &str, value: &str) -> bool {
    !key.is_empty() && key.len() <= 512 && value.len() <= 2048
}

fn valid_resource(resource: &ForecastResource) -> bool {
    let identity_bytes: usize = resource
        .attributes
        .iter()
        .chain(&resource.resource_attributes)
        .map(|(key, value)| key.len() + value.len())
        .sum();
    resource.service.len() <= 1024
        && identity_bytes <= 8192
        && [&resource.attributes, &resource.resource_attributes]
            .iter()
            .all(|labels| {
                labels.len() <= MAX_LABELS_PER_MAP
                    && labels.iter().all(|(key, value)| valid_label(key, value))
            })
}

#[derive(Debug, Serialize)]
pub struct ResourceChoices {
    pub resources: Vec<ForecastResource>,
    pub truncated: bool,
}

#[derive(Row, Deserialize)]
struct ResourceRow {
    service: String,
    attributes: Vec<(String, String)>,
    resource_attributes: Vec<(String, String)>,
}

pub async fn resources(
    ch: &Client,
    tenant: &str,
    metric: &str,
    filter: &str,
    to_ms: i64,
) -> anyhow::Result<ResourceChoices> {
    let sql = "SELECT ServiceName AS service, mapSort(Attributes) AS attributes,
                      mapSort(ResourceAttributes) AS resource_attributes
               FROM metrics_gauge
               PREWHERE tenant_id = ? AND MetricName = ?
                 AND TimeUnix >= fromUnixTimestamp64Milli(?) AND TimeUnix <= fromUnixTimestamp64Milli(?)
               WHERE bitAnd(Flags, 1) = 0 AND (
                 positionCaseInsensitiveUTF8(service, ?) > 0
                 OR arrayExists(value -> positionCaseInsensitiveUTF8(value, ?) > 0, mapValues(Attributes))
                 OR arrayExists(value -> positionCaseInsensitiveUTF8(value, ?) > 0, mapValues(ResourceAttributes))
               )
               GROUP BY service, attributes, resource_attributes
               ORDER BY service, attributes, resource_attributes LIMIT ?";
    let rows = crate::tenant_query(ch, sql, tenant)
        .bind(tenant)
        .bind(metric)
        .bind(to_ms - HISTORY_SECS * 1000)
        .bind(to_ms)
        .bind(filter)
        .bind(filter)
        .bind(filter)
        .bind(SERIES_LIMIT + 1)
        .fetch_all::<ResourceRow>()
        .await?;
    let truncated = rows.len() > SERIES_LIMIT;
    let resources = rows
        .into_iter()
        .take(SERIES_LIMIT)
        .map(|row| ForecastResource {
            service: row.service,
            attributes: row.attributes.into_iter().collect(),
            resource_attributes: row.resource_attributes.into_iter().collect(),
        })
        .filter(valid_resource)
        .collect();
    Ok(ResourceChoices {
        resources,
        truncated,
    })
}

#[derive(Row, Deserialize)]
struct GaugeSample {
    timestamp_ms: i64,
    value: f64,
}

#[derive(Debug, Serialize)]
pub struct ForecastPreview {
    pub evaluated_at: String,
    pub history_from: String,
    pub horizon_end: String,
    pub threshold_pct: f64,
    pub current_pct: Option<f64>,
    pub projected_pct: Option<f64>,
    pub crossing_at: Option<String>,
    pub no_data: Option<NoData>,
    pub history: Vec<ForecastPoint>,
    pub projection: Vec<ForecastPoint>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NoData {
    InsufficientHistory,
    Stale,
    InvalidUnit,
}

#[derive(Debug, Serialize)]
pub struct ForecastPoint {
    pub timestamp: String,
    pub value: f64,
}

impl ForecastPreview {
    pub(crate) fn reading(&self) -> Option<f64> {
        Some(self.current_pct?.max(self.projected_pct?))
    }
}

fn iso(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("forecast time derived from current evaluation time")
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub async fn evaluate(
    ch: &Client,
    tenant: &str,
    config: &ForecastConfig,
    threshold: f64,
    to_ms: i64,
) -> anyhow::Result<ForecastPreview> {
    // Keep one actual sample per hour. Restrict to the exact series chosen in
    // the editor, including resource attributes, so separate hosts never mix.
    let sql = "SELECT toUnixTimestamp64Milli(max(TimeUnix)) AS timestamp_ms,
                      argMax(Value, tuple(TimeUnix, Value)) AS value
               FROM metrics_gauge
               PREWHERE tenant_id = ? AND MetricName = ? AND ServiceName = ?
                 AND TimeUnix >= fromUnixTimestamp64Milli(?) AND TimeUnix <= fromUnixTimestamp64Milli(?)
               WHERE bitAnd(Flags, 1) = 0
                 AND mapSort(Attributes) = mapFromArrays(CAST(? AS Array(String)), CAST(? AS Array(String)))
                 AND mapSort(ResourceAttributes) = mapFromArrays(CAST(? AS Array(String)), CAST(? AS Array(String)))
               GROUP BY intDiv(? - toUnixTimestamp64Milli(TimeUnix), ?)
               ORDER BY timestamp_ms";
    let samples = crate::tenant_query(ch, sql, tenant)
        .bind(tenant)
        .bind(&config.metric_name)
        .bind(&config.resource.service)
        .bind(to_ms - HISTORY_SECS * 1000)
        .bind(to_ms)
        .bind(config.resource.attributes.keys().collect::<Vec<_>>())
        .bind(config.resource.attributes.values().collect::<Vec<_>>())
        .bind(
            config
                .resource
                .resource_attributes
                .keys()
                .collect::<Vec<_>>(),
        )
        .bind(
            config
                .resource
                .resource_attributes
                .values()
                .collect::<Vec<_>>(),
        )
        .bind(to_ms)
        .bind(BUCKET_SECS * 1000)
        .fetch_all::<GaugeSample>()
        .await?;
    Ok(forecast(config, threshold, to_ms, samples))
}

fn forecast(
    config: &ForecastConfig,
    threshold: f64,
    to_ms: i64,
    samples: Vec<GaugeSample>,
) -> ForecastPreview {
    let scale = match config.input_unit {
        InputUnit::Ratio => 100.0,
        InputUnit::Percent => 1.0,
    };
    let horizon_ms = config.horizon_secs * 1000;
    let valid_pct = |value: f64| (0.0..=100.0).contains(&(value * scale));
    let mut result = ForecastPreview {
        evaluated_at: iso(to_ms),
        history_from: iso(to_ms - HISTORY_SECS * 1000),
        horizon_end: iso(to_ms + horizon_ms),
        threshold_pct: threshold,
        current_pct: samples
            .last()
            .filter(|sample| valid_pct(sample.value))
            .map(|sample| sample.value * scale),
        projected_pct: None,
        crossing_at: None,
        no_data: None,
        history: samples
            .iter()
            .filter(|sample| valid_pct(sample.value))
            .map(|sample| ForecastPoint {
                timestamp: iso(sample.timestamp_ms),
                value: sample.value * scale,
            })
            .collect(),
        projection: Vec::new(),
    };

    let Some((first, last)) = samples.first().zip(samples.last()) else {
        result.no_data = Some(NoData::InsufficientHistory);
        return result;
    };
    if samples.iter().any(|sample| !valid_pct(sample.value)) {
        result.no_data = Some(NoData::InvalidUnit);
        return result;
    }
    if to_ms - last.timestamp_ms >= 1000 * BUCKET_SECS {
        result.no_data = Some(NoData::Stale);
        return result;
    }
    if samples.len() < MIN_SAMPLES
        || last.timestamp_ms - first.timestamp_ms < 1000 * MIN_COVERAGE_SECS
    {
        result.no_data = Some(NoData::InsufficientHistory);
        return result;
    }

    // Center timestamps before regression to retain precision at Unix-epoch scale.
    // Anchor the forecast at the latest actual value, as shown in the rule editor.
    let regression = samples
        .iter()
        .map(|sample| {
            (
                (sample.timestamp_ms - last.timestamp_ms) as f64 / 1000.0,
                sample.value * scale,
            )
        })
        .collect::<Vec<_>>();
    let slope = linear_regression_slope(&regression).expect("history spans distinct times");
    let latest = last.value * scale;
    let projected = (latest + slope * config.horizon_secs as f64).clamp(0.0, 100.0);
    result.projected_pct = Some(projected);
    result.projection.push(ForecastPoint {
        timestamp: result.evaluated_at.clone(),
        value: latest,
    });
    if slope != 0.0 {
        let bound = if slope > 0.0 { 100.0 } else { 0.0 };
        let bound_ms = ((bound - latest) / slope * 1000.0).ceil() as i64;
        if (1..horizon_ms).contains(&bound_ms) {
            result.projection.push(ForecastPoint {
                timestamp: iso(to_ms + bound_ms),
                value: bound,
            });
        }
    }
    result.projection.push(ForecastPoint {
        timestamp: result.horizon_end.clone(),
        value: projected,
    });
    result.crossing_at = if latest >= threshold {
        Some(result.evaluated_at.clone())
    } else if projected >= threshold && slope > 0.0 {
        let crossing_ms = (((threshold - latest) / slope * 1000.0).ceil() as i64).min(horizon_ms);
        Some(iso(to_ms + crossing_ms))
    } else {
        None
    };
    result
}

#[cfg(test)]
mod tests;
