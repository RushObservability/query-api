use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Top-level config loaded from `wide.toml`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct WideConfig {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageConfig {
    pub s3: Option<S3Config>,
    #[serde(default)]
    pub tiering: TieringConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TieringConfig {
    /// Move metric parts to S3 after N days. 0 = keep on local disk only.
    #[serde(default = "default_tiering_days")]
    pub metrics_move_after_days: u32,
    /// Move trace/span parts to S3 after N days. 0 = keep on local disk only.
    #[serde(default = "default_tiering_days")]
    pub traces_move_after_days: u32,
    /// Move log parts to S3 after N days. 0 = keep on local disk only.
    #[serde(default = "default_tiering_days")]
    pub logs_move_after_days: u32,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            metrics_move_after_days: default_tiering_days(),
            traces_move_after_days: default_tiering_days(),
            logs_move_after_days: default_tiering_days(),
        }
    }
}

fn default_tiering_days() -> u32 {
    3
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RetentionConfig {
    #[serde(default)]
    pub defaults: RetentionDefaults,
    #[serde(default)]
    pub metrics: Vec<MetricRetentionRule>,
    #[serde(default)]
    pub traces: Vec<TraceRetentionRule>,
    #[serde(default)]
    pub enforcer: EnforcerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetentionDefaults {
    #[serde(default = "default_30")]
    pub metrics_days: u32,
    #[serde(default = "default_30")]
    pub traces_days: u32,
    #[serde(default = "default_30")]
    pub logs_days: u32,
}

impl Default for RetentionDefaults {
    fn default() -> Self {
        Self {
            metrics_days: 30,
            traces_days: 30,
            logs_days: 30,
        }
    }
}

fn default_30() -> u32 {
    30
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricRetentionRule {
    /// Glob pattern on MetricName (e.g. `http_*`)
    pub name: Option<String>,
    /// Regex on MetricName
    pub name_regex: Option<String>,
    /// Key=value matches on the Attributes map
    #[serde(default)]
    pub labels: HashMap<String, String>,
    pub retain_days: u32,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TraceRetentionRule {
    pub service_name: Option<String>,
    pub attribute: Option<AttributeMatch>,
    pub retain_days: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AttributeMatch {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnforcerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_3600")]
    pub interval_secs: u64,
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for EnforcerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 3600,
            dry_run: false,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_3600() -> u64 {
    3600
}

impl WideConfig {
    /// Load config from a TOML file. Returns defaults if the file doesn't exist.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            tracing::info!("config file not found at {}, using defaults", path.display());
            return Ok(Self::default());
        }
        let contents = std::fs::read_to_string(path)?;
        let config: WideConfig = toml::from_str(&contents)?;
        tracing::info!("loaded config from {}", path.display());
        Ok(config)
    }

    /// The table-level TTL for metrics must be >= all per-rule retain_days so that
    /// ClickHouse's part-level TTL doesn't prematurely drop parts containing rows
    /// that have longer per-rule retention. The enforcer handles finer-grained
    /// deletion via `ALTER TABLE DELETE WHERE ...`.
    pub fn effective_metrics_ttl_days(&self) -> u32 {
        let base = self.retention.defaults.metrics_days;
        self.retention
            .metrics
            .iter()
            .map(|r| r.retain_days)
            .fold(base, u32::max)
    }

    /// Same logic for traces — table TTL = max(default, all overrides).
    pub fn effective_traces_ttl_days(&self) -> u32 {
        let base = self.retention.defaults.traces_days;
        self.retention
            .traces
            .iter()
            .map(|r| r.retain_days)
            .fold(base, u32::max)
    }

    pub fn effective_logs_ttl_days(&self) -> u32 {
        self.retention.defaults.logs_days
    }
}
