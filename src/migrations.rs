use clickhouse::Client;

use crate::config::RushConfig;

/// Ordered list of DDL statements to ensure the observability schema exists.
/// v2: Multi-tenant schema — every table carries tenant_id as the first ORDER BY column.
/// Starts with DROP TABLE IF EXISTS for all v1 tables, then recreates with v2 schemas.
const MIGRATIONS: &[&str] = &[
    // ── Database ──
    "CREATE DATABASE IF NOT EXISTS observability",
    // ── OTel traces ──
    // NOTE: `spans_raw` (the OTel-native landing table) and its `spans_mv` transform are
    // GONE. query-api now reshapes OTel spans into the wide `spans` row in Rust at ingest
    // (see `impl From<TraceInsertRow> for WideEvent`) and inserts directly into `spans` —
    // one physical table for spans, no duplicate raw copy. The `spans_by_trace` and
    // `services` MVs (below) read FROM `spans` and fire on the direct insert. Existing
    // deployments drop the old objects via the DROP statements below.

    // ── Wide events (v2: multi-tenant flattened query-friendly schema) ──
    r"CREATE TABLE IF NOT EXISTS observability.spans
(
    `tenant_id` LowCardinality(String),
    `timestamp` DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `trace_id` String CODEC(ZSTD(1)),
    `span_id` String CODEC(ZSTD(1)),
    `parent_span_id` String CODEC(ZSTD(1)),
    `service_name` LowCardinality(String) CODEC(ZSTD(1)),
    `span_name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` LowCardinality(String) CODEC(ZSTD(1)),
    `status` LowCardinality(String) CODEC(ZSTD(1)),
    `duration_ns` UInt64 CODEC(ZSTD(1)),
    `http_method` LowCardinality(String) CODEC(ZSTD(1)),
    `http_path` String CODEC(ZSTD(1)),
    `http_status_code` UInt16 CODEC(ZSTD(1)),
    `attributes` String CODEC(ZSTD(1)),
    `event_names` Array(LowCardinality(String)),
    `event_timestamps` Array(DateTime64(9, 'UTC')),
    `event_attributes` Array(String),
    `link_trace_ids` Array(String),
    `link_span_ids` Array(String),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_span_id span_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_parent_span_id parent_span_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_service_name service_name TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_name span_name TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_http_method http_method TYPE set(16) GRANULARITY 4,
    INDEX idx_status status TYPE set(8) GRANULARITY 4,
    INDEX idx_http_status http_status_code TYPE minmax GRANULARITY 1,
    INDEX idx_duration duration_ns TYPE minmax GRANULARITY 1
    -- Free-text search index (text on 26.2+, ngrambf fallback below) is added
    -- version-aware by apply_skip_indexes(), not inline, so CREATE TABLE works on any version.
)
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, timestamp, service_name, trace_id, span_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Drop the legacy spans_raw landing table + its transform MV ──
    // Spans are now ingested directly into `spans` (the transform moved to Rust). Drop the
    // MV before the table (the MV depends on it). No-ops on a fresh install; on existing
    // deployments they reclaim the duplicate raw copy. Order matters: these run after the
    // `spans` CREATE above and before the `spans_by_trace`/`services` MVs below.
    "DROP VIEW IF EXISTS observability.spans_mv",
    "DROP TABLE IF EXISTS observability.spans_raw SYNC",
    // ── MV: trace index for fast trace-id lookups (v2: tenant-scoped) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_trace
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, trace_id, timestamp)
AS SELECT
    tenant_id, trace_id, span_id, parent_span_id, service_name,
    http_method, http_path, http_status_code,
    duration_ns, status, timestamp
FROM observability.spans",
    // ── MV: service catalog (v2: tenant-scoped) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.services
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (tenant_id, service_name, http_path, http_method)
AS SELECT
    tenant_id, service_name, http_path, http_method,
    max(timestamp) AS last_seen,
    count() AS request_count
FROM observability.spans
GROUP BY tenant_id, service_name, http_path, http_method",
    // ── Gauge metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_gauge
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount` UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricDescription` String CODEC(ZSTD(1)),
    `MetricUnit` String CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `TimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `Value` Float64 CODEC(Gorilla, ZSTD(1)),
    `Flags` UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_value Value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Sum metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_sum
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount` UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricDescription` String CODEC(ZSTD(1)),
    `MetricUnit` String CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `TimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `Value` Float64 CODEC(Gorilla, ZSTD(1)),
    `Flags` UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    `AggregationTemporality` Int32 CODEC(ZSTD(1)),
    `IsMonotonic` Boolean CODEC(Delta, ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_value Value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Histogram metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_histogram
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount` UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricDescription` String CODEC(ZSTD(1)),
    `MetricUnit` String CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `TimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `Count` UInt64 CODEC(Delta, ZSTD(1)),
    `Sum` Float64 CODEC(ZSTD(1)),
    `BucketCounts` Array(UInt64) CODEC(ZSTD(1)),
    `ExplicitBounds` Array(Float64) CODEC(ZSTD(1)),
    `Flags` UInt32 CODEC(ZSTD(1)),
    `Min` Float64 CODEC(ZSTD(1)),
    `Max` Float64 CODEC(ZSTD(1)),
    `AggregationTemporality` Int32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_min Min TYPE minmax GRANULARITY 1,
    INDEX idx_max Max TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Exponential Histogram metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_exp_histogram
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount` UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricDescription` String CODEC(ZSTD(1)),
    `MetricUnit` String CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `TimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `Count` UInt64 CODEC(Delta, ZSTD(1)),
    `Sum` Float64 CODEC(ZSTD(1)),
    `Scale` Int32 CODEC(ZSTD(1)),
    `ZeroCount` UInt64 CODEC(ZSTD(1)),
    `PositiveOffset` Int32 CODEC(ZSTD(1)),
    `PositiveBucketCounts` Array(UInt64) CODEC(ZSTD(1)),
    `NegativeOffset` Int32 CODEC(ZSTD(1)),
    `NegativeBucketCounts` Array(UInt64) CODEC(ZSTD(1)),
    `Flags` UInt32 CODEC(ZSTD(1)),
    `Min` Float64 CODEC(ZSTD(1)),
    `Max` Float64 CODEC(ZSTD(1)),
    `AggregationTemporality` Int32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_min Min TYPE minmax GRANULARITY 1,
    INDEX idx_max Max TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Summary metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_summary
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount` UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName` LowCardinality(String) CODEC(ZSTD(1)),
    `MetricDescription` String CODEC(ZSTD(1)),
    `MetricUnit` String CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `TimeUnix` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `Count` UInt64 CODEC(Delta, ZSTD(1)),
    `Sum` Float64 CODEC(ZSTD(1)),
    `ValueAtQuantiles` Nested(Quantile Float64, Value Float64) CODEC(ZSTD(1)),
    `Flags` UInt32 CODEC(ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sum Sum TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── OTel Logs (v2: multi-tenant with SIEM materialized columns) ──
    r"CREATE TABLE IF NOT EXISTS observability.logs
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimestampDate` Date DEFAULT toDate(Timestamp),
    `TimestampTime` DateTime DEFAULT toDateTime(Timestamp),
    `TraceId` String CODEC(ZSTD(1)),
    `SpanId` String CODEC(ZSTD(1)),
    `TraceFlags` UInt32 CODEC(ZSTD(1)),
    `SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
    `SeverityNumber` UInt8 CODEC(ZSTD(1)),
    `Body` String CODEC(ZSTD(3)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl` String CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeSchemaUrl` String CODEC(ZSTD(1)),
    `ScopeName` String CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `EventName` LowCardinality(String) CODEC(ZSTD(1)),
    `mat_k8s_namespace` String MATERIALIZED ResourceAttributes['k8s.namespace.name'],
    `mat_k8s_pod` String MATERIALIZED ResourceAttributes['k8s.pod.name'],
    `mat_k8s_container` String MATERIALIZED ResourceAttributes['k8s.container.name'],
    `mat_k8s_deployment` String MATERIALIZED ResourceAttributes['k8s.deployment.name'],
    `mat_environment` LowCardinality(String) MATERIALIZED ResourceAttributes['deployment.environment'],
    `mat_source_ip` String MATERIALIZED LogAttributes['net.peer.ip'],
    `mat_user_id` String MATERIALIZED LogAttributes['enduser.id'],
    `mat_action` LowCardinality(String) MATERIALIZED LogAttributes['audit.action'],
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_service_name ServiceName TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_severity SeverityText TYPE set(8) GRANULARITY 4,
    INDEX idx_source_ip mat_source_ip TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_user_id mat_user_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY TimestampDate
PRIMARY KEY (tenant_id, TimestampDate, TimestampTime, ServiceName, SeverityText)
ORDER BY (tenant_id, TimestampDate, TimestampTime, ServiceName, SeverityText, Timestamp)
TTL TimestampDate + toIntervalDay(30)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1,
    enable_block_number_column = 1, enable_block_offset_column = 1",
    // Persist original insert coordinates so slim list rows can lazy-load full
    // attributes after background part merges. Metadata-only and idempotent.
    "ALTER TABLE observability.logs MODIFY SETTING \
        enable_block_number_column = 1, enable_block_offset_column = 1",
    // ── Signal usage tracking (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.signal_usage
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `signal_name` LowCardinality(String),
    `signal_type` LowCardinality(String),
    `source` LowCardinality(String),
    `last_queried_at` DateTime64(3) DEFAULT now64(3),
    `query_count` UInt64 DEFAULT 1
)
ENGINE = ReplacingMergeTree(last_queried_at)
ORDER BY (tenant_id, signal_type, signal_name, source)
TTL toDateTime(last_queried_at) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192",
    // ── RUM (Real User Monitoring) events (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.rum
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimestampTime` DateTime DEFAULT toDateTime(Timestamp),
    `AppName` LowCardinality(String) CODEC(ZSTD(1)),
    `AppVersion` LowCardinality(String) CODEC(ZSTD(1)),
    `Environment` LowCardinality(String) CODEC(ZSTD(1)),
    `SessionId` String CODEC(ZSTD(1)),
    `UserId` String CODEC(ZSTD(1)),
    `PageUrl` String CODEC(ZSTD(1)),
    `PagePath` String CODEC(ZSTD(1)),
    `ViewName` String CODEC(ZSTD(1)),
    `Referrer` String CODEC(ZSTD(1)),
    `BrowserName` LowCardinality(String) CODEC(ZSTD(1)),
    `BrowserVersion` LowCardinality(String) CODEC(ZSTD(1)),
    `OsName` LowCardinality(String) CODEC(ZSTD(1)),
    `OsVersion` LowCardinality(String) CODEC(ZSTD(1)),
    `DeviceType` LowCardinality(String) CODEC(ZSTD(1)),
    `ScreenWidth` UInt16 CODEC(ZSTD(1)),
    `ScreenHeight` UInt16 CODEC(ZSTD(1)),
    `EventType` LowCardinality(String) CODEC(ZSTD(1)),
    `EventName` String CODEC(ZSTD(1)),
    `VitalName` LowCardinality(String) CODEC(ZSTD(1)),
    `VitalValue` Float64 CODEC(Gorilla, ZSTD(1)),
    `VitalRating` LowCardinality(String) CODEC(ZSTD(1)),
    `ErrorMessage` String CODEC(ZSTD(1)),
    `ErrorStack` String CODEC(ZSTD(1)),
    `ErrorType` LowCardinality(String) CODEC(ZSTD(1)),
    `InteractionTarget` String CODEC(ZSTD(1)),
    `InteractionType` LowCardinality(String) CODEC(ZSTD(1)),
    `DurationMs` Float64 CODEC(Gorilla, ZSTD(1)),
    `TraceId` String CODEC(ZSTD(1)),
    `SpanId` String CODEC(ZSTD(1)),
    `Attributes` String CODEC(ZSTD(1)),
    INDEX idx_session_id SessionId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_user_id UserId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_error_message ErrorMessage TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8,
    INDEX idx_vital_name VitalName TYPE set(20) GRANULARITY 4,
    INDEX idx_vital_value VitalValue TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (tenant_id, AppName, EventType, TimestampTime)
ORDER BY (tenant_id, AppName, EventType, TimestampTime, PagePath, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Session replay chunks (rrweb DOM snapshot + mutation events) ──
    r"CREATE TABLE IF NOT EXISTS observability.rum_replay
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `session_id` String CODEC(ZSTD(1)),
    `app_name` LowCardinality(String) CODEC(ZSTD(1)),
    `chunk_idx` UInt32,
    `chunk_ts` DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    `events_json` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (tenant_id, session_id, chunk_idx)
TTL toDateTime(chunk_ts) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
    // ── Tenant usage metering (per-tenant ingest volume tracking) ──
    r"CREATE TABLE IF NOT EXISTS observability.tenant_usage
(
    `tenant_id` LowCardinality(String),
    `signal` LowCardinality(String),
    `bucket` DateTime DEFAULT toStartOfHour(now()),
    `events_count` UInt64,
    `bytes_count` UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, signal, bucket)
TTL bucket + INTERVAL 400 DAY DELETE
SETTINGS index_granularity = 8192",
    // ════════════════════════════════════════════════════════════════════
    // Metric rollups (1m + 1h pre-aggregation for gauge + sum)
    //
    // Goal: coarse-window PromQL/stat reads scan tiny pre-bucketed tables instead of
    // millions of raw samples, while staying NUMERICALLY IDENTICAL to a raw read for
    // the safe aggregations.
    //
    // Engine choice: AggregatingMergeTree with *State columns. This is the only engine
    // that expresses avg AND last (argMax) correctly:
    //   - avg via avgState/avgMerge carries (sum,count) internally, so merging partial
    //     states across buckets yields the exact weighted mean (NOT avg-of-avgs).
    //   - last via argMaxState(Value, TimeUnix)/argMaxMerge picks the true latest sample
    //     by wall-clock time across merged buckets.
    //   - min/max/count merge trivially and exactly.
    // SummingMergeTree was rejected: it cannot express avg or last.
    //
    // GAUGE rollups store: avg, min, max, last(argMax by TimeUnix), count + anyLast
    // MetricDescription/MetricUnit. A coarse gauge query (avg/min/max/last/count over a
    // step) reads these directly.
    //
    // SUM (counter) rollups store: last(argMax by TimeUnix), min, max, count. They do
    // NOT store a precomputed rate and do NOT store sum-of-values: rate()/increase()
    // need adjacent RAW samples for counter-reset detection and MUST read raw (see
    // promql/eval.rs source-selection). The rollup only serves a counter's *instant*
    // value (the last sample in a bucket).
    //
    // Both 1m and 1h are built directly FROM RAW (not cascaded 1h-from-1m). ClickHouse
    // has no generic "re-aggregate an existing State into a coarser State" combinator
    // usable inside a chained MV, so a pure-MV cascade can't be expressed without an
    // approximation. Building both from raw was verified numerically identical to
    // 1h-from-raw on live CH, so correctness is guaranteed and the extra raw read for
    // the 1h MV happens once per insert batch (cheap, amortized).
    //
    // Backfill of pre-existing raw data is handled separately in backfill_rollups();
    // MVs only capture rows inserted after creation.
    // ════════════════════════════════════════════════════════════════════

    // ── Gauge 1-minute rollup target ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_gauge_1m
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ServiceName` LowCardinality(String),
    `MetricName` LowCardinality(String),
    `Attributes` Map(LowCardinality(String), String),
    `bucket` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `MetricDescription` AggregateFunction(anyLast, String),
    `MetricUnit` AggregateFunction(anyLast, String),
    `avg_state` AggregateFunction(avg, Float64),
    `min_state` AggregateFunction(min, Float64),
    `max_state` AggregateFunction(max, Float64),
    `last_state` AggregateFunction(argMax, Float64, DateTime64(9)),
    `cnt_state` AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, MetricName, ServiceName, Attributes, bucket)
TTL toDateTime(bucket) + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, storage_policy = 'tiered'",
    // ── Gauge 1-hour rollup target ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_gauge_1h
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ServiceName` LowCardinality(String),
    `MetricName` LowCardinality(String),
    `Attributes` Map(LowCardinality(String), String),
    `bucket` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `MetricDescription` AggregateFunction(anyLast, String),
    `MetricUnit` AggregateFunction(anyLast, String),
    `avg_state` AggregateFunction(avg, Float64),
    `min_state` AggregateFunction(min, Float64),
    `max_state` AggregateFunction(max, Float64),
    `last_state` AggregateFunction(argMax, Float64, DateTime64(9)),
    `cnt_state` AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, MetricName, ServiceName, Attributes, bucket)
TTL toDateTime(bucket) + INTERVAL 730 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, storage_policy = 'tiered'",
    // ── Sum 1-minute rollup target ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_sum_1m
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ServiceName` LowCardinality(String),
    `MetricName` LowCardinality(String),
    `Attributes` Map(LowCardinality(String), String),
    `bucket` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `MetricDescription` AggregateFunction(anyLast, String),
    `MetricUnit` AggregateFunction(anyLast, String),
    `last_state` AggregateFunction(argMax, Float64, DateTime64(9)),
    `min_state` AggregateFunction(min, Float64),
    `max_state` AggregateFunction(max, Float64),
    `cnt_state` AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, MetricName, ServiceName, Attributes, bucket)
TTL toDateTime(bucket) + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, storage_policy = 'tiered'",
    // ── Sum 1-hour rollup target ──
    r"CREATE TABLE IF NOT EXISTS observability.metrics_sum_1h
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `ServiceName` LowCardinality(String),
    `MetricName` LowCardinality(String),
    `Attributes` Map(LowCardinality(String), String),
    `bucket` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `MetricDescription` AggregateFunction(anyLast, String),
    `MetricUnit` AggregateFunction(anyLast, String),
    `last_state` AggregateFunction(argMax, Float64, DateTime64(9)),
    `min_state` AggregateFunction(min, Float64),
    `max_state` AggregateFunction(max, Float64),
    `cnt_state` AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, MetricName, ServiceName, Attributes, bucket)
TTL toDateTime(bucket) + INTERVAL 730 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, storage_policy = 'tiered'",
    // ── MV: metrics_gauge → metrics_gauge_1m ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauge_1m_mv
TO observability.metrics_gauge_1m
AS SELECT
    tenant_id, ServiceName, MetricName, Attributes,
    toStartOfInterval(TimeUnix, INTERVAL 1 MINUTE) AS bucket,
    anyLastState(MetricDescription) AS MetricDescription,
    anyLastState(MetricUnit) AS MetricUnit,
    avgState(Value) AS avg_state,
    minState(Value) AS min_state,
    maxState(Value) AS max_state,
    argMaxState(Value, TimeUnix) AS last_state,
    countState() AS cnt_state
FROM observability.metrics_gauge
GROUP BY tenant_id, ServiceName, MetricName, Attributes, bucket",
    // ── MV: metrics_gauge → metrics_gauge_1h (from raw, not cascaded) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauge_1h_mv
TO observability.metrics_gauge_1h
AS SELECT
    tenant_id, ServiceName, MetricName, Attributes,
    toStartOfInterval(TimeUnix, INTERVAL 1 HOUR) AS bucket,
    anyLastState(MetricDescription) AS MetricDescription,
    anyLastState(MetricUnit) AS MetricUnit,
    avgState(Value) AS avg_state,
    minState(Value) AS min_state,
    maxState(Value) AS max_state,
    argMaxState(Value, TimeUnix) AS last_state,
    countState() AS cnt_state
FROM observability.metrics_gauge
GROUP BY tenant_id, ServiceName, MetricName, Attributes, bucket",
    // ── MV: metrics_sum → metrics_sum_1m ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_sum_1m_mv
TO observability.metrics_sum_1m
AS SELECT
    tenant_id, ServiceName, MetricName, Attributes,
    toStartOfInterval(TimeUnix, INTERVAL 1 MINUTE) AS bucket,
    anyLastState(MetricDescription) AS MetricDescription,
    anyLastState(MetricUnit) AS MetricUnit,
    argMaxState(Value, TimeUnix) AS last_state,
    minState(Value) AS min_state,
    maxState(Value) AS max_state,
    countState() AS cnt_state
FROM observability.metrics_sum
GROUP BY tenant_id, ServiceName, MetricName, Attributes, bucket",
    // ── MV: metrics_sum → metrics_sum_1h (from raw, not cascaded) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_sum_1h_mv
TO observability.metrics_sum_1h
AS SELECT
    tenant_id, ServiceName, MetricName, Attributes,
    toStartOfInterval(TimeUnix, INTERVAL 1 HOUR) AS bucket,
    anyLastState(MetricDescription) AS MetricDescription,
    anyLastState(MetricUnit) AS MetricUnit,
    argMaxState(Value, TimeUnix) AS last_state,
    minState(Value) AS min_state,
    maxState(Value) AS max_state,
    countState() AS cnt_state
FROM observability.metrics_sum
GROUP BY tenant_id, ServiceName, MetricName, Attributes, bucket",
];

/// Row-level security policies for tenant isolation (defense-in-depth).
///
/// These are applied ONLY when ClickHouse is configured with
/// `custom_settings_prefixes = 'rush_'`. Without that server config,
/// `getSetting('rush_tenant_id')` is a hard error that breaks all queries.
///
/// Call `apply_row_policies()` after `probe_row_policy_support()` confirms
/// the custom setting is accepted.
const ROW_POLICY_TABLES: &[&str] = &[
    "logs",
    "spans",
    "spans_by_trace",
    "services",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
    // Metric rollups carry tenant_id too — same row-level isolation as raw.
    "metrics_gauge_1m",
    "metrics_gauge_1h",
    "metrics_sum_1m",
    "metrics_sum_1h",
    "rum",
    "rum_replay",
    "signal_usage",
    "tenant_usage",
];

fn validate_clickhouse_identifier(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        anyhow::bail!("invalid ClickHouse identifier: {value:?}");
    }
    Ok(())
}

fn row_policy_sql(table: &str, read_user: &str, alter: bool) -> String {
    let verb = if alter {
        "ALTER ROW POLICY"
    } else {
        "CREATE ROW POLICY IF NOT EXISTS"
    };
    format!(
        "{verb} tenant_isolation ON observability.{table} \
         FOR SELECT USING notEmpty(getSetting('rush_tenant_id')) \
         AND tenant_id = getSetting('rush_tenant_id') TO {read_user}"
    )
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TenantTableRow {
    table: String,
}

/// Fail startup when a new tenant-bearing telemetry table is added without
/// being enrolled in the policy and least-privilege grant set. Config and
/// audit tables deliberately stay on the privileged client; `*_mv` objects
/// write into already-protected target tables and are not granted to readers.
async fn verify_policy_table_coverage(client: &Client) -> anyhow::Result<()> {
    let rows = client
        .query(
            "SELECT DISTINCT table FROM system.columns \
             WHERE database = 'observability' AND name = 'tenant_id' \
               AND NOT startsWith(table, 'config_') \
               AND table != 'audit_events' \
               AND NOT endsWith(table, '_mv')",
        )
        .fetch_all::<TenantTableRow>()
        .await?;
    let missing: Vec<String> = rows
        .into_iter()
        .filter(|row| !ROW_POLICY_TABLES.contains(&row.table.as_str()))
        .map(|row| row.table)
        .collect();
    if !missing.is_empty() {
        anyhow::bail!(
            "tenant-bearing telemetry tables lack row-policy enrollment: {}",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Create or replace strict row policies on every tenant-scoped telemetry
/// table. Existing permissive policies are altered in place during upgrades.
pub async fn apply_row_policies(client: &Client, read_user: &str) -> anyhow::Result<()> {
    validate_clickhouse_identifier(read_user)?;
    verify_policy_table_coverage(client).await?;
    tracing::info!(
        read_user,
        "applying row-level security policies ({} tables)",
        ROW_POLICY_TABLES.len()
    );
    for table in ROW_POLICY_TABLES {
        let create = row_policy_sql(table, read_user, false);
        client.query(&create).execute().await?;

        let alter = row_policy_sql(table, read_user, true);
        client.query(&alter).execute().await?;
    }
    Ok(())
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ShowPolicyRow {
    statement: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct IsolationProbeRow {
    leaked: u64,
}

/// Verify policy definition and behavior through the SELECT-only application
/// principal. This checks more than custom-setting support: every expected
/// table must have the strict policy, target the read user, and suppress rows
/// whose tenant differs from the per-query setting.
pub async fn verify_row_policies(
    admin: &Client,
    read: &Client,
    read_user: &str,
) -> anyhow::Result<()> {
    validate_clickhouse_identifier(read_user)?;
    for table in ROW_POLICY_TABLES {
        let show = format!("SHOW CREATE ROW POLICY tenant_isolation ON observability.{table}");
        let ddl = admin.query(&show).fetch_one::<ShowPolicyRow>().await?;
        if !ddl
            .statement
            .contains("tenant_id = getSetting('rush_tenant_id')")
            || !ddl
                .statement
                .contains("notEmpty(getSetting('rush_tenant_id'))")
            || ddl.statement.contains("getSetting('rush_tenant_id') = ''")
            || !ddl.statement.contains(read_user)
        {
            anyhow::bail!(
                "row policy on observability.{table} is missing, permissive, or targets the wrong user"
            );
        }

        let probe = format!(
            "SELECT count() AS leaked FROM observability.{table} \
             WHERE tenant_id != getSetting('rush_tenant_id')"
        );
        let row = read
            .query(&probe)
            .with_option("rush_tenant_id", "__rush_policy_probe__")
            .with_option("max_execution_time", "10")
            .fetch_one::<IsolationProbeRow>()
            .await?;
        if row.leaked != 0 {
            anyhow::bail!("row policy leak detected on observability.{table}");
        }
    }
    Ok(())
}

/// Run all migrations against ClickHouse.
///
/// Connects **without** a default database so that `CREATE DATABASE` succeeds
/// even on a fresh instance. Every statement uses `IF NOT EXISTS` so this is
/// safe to call on every startup.
pub async fn run(
    url: &str,
    user: &str,
    password: &str,
    _config: &RushConfig,
) -> anyhow::Result<()> {
    let client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password);

    tracing::info!(
        "running clickhouse migrations ({} statements)",
        MIGRATIONS.len()
    );

    for (i, sql) in MIGRATIONS.iter().enumerate() {
        let preview: String = sql.chars().take(80).collect();
        tracing::debug!("migration {}/{}: {}...", i + 1, MIGRATIONS.len(), preview);
        client.query(sql).execute().await.map_err(|e| {
            tracing::error!("migration {}/{} failed: {e}", i + 1, MIGRATIONS.len());
            e
        })?;
    }

    // Audit log table. Built separately from the static MIGRATIONS list because
    // its TTL is env-driven (RUSH_AUDIT_RETENTION_DAYS) and must be format!ed in.
    if let Err(e) = create_audit_table(&client).await {
        tracing::error!("audit_events migration failed: {e}");
        return Err(e);
    }

    tracing::info!("clickhouse migrations complete");

    // One-time backfill of the metric rollups from pre-existing raw data. The MVs only
    // capture rows inserted AFTER they exist, so historical windows would be empty
    // without this. Guarded to run only when a rollup table is empty, so re-running
    // migrations on a populated DB is a no-op.
    if let Err(e) = backfill_rollups(&client).await {
        // Non-fatal: an empty rollup just means coarse-window reads fall back to raw
        // (source selection always treats an empty/short rollup window conservatively).
        tracing::warn!("metric rollup backfill failed (non-fatal): {e}");
    }

    Ok(())
}

/// Create the append-only, hash-chained audit log table.
///
/// `observability.audit_events` is an **immutable** `MergeTree` (NOT
/// ReplacingMergeTree) — rows are never updated or merged-away by key, so the
/// hash chain stays intact. It is ORDER BY `seq` (the monotonic chain index)
/// and partitioned monthly. Retention is its OWN long TTL (default 730 days,
/// overridable via `RUSH_AUDIT_RETENTION_DAYS`) and is deliberately exempt from
/// the per-tenant retention enforcer (see `retention_enforcer.rs`).
async fn create_audit_table(client: &Client) -> anyhow::Result<()> {
    let retention_days: u64 = std::env::var("RUSH_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(730);

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS observability.audit_events (
  id String,
  seq UInt64,
  timestamp DateTime64(9),
  tenant_id String DEFAULT '_audit',
  actor_id String, actor_name String, actor_type String,
  action String,
  resource_type String, resource_id String,
  outcome String,
  ip_address String, user_agent String, request_id String,
  changes String DEFAULT '',
  description String DEFAULT '',
  metadata String DEFAULT '',
  key_id String DEFAULT '',
  segment_id String DEFAULT '',
  prev_hash String DEFAULT '', hash String DEFAULT ''
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY seq
TTL toDateTime(timestamp) + INTERVAL {retention_days} DAY
SETTINGS index_granularity = 8192"
    );

    tracing::info!(retention_days, "creating audit_events table");
    client.query(&ddl).execute().await?;
    // Additive migration for QAPI-SEC-11. Empty identifiers preserve the
    // canonical format and verification behavior of pre-rotation rows.
    client
        .query("ALTER TABLE observability.audit_events ADD COLUMN IF NOT EXISTS key_id String DEFAULT '' AFTER metadata")
        .execute()
        .await?;
    client
        .query("ALTER TABLE observability.audit_events ADD COLUMN IF NOT EXISTS segment_id String DEFAULT '' AFTER key_id")
        .execute()
        .await?;
    Ok(())
}

/// Number of distinct (table, source, interval) rollup backfills.
const ROLLUP_BACKFILLS: &[(&str, &str, &str)] = &[
    // (target_table, source_raw_table, interval_expr)
    ("metrics_gauge_1m", "metrics_gauge", "1 MINUTE"),
    ("metrics_gauge_1h", "metrics_gauge", "1 HOUR"),
    ("metrics_sum_1m", "metrics_sum", "1 MINUTE"),
    ("metrics_sum_1h", "metrics_sum", "1 HOUR"),
];

/// One-time backfill of metric rollups from existing raw data.
///
/// MVs only see rows inserted after they are created, so without a backfill any query
/// over historical (pre-MV) windows would find the rollups empty. We populate each
/// rollup once from the full raw history via `INSERT ... SELECT ... GROUP BY`.
///
/// Idempotency / safety: each target is backfilled ONLY if it is currently empty. On a
/// DB that already has rollup data (from a previous boot or live MV ingestion) this is a
/// no-op, so the migration stays safe to re-run. The aggregate expressions are byte-for-
/// byte the same as the MV definitions, so backfilled buckets and MV-captured buckets are
/// the same AggregatingMergeTree states and merge cleanly.
///
/// NOTE: there is a benign double-count window: a row inserted into raw *after* the MV
/// is created but *before* the backfill SELECT runs is captured by both the MV and the
/// backfill. For count/avg this would double it. To avoid that we only backfill the
/// CLOSED past — strictly before the backfill start instant — and let the MV own
/// everything from that instant forward. We compute the cutover as the max bucket the
/// MV could already have produced is irrelevant; instead we bound the backfill to
/// `TimeUnix < <cutover>` where cutover = start-of-current-minute/hour, and rely on the
/// emptiness guard so we never backfill a table the MV has already started filling.
async fn backfill_rollups(client: &Client) -> anyhow::Result<()> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct CountRow {
        c: u64,
    }

    for (target, source, interval) in ROLLUP_BACKFILLS {
        // Emptiness guard: skip if the rollup already has any data.
        let count_sql = format!("SELECT count() AS c FROM observability.{target}");
        let existing = client
            .query(&count_sql)
            .fetch_one::<CountRow>()
            .await
            .map(|r| r.c)
            .unwrap_or(0);
        if existing > 0 {
            tracing::info!("rollup {target} already has {existing} rows, skipping backfill");
            continue;
        }

        // Build the State-producing SELECT. For sums we omit avg (rate uses raw only);
        // for gauges we include avg. Cutover bound: only backfill buckets that have fully
        // closed (strictly before the current interval start) so the live MV — which
        // starts capturing the moment it exists — owns the in-progress bucket and there
        // is no overlap/double-count between backfill and MV.
        let is_gauge = source.contains("gauge");
        let cutover = if interval.contains("MINUTE") {
            "toStartOfMinute(now64(9))"
        } else {
            "toStartOfHour(now64(9))"
        };

        let avg_col = if is_gauge {
            "avgState(Value) AS avg_state,\n    "
        } else {
            ""
        };
        // Column order MUST match the target table definition exactly.
        let select_cols = if is_gauge {
            format!(
                "anyLastState(MetricDescription) AS MetricDescription,\n    \
                 anyLastState(MetricUnit) AS MetricUnit,\n    \
                 {avg_col}minState(Value) AS min_state,\n    \
                 maxState(Value) AS max_state,\n    \
                 argMaxState(Value, TimeUnix) AS last_state,\n    \
                 countState() AS cnt_state"
            )
        } else {
            "anyLastState(MetricDescription) AS MetricDescription,\n    \
             anyLastState(MetricUnit) AS MetricUnit,\n    \
             argMaxState(Value, TimeUnix) AS last_state,\n    \
             minState(Value) AS min_state,\n    \
             maxState(Value) AS max_state,\n    \
             countState() AS cnt_state"
                .to_string()
        };

        let insert_sql = format!(
            "INSERT INTO observability.{target} \
             SELECT tenant_id, ServiceName, MetricName, Attributes, \
             toStartOfInterval(TimeUnix, INTERVAL {interval}) AS bucket, \
             {select_cols} \
             FROM observability.{source} \
             WHERE TimeUnix < {cutover} \
             GROUP BY tenant_id, ServiceName, MetricName, Attributes, bucket"
        );

        tracing::info!("backfilling rollup {target} from {source} (interval {interval})");
        client
            .query(&insert_sql)
            .with_option("max_execution_time", "600")
            .execute()
            .await?;
        tracing::info!("rollup {target} backfill complete");
    }

    Ok(())
}

/// Spawn background maintenance tasks (retention TTLs, storage policies).
/// These run asynchronously so the API starts serving immediately.
pub fn spawn_maintenance(url: String, user: String, password: String, config: RushConfig) {
    tokio::spawn(async move {
        let client = Client::default()
            .with_url(&url)
            .with_user(&user)
            .with_password(&password);

        if let Err(e) = apply_retention_ttl(&client, &config).await {
            tracing::error!("background retention TTL application failed: {e}");
        }
        apply_storage_policy(&client, &config).await;
        apply_skip_indexes(&client).await;
        tracing::info!("background maintenance tasks complete");
    });
}

/// Maintain the free-text search skip indexes on spans and logs. Idempotent
/// and version-aware.
///
/// On ClickHouse 26.2+ the search index is a native `text` (inverted) index: exact
/// token→row postings that don't saturate as the vocabulary grows (the spans
/// search blob has ~97k distinct 4-grams per granule, which over-saturated the old
/// 65536-bit ngrambf filter). Below 26.2, `text` indexes don't exist, so we fall back
/// to an `ngrambf_v1` index on the same expression. The `ngrams(4)` tokenizer (and the
/// 4-gram bloom filter) both preserve substring `LIKE '%term%'` pruning.
///
/// CRITICAL: this is **create-before-drop**. We create/verify the desired index FIRST,
/// and only drop superseded indexes once the desired one exists. A drop-then-create
/// order would, on a version where the create fails, leave the table with NO search
/// index — turning every free-text query into a full scan.
async fn apply_skip_indexes(client: &Client) {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct IndexRow {
        count: u64,
    }
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VerRow {
        v: String,
    }

    async fn index_exists(client: &Client, table: &str, name: &str) -> bool {
        let sql = format!(
            "SELECT count() as count FROM system.data_skipping_indices \
             WHERE database = 'observability' AND table = '{table}' AND name = '{name}'"
        );
        client
            .query(&sql)
            .fetch_one::<IndexRow>()
            .await
            .map(|r| r.count > 0)
            .unwrap_or(false)
    }

    // Full type string of an existing skip index (e.g. `text(tokenizer = ngrams(4))`),
    // used to detect a stale definition that needs rebuilding.
    async fn index_type_full(client: &Client, table: &str, name: &str) -> Option<String> {
        let sql = format!(
            "SELECT type_full AS v FROM system.data_skipping_indices \
             WHERE database = 'observability' AND table = '{table}' AND name = '{name}' LIMIT 1"
        );
        client
            .query(&sql)
            .fetch_one::<VerRow>()
            .await
            .ok()
            .map(|r| r.v)
    }

    // Native `text` indexes are GA in 26.2+. Be conservative on parse failure.
    let text_supported = match client
        .query("SELECT version() AS v")
        .fetch_one::<VerRow>()
        .await
    {
        Ok(r) => {
            let mut parts = r.v.split('.');
            let major: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let minor: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            major > 26 || (major == 26 && minor >= 2)
        }
        Err(_) => false,
    };
    tracing::info!(text_supported, "selecting search index strategy");

    // Per table: the desired index for each strategy, and what to drop on each path.
    // (table, text_name, text_ddl, ngram_name, ngram_ddl, drop_on_text, drop_on_ngram)
    struct Plan {
        table: &'static str,
        text_name: &'static str,
        text_ddl: &'static str,
        ngram_name: &'static str,
        ngram_ddl: &'static str,
        drop_on_text: &'static [&'static str],
        drop_on_ngram: &'static [&'static str],
    }
    let plans = [
        // NOTE: spans have NO full-text search index. The attribute-blob text index cost
        // ~66% of span storage for a path used ~2.5% of the time; spans free-text search
        // now matches span_name/service_name (cheap LowCardinality columns) in
        // query_builder, and trace/span-id lookups use bloom filters. Any pre-existing
        // spans search index is dropped below so deployments converge. Only logs keep a
        // full-text index (message search is a core logs feature).
        Plan {
            table: "logs",
            text_name: "idx_body_text",
            text_ddl: "ALTER TABLE observability.logs ADD INDEX IF NOT EXISTS \
                idx_body_text lower(Body) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1",
            ngram_name: "idx_body_ngram",
            ngram_ddl: "ALTER TABLE observability.logs ADD INDEX IF NOT EXISTS \
                idx_body_ngram lower(Body) TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1",
            // On the text path we keep BOTH indexes on lower(Body): the `text` token
            // index for whole-word search AND the ngrambf_v1 substring index for
            // partial-word wildcards (LIKE '%...%'), which the token index cannot serve.
            // The bloom substring index is tiny (~3.7 GiB full-table at our volume), so it
            // covers the whole retained table. Drop only the legacy `idx_body` tokenbf.
            drop_on_text: &["idx_body"],
            // On ngram path, keep the existing bloom indexes (idx_body tokenbf is a useful
            // word index; idx_body_ngram is the desired substring index). Drop only a stale text index.
            drop_on_ngram: &["idx_body_text"],
        },
    ];

    for p in &plans {
        let (want_name, want_ddl, drops): (&str, &str, &[&str]) = if text_supported {
            (p.text_name, p.text_ddl, p.drop_on_text)
        } else {
            (p.ngram_name, p.ngram_ddl, p.drop_on_ngram)
        };

        // 0. Self-heal a stale text index. If idx_body_text exists but with an outdated
        // definition (the old `ngrams(4)` tokenizer — ~9× the data in storage and
        // net-negative for common terms — or a coarse granularity that only pruned at
        // part level), drop it so the create step below rebuilds it with the current
        // `splitByNonAlpha` DDL. This briefly leaves lower(Body) without a text index,
        // which is unavoidable (a column may carry only ONE text index, so we cannot
        // create-before-drop) and acceptable during a migration: free-text queries stay
        // correct, just unaccelerated, until MATERIALIZE finishes in the background.
        if text_supported && index_exists(client, p.table, want_name).await {
            if let Some(tf) = index_type_full(client, p.table, want_name).await {
                if !tf.contains("splitByNonAlpha") {
                    tracing::info!(table = p.table, index = want_name, current = %tf,
                        "rebuilding stale search index with current tokenizer");
                    let drop_ddl = format!(
                        "ALTER TABLE observability.{} DROP INDEX IF EXISTS {}",
                        p.table, want_name
                    );
                    if let Err(e) = client.query(&drop_ddl).execute().await {
                        tracing::warn!(table = p.table, index = want_name, error = %e,
                            "failed to drop stale search index — leaving it in place");
                    }
                }
            }
        }

        // 1. Ensure the desired index exists (create + materialize) BEFORE dropping anything.
        if !index_exists(client, p.table, want_name).await {
            tracing::info!(table = p.table, index = want_name, "creating search index");
            if let Err(e) = client.query(want_ddl).execute().await {
                tracing::warn!(table = p.table, index = want_name, error = %e,
                    "failed to create search index — leaving existing indexes intact");
                continue; // do NOT drop anything if we couldn't create the replacement
            }
            let materialize = format!(
                "ALTER TABLE observability.{} MATERIALIZE INDEX {}",
                p.table, want_name
            );
            if let Err(e) = client.query(&materialize).execute().await {
                tracing::warn!(table = p.table, index = want_name, error = %e, "failed to materialize search index");
            }
        }

        // 2. Desired index is present — now it's safe to drop superseded ones.
        for name in drops {
            if *name == want_name {
                continue;
            }
            if index_exists(client, p.table, name).await {
                tracing::info!(
                    table = p.table,
                    index = name,
                    "dropping superseded search index"
                );
                let drop_ddl = format!(
                    "ALTER TABLE observability.{} DROP INDEX IF EXISTS {}",
                    p.table, name
                );
                if let Err(e) = client.query(&drop_ddl).execute().await {
                    tracing::warn!(table = p.table, index = name, error = %e, "failed to drop superseded index");
                }
            }
        }

        // 3. On the text path, ALSO ensure the ngrambf_v1 substring index exists alongside
        //    the `text` token index. Partial-word wildcards (LIKE '%foo%') can't use the
        //    token index and otherwise force a full Body scan; the bloom n-gram index prunes
        //    granules for substrings >= 4 chars. It's a compact fixed-size bloom filter
        //    (~3.7 GiB across the whole table), so it covers all retained data — no windowing.
        //    On the non-text path the loop above already created idx_body_ngram as `want`.
        if text_supported && !index_exists(client, p.table, p.ngram_name).await {
            tracing::info!(
                table = p.table,
                index = p.ngram_name,
                "creating substring (ngrambf_v1) index"
            );
            if let Err(e) = client.query(p.ngram_ddl).execute().await {
                tracing::warn!(table = p.table, index = p.ngram_name, error = %e, "failed to create substring index");
            } else {
                let materialize = format!(
                    "ALTER TABLE observability.{} MATERIALIZE INDEX {}",
                    p.table, p.ngram_name
                );
                if let Err(e) = client.query(&materialize).execute().await {
                    tracing::warn!(table = p.table, index = p.ngram_name, error = %e, "failed to materialize substring index");
                }
            }
        }
    }

    // Spans no longer carry a full-text search index. Drop any pre-existing spans search
    // indexes so existing deployments reclaim the storage (the text index was ~66% of the
    // spans table); fresh installs never create them.
    for name in [
        "idx_search_text",
        "idx_search_blob",
        "idx_attributes_ngram",
        "idx_event_attributes_ngram",
    ] {
        if index_exists(client, "spans", name).await {
            tracing::info!(index = name, "dropping obsolete spans full-text index");
            let drop_ddl = format!("ALTER TABLE observability.spans DROP INDEX IF EXISTS {name}");
            if let Err(e) = client.query(&drop_ddl).execute().await {
                tracing::warn!(index = name, error = %e, "failed to drop obsolete spans search index");
            }
        }
    }
}

/// Check if a table's TTL expression already contains the desired interval,
/// returning true if the ALTER can be skipped.
async fn ttl_matches(client: &Client, table: &str, days: u32) -> bool {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct EngineRow {
        engine_full: String,
    }
    let sql = format!(
        "SELECT engine_full FROM system.tables WHERE database = 'observability' AND name = '{table}'"
    );
    match client.query(&sql).fetch_one::<EngineRow>().await {
        Ok(row) => {
            // The engine_full string contains something like "INTERVAL 30 DAY DELETE"
            let needle = format!("INTERVAL {days} DAY");
            row.engine_full.contains(&needle)
        }
        Err(_) => false,
    }
}

/// Adjust table-level TTLs based on config. Uses the effective (max) TTL so
/// that part-level drops don't remove data that has longer per-rule retention.
///
/// Skips tables whose TTL already matches the desired interval to avoid
/// blocking on redundant ALTER TABLE mutations at every boot.
async fn apply_retention_ttl(client: &Client, config: &RushConfig) -> anyhow::Result<()> {
    apply_retention_ttls(
        client,
        config.effective_logs_ttl_days(),
        config.effective_metrics_ttl_days(),
        config.effective_traces_ttl_days(),
    )
    .await
}

/// Apply table-level retention TTLs from explicit per-signal day counts. Used at
/// boot from rush.toml and periodically by the retention enforcer from the
/// UI-editable global-retention store. `apm` covers traces (spans) and RUM.
///
/// Safety: each value is floored to 1 day so a stray 0 can never produce an
/// `INTERVAL 0 DAY` (delete-everything-now) TTL.
pub async fn apply_retention_ttls(
    client: &Client,
    logs_days: u32,
    metrics_days: u32,
    apm_days: u32,
) -> anyhow::Result<()> {
    let logs_days = logs_days.max(1);
    let metrics_days = metrics_days.max(1);
    let apm_days = apm_days.max(1);

    tracing::info!(
        "applying retention TTLs: metrics={metrics_days}d, apm={apm_days}d (incl. RUM), logs={logs_days}d"
    );

    // Rollup retention: 1m rollups live at least as long as raw so any window servable
    // from raw is also servable from the 1m rollup; 1h rollups live longest so coarse
    // history survives even after raw + 1m have been dropped.
    let rollup_1m_days = metrics_days;
    let rollup_1h_days = metrics_days.saturating_mul(2).max(metrics_days);

    // (table, timestamp expression, days)
    let specs: &[(&str, &str, u32)] = &[
        ("metrics_gauge", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_sum", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_histogram", "toDateTime(TimeUnix)", metrics_days),
        (
            "metrics_exp_histogram",
            "toDateTime(TimeUnix)",
            metrics_days,
        ),
        ("metrics_summary", "toDateTime(TimeUnix)", metrics_days),
        // Rollups keyed on `bucket` (a DateTime64), not TimeUnix.
        ("metrics_gauge_1m", "toDateTime(bucket)", rollup_1m_days),
        ("metrics_sum_1m", "toDateTime(bucket)", rollup_1m_days),
        ("metrics_gauge_1h", "toDateTime(bucket)", rollup_1h_days),
        ("metrics_sum_1h", "toDateTime(bucket)", rollup_1h_days),
        ("spans", "toDateTime(timestamp)", apm_days),
        ("rum", "toDateTime(Timestamp)", apm_days),
        ("rum_replay", "toDateTime(chunk_ts)", apm_days),
        ("logs", "toDateTime(Timestamp)", logs_days),
    ];

    for (table, ts_expr, days) in specs {
        if ttl_matches(client, table, *days).await {
            tracing::debug!("TTL on {table} already {days}d, skipping");
            continue;
        }
        // materialize_ttl_after_modify=0: change only the table's TTL metadata, do
        // NOT rewrite existing parts. Re-materializing TTL on large parts spawns a
        // mutation that needs ~the whole part in memory (hits max_server_memory_usage,
        // fails, and locks the part against MOVE). The background TTL task still
        // moves/drops parts lazily on merges using the new rule.
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL {ts_expr} + INTERVAL {days} DAY DELETE \
             SETTINGS materialize_ttl_after_modify = 0"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to set TTL on {table}: {e}");
        }
    }

    Ok(())
}

/// Apply the tiered storage policy and per-signal TTL MOVE rules.
///
/// Each signal type (metrics, traces, logs) can independently control when
/// parts are moved from the local (hot) volume to S3 (cold) via
/// `*_move_after_days` in `[storage.tiering]`.  Set to 0 to disable tiering
/// for that signal type — the table keeps the `tiered` policy but no TTL MOVE
/// rule is added, so data stays on the hot volume.
///
/// Non-fatal — if ClickHouse doesn't have the s3_disk registered yet (e.g.
/// first boot before MinIO is ready), we just log and continue.
async fn apply_storage_policy(client: &Client, config: &RushConfig) {
    if config.storage.s3.is_none() {
        tracing::debug!("no S3 config, skipping storage policy");
        return;
    }

    let tiering = &config.storage.tiering;

    // (table, timestamp_expr, move_after_days)
    let specs: &[(&str, &str, u32)] = &[
        // Metrics
        (
            "metrics_gauge",
            "toDateTime(TimeUnix)",
            tiering.metrics_move_after_days,
        ),
        (
            "metrics_sum",
            "toDateTime(TimeUnix)",
            tiering.metrics_move_after_days,
        ),
        (
            "metrics_histogram",
            "toDateTime(TimeUnix)",
            tiering.metrics_move_after_days,
        ),
        (
            "metrics_exp_histogram",
            "toDateTime(TimeUnix)",
            tiering.metrics_move_after_days,
        ),
        (
            "metrics_summary",
            "toDateTime(TimeUnix)",
            tiering.metrics_move_after_days,
        ),
        // Traces / spans
        (
            "spans",
            "toDateTime(timestamp)",
            tiering.traces_move_after_days,
        ),
        // Logs
        (
            "logs",
            "toDateTime(Timestamp)",
            tiering.logs_move_after_days,
        ),
    ];

    for (table, ts_expr, move_days) in specs {
        // Always assign the tiered policy so the cold volume is available
        let policy_sql =
            format!("ALTER TABLE observability.{table} MODIFY SETTING storage_policy = 'tiered'");
        if let Err(e) = client.query(&policy_sql).execute().await {
            tracing::warn!("could not set tiered storage on {table} (non-fatal): {e}");
            continue; // no point setting TTL MOVE if the policy didn't apply
        }

        if *move_days == 0 {
            tracing::info!("tiering disabled for {table} (move_after_days=0)");
            continue;
        }

        // Add TTL MOVE rule: parts older than N days move to the cold (S3) volume
        // We use MODIFY TTL which replaces any existing TTL expression, so we must
        // include the existing DELETE TTL alongside the new MOVE TTL.
        let delete_days = match *table {
            t if t.starts_with("metrics_") => config.effective_metrics_ttl_days(),
            "spans" => config.effective_traces_ttl_days(),
            "logs" => config.effective_logs_ttl_days(),
            _ => 30,
        };
        // materialize_ttl_after_modify=0: set the move/delete TTL as metadata only.
        // Re-materializing on large existing parts spawns a per-part mutation that
        // exceeds max_server_memory_usage and locks the part against MOVE; the
        // background mover relocates parts to the cold volume lazily instead.
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL \
             {ts_expr} + INTERVAL {move_days} DAY TO VOLUME 'cold', \
             {ts_expr} + INTERVAL {delete_days} DAY DELETE \
             SETTINGS materialize_ttl_after_modify = 0"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("could not set TTL MOVE on {table} (non-fatal): {e}");
        }
    }

    tracing::info!(
        "tiered storage policy applied (metrics={}d, traces={}d, logs={}d)",
        tiering.metrics_move_after_days,
        tiering.traces_move_after_days,
        tiering.logs_move_after_days,
    );
}

#[cfg(test)]
mod row_policy_tests {
    use super::*;

    #[test]
    fn policy_table_set_covers_all_tenant_telemetry_stores() {
        for table in [
            "logs",
            "spans",
            "spans_by_trace",
            "services",
            "metrics_gauge",
            "metrics_sum",
            "metrics_histogram",
            "metrics_exp_histogram",
            "metrics_summary",
            "metrics_gauge_1m",
            "metrics_gauge_1h",
            "metrics_sum_1m",
            "metrics_sum_1h",
            "rum",
            "rum_replay",
            "signal_usage",
            "tenant_usage",
        ] {
            assert!(ROW_POLICY_TABLES.contains(&table), "missing {table}");
        }
    }

    #[test]
    fn read_principal_must_be_a_plain_identifier() {
        assert!(validate_clickhouse_identifier("rush_query").is_ok());
        assert!(validate_clickhouse_identifier("rush-query").is_err());
        assert!(validate_clickhouse_identifier("rush_query TO ALL").is_err());
    }

    #[test]
    fn policy_sql_is_strict_and_targets_only_the_read_principal() {
        let sql = row_policy_sql("logs", "rushquery", true);
        assert!(sql.starts_with("ALTER ROW POLICY tenant_isolation"));
        assert!(sql.contains("notEmpty(getSetting('rush_tenant_id'))"));
        assert!(sql.contains("tenant_id = getSetting('rush_tenant_id')"));
        assert!(sql.ends_with("TO rushquery"));
        assert!(!sql.contains(" OR "));
        assert!(!sql.contains("TO ALL"));
    }
}
