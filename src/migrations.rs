use clickhouse::Client;

use crate::config::RushConfig;

/// Ordered list of DDL statements to ensure the observability schema exists.
/// v2: Multi-tenant schema — every table carries tenant_id as the first ORDER BY column.
/// Starts with DROP TABLE IF EXISTS for all v1 tables, then recreates with v2 schemas.
const MIGRATIONS: &[&str] = &[
    // ── Database ──
    "CREATE DATABASE IF NOT EXISTS observability",

    // ── DROP v1 tables (MVs first, then base tables) ──
    "DROP TABLE IF EXISTS observability.service_catalog",
    "DROP TABLE IF EXISTS observability.trace_index",
    "DROP TABLE IF EXISTS observability.otel_to_wide",
    "DROP TABLE IF EXISTS observability.wide_events",
    "DROP TABLE IF EXISTS observability.otel_traces",
    "DROP TABLE IF EXISTS observability.otel_logs",
    "DROP TABLE IF EXISTS observability.otel_metrics_gauge",
    "DROP TABLE IF EXISTS observability.otel_metrics_sum",
    "DROP TABLE IF EXISTS observability.otel_metrics_histogram",
    "DROP TABLE IF EXISTS observability.otel_metrics_exponential_histogram",
    "DROP TABLE IF EXISTS observability.otel_metrics_summary",
    "DROP TABLE IF EXISTS observability.rum_events",
    "DROP TABLE IF EXISTS observability.signal_usage",

    // ── OTel traces (v2: multi-tenant with materialized HTTP columns) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_traces
(
    `tenant_id` LowCardinality(String) DEFAULT 'default',
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId` String CODEC(ZSTD(1)),
    `SpanId` String CODEC(ZSTD(1)),
    `ParentSpanId` String CODEC(ZSTD(1)),
    `TraceState` String CODEC(ZSTD(1)),
    `SpanName` LowCardinality(String) CODEC(ZSTD(1)),
    `SpanKind` LowCardinality(String) CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeName` String CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `SpanAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `Duration` UInt64 CODEC(ZSTD(1)),
    `StatusCode` LowCardinality(String) CODEC(ZSTD(1)),
    `StatusMessage` String CODEC(ZSTD(1)),
    `Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Links.TraceId` Array(String) CODEC(ZSTD(1)),
    `Links.SpanId` Array(String) CODEC(ZSTD(1)),
    `Links.TraceState` Array(String) CODEC(ZSTD(1)),
    `Links.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `mat_http_method` LowCardinality(String) MATERIALIZED SpanAttributes['http.request.method'],
    `mat_http_path` String MATERIALIZED SpanAttributes['url.path'],
    `mat_http_status` UInt16 MATERIALIZED toUInt16OrZero(SpanAttributes['http.response.status_code']),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_span_id SpanId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_status_code StatusCode TYPE set(4) GRANULARITY 4,
    INDEX idx_http_status mat_http_status TYPE minmax GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (tenant_id, ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Wide events (v2: multi-tenant flattened query-friendly schema) ──
    r"CREATE TABLE IF NOT EXISTS observability.wide_events
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
    INDEX idx_duration duration_ns TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, service_name, http_path, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── MV: OTel traces → wide events (v2: passes tenant_id through) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.otel_to_wide
TO observability.wide_events
AS SELECT
    tenant_id,
    Timestamp AS timestamp,
    TraceId AS trace_id,
    SpanId AS span_id,
    ParentSpanId AS parent_span_id,
    ServiceName AS service_name,
    SpanName AS span_name,
    SpanKind AS kind,
    StatusCode AS status,
    Duration AS duration_ns,
    SpanAttributes['http.method'] AS http_method,
    COALESCE(
        nullIf(SpanAttributes['http.route'], ''),
        nullIf(SpanAttributes['http.target'], ''),
        nullIf(SpanAttributes['url.path'], ''),
        SpanName
    ) AS http_path,
    toUInt16OrZero(COALESCE(
        nullIf(SpanAttributes['http.status_code'], ''),
        nullIf(SpanAttributes['http.response.status_code'], ''),
        '0'
    )) AS http_status_code,
    toJSONString(SpanAttributes) AS attributes,
    `Events.Name` AS event_names,
    `Events.Timestamp` AS event_timestamps,
    arrayMap(x -> toJSONString(x), `Events.Attributes`) AS event_attributes,
    `Links.TraceId` AS link_trace_ids,
    `Links.SpanId` AS link_span_ids
FROM observability.otel_traces",

    // ── MV: trace index for fast trace-id lookups (v2: tenant-scoped) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.trace_index
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, trace_id, timestamp)
AS SELECT
    tenant_id, trace_id, span_id, parent_span_id, service_name,
    http_method, http_path, http_status_code,
    duration_ns, status, timestamp
FROM observability.wide_events",

    // ── MV: service catalog (v2: tenant-scoped) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.service_catalog
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (tenant_id, service_name, http_path, http_method)
AS SELECT
    tenant_id, service_name, http_path, http_method,
    max(timestamp) AS last_seen,
    count() AS request_count
FROM observability.wide_events
GROUP BY tenant_id, service_name, http_path, http_method",

    // ── Gauge metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_gauge
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
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Sum metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_sum
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
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Histogram metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_histogram
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
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Exponential Histogram metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_exponential_histogram
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
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Summary metrics (v2: multi-tenant) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_summary
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
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (tenant_id, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── OTel Logs (v2: multi-tenant with SIEM materialized columns) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_logs
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
    INDEX idx_body lower(Body) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
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
PRIMARY KEY (tenant_id, ServiceName, SeverityText, TimestampTime, Timestamp)
ORDER BY (tenant_id, ServiceName, SeverityText, TimestampTime, Timestamp)
TTL TimestampDate + toIntervalDay(30)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

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
    r"CREATE TABLE IF NOT EXISTS observability.rum_events
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
    INDEX idx_error_message ErrorMessage TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (tenant_id, AppName, EventType, TimestampTime)
ORDER BY (tenant_id, AppName, EventType, TimestampTime, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 14 DAY DELETE
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
    "otel_traces",
    "otel_logs",
    "wide_events",
    "otel_metrics_gauge",
    "otel_metrics_sum",
    "otel_metrics_histogram",
    "otel_metrics_exponential_histogram",
    "otel_metrics_summary",
    "rum_events",
];

/// Create row policies on all tenant-scoped tables. Only safe to call when
/// ClickHouse supports the `rush_tenant_id` custom setting.
pub async fn apply_row_policies(client: &Client) {
    tracing::info!("applying row-level security policies ({} tables)", ROW_POLICY_TABLES.len());
    for table in ROW_POLICY_TABLES {
        let sql = format!(
            "CREATE ROW POLICY IF NOT EXISTS tenant_isolation ON observability.{table} \
             FOR SELECT USING tenant_id = getSetting('rush_tenant_id') OR getSetting('rush_tenant_id') = '' \
             TO ALL"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to create row policy on {table}: {e}");
        }
    }
}

/// Run all migrations against ClickHouse.
///
/// Connects **without** a default database so that `CREATE DATABASE` succeeds
/// even on a fresh instance. Every statement uses `IF NOT EXISTS` so this is
/// safe to call on every startup.
pub async fn run(url: &str, user: &str, password: &str, _config: &RushConfig) -> anyhow::Result<()> {
    let client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password);

    tracing::info!("running clickhouse migrations ({} statements)", MIGRATIONS.len());

    for (i, sql) in MIGRATIONS.iter().enumerate() {
        let preview: String = sql.chars().take(80).collect();
        tracing::debug!("migration {}/{}: {}...", i + 1, MIGRATIONS.len(), preview);
        client.query(sql).execute().await.map_err(|e| {
            tracing::error!("migration {}/{} failed: {e}", i + 1, MIGRATIONS.len());
            e
        })?;
    }

    tracing::info!("clickhouse migrations complete");

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
        tracing::info!("background maintenance tasks complete");
    });
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
    let metrics_days = config.effective_metrics_ttl_days();
    let traces_days = config.effective_traces_ttl_days();
    let logs_days = config.effective_logs_ttl_days();

    tracing::info!(
        "applying retention TTLs: metrics={metrics_days}d, traces={traces_days}d, logs={logs_days}d"
    );

    // Metrics tables
    let metric_tables = [
        "otel_metrics_gauge",
        "otel_metrics_sum",
        "otel_metrics_histogram",
        "otel_metrics_exponential_histogram",
        "otel_metrics_summary",
    ];
    for table in metric_tables {
        if ttl_matches(client, table, metrics_days).await {
            tracing::debug!("TTL on {table} already {metrics_days}d, skipping");
            continue;
        }
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL toDateTime(TimeUnix) + INTERVAL {metrics_days} DAY DELETE"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to set TTL on {table}: {e}");
        }
    }

    // Trace tables
    let trace_ttl_specs: &[(&str, &str)] = &[
        ("otel_traces", "toDateTime(Timestamp)"),
        ("wide_events", "toDateTime(timestamp)"),
    ];
    for (table, ts_expr) in trace_ttl_specs {
        if ttl_matches(client, table, traces_days).await {
            tracing::debug!("TTL on {table} already {traces_days}d, skipping");
            continue;
        }
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL {ts_expr} + INTERVAL {traces_days} DAY DELETE"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to set TTL on {table}: {e}");
        }
    }

    // Log table
    if !ttl_matches(client, "otel_logs", logs_days).await {
        let sql = format!(
            "ALTER TABLE observability.otel_logs MODIFY TTL toDateTime(Timestamp) + INTERVAL {logs_days} DAY DELETE"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to set TTL on otel_logs: {e}");
        }
    } else {
        tracing::debug!("TTL on otel_logs already {logs_days}d, skipping");
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
        ("otel_metrics_gauge", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("otel_metrics_sum", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("otel_metrics_histogram", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("otel_metrics_exponential_histogram", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("otel_metrics_summary", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        // Traces / spans
        ("otel_traces", "toDateTime(Timestamp)", tiering.traces_move_after_days),
        ("wide_events", "toDateTime(timestamp)", tiering.traces_move_after_days),
        // Logs
        ("otel_logs", "toDateTime(Timestamp)", tiering.logs_move_after_days),
    ];

    for (table, ts_expr, move_days) in specs {
        // Always assign the tiered policy so the cold volume is available
        let policy_sql = format!(
            "ALTER TABLE observability.{table} MODIFY SETTING storage_policy = 'tiered'"
        );
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
            t if t.starts_with("otel_metrics") => config.effective_metrics_ttl_days(),
            "otel_traces" | "wide_events" => config.effective_traces_ttl_days(),
            "otel_logs" => config.effective_logs_ttl_days(),
            _ => 30,
        };
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL \
             {ts_expr} + INTERVAL {move_days} DAY TO VOLUME 'cold', \
             {ts_expr} + INTERVAL {delete_days} DAY DELETE"
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
