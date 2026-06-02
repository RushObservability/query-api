use clickhouse::Client;

use crate::config::RushConfig;

/// Ordered list of DDL statements to ensure the observability schema exists.
/// v2: Multi-tenant schema — every table carries tenant_id as the first ORDER BY column.
/// Starts with DROP TABLE IF EXISTS for all v1 tables, then recreates with v2 schemas.
const MIGRATIONS: &[&str] = &[
    // ── Database ──
    "CREATE DATABASE IF NOT EXISTS observability",

    // ── OTel traces (v2: multi-tenant with materialized HTTP columns) ──
    r"CREATE TABLE IF NOT EXISTS observability.spans_raw
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

    // ── MV: OTel traces → wide events (v2: passes tenant_id through) ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_mv
TO observability.spans
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
FROM observability.spans_raw",

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
    "spans_raw",
    "logs",
    "spans",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
    "rum",
    "rum_replay",
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
    struct IndexRow { count: u64 }
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VerRow { v: String }

    async fn index_exists(client: &Client, table: &str, name: &str) -> bool {
        let sql = format!(
            "SELECT count() as count FROM system.data_skipping_indices \
             WHERE database = 'observability' AND table = '{table}' AND name = '{name}'"
        );
        client.query(&sql).fetch_one::<IndexRow>().await.map(|r| r.count > 0).unwrap_or(false)
    }

    // Native `text` indexes are GA in 26.2+. Be conservative on parse failure.
    let text_supported = match client.query("SELECT version() AS v").fetch_one::<VerRow>().await {
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
        Plan {
            table: "spans",
            text_name: "idx_search_text",
            text_ddl: "ALTER TABLE observability.spans ADD INDEX IF NOT EXISTS \
                idx_search_text lower(concat(attributes, ' ', arrayStringConcat(event_attributes, ' '))) \
                TYPE text(tokenizer = ngrams(4)) GRANULARITY 1",
            ngram_name: "idx_search_blob",
            ngram_ddl: "ALTER TABLE observability.spans ADD INDEX IF NOT EXISTS \
                idx_search_blob lower(concat(attributes, ' ', arrayStringConcat(event_attributes, ' '))) \
                TYPE ngrambf_v1(4, 65536, 3, 0) GRANULARITY 1",
            // On text path, drop the ngram blob + the original per-column ngram indexes.
            drop_on_text: &["idx_search_blob", "idx_attributes_ngram", "idx_event_attributes_ngram"],
            // On ngram path, keep idx_search_blob (desired); drop only the superseded
            // per-column ngram indexes and any stale text index.
            drop_on_ngram: &["idx_attributes_ngram", "idx_event_attributes_ngram", "idx_search_text"],
        },
        Plan {
            table: "logs",
            text_name: "idx_body_text",
            text_ddl: "ALTER TABLE observability.logs ADD INDEX IF NOT EXISTS \
                idx_body_text lower(Body) TYPE text(tokenizer = ngrams(4)) GRANULARITY 1",
            ngram_name: "idx_body_ngram",
            ngram_ddl: "ALTER TABLE observability.logs ADD INDEX IF NOT EXISTS \
                idx_body_ngram lower(Body) TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1",
            // On text path, the text index supersedes both tokenbf + ngrambf on Body.
            drop_on_text: &["idx_body", "idx_body_ngram"],
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

        // 1. Ensure the desired index exists (create + materialize) BEFORE dropping anything.
        if !index_exists(client, p.table, want_name).await {
            tracing::info!(table = p.table, index = want_name, "creating search index");
            if let Err(e) = client.query(want_ddl).execute().await {
                tracing::warn!(table = p.table, index = want_name, error = %e,
                    "failed to create search index — leaving existing indexes intact");
                continue; // do NOT drop anything if we couldn't create the replacement
            }
            let materialize = format!("ALTER TABLE observability.{} MATERIALIZE INDEX {}", p.table, want_name);
            if let Err(e) = client.query(&materialize).execute().await {
                tracing::warn!(table = p.table, index = want_name, error = %e, "failed to materialize search index");
            }
        }

        // 2. Desired index is present — now it's safe to drop superseded ones.
        for name in drops {
            if *name == want_name { continue; }
            if index_exists(client, p.table, name).await {
                tracing::info!(table = p.table, index = name, "dropping superseded search index");
                let drop_ddl = format!("ALTER TABLE observability.{} DROP INDEX IF EXISTS {}", p.table, name);
                if let Err(e) = client.query(&drop_ddl).execute().await {
                    tracing::warn!(table = p.table, index = name, error = %e, "failed to drop superseded index");
                }
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

    // (table, timestamp expression, days)
    let specs: &[(&str, &str, u32)] = &[
        ("metrics_gauge", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_sum", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_histogram", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_exp_histogram", "toDateTime(TimeUnix)", metrics_days),
        ("metrics_summary", "toDateTime(TimeUnix)", metrics_days),
        ("spans_raw", "toDateTime(Timestamp)", apm_days),
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
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL {ts_expr} + INTERVAL {days} DAY DELETE"
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
        ("metrics_gauge", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("metrics_sum", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("metrics_histogram", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("metrics_exp_histogram", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        ("metrics_summary", "toDateTime(TimeUnix)", tiering.metrics_move_after_days),
        // Traces / spans
        ("spans_raw", "toDateTime(Timestamp)", tiering.traces_move_after_days),
        ("spans", "toDateTime(timestamp)", tiering.traces_move_after_days),
        // Logs
        ("logs", "toDateTime(Timestamp)", tiering.logs_move_after_days),
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
            t if t.starts_with("metrics_") => config.effective_metrics_ttl_days(),
            "spans_raw" | "spans" => config.effective_traces_ttl_days(),
            "logs" => config.effective_logs_ttl_days(),
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
