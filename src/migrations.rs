use clickhouse::Client;

use crate::config::WideConfig;

/// Ordered list of DDL statements to ensure the observability schema exists.
/// Every statement is idempotent (`IF NOT EXISTS`) so safe to run on every startup.
const MIGRATIONS: &[&str] = &[
    // ── Database ──
    "CREATE DATABASE IF NOT EXISTS observability",

    // ── OTel traces (collector exporter target) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_traces
(
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
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Wide events (flattened query-friendly schema) ──
    r"CREATE TABLE IF NOT EXISTS observability.wide_events
(
    timestamp          DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    trace_id           String,
    span_id            String,
    parent_span_id     String,
    service_name       LowCardinality(String),
    service_version    LowCardinality(String),
    environment        LowCardinality(String),
    host_name          LowCardinality(String),
    http_method        LowCardinality(String),
    http_path          String,
    http_status_code   UInt16,
    duration_ns        UInt64,
    status             LowCardinality(String),
    attributes         String,
    event_timestamps   Array(DateTime64(9, 'UTC')),
    event_names        Array(String),
    event_attributes   Array(String),
    link_trace_ids     Array(String),
    link_span_ids      Array(String)
)
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (service_name, http_path, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192",

    // ── MV: OTel traces → wide events ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.otel_to_wide
TO observability.wide_events
AS SELECT
    Timestamp AS timestamp,
    TraceId AS trace_id,
    SpanId AS span_id,
    ParentSpanId AS parent_span_id,
    ServiceName AS service_name,
    ResourceAttributes['service.version'] AS service_version,
    ResourceAttributes['deployment.environment'] AS environment,
    ResourceAttributes['host.name'] AS host_name,
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
    Duration AS duration_ns,
    StatusCode AS status,
    toJSONString(SpanAttributes) AS attributes,
    `Events.Timestamp` AS event_timestamps,
    `Events.Name` AS event_names,
    arrayMap(x -> toJSONString(x), `Events.Attributes`) AS event_attributes,
    `Links.TraceId` AS link_trace_ids,
    `Links.SpanId` AS link_span_ids
FROM observability.otel_traces",

    // ── MV: trace index for fast trace-id lookups ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.trace_index
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (trace_id, timestamp)
AS SELECT
    trace_id, span_id, parent_span_id, service_name,
    http_method, http_path, http_status_code,
    duration_ns, status, timestamp
FROM observability.wide_events",

    // ── MV: service catalog ──
    r"CREATE MATERIALIZED VIEW IF NOT EXISTS observability.service_catalog
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (service_name, http_path, http_method)
AS SELECT
    service_name, http_path, http_method,
    max(timestamp) AS last_seen,
    count() AS request_count
FROM observability.wide_events
GROUP BY service_name, http_path, http_method",

    // ── Gauge metrics ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_gauge
(
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String CODEC(ZSTD(1)),
    ScopeName             String CODEC(ZSTD(1)),
    ScopeVersion          String CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl        String CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    MetricName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription     String CODEC(ZSTD(1)),
    MetricUnit            String CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix         DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix              DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value                 Float64 CODEC(Gorilla, ZSTD(1)),
    Flags                 UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`           Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`              Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`             Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`            Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key  mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key        mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value      mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Sum metrics (counters, cumulative sums) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_sum
(
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String CODEC(ZSTD(1)),
    ScopeName             String CODEC(ZSTD(1)),
    ScopeVersion          String CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl        String CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    MetricName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription     String CODEC(ZSTD(1)),
    MetricUnit            String CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix         DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix              DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value                 Float64 CODEC(Gorilla, ZSTD(1)),
    Flags                 UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`           Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`              Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`             Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`            Array(String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic            Boolean CODEC(Delta, ZSTD(1)),
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key  mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key        mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value      mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Histogram metrics ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_histogram
(
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String CODEC(ZSTD(1)),
    ScopeName             String CODEC(ZSTD(1)),
    ScopeVersion          String CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl        String CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    MetricName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription     String CODEC(ZSTD(1)),
    MetricUnit            String CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix         DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix              DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                 UInt64 CODEC(Delta, ZSTD(1)),
    Sum                   Float64 CODEC(ZSTD(1)),
    BucketCounts          Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds        Array(Float64) CODEC(ZSTD(1)),
    Flags                 UInt32 CODEC(ZSTD(1)),
    Min                   Float64 CODEC(ZSTD(1)),
    Max                   Float64 CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`           Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`              Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`             Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`            Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key  mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key        mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value      mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Exponential Histogram metrics ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_exponential_histogram
(
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String CODEC(ZSTD(1)),
    ScopeName             String CODEC(ZSTD(1)),
    ScopeVersion          String CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl        String CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    MetricName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription     String CODEC(ZSTD(1)),
    MetricUnit            String CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix         DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix              DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                 UInt64 CODEC(Delta, ZSTD(1)),
    Sum                   Float64 CODEC(ZSTD(1)),
    Scale                 Int32 CODEC(ZSTD(1)),
    ZeroCount             UInt64 CODEC(ZSTD(1)),
    PositiveOffset        Int32 CODEC(ZSTD(1)),
    PositiveBucketCounts  Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset        Int32 CODEC(ZSTD(1)),
    NegativeBucketCounts  Array(UInt64) CODEC(ZSTD(1)),
    Flags                 UInt32 CODEC(ZSTD(1)),
    Min                   Float64 CODEC(ZSTD(1)),
    Max                   Float64 CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`           Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`              Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`             Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`            Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key  mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key        mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value      mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Summary metrics (drop first to fix Nested column schema) ──
    "DROP TABLE IF EXISTS observability.otel_metrics_summary",
    r"CREATE TABLE IF NOT EXISTS observability.otel_metrics_summary
(
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl     String CODEC(ZSTD(1)),
    ScopeName             String CODEC(ZSTD(1)),
    ScopeVersion          String CODEC(ZSTD(1)),
    ScopeAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl        String CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    MetricName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription     String CODEC(ZSTD(1)),
    MetricUnit            String CODEC(ZSTD(1)),
    Attributes            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix         DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix              DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                 UInt64 CODEC(Delta, ZSTD(1)),
    Sum                   Float64 CODEC(ZSTD(1)),
    ValueAtQuantiles Nested(Quantile Float64, Value Float64) CODEC(ZSTD(1)),
    Flags                 UInt32 CODEC(ZSTD(1)),
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key  mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key        mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value      mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── Signal usage tracking ──
    r"CREATE TABLE IF NOT EXISTS observability.signal_usage
(
    signal_name     LowCardinality(String),
    signal_type     LowCardinality(String),
    source          LowCardinality(String),
    last_queried_at DateTime64(3) DEFAULT now64(3),
    query_count     UInt64 DEFAULT 1
)
ENGINE = ReplacingMergeTree(last_queried_at)
ORDER BY (signal_type, signal_name, source)
TTL toDateTime(last_queried_at) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192",

    // ── OTel Logs table (matches otel-collector-contrib clickhouse exporter schema) ──
    r"CREATE TABLE IF NOT EXISTS observability.otel_logs
(
    Timestamp          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimestampTime      DateTime DEFAULT toDateTime(Timestamp),
    TraceId            String CODEC(ZSTD(1)),
    SpanId             String CODEC(ZSTD(1)),
    TraceFlags         UInt8,
    SeverityText       LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber     UInt8,
    ServiceName        LowCardinality(String) CODEC(ZSTD(1)),
    Body               String CODEC(ZSTD(1)),
    ResourceSchemaUrl  LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl     LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName          String CODEC(ZSTD(1)),
    ScopeVersion       LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName          String CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",

    // ── RUM (Real User Monitoring) events ──
    r"CREATE TABLE IF NOT EXISTS observability.rum_events
(
    Timestamp          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimestampTime      DateTime DEFAULT toDateTime(Timestamp),
    AppName            LowCardinality(String) CODEC(ZSTD(1)),
    AppVersion         LowCardinality(String) CODEC(ZSTD(1)),
    Environment        LowCardinality(String) CODEC(ZSTD(1)),
    SessionId          String CODEC(ZSTD(1)),
    UserId             String CODEC(ZSTD(1)),
    PageUrl            String CODEC(ZSTD(1)),
    PagePath           String CODEC(ZSTD(1)),
    ViewName           String CODEC(ZSTD(1)),
    Referrer           String CODEC(ZSTD(1)),
    BrowserName        LowCardinality(String) CODEC(ZSTD(1)),
    BrowserVersion     LowCardinality(String) CODEC(ZSTD(1)),
    OsName             LowCardinality(String) CODEC(ZSTD(1)),
    OsVersion          LowCardinality(String) CODEC(ZSTD(1)),
    DeviceType         LowCardinality(String) CODEC(ZSTD(1)),
    ScreenWidth        UInt16 CODEC(ZSTD(1)),
    ScreenHeight       UInt16 CODEC(ZSTD(1)),
    EventType          LowCardinality(String) CODEC(ZSTD(1)),
    EventName          String CODEC(ZSTD(1)),
    VitalName          LowCardinality(String) CODEC(ZSTD(1)),
    VitalValue         Float64 CODEC(Gorilla, ZSTD(1)),
    VitalRating        LowCardinality(String) CODEC(ZSTD(1)),
    ErrorMessage       String CODEC(ZSTD(1)),
    ErrorStack         String CODEC(ZSTD(1)),
    ErrorType          LowCardinality(String) CODEC(ZSTD(1)),
    InteractionTarget  String CODEC(ZSTD(1)),
    InteractionType    LowCardinality(String) CODEC(ZSTD(1)),
    DurationMs         Float64 CODEC(Gorilla, ZSTD(1)),
    TraceId            String CODEC(ZSTD(1)),
    SpanId             String CODEC(ZSTD(1)),
    Attributes         String CODEC(ZSTD(1)),
    INDEX idx_session_id SessionId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_user_id UserId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_error_message ErrorMessage TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (AppName, EventType, TimestampTime)
ORDER BY (AppName, EventType, TimestampTime, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
];

/// Run all migrations against ClickHouse.
///
/// Connects **without** a default database so that `CREATE DATABASE` succeeds
/// even on a fresh instance. Every statement uses `IF NOT EXISTS` so this is
/// safe to call on every startup.
pub async fn run(url: &str, user: &str, password: &str, config: &WideConfig) -> anyhow::Result<()> {
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

    apply_retention_ttl(&client, config).await?;
    apply_storage_policy(&client, config).await;

    Ok(())
}

/// Adjust table-level TTLs based on config. Uses the effective (max) TTL so
/// that part-level drops don't remove data that has longer per-rule retention.
async fn apply_retention_ttl(client: &Client, config: &WideConfig) -> anyhow::Result<()> {
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
        let sql = format!(
            "ALTER TABLE observability.{table} MODIFY TTL {ts_expr} + INTERVAL {traces_days} DAY DELETE"
        );
        if let Err(e) = client.query(&sql).execute().await {
            tracing::warn!("failed to set TTL on {table}: {e}");
        }
    }

    // Log table
    let sql = format!(
        "ALTER TABLE observability.otel_logs MODIFY TTL toDateTime(Timestamp) + INTERVAL {logs_days} DAY DELETE"
    );
    if let Err(e) = client.query(&sql).execute().await {
        tracing::warn!("failed to set TTL on otel_logs: {e}");
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
async fn apply_storage_policy(client: &Client, config: &WideConfig) {
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
