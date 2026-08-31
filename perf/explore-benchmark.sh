#!/usr/bin/env bash
set -euo pipefail

# Focused QFP-PERF-03 benchmark. It compares the former six-query default
# Explore interaction with the coordinated two-query plan against the same
# tenant, time range, warm cache state, and ClickHouse instance.

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-observability}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-rushdev}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-rush-local-clickhouse-password}"
PERF03_RUNS="${PERF03_RUNS:-20}"
PERF03_WARMUPS="${PERF03_WARMUPS:-3}"
PERF03_WINDOW_MINUTES="${PERF03_WINDOW_MINUTES:-60}"
PERF03_TENANT="${PERF03_TENANT:-}"

if ! [[ "$PERF03_RUNS" =~ ^[1-9][0-9]*$ ]] \
  || ! [[ "$PERF03_WARMUPS" =~ ^[0-9]+$ ]] \
  || ! [[ "$PERF03_WINDOW_MINUTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "PERF03_RUNS, PERF03_WARMUPS, and PERF03_WINDOW_MINUTES must be positive integers" >&2
  exit 2
fi

ch() {
  curl --fail-with-body --silent --show-error \
    --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "$1" \
    "${CLICKHOUSE_URL}/?database=${CLICKHOUSE_DATABASE}&log_queries=1"
}

run_query() {
  local query_id="$1"
  local sql="$2"
  curl --fail-with-body --silent --show-error \
    --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --header "X-ClickHouse-Query-Id: ${query_id}" \
    --data-binary "${sql} FORMAT Null" \
    "${CLICKHOUSE_URL}/?database=${CLICKHOUSE_DATABASE}&log_queries=1"
}

if [[ -z "$PERF03_TENANT" ]]; then
  PERF03_TENANT="$(ch "SELECT tenant_id FROM spans GROUP BY tenant_id ORDER BY count() DESC LIMIT 1 FORMAT TSVRaw")"
fi
if [[ ! "$PERF03_TENANT" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "PERF03_TENANT contains unsupported characters" >&2
  exit 2
fi

ANCHOR="$(ch "SELECT formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S', 'UTC') FROM spans WHERE tenant_id = '${PERF03_TENANT}' FORMAT TSVRaw")"
if [[ -z "$ANCHOR" || "$ANCHOR" == "1970-01-01 00:00:00" ]]; then
  echo "No spans found for tenant ${PERF03_TENANT}" >&2
  exit 1
fi

SESSION="$(date -u +%Y%m%dT%H%M%SZ)$$"
PREDICATE="PREWHERE tenant_id = '${PERF03_TENANT}' WHERE timestamp > parseDateTime64BestEffort('${ANCHOR}', 9, 'UTC') - INTERVAL ${PERF03_WINDOW_MINUTES} MINUTE AND timestamp <= parseDateTime64BestEffort('${ANCHOR}', 9, 'UTC')"

ROWS_SQL="SELECT * FROM spans ${PREDICATE} ORDER BY timestamp DESC LIMIT 100"
COUNT_SQL="SELECT count() FROM spans ${PREDICATE}"
HISTOGRAM_SQL="SELECT toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS bucket, count() AS count, countIf(status IN ('ERROR', 'STATUS_CODE_ERROR') OR http_status_code >= 500) AS error_count FROM spans ${PREDICATE} GROUP BY bucket ORDER BY bucket"
SERVICE_SQL="SELECT service_name, count() AS count FROM spans ${PREDICATE} GROUP BY service_name ORDER BY count DESC LIMIT 100"
STATUS_SQL="SELECT toString(multiIf(http_status_code >= 500, 500, http_status_code >= 400, 400, 200)) AS status_key, count() AS count FROM spans ${PREDICATE} GROUP BY status_key ORDER BY count DESC LIMIT 100"
METHOD_SQL="SELECT http_method, count() AS count FROM spans ${PREDICATE} GROUP BY http_method ORDER BY count DESC LIMIT 20"
SUMMARY_SQL="SELECT kind, bucket_value AS bucket, key, count, error_count, matched_bytes FROM (SELECT multiIf(grouping(bucket) = 0, 'histogram', grouping(service_key) = 0, 'service', grouping(status_key) = 0, 'status', grouping(method_key) = 0, 'method', 'total') AS kind, if(grouping(bucket) = 0, toString(bucket), '') AS bucket_value, multiIf(grouping(service_key) = 0, service_key, grouping(status_key) = 0, status_key, grouping(method_key) = 0, method_key, '') AS key, count() AS count, countIf(is_error) AS error_count, sum(logical_bytes) AS matched_bytes FROM (SELECT toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS bucket, toString(service_name) AS service_key, toString(multiIf(http_status_code >= 500, 500, http_status_code >= 400, 400, 200)) AS status_key, toString(http_method) AS method_key, (status IN ('ERROR', 'STATUS_CODE_ERROR') OR http_status_code >= 500) AS is_error, toUInt64(length(service_name) + length(span_name) + length(http_path) + length(attributes)) AS logical_bytes FROM spans ${PREDICATE}) GROUP BY GROUPING SETS ((bucket), (service_key), (status_key), (method_key), ())) ORDER BY kind ASC, count DESC LIMIT 200 BY kind"

run_parallel() {
  local plan="$1"
  local run="$2"
  local prefix="rush-perf03-${SESSION}-${plan}-${run}"
  local failed=0
  local pid
  local pids=()

  if [[ "$plan" == "legacy" ]]; then
    run_query "${prefix}-rows" "$ROWS_SQL" & pids+=("$!")
    run_query "${prefix}-count" "$COUNT_SQL" & pids+=("$!")
    run_query "${prefix}-histogram" "$HISTOGRAM_SQL" & pids+=("$!")
    run_query "${prefix}-service" "$SERVICE_SQL" & pids+=("$!")
    run_query "${prefix}-status" "$STATUS_SQL" & pids+=("$!")
    run_query "${prefix}-method" "$METHOD_SQL" & pids+=("$!")
  else
    run_query "${prefix}-rows" "$ROWS_SQL" & pids+=("$!")
    run_query "${prefix}-summary" "$SUMMARY_SQL" & pids+=("$!")
  fi

  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  return "$failed"
}

echo "Warming both plans (${PERF03_WARMUPS} iterations each)..." >&2
for ((run = 1; run <= PERF03_WARMUPS; run++)); do
  run_parallel legacy "warmup-${run}"
  run_parallel coordinated "warmup-${run}"
done

echo "Measuring ${PERF03_RUNS} alternating interactions per plan..." >&2
for ((run = 1; run <= PERF03_RUNS; run++)); do
  if ((run % 2 == 1)); then
    run_parallel legacy "$run"
    run_parallel coordinated "$run"
  else
    run_parallel coordinated "$run"
    run_parallel legacy "$run"
  fi
done

ch "SYSTEM FLUSH LOGS" >/dev/null

RESULT_SQL="WITH per_interaction AS (
  SELECT
    splitByChar('-', query_id)[4] AS plan,
    splitByChar('-', query_id)[5] AS run,
    count() AS clickhouse_queries,
    maxIf(query_duration_ms, endsWith(query_id, '-rows')) AS rows_ready_ms,
    max(query_duration_ms) AS full_interaction_ms,
    sum(read_rows) AS read_rows,
    sum(read_bytes) AS read_bytes
  FROM system.query_log
  WHERE type = 'QueryFinish'
    AND startsWith(query_id, 'rush-perf03-${SESSION}-')
    AND match(splitByChar('-', query_id)[5], '^[0-9]+$')
  GROUP BY plan, run
), per_plan AS (
  SELECT
    plan,
    count() AS samples,
    avg(clickhouse_queries) AS queries_per_interaction,
    quantileExact(0.50)(rows_ready_ms) AS p50_first_results_ms,
    quantileExact(0.95)(rows_ready_ms) AS p95_first_results_ms,
    quantileExact(0.99)(rows_ready_ms) AS p99_first_results_ms,
    quantileExact(0.95)(full_interaction_ms) AS p95_full_interaction_ms,
    avg(read_rows) AS avg_read_rows,
    avg(read_bytes) AS avg_read_bytes
  FROM per_interaction
  GROUP BY plan
)
SELECT
  '${SESSION}' AS benchmark_id,
  '${PERF03_TENANT}' AS tenant,
  '${ANCHOR}' AS anchor_utc,
  ${PERF03_WINDOW_MINUTES} AS window_minutes,
  ${PERF03_WARMUPS} AS warmups,
  ${PERF03_RUNS} AS requested_samples,
  groupArray(map(
    'plan', plan,
    'samples', toString(samples),
    'queries_per_interaction', toString(queries_per_interaction),
    'p50_first_results_ms', toString(p50_first_results_ms),
    'p95_first_results_ms', toString(p95_first_results_ms),
    'p99_first_results_ms', toString(p99_first_results_ms),
    'p95_full_interaction_ms', toString(p95_full_interaction_ms),
    'avg_read_rows', toString(avg_read_rows),
    'avg_read_bytes', toString(avg_read_bytes)
  )) AS plans
FROM per_plan
FORMAT JSONEachRow"

ch "$RESULT_SQL"
