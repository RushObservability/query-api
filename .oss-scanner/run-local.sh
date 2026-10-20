#!/usr/bin/env bash
# Start ClickHouse and query-api inside the OSS Scanner image, with no network access.
#
#   .oss-scanner/run-local.sh           start both in the background and print how to reach them
#   .oss-scanner/run-local.sh --check   start both, sign in as admin, then stop (used by the image build)
#   .oss-scanner/run-local.sh --stop    stop both
#
# query-api runs in development mode with the production read path: writes use the ClickHouse `default` user, and
# every telemetry read goes through the separate `rushquery` user under per-tenant row policies, as in the Helm
# chart. All secrets below are fixed local values for this sandbox only.
set -euo pipefail

src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root="${RUSH_LOCAL_DIR:-/tmp/rush-local}"
ch_password="rush-local-clickhouse-password"
read_password="rush-local-clickhouse-read-password"
admin_password="rush-local-admin-password"
api="http://127.0.0.1:${RUSH_PORT:-8080}"

stop() {
  for pidfile in "$root/rush-api.pid" "$root/clickhouse.pid"; do
    if [[ -f "$pidfile" ]]; then
      kill "$(cat "$pidfile")" 2>/dev/null || true
      rm -f "$pidfile"
    fi
  done
}

wait_for() {
  local what="$1" tries="$2"
  shift 2
  for _ in $(seq 1 "$tries"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "$what did not become ready" >&2
  return 1
}

if [[ "${1:-}" == "--stop" ]]; then
  stop
  exit 0
fi

binary="$src/target/debug/rush-api"
[[ -x "$binary" ]] || { echo "build query-api first: cargo build --all-features" >&2; exit 1; }

mkdir -p "$root/clickhouse/config.d" "$root/clickhouse/data" "$root/api"
cp "$src/clickhouse/config.d/local-storage.xml" "$root/clickhouse/config.d/"

cat >"$root/clickhouse/config.xml" <<EOF
<clickhouse>
  <logger>
    <level>warning</level>
    <log>$root/clickhouse/server.log</log>
    <errorlog>$root/clickhouse/error.log</errorlog>
  </logger>
  <listen_host>127.0.0.1</listen_host>
  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>
  <path>$root/clickhouse/data/</path>
  <tmp_path>$root/clickhouse/data/tmp/</tmp_path>
  <user_files_path>$root/clickhouse/data/user_files/</user_files_path>
  <format_schema_path>$root/clickhouse/data/format_schemas/</format_schema_path>
  <mark_cache_size>268435456</mark_cache_size>
  <!-- Required for the per-query rush_tenant_id setting the row policies read. -->
  <custom_settings_prefixes>rush_</custom_settings_prefixes>
  <user_directories>
    <users_xml><path>$root/clickhouse/users.xml</path></users_xml>
    <local_directory><path>$root/clickhouse/data/access/</path></local_directory>
  </user_directories>
</clickhouse>
EOF

# The rushquery grants match the Helm chart's read_user.xml.
read_grants=""
for table in logs spans spans_by_trace services metrics_gauge metrics_sum metrics_histogram metrics_exp_histogram \
  metrics_summary metrics_gauge_1m metrics_gauge_1h metrics_sum_1m metrics_sum_1h rum rum_replay signal_usage \
  tenant_usage profile_samples signal_query_stats; do
  read_grants+="<query>GRANT SELECT ON observability.${table}</query>"
done
read_grants+="<query>GRANT SELECT ON observability.\`.inner_id.\`*</query>"

cat >"$root/clickhouse/users.xml" <<EOF
<clickhouse>
  <profiles><default/></profiles>
  <quotas><default/></quotas>
  <users>
    <default>
      <password>$ch_password</password>
      <networks><ip>127.0.0.1</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
    <rushquery>
      <password>$read_password</password>
      <networks><ip>127.0.0.1</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>0</access_management>
      <grants>$read_grants</grants>
    </rushquery>
  </users>
</clickhouse>
EOF

stop
clickhouse server --config-file="$root/clickhouse/config.xml" >"$root/clickhouse/stdout.log" 2>&1 &
echo $! >"$root/clickhouse.pid"
wait_for ClickHouse 60 clickhouse client --password "$ch_password" --query 'SELECT 1'

(
  cd "$root/api"
  export CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_DATABASE=observability \
    CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD="$ch_password" \
    CLICKHOUSE_READ_USER=rushquery CLICKHOUSE_READ_PASSWORD="$read_password" \
    RUSH_ENVIRONMENT=development RUSH_INSECURE_COOKIES=true \
    RUSH_BASE_URL="$api" RUSH_PORT="${RUSH_PORT:-8080}" \
    RUSH_API_KEY_SECRET=rush-local-api-key-hmac-secret-0123456789abcdef \
    RUSH_SESSION_HMAC_SECRET=rush-local-session-hmac-secret-0123456789abcdef \
    RUSH_AUDIT_HMAC_SECRET=rush-local-audit-hmac-secret-0123456789abcdef \
    RUSH_CONFIG_ENCRYPTION_KEY=rush-local-config-encryption-key-0123456789abcdef \
    INITIAL_ADMIN_PASSWORD="$admin_password" \
    RUSH_SPOOL_DIR="$root/api/spool" RUSH_AUDIT_SPOOL_DIR="$root/api/audit-spool" RUSH_EXPORT_DIR="$root/api/exports" \
    RUST_LOG="${RUST_LOG:-rush_api=info}"
  exec "$binary" >"$root/api/rush-api.log" 2>&1
) &
echo $! >"$root/rush-api.pid"
if ! wait_for query-api 120 curl -fsS "$api/readyz"; then
  tail -n 40 "$root/api/rush-api.log" >&2
  stop
  exit 1
fi

if [[ "${1:-}" == "--check" ]]; then
  status=0
  curl -fsS -c "$root/cookies" -H 'Content-Type: application/json' -H "Origin: $api" \
    -d "{\"username\":\"admin\",\"password\":\"$admin_password\"}" "$api/api/v1/auth/login" >/dev/null || status=1
  stop
  if [[ $status -ne 0 ]]; then echo "admin sign-in failed" >&2; exit 1; fi
  echo "query-api started and admin sign-in works"
  exit 0
fi

cat <<EOF
query-api:  $api   (log: $root/api/rush-api.log)
ClickHouse: http://127.0.0.1:8123, user default / $ch_password (read user rushquery / $read_password)
Admin:      admin / $admin_password  (sign in with POST /api/v1/auth/login and an Origin: $api header)
Stop with:  .oss-scanner/run-local.sh --stop
EOF
