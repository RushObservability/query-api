#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
container="rush-frontend-telemetry-test-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
# Pinned official ClickHouse 26.10.3.7; all API credentials are isolated test
# values generated inside the integration test, never read from the developer shell.
docker run -d --rm --name "$container" --memory=1536m --cpus=2 \
  -p 127.0.0.1::8123 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  --mount "type=bind,source=$repo_root/tests/fixtures/frontend-clickhouse.xml,target=/etc/clickhouse-server/config.d/frontend-test.xml,readonly" \
  clickhouse/clickhouse-server@sha256:739e099abd1157390960a59e4532d21781d8fde57e6e4151cb920ea02c30b1ce >/dev/null
address="$(docker port "$container" 8123/tcp)"
export RUSH_TEST_CLICKHOUSE_URL="http://$address"
ready=false
for attempt in {1..60}; do
  if curl --fail --silent --max-time 2 "$RUSH_TEST_CLICKHOUSE_URL/ping" >/dev/null; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then docker logs "$container"; exit 1; fi
cargo test --test frontend_telemetry_session --all-features -- --ignored
