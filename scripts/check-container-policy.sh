#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

failed=0

if rg -n --glob 'Dockerfile*' --glob 'docker-compose*.yml' --glob 'docker-compose*.yaml' \
  '(^|[[:space:]])(FROM|image:)[[:space:]]+[^[:space:]]*:latest([[:space:]]|$)|image:[[:space:]]+[^[:space:]]*:latest-' .; then
  echo 'floating latest image reference detected' >&2
  failed=1
fi

if rg -n '^\s*-\s*"[0-9]+:[0-9]+' docker-compose.yml; then
  echo 'Compose publishes a port on all interfaces; bind it to 127.0.0.1' >&2
  failed=1
fi

if rg -n 'CLICKHOUSE_PASSWORD=$|CLICKHOUSE_PASSWORD:[[:space:]]*$' docker-compose.yml; then
  echo 'Compose contains an empty ClickHouse password' >&2
  failed=1
fi

if (( failed != 0 )); then
  exit 1
fi

echo 'container deployment policy passed'
