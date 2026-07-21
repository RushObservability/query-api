#!/usr/bin/env bash
set -euo pipefail

: "${RUSH_POSTGRES_COLLECTOR_VERSION:?RUSH_POSTGRES_COLLECTOR_VERSION is required}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN is required to download the private collector release}"
: "${DEST_DIR:?DEST_DIR is required}"

repo="${RUSH_POSTGRES_COLLECTOR_REPO:-RushObservability/postgresql-collector}"
base="https://github.com/${repo}/releases/download/${RUSH_POSTGRES_COLLECTOR_VERSION}"
asset="postgres-collector-linux-amd64.tar.gz"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

curl --fail --silent --show-error --location \
  --header "Authorization: Bearer ${GITHUB_TOKEN}" \
  --header "Accept: application/octet-stream" \
  "${base}/${asset}" \
  --output "${tmp_dir}/${asset}"
curl --fail --silent --show-error --location \
  --header "Authorization: Bearer ${GITHUB_TOKEN}" \
  --header "Accept: application/octet-stream" \
  "${base}/${asset}.sha256" \
  --output "${tmp_dir}/${asset}.sha256"

expected="$(awk '{print $1}' "${tmp_dir}/${asset}.sha256")"
actual="$(sha256sum "${tmp_dir}/${asset}" | awk '{print $1}')"
if [[ -z "${expected}" || "${expected}" != "${actual}" ]]; then
  echo "collector release checksum verification failed" >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
tar -xzf "${tmp_dir}/${asset}" -C "${tmp_dir}"
install -m 0755 "${tmp_dir}/postgres-collector" "${DEST_DIR}/postgres-collector"
echo "installed PostgreSQL collector ${RUSH_POSTGRES_COLLECTOR_VERSION}"
