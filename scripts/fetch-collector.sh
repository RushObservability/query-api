#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_TOKEN:?GITHUB_TOKEN is required to download the private collector release}"
: "${DEST_DIR:?DEST_DIR is required}"

kind="${RUSH_COLLECTOR_KIND:-postgres}"
case "${kind}" in
  postgres)
    : "${RUSH_POSTGRES_COLLECTOR_VERSION:?RUSH_POSTGRES_COLLECTOR_VERSION is required}"
    version="${RUSH_POSTGRES_COLLECTOR_VERSION}"
    repo="${RUSH_POSTGRES_COLLECTOR_REPO:-RushObservability/postgresql-collector}"
    binary="postgres-collector"
    label="PostgreSQL"
    ;;
  mysql)
    : "${RUSH_MYSQL_COLLECTOR_VERSION:?RUSH_MYSQL_COLLECTOR_VERSION is required}"
    version="${RUSH_MYSQL_COLLECTOR_VERSION}"
    repo="${RUSH_MYSQL_COLLECTOR_REPO:-RushObservability/mysql-collector}"
    binary="mysql-collector"
    label="MySQL"
    ;;
  *)
    echo "unsupported collector kind: ${kind}" >&2
    exit 1
    ;;
esac
base="https://github.com/${repo}/releases/download/${version}"
asset="${binary}-linux-amd64.tar.gz"
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
install -m 0755 "${tmp_dir}/${binary}" "${DEST_DIR}/${binary}"
echo "installed ${label} collector ${version}"
