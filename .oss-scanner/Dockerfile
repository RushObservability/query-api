# Image for Anthropic's OSS Scanner (https://github.com/anthropics/oss-scanner).
#
# The audit runs offline, so this build installs the pinned Rust toolchain and ClickHouse, fetches every crate,
# and compiles query-api, its engines and its tests with every feature. .oss-scanner/run-local.sh starts ClickHouse
# and query-api inside the image; threat_model.md, next to this file, says what matters and how to exercise it.
FROM rust:1.88-bookworm
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential clang pkg-config libssl-dev ca-certificates curl git jq python3 ripgrep \
    && rm -rf /var/lib/apt/lists/*

# ClickHouse, at the version docker-compose.yml runs, checked against the release's published SHA-512.
ARG CLICKHOUSE_VERSION=26.6.1.1193
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    base="https://github.com/ClickHouse/ClickHouse/releases/download/v${CLICKHOUSE_VERSION}-stable"; \
    tgz="clickhouse-common-static-${CLICKHOUSE_VERSION}-${arch}.tgz"; \
    cd /tmp; \
    curl -fsSLO "${base}/${tgz}"; \
    curl -fsSLO "${base}/${tgz}.sha512"; \
    sha512sum -c "${tgz}.sha512"; \
    tar -xzf "${tgz}"; \
    install -m 0755 "clickhouse-common-static-${CLICKHOUSE_VERSION}/usr/bin/clickhouse" /usr/local/bin/clickhouse; \
    rm -rf /tmp/clickhouse-common-static-*; \
    clickhouse local --query 'SELECT version()'

# scanner contract: the checkout lives inside the image, at /src
COPY . /src
WORKDIR /src

# An ARG, so that it bounds the memory this build uses without reaching the audit environment.
ARG CARGO_BUILD_JOBS=8

# Every crate, then the server, the anomaly engine and the tests, as CI builds them (with debug info).
# A step that fails fails the build, and primary_contact gets the log.
RUN cargo fetch --locked
RUN cargo build --locked --all-targets --all-features
RUN cargo test --locked --all-targets --all-features --no-run

# Run the tests so the log shows the image works, without letting a failure abort the build.
RUN timeout 1800 cargo test --locked --all-targets --all-features || echo "WARNING: cargo test reported failures"

# Start ClickHouse and query-api once, so a broken harness shows up in the build log rather than in the audit.
RUN timeout 300 .oss-scanner/run-local.sh --check || echo "WARNING: run-local.sh --check failed"

# Everything is fetched. From here on cargo must not wait for a registry that it cannot reach.
ENV CARGO_NET_OFFLINE=true
