# syntax=docker/dockerfile:1.7
# Same builder and runtime as the collector images: a digest-pinned Rust
# builder and the distroless Chainguard glibc-dynamic runtime.
ARG RUST_IMAGE=rust:1.88-slim@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89
ARG CHAINGUARD_RUNTIME_IMAGE=cgr.dev/chainguard/glibc-dynamic@sha256:6acf5a19a988abdaf0f3d30247561431a206034e702871442bed66a2c68cc1a2
ARG CHAINGUARD_WOLFI_IMAGE=cgr.dev/chainguard/wolfi-base@sha256:08df5982c3d27e70a4ce1607e3bb9af09d746f8722cf135a7694afef879fc5a2

FROM ${RUST_IMAGE} AS builder

# build-essential (gcc + make) is required to compile jemalloc-sys, which runs
# jemalloc's own configure + make during the build of the tikv-jemallocator dep.
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release --no-default-features --features oss

FROM ${CHAINGUARD_RUNTIME_IMAGE} AS runtime-base

# rush-api links OpenSSL (SAML, SSO, config encryption, SMTP TLS), which
# glibc-dynamic does not ship. Install Wolfi's libssl3/libcrypto3, built for
# the same glibc, and register them in a copy of the runtime's apk database so
# Trivy and the SBOM report the OpenSSL version the image carries.
FROM ${CHAINGUARD_WOLFI_IMAGE} AS openssl
COPY --from=runtime-base /usr/lib/apk/db/installed /tmp/runtime-installed
RUN apk add --no-cache libssl3 libcrypto3 \
    && awk 'BEGIN { RS = ""; ORS = "\n\n" } /(^|\n)P:(libssl3|libcrypto3)(\n|$)/' /usr/lib/apk/db/installed > /tmp/openssl-installed \
    && test "$(grep -c '^P:' /tmp/openssl-installed)" -eq 2 \
    && awk 'BEGIN { RS = ""; ORS = "\n\n" } 1' /tmp/runtime-installed /tmp/openssl-installed > /tmp/installed \
    && mkdir -p /tmp/sbom \
    && cp /var/lib/db/sbom/libssl3-*.spdx.json /var/lib/db/sbom/libcrypto3-*.spdx.json /tmp/sbom/

FROM runtime-base

COPY --from=openssl /usr/lib/libssl.so.3 /usr/lib/libcrypto.so.3 /usr/lib/
COPY --from=openssl /tmp/installed /usr/lib/apk/db/installed
COPY --from=openssl /tmp/sbom/ /var/lib/db/sbom/
COPY --from=builder /app/target/release/rush-api /usr/local/bin/rush-api
COPY --from=builder /app/target/release/rush-anomaly-engine /usr/local/bin/anomaly_engine

# Numeric IDs let Kubernetes verify runAsNonRoot without resolving a passwd
# entry, which this distroless runtime doesn't have. 999 matches the Helm
# chart's fsGroup.
USER 999:999
EXPOSE 8080

CMD ["rush-api"]
