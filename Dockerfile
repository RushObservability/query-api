# syntax=docker/dockerfile:1.7
FROM rust:1.88-slim AS builder

# build-essential (gcc + make) is required to compile jemalloc-sys, which runs
# jemalloc's own configure + make during the build of the tikv-jemallocator dep.
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY scripts ./scripts

ARG RUSH_FEATURES=oss
ARG RUSH_POSTGRES_COLLECTOR_VERSION=
ARG RUSH_MYSQL_COLLECTOR_VERSION=
RUN --mount=type=secret,id=github_token,required=false \
    set -eu; \
    mkdir -p /app/collector; \
    case ",${RUSH_FEATURES}," in \
      *,postgres-collector,*) \
        test -n "${RUSH_POSTGRES_COLLECTOR_VERSION}" || (echo "RUSH_POSTGRES_COLLECTOR_VERSION is required for licensed builds" >&2; exit 1); \
        test -s /run/secrets/github_token || (echo "BuildKit secret github_token is required for licensed builds" >&2; exit 1); \
        GITHUB_TOKEN="$$(cat /run/secrets/github_token)" \
        RUSH_POSTGRES_COLLECTOR_VERSION="${RUSH_POSTGRES_COLLECTOR_VERSION}" \
        DEST_DIR=/app/collector ./scripts/fetch-collector.sh; \
        ;; \
      *) \
        touch /app/collector/.community-build; \
        ;; \
    esac
RUN --mount=type=secret,id=github_token,required=false \
    set -eu; \
    case ",${RUSH_FEATURES}," in \
      *,mysql-collector,*) \
        test -n "${RUSH_MYSQL_COLLECTOR_VERSION}" || (echo "RUSH_MYSQL_COLLECTOR_VERSION is required for licensed builds" >&2; exit 1); \
        test -s /run/secrets/github_token || (echo "BuildKit secret github_token is required for licensed builds" >&2; exit 1); \
        GITHUB_TOKEN="$$(cat /run/secrets/github_token)" \
        RUSH_COLLECTOR_KIND=mysql \
        RUSH_MYSQL_COLLECTOR_VERSION="${RUSH_MYSQL_COLLECTOR_VERSION}" \
        DEST_DIR=/app/collector ./scripts/fetch-collector.sh; \
        ;; \
    esac
RUN cargo build --release --no-default-features --features ${RUSH_FEATURES}

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --system appgroup && useradd --system --gid appgroup --no-create-home appuser

COPY --from=builder /app/target/release/rush-api /usr/local/bin/rush-api
COPY --from=builder /app/target/release/rush-anomaly-engine /usr/local/bin/anomaly_engine
COPY --from=builder /app/collector/ /usr/local/lib/rush/collectors/
ENV RUSH_POSTGRES_COLLECTOR_BIN=/usr/local/lib/rush/collectors/postgres-collector
ENV RUSH_MYSQL_COLLECTOR_BIN=/usr/local/lib/rush/collectors/mysql-collector

# Use the numeric IDs created above so Kubernetes can verify runAsNonRoot without
# needing to resolve the image's passwd entry during admission.
USER 999:999
EXPOSE 8080

CMD ["rush-api"]
