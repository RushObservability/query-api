FROM rust:1.88-slim AS builder

# build-essential (gcc + make) is required to compile jemalloc-sys, which runs
# jemalloc's own configure + make during the build of the tikv-jemallocator dep.
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd --system appgroup && useradd --system --gid appgroup --no-create-home appuser

COPY --from=builder /app/target/release/rush-api /usr/local/bin/rush-api
COPY --from=builder /app/target/release/rush-anomaly-engine /usr/local/bin/anomaly_engine

USER appuser
EXPOSE 8080

CMD ["rush-api"]
