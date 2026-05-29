FROM rust:1.87-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

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
