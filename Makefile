BINARY  := rush-api
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

.PHONY: build release run run-anomaly dev check test fmt lint clean docker package \
        up up-full down deps logs run-local watch watch-anomaly

## Development — local binary + ClickHouse in Docker

deps:                 ## Start ClickHouse in Docker
	docker compose up -d clickhouse
	@echo "Waiting for ClickHouse..."
	@until curl -sf http://localhost:8123/ping >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse ready on :8123"

run-local: deps       ## Run query-api locally (ClickHouse in Docker)
	RUST_LOG=rush_api=debug,tower_http=debug cargo run --bin rush-api

build:                ## Build debug binary
	cargo build

release:              ## Build optimised release binary
	cargo build --release

run:                  ## Run query-api in debug mode (no dependency start)
	RUST_LOG=rush_api=debug,tower_http=debug cargo run --bin rush-api

run-anomaly:          ## Run anomaly engine in debug mode
	WIDE_PROM_BASE_URL=http://localhost:8080 \
	RUST_LOG=rush_api=debug \
	cargo run --bin wide-anomaly-engine

dev: run-local        ## Alias for run-local

watch: deps           ## Watch & restart query-api on code changes
	RUST_LOG=rush_api=debug,tower_http=debug \
	cargo watch -x 'run --bin rush-api'

watch-anomaly:        ## Watch & restart anomaly engine on code changes
	WIDE_PROM_BASE_URL=http://localhost:8080 \
	RUST_LOG=rush_api=debug \
	cargo watch -x 'run --bin wide-anomaly-engine'

## Quality

check:                ## Type-check without building
	cargo check

test:                 ## Run tests
	cargo test

fmt:                  ## Format code
	cargo fmt

lint:                 ## Run clippy lints
	cargo clippy -- -D warnings

## Docker Compose

up:                   ## Start ClickHouse only (for local dev)
	docker compose up -d clickhouse

up-full:              ## Start everything in Docker (ClickHouse + query-api)
	docker compose --profile full up -d --build

down:                 ## Stop all Docker services
	docker compose --profile full down

logs:                 ## Tail Docker compose logs
	docker compose --profile full logs -f

## Docker (standalone)

docker:               ## Build Docker image
	docker build -t $(BINARY):$(VERSION) -t $(BINARY):latest .

docker-run:           ## Run via Docker (connects to host ClickHouse)
	docker run --rm -p 8080:8080 \
		-e CLICKHOUSE_URL=http://host.docker.internal:8123 \
		$(BINARY):latest

## Package

package: release      ## Package release binary into tarball
	@mkdir -p dist
	cp target/release/$(BINARY) dist/
	cd dist && tar czf $(BINARY)-$(VERSION)-$(COMMIT).tar.gz $(BINARY)
	@rm dist/$(BINARY)
	@echo "Packaged: dist/$(BINARY)-$(VERSION)-$(COMMIT).tar.gz"

## Cleanup

clean:                ## Remove build artefacts
	cargo clean
	rm -rf dist

clean-all: clean down ## Remove build artefacts + Docker volumes
	docker compose --profile full down -v

## Help

help:                 ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
