BINARY  := rush-api
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
FEATURES ?= oss
CARGO_FEATURES := --no-default-features --features $(FEATURES)
RUSH_POSTGRES_COLLECTOR_VERSION ?=
RUSH_MYSQL_COLLECTOR_VERSION ?=

# If the collector repository is checked out next to query-api, local
# development builds use it automatically. Override this path when the
# checkout lives elsewhere, for example:
#   make watch LOCAL_COLLECTOR_DIR=~/src/postgresql-collector
LOCAL_COLLECTOR_DIR      ?= ../postgres-collector
LOCAL_COLLECTOR_MANIFEST := $(LOCAL_COLLECTOR_DIR)/Cargo.toml
LOCAL_COLLECTOR_BIN      := $(LOCAL_COLLECTOR_DIR)/target/debug/postgres-collector
LOCAL_COLLECTOR_CONFIG   := $(LOCAL_COLLECTOR_DIR)/config.yaml
LOCAL_COLLECTOR_AVAILABLE := $(wildcard $(LOCAL_COLLECTOR_MANIFEST))
LOCAL_COLLECTOR_CONFIG_AVAILABLE := $(wildcard $(LOCAL_COLLECTOR_CONFIG))
LOCAL_MYSQL_COLLECTOR_DIR      ?= ../mysql-collector
LOCAL_MYSQL_COLLECTOR_MANIFEST := $(LOCAL_MYSQL_COLLECTOR_DIR)/Cargo.toml
LOCAL_MYSQL_COLLECTOR_BIN      := $(LOCAL_MYSQL_COLLECTOR_DIR)/target/debug/mysql-collector
LOCAL_MYSQL_COLLECTOR_CONFIG   := $(LOCAL_MYSQL_COLLECTOR_DIR)/config.yaml
LOCAL_MYSQL_COLLECTOR_AVAILABLE := $(wildcard $(LOCAL_MYSQL_COLLECTOR_MANIFEST))
LOCAL_MYSQL_COLLECTOR_CONFIG_AVAILABLE := $(wildcard $(LOCAL_MYSQL_COLLECTOR_CONFIG))
empty :=
space := $(empty) $(empty)
comma := ,
LOCAL_DEV_FEATURES       := $(strip $(if $(LOCAL_COLLECTOR_AVAILABLE),postgres-collector) $(if $(LOCAL_MYSQL_COLLECTOR_AVAILABLE),mysql-collector))
DEV_FEATURES             := $(if $(LOCAL_DEV_FEATURES),$(subst $(space),$(comma),$(LOCAL_DEV_FEATURES)),$(FEATURES))
DEV_CARGO_FEATURES       := --no-default-features --features $(DEV_FEATURES)
LOCAL_COLLECTOR_ENV      := $(if $(LOCAL_COLLECTOR_AVAILABLE),RUSH_POSTGRES_COLLECTOR_BIN="$(LOCAL_COLLECTOR_BIN)") $(if $(LOCAL_COLLECTOR_CONFIG_AVAILABLE),RUSH_POSTGRES_COLLECTOR_CONFIG="$(LOCAL_COLLECTOR_CONFIG)") $(if $(LOCAL_MYSQL_COLLECTOR_AVAILABLE),RUSH_MYSQL_COLLECTOR_BIN="$(LOCAL_MYSQL_COLLECTOR_BIN)") $(if $(LOCAL_MYSQL_COLLECTOR_CONFIG_AVAILABLE),RUSH_MYSQL_COLLECTOR_CONFIG="$(LOCAL_MYSQL_COLLECTOR_CONFIG)")
LOCAL_COLLECTOR_WATCH    := $(if $(LOCAL_COLLECTOR_AVAILABLE),-w "$(LOCAL_COLLECTOR_DIR)/src" -w "$(LOCAL_COLLECTOR_MANIFEST)" $(if $(LOCAL_COLLECTOR_CONFIG_AVAILABLE),-w "$(LOCAL_COLLECTOR_CONFIG)")) $(if $(LOCAL_MYSQL_COLLECTOR_AVAILABLE),-w "$(LOCAL_MYSQL_COLLECTOR_DIR)/src" -w "$(LOCAL_MYSQL_COLLECTOR_MANIFEST)" $(if $(LOCAL_MYSQL_COLLECTOR_CONFIG_AVAILABLE),-w "$(LOCAL_MYSQL_COLLECTOR_CONFIG)"))
LOCAL_COLLECTOR_BUILD    := $(if $(LOCAL_COLLECTOR_AVAILABLE),cargo build --manifest-path "$(LOCAL_COLLECTOR_MANIFEST)" --bin postgres-collector &&) $(if $(LOCAL_MYSQL_COLLECTOR_AVAILABLE),cargo build --manifest-path "$(LOCAL_MYSQL_COLLECTOR_MANIFEST)" --bin mysql-collector &&)

# Local development wiring. These values are used only by `make dev` and
# `make watch`; `make run` sources the production-style `.env` file instead.
DEV_RUSH_PORT                := 8080
DEV_CLICKHOUSE_URL           := http://localhost:8123
DEV_SRE_AGENT_URL            := http://localhost:8081
DEV_SRE_AGENT_INTERNAL_TOKEN := dev-local-agent-token
DEV_COLLECTOR_MANAGER        := true
DEV_INTEGRATION_KEY           := rush-local-integration-key-change-me
DEV_CONFIG_ENCRYPTION_KEY     := rush-local-config-encryption-key-32-bytes
DEV_ALLOW_PRIVATE_NOTIFICATION_URLS := true
DEV_ALLOWED_ORIGINS           := http://localhost:5173,http://localhost:8080
DEV_ALLOW_ANONYMOUS_DEFAULT  := true

.PHONY: build release fetch-collector fetch-mysql-collector prepare-local-collector run run-anomaly dev check test fmt lint security security-audit security-policy clean docker package \
        up up-full down deps logs run-local watch watch-anomaly

## Development — local binary + ClickHouse in Docker

deps:                 ## Start ClickHouse in Docker
	docker compose up -d clickhouse
	@echo "Waiting for ClickHouse..."
	@until curl -sf http://localhost:8123/ping >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse ready on :8123"

prepare-local-collector: ## Build checked-out PostgreSQL and MySQL collectors for local development
	@if [ -n "$(LOCAL_COLLECTOR_AVAILABLE)" ]; then \
		echo "Building local PostgreSQL collector from $(LOCAL_COLLECTOR_DIR)..."; \
		cargo build --manifest-path "$(LOCAL_COLLECTOR_MANIFEST)" --bin postgres-collector; \
	fi
	@if [ -n "$(LOCAL_MYSQL_COLLECTOR_AVAILABLE)" ]; then \
		echo "Building local MySQL collector from $(LOCAL_MYSQL_COLLECTOR_DIR)..."; \
		cargo build --manifest-path "$(LOCAL_MYSQL_COLLECTOR_MANIFEST)" --bin mysql-collector; \
	fi

dev: deps prepare-local-collector ## Run query-api with local development wiring
	if [ -f ../.env ]; then set -a; . ../.env; set +a; fi; \
	RUSH_PORT=$(DEV_RUSH_PORT) \
	CLICKHOUSE_URL=$(DEV_CLICKHOUSE_URL) \
	SRE_AGENT_URL=$(DEV_SRE_AGENT_URL) \
	SRE_AGENT_INTERNAL_TOKEN=$(DEV_SRE_AGENT_INTERNAL_TOKEN) \
	RUSH_COLLECTOR_MANAGER_ENABLED=$(DEV_COLLECTOR_MANAGER) \
	RUSH_INTEGRATION_ENCRYPTION_KEY=$(DEV_INTEGRATION_KEY) \
	RUSH_CONFIG_ENCRYPTION_KEY="$${RUSH_CONFIG_ENCRYPTION_KEY:-$(DEV_CONFIG_ENCRYPTION_KEY)}" \
	RUSH_ALLOW_PRIVATE_NOTIFICATION_URLS=$(DEV_ALLOW_PRIVATE_NOTIFICATION_URLS) \
	RUSH_ALLOWED_ORIGINS=$(DEV_ALLOWED_ORIGINS) \
	RUSH_ALLOW_ANONYMOUS_DEFAULT=$(DEV_ALLOW_ANONYMOUS_DEFAULT) \
	$(LOCAL_COLLECTOR_ENV) \
	RUST_LOG=rush_api=debug,tower_http=debug \
	cargo run $(DEV_CARGO_FEATURES) --bin $(BINARY)

run-local: dev        ## Backwards-compatible alias for dev

build:                ## Build debug binary
	cargo build $(CARGO_FEATURES)

release:              ## Build optimised release binary
	cargo build --release $(CARGO_FEATURES)

fetch-collector:       ## Download and verify a private PostgreSQL collector release
	@test -n "$(RUSH_POSTGRES_COLLECTOR_VERSION)" || { echo "RUSH_POSTGRES_COLLECTOR_VERSION is required" >&2; exit 1; }
	@test -n "$${GITHUB_TOKEN:-}" || { echo "GITHUB_TOKEN is required" >&2; exit 1; }
	RUSH_POSTGRES_COLLECTOR_VERSION=$(RUSH_POSTGRES_COLLECTOR_VERSION) \
	GITHUB_TOKEN="$${GITHUB_TOKEN}" \
	DEST_DIR="$${DEST_DIR:-$(CURDIR)/target/managed-collectors}" \
	./scripts/fetch-collector.sh

fetch-mysql-collector: ## Download and verify a private MySQL collector release
	@test -n "$(RUSH_MYSQL_COLLECTOR_VERSION)" || { echo "RUSH_MYSQL_COLLECTOR_VERSION is required" >&2; exit 1; }
	@test -n "$${GITHUB_TOKEN:-}" || { echo "GITHUB_TOKEN is required" >&2; exit 1; }
	RUSH_COLLECTOR_KIND=mysql \
	RUSH_MYSQL_COLLECTOR_VERSION=$(RUSH_MYSQL_COLLECTOR_VERSION) \
	GITHUB_TOKEN="$${GITHUB_TOKEN}" \
	DEST_DIR="$${DEST_DIR:-$(CURDIR)/target/managed-collectors}" \
	./scripts/fetch-collector.sh

run:                  ## Run query-api in debug mode (no dependency start)
	@set -e; \
	if [ -f ../.env ]; then set -a; . ../.env; set +a; fi; \
	test -f .env || { echo "ERROR: query-api/.env is required for make run" >&2; exit 1; }; \
	set -a; . ./.env; set +a; \
	RUSH_COLLECTOR_MANAGER_ENABLED="$${RUSH_COLLECTOR_MANAGER_ENABLED:-$(DEV_COLLECTOR_MANAGER)}" \
	RUSH_INTEGRATION_ENCRYPTION_KEY="$${RUSH_INTEGRATION_ENCRYPTION_KEY:-$(DEV_INTEGRATION_KEY)}" \
	RUSH_CONFIG_ENCRYPTION_KEY="$${RUSH_CONFIG_ENCRYPTION_KEY:-$(DEV_CONFIG_ENCRYPTION_KEY)}" \
	RUST_LOG="$${RUST_LOG:-rush_api=info,tower_http=info}" cargo run $(CARGO_FEATURES) --bin $(BINARY)

run-anomaly:          ## Run anomaly engine in debug mode
	RUSH_PROM_BASE_URL=http://localhost:8080 \
	RUST_LOG=rush_api=debug \
	cargo run --bin wide-anomaly-engine

watch: prepare-local-collector ## Watch query-api and a checked-out collector with local development wiring
	if [ -f ../.env ]; then set -a; . ../.env; set +a; fi; \
	RUSH_PORT=$(DEV_RUSH_PORT) \
	CLICKHOUSE_URL=$(DEV_CLICKHOUSE_URL) \
	SRE_AGENT_URL=$(DEV_SRE_AGENT_URL) \
	SRE_AGENT_INTERNAL_TOKEN=$(DEV_SRE_AGENT_INTERNAL_TOKEN) \
	RUSH_COLLECTOR_MANAGER_ENABLED=$(DEV_COLLECTOR_MANAGER) \
	RUSH_INTEGRATION_ENCRYPTION_KEY=$(DEV_INTEGRATION_KEY) \
	RUSH_CONFIG_ENCRYPTION_KEY="$${RUSH_CONFIG_ENCRYPTION_KEY:-$(DEV_CONFIG_ENCRYPTION_KEY)}" \
	RUSH_ALLOW_PRIVATE_NOTIFICATION_URLS=$(DEV_ALLOW_PRIVATE_NOTIFICATION_URLS) \
	RUSH_ALLOWED_ORIGINS=$(DEV_ALLOWED_ORIGINS) \
	RUSH_ALLOW_ANONYMOUS_DEFAULT=$(DEV_ALLOW_ANONYMOUS_DEFAULT) \
	$(LOCAL_COLLECTOR_ENV) \
	RUST_LOG=rush_api=debug,tower_http=debug \
	$(if $(LOCAL_DEV_FEATURES),cargo watch -w src $(LOCAL_COLLECTOR_WATCH) -s '$(LOCAL_COLLECTOR_BUILD) cargo run $(DEV_CARGO_FEATURES) --bin $(BINARY)',cargo watch -x 'run $(DEV_CARGO_FEATURES) --bin $(BINARY)')

watch-anomaly:        ## Watch & restart anomaly engine on code changes
	RUSH_PROM_BASE_URL=http://localhost:8080 \
	RUST_LOG=rush_api=debug \
	cargo watch -x 'run --bin wide-anomaly-engine'

## Quality

check:                ## Type-check without building
	cargo check $(CARGO_FEATURES)

test:                 ## Run tests
	cargo test $(CARGO_FEATURES)

fmt:                  ## Format code
	cargo fmt

lint:                 ## Run clippy lints
	cargo clippy --all-targets --all-features -- -D clippy::correctness -D clippy::suspicious

security: security-audit security-policy ## Run dependency vulnerability, license, source, and yank checks

security-audit:       ## Scan Cargo.lock against RustSec (one documented unreachable exception)
	cargo audit --ignore RUSTSEC-2023-0071

security-policy:      ## Enforce deny.toml advisory, license, source, and dependency policy
	cargo deny check advisories bans licenses sources

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
	@if [ "$(FEATURES)" = "oss" ]; then \
		docker build --build-arg RUSH_FEATURES=$(FEATURES) -t $(BINARY):$(VERSION) -t $(BINARY):latest .; \
	else \
		test -n "$${GITHUB_TOKEN:-}" || { echo "GITHUB_TOKEN is required for licensed image builds" >&2; exit 1; }; \
		case ",$(FEATURES)," in *,postgres-collector,*) test -n "$(RUSH_POSTGRES_COLLECTOR_VERSION)" || { echo "RUSH_POSTGRES_COLLECTOR_VERSION is required for licensed image builds" >&2; exit 1; };; esac; \
		case ",$(FEATURES)," in *,mysql-collector,*) test -n "$(RUSH_MYSQL_COLLECTOR_VERSION)" || { echo "RUSH_MYSQL_COLLECTOR_VERSION is required for licensed image builds" >&2; exit 1; };; esac; \
		docker build --build-arg RUSH_FEATURES=$(FEATURES) --build-arg RUSH_POSTGRES_COLLECTOR_VERSION=$(RUSH_POSTGRES_COLLECTOR_VERSION) --build-arg RUSH_MYSQL_COLLECTOR_VERSION=$(RUSH_MYSQL_COLLECTOR_VERSION) --secret id=github_token,env=GITHUB_TOKEN -t $(BINARY):$(VERSION) -t $(BINARY):latest .; \
	fi

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
