BINARY  := wide-query-api
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

.PHONY: build release run dev check test fmt lint clean docker package

## Development

build:                ## Build debug binary
	cargo build

release:              ## Build optimised release binary
	cargo build --release

run:                  ## Run in debug mode
	RUST_LOG=wide_query_api=debug,tower_http=debug cargo run

dev: run              ## Alias for run

## Quality

check:                ## Type-check without building
	cargo check

test:                 ## Run tests
	cargo test

fmt:                  ## Format code
	cargo fmt

lint:                 ## Run clippy lints
	cargo clippy -- -D warnings

## Docker

docker:               ## Build Docker image
	docker build -t $(BINARY):$(VERSION) -t $(BINARY):latest .

docker-run:           ## Run via Docker
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

## Help

help:                 ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
