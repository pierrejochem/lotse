# Shortcuts for the common tasks. `make` lists them.
ADDR ?= 127.0.0.1:8080

.DEFAULT_GOAL := help
.PHONY: help dev stop build web test lint fmt check clean sdk-test sdk-vectors sdk-vectors-check

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-18s %s\n", $$1, $$2}'

console/web/node_modules: console/web/package-lock.json
	cd console/web && npm ci
	@touch $@

web: console/web/node_modules ## Build the web app
	cd console/web && npm run build

console/web/dist:
	$(MAKE) web

dev: console/web/dist ## Three nodes and the console on ADDR, with sample traffic (Ctrl-C stops)
	cargo run -p lotse-console --example local_cluster -- $(ADDR)

stop: ## Stop a dev environment running in the background
	-pkill -f "[e]xamples/local_cluster"

build: web ## Build the web app and the whole workspace
	cargo build --workspace

test: ## Run all tests and short simulations
	cargo test --workspace

lint: ## Clippy, warnings are errors
	cargo clippy --workspace --all-targets -- -D warnings

fmt: ## Format the code
	cargo fmt --all

check: ## What CI checks first: format, clippy, tests
	cargo fmt --all --check
	$(MAKE) lint test

VECTORS = cargo run -q -p lotse-proto --features samples --example vectors --

sdk-vectors: ## Regenerate the golden vectors in sdk/conformance from the Rust codec
	$(VECTORS) sdk/conformance

sdk-vectors-check: ## Fail if sdk/conformance differs from what the Rust codec writes now
	@dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
		$(VECTORS) "$$dir" && \
		diff -r sdk/conformance "$$dir" && echo "sdk/conformance matches the Rust codec"

sdk-test: sdk-vectors-check ## Check the vectors, then test the Java SDK against them and the Kotlin layer on it
	cd sdk/java && ./gradlew test testWithoutCodecs
	cd sdk/kotlin && ./gradlew test

clean: ## Remove build output of the web app and cargo
	rm -rf console/web/dist
	cargo clean
