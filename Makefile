# Shortcuts for the common tasks. `make` lists them.
ADDR ?= 127.0.0.1:8080

.DEFAULT_GOAL := help
.PHONY: help dev stop build web test lint fmt check clean

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-8s %s\n", $$1, $$2}'

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

clean: ## Remove build output of the web app and cargo
	rm -rf console/web/dist
	cargo clean
