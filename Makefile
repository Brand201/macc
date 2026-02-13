.PHONY: fmt lint test all check check-generic

all: fmt lint test check-generic

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace -- -D warnings

test:
	cargo test --workspace
	cd adapters && cargo test --workspace

test-contract:
	cargo test -p macc-registry --test contract

check-generic:
	@./scripts/check-ui-tool-transparency.sh

check: fmt-check lint test check-generic
