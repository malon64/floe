.PHONY: fmt clippy test build run

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all

build:
	cargo build --all

run:
	@echo "TODO: implement CLI; use 'cargo run -p floe-cli -- ...' once available"
