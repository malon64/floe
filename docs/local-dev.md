# Local development

## Prerequisites

- Git
- Rust toolchain (stable): `rustup` + `cargo`
- C/C++ build tools in `PATH` (required by Rust dependencies)

Quick check:

```bash
git --version
rustc --version
cargo --version
```

## Setup

```bash
git clone https://github.com/malon64/floe.git
cd floe

# Optional: ensure stable toolchain is available
rustup default stable

# Pre-fetch dependencies for faster first build/test
cargo fetch
```

## Run

Use the example contract in this repo:

```bash
cargo run -p floe-cli -- validate -c example/config.yml
cargo run -p floe-cli -- run -c example/config.yml
```

## Test

Run the main crate tests:

```bash
cargo test -p floe-core
cargo test -p floe-cli
```

## Quality checks

Run formatting and lint checks before opening a PR:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## Troubleshooting

- `cargo` command not found: install Rust with `rustup`, then restart your shell.
- Build fails on native deps: install your OS C/C++ toolchain, then rerun `cargo build`.
- `example/config.yml` not found: run commands from the repository root (`floe/`).
- CLI command succeeds but output is unexpected: run `floe validate` first to catch contract issues early.
