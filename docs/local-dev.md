# Local Development (Rust/Cargo)

This guide helps first-time contributors run Floe from a local checkout.

## Prerequisites

- Git
- Rust toolchain (`stable`), managed by `rustup`
- A C/C++ toolchain in your `PATH` (required by some Rust dependencies)

Verify your environment:

```bash
rustup show
cargo --version
```

## Setup

Clone and enter the repository:

```bash
git clone https://github.com/malon64/floe.git
cd floe
```

Build the workspace once to fetch dependencies:

```bash
cargo build
```

## Run

Check CLI help:

```bash
cargo run -p floe-cli -- --help
```

Run ingestion with the example config:

```bash
cargo run -p floe-cli -- run -c example/config.yml
```

Validate config without running ingestion:

```bash
cargo run -p floe-cli -- validate -c example/config.yml
```

## Test

Run Rust tests:

```bash
cargo test
```

## Quality Checks

Formatting check:

```bash
cargo fmt -- --check
```

If formatting check fails, apply formatting and rerun checks:

```bash
cargo fmt
```

Lint with warnings treated as errors:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## Troubleshooting

- `cargo` or `rustup` not found:
  Install Rust via `https://rustup.rs`, then open a new shell and run `cargo --version`.
- Build errors mentioning a missing linker or C compiler:
  Install your platform build tools (for example, Xcode Command Line Tools on macOS or `build-essential` on Debian/Ubuntu), then rerun `cargo build`.
- `example/config.yml` not found:
  Ensure commands are run from the repository root (`pwd` should end with `floe`).
- `cargo fmt -- --check` fails:
  Run `cargo fmt`, review the diff, and rerun `cargo fmt -- --check`.
- Clippy fails on warnings:
  Run `cargo clippy --workspace --all-targets -- -D warnings`, fix reported issues, then rerun.
