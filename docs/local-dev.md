# Local Development (Rust/Cargo)

This guide helps you run Floe locally from a fresh clone.
All commands below assume you are in the repository root.

## Prerequisites

- Git
- Rust stable toolchain (`rustup`)
- A working C/C++ build toolchain in your `PATH` (required by some Rust dependencies)

Quick check:

```bash
rustc --version
cargo --version
```

## Setup

1. Clone the repository and enter it.

```bash
git clone https://github.com/malon64/floe.git
cd floe
```

2. Build the workspace once to download dependencies and verify toolchain setup.

```bash
cargo build
```

## Run

- Show CLI help:

```bash
cargo run -p floe-cli -- --help
```

- Validate the example config:

```bash
cargo run -p floe-cli -- validate -c example/config.yml
```

- Run Floe with the example config:

```bash
cargo run -p floe-cli -- run -c example/config.yml
```

## Test

Run all Rust tests:

```bash
cargo test
```

## Quality checks

- Formatting check (CI style):

```bash
cargo fmt -- --check
```

- If `--check` output is hard to read locally, apply formatting directly:

```bash
cargo fmt
```

- Lints (treat warnings as errors):

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## Troubleshooting

- `cargo build` fails with linker/compiler errors:
  - Install or update your system C/C++ toolchain, then re-run `cargo build`.
- `cargo run -p floe-cli -- run -c example/config.yml` fails with file path errors:
  - Confirm you are running commands from the repository root and that `example/config.yml` exists.
- `cargo fmt -- --check` fails:
  - Run `cargo fmt` to apply formatting, then run `cargo fmt -- --check` again.
- `cargo clippy ... -D warnings` fails:
  - Fix the reported warnings, then re-run the same clippy command.
- Build/test behavior seems stale after branch switches:
  - Clear old artifacts with `cargo clean`, then run `cargo build` and `cargo test` again.
