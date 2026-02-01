# Installation

Floe ships as a CLI (`floe`). The recommended install method is Homebrew.
If Homebrew is not available, you can install from source with Cargo.

## Homebrew (recommended)

```bash
brew tap malon64/floe
brew install floe
floe --version
```

## Cargo (from source)

Prereqs:
- Rust toolchain (stable)
- A C/C++ toolchain available in `PATH`

Install:

```bash
cargo install floe-cli
floe --version
```

If you want to build from a local checkout instead:

```bash
cargo build --release
./target/release/floe --version
```

## Troubleshooting

- If `brew` is unavailable, use the Cargo path above.
- If compilation fails, ensure you have a recent Rust toolchain and a system C
  compiler installed.
