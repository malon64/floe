# Installation

Floe ships as a CLI (`floe`). The recommended install is Homebrew. If Homebrew is unavailable, you can install from source with Cargo. Docker is also supported.

## Homebrew (recommended)

```bash
brew tap malon64/floe
brew install floe
floe --version
```

## Cargo (from source)

Prereqs:
- Rust toolchain (stable)
- A C/C++ toolchain in `PATH`

Install:

```bash
cargo install floe-cli
floe --version
```

Build from a local checkout:

```bash
cargo build --release
./target/release/floe --version
```

## Docker (GHCR)

Pull the latest image:

```bash
docker pull ghcr.io/malon64/floe:latest
```

Run (mount the current directory to `/work`):

```bash
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/example/config.yml
```

Notes:
- CLI arguments are identical to local usage.
- Provide cloud credentials via environment variables or runtime identity.

## Verification

```bash
floe --version
floe validate -c example/config.yml
```
