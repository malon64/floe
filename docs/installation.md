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

## Docker (GHCR)

Floe is published as a Docker image to GitHub Container Registry (GHCR).

Pull:

```bash
docker pull ghcr.io/malon64/floe:latest
```

Run (mount the current directory to `/work`):

```bash
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/example/config.yml
```

Notes:
- All CLI arguments are identical to local usage.
- Cloud credentials should be provided via environment variables or runtime identity.

## Troubleshooting

- If `brew` is unavailable, use the Cargo path above.
- If compilation fails, ensure you have a recent Rust toolchain and a system C
  compiler installed.
