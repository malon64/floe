# Installation

Floe ships as a single CLI binary (`floe`). Choose the method that matches your OS.

## macOS / Linux — Homebrew (recommended)

If you don't have Homebrew yet, install it from [brew.sh](https://brew.sh).

```bash
brew tap malon64/floe
brew install floe
floe --version
```

## Windows — Scoop (recommended)

If you don't have Scoop yet, install it from [scoop.sh](https://scoop.sh).

```powershell
scoop bucket add floe https://github.com/malon64/scoop-floe
scoop install floe
floe --version
```

## Prebuilt binary (all platforms)

Download the archive for your platform from [GitHub Releases](https://github.com/malon64/floe/releases),
extract it, and place the `floe` (or `floe.exe`) binary somewhere on your `PATH`.

| Platform | Archive |
|---|---|
| macOS arm64 (Apple Silicon) | `floe-vX.Y.Z-aarch64-apple-darwin.tar.gz` |
| macOS x86_64 | `floe-vX.Y.Z-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 | `floe-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz` |
| Linux arm64 | `floe-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `floe-vX.Y.Z-x86_64-pc-windows-msvc.zip` |

## Cargo (from source)

Prereqs:
- Rust toolchain (stable) — install from [rustup.rs](https://rustup.rs)
- A C/C++ compiler available in `PATH`

```bash
cargo install floe-cli
floe --version
```

To build from a local checkout instead:

```bash
cargo build --release
./target/release/floe --version
```

## Docker (GHCR)

Floe is published as a multi-arch Docker image (linux/amd64 and linux/arm64).

```bash
docker pull ghcr.io/malon64/floe:latest
```

Run (mount the current directory to `/work`):

```bash
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/example/config.yml
```

Cloud credentials should be provided via environment variables or runtime identity — not baked into the image.

## Troubleshooting

- **`brew` not found**: install Homebrew from [brew.sh](https://brew.sh), then retry.
- **`scoop` not found**: install Scoop from [scoop.sh](https://scoop.sh), then retry.
- **Cargo compilation fails**: ensure you have a recent stable Rust toolchain (`rustup update stable`) and a system C compiler installed.
