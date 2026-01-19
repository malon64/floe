# Release & CI/CD

This document describes the GitHub Actions workflows and the secrets required
to publish Floe.

## Workflows

- `CI` (`.github/workflows/ci.yml`)
  - Runs on PRs and pushes to `main`.
  - Executes `cargo fmt`, `cargo clippy`, and `cargo test`.

- `Release` (`.github/workflows/release.yml`)
  - Runs on version tags `vX.Y.Z`.
  - Publishes crates (`floe-core` then `floe-cli`).
  - Builds binaries for macOS (Intel + Apple Silicon) and Linux x86_64.
  - Uploads release assets to GitHub Releases.
  - Updates the Homebrew tap formula in `malon64/homebrew-floe`.

## Required secrets

Set these secrets in the GitHub repository settings:

- `CRATES_IO_TOKEN`
  - A crates.io API token with publish rights for `floe-core` and `floe-cli`.
- `HOMEBREW_TAP_TOKEN`
  - A GitHub personal access token with write access to
    `https://github.com/malon64/homebrew-floe`.
  - The built-in `GITHUB_TOKEN` cannot push to another repo unless you grant it
    explicit access. Use a PAT for reliability.

## Release flow

1. Update versions in `crates/floe-core/Cargo.toml` and `crates/floe-cli/Cargo.toml`.
2. Tag the release:
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`
3. The Release workflow publishes crates, creates GitHub Release assets, and
   updates the Homebrew formula.

For dry runs, use the manual `workflow_dispatch` with `dry_run: true` to test
builds without publishing or updating Homebrew.
