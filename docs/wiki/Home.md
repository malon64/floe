# Floe Wiki

Floe is a YAML-driven technical ingestion tool. It reads CSV/JSON/Parquet, applies a schema and quality checks, and writes accepted data (Parquet or Delta) plus rejected rows and reports.

This wiki is a practical guide for using Floe with the CLI or Docker. It complements the in-repo docs (which remain the source of truth for versioned behavior).

## Quick start

1. Install the CLI (Homebrew or Cargo) or use Docker.
2. Start from the minimal animal config in `docs/wiki/Configuration.md`.
3. Move to the full config example for advanced features.

## Pages

- Installation
- CLI Usage
- Configuration
- Full Config Example
- Write Modes and Outputs
- Docker Usage
- Dagster Integration
- Examples
- Troubleshooting
- FAQ

## In-repo references

- `docs/installation.md`
- `docs/cli.md`
- `docs/config.md`
- `docs/how-it-works.md`
- `docs/checks.md`
- `docs/logging.md`
- `docs/report.md`
- `docs/support-matrix.md`

If you need a missing guide, open an issue or add a page in this wiki folder.
