# Floe Documentation Summary

This page is the entry point to Floe documentation. It groups the most
important references so you can quickly find the right guide.

## Quick start

- Install and run: see the repository README for a minimal example.
- Guided onboarding and examples: [GitHub Wiki](https://github.com/malon64/floe/wiki).
- CLI usage: [docs/cli.md](cli.md)
- Installation options (Homebrew, Cargo, Docker): [docs/installation.md](installation.md)

## Core concepts

- How a run works (file → row → entity checks): [docs/how-it-works.md](how-it-works.md)
- Checks and policy behavior: [docs/checks.md](checks.md)
- Run reports and JSON schema: [docs/report.md](report.md)
- Logging for orchestrators (`--log-format`): [docs/logging.md](logging.md)

## Configuration

- Full config reference (all keys + defaults): [docs/config.md](config.md)
- Storage registry and cloud paths:
  - S3: [docs/storages/s3.md](storages/s3.md)
  - ADLS: [docs/storages/adls.md](storages/adls.md)
  - GCS: [docs/storages/gcs.md](storages/gcs.md)

## Formats & sinks

- Supported inputs/outputs across storages: [docs/support-matrix.md](support-matrix.md)
- Parquet/Delta sink details:
  - Delta sink: [docs/sinks/delta.md](sinks/delta.md)
  - Sink options (parquet settings): [docs/sinks/options.md](sinks/options.md)
  - Iceberg (not implemented): [docs/sinks/iceberg.md](sinks/iceberg.md)

## Benchmarking & development

- Bench setup and results: [docs/benchmarking.md](benchmarking.md)

## Notes

- Recent additions:
  - New input formats: TSV, XLSX, fixed-width, ORC, Avro, and XML.
  - `schema.columns[].source` now supports nested JSON selectors and XML selectors.
  - Dry-run now resolves inputs ahead of execution and previews resolved files.
- Cloud IO for CSV/JSON/Parquet uses temp download/upload (file-level IO).
- Delta writes to cloud use transactional object_store (no temp upload).
- Reports can be written to cloud storages via temp upload.

If you are missing a document or a section feels out of date, please open an
issue or PR so we can keep this summary aligned with current behavior.
