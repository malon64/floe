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

## Orchestrators & manifests

- CLI manifest generation (`floe manifest generate`): [docs/cli.md](cli.md)
- Common orchestrator manifest schema: [orchestrators/schemas/floe.manifest.v1.json](../orchestrators/schemas/floe.manifest.v1.json)
- Dagster integration package + examples: [orchestrators/dagster-floe/README.md](../orchestrators/dagster-floe/README.md)
- Airflow integration package + examples: [orchestrators/airflow-floe/README.md](../orchestrators/airflow-floe/README.md)
- Local integration dev notes: [orchestrators/LOCAL_DEV.md](../orchestrators/LOCAL_DEV.md)

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
  - Iceberg (filesystem catalog on local/S3/GCS, plus AWS Glue catalog on S3): [docs/sinks/iceberg.md](sinks/iceberg.md)

## Bootstrap & CLI productivity

- CLI usage (`validate`, `run`, `manifest generate`, `add-entity`): [docs/cli.md](cli.md)
- `floe add-entity` can bootstrap a missing config file and infer entity name/format from input path extensions (CSV/JSON/Parquet).

## Benchmarking & development

- Bench setup and results: [docs/benchmarking.md](benchmarking.md)

## Current boundaries

- Floe supports partitioned Delta (`partition_by`) and partitioned Iceberg (`partition_spec`) writes.
- Floe reports write-time metrics/metadata for accepted outputs (format-dependent), including Delta/Iceberg table versioning metadata and file sizing metrics where available.
- Floe does not perform table optimization/maintenance (for example Delta optimize/vacuum or Iceberg compaction/maintenance); run those as separate jobs.
- Iceberg schema evolution and merge/upsert workflows are out of scope in current releases.
- Target-aware uniqueness checks against existing sink tables in append mode are future work (current uniqueness checks operate within Floe's processed data scope for the run).

If you are missing a document or a section feels out of date, please open an
issue or PR so we can keep this summary aligned with current behavior.
