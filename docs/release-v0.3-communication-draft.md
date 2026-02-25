# Floe v0.3 Release Communication Draft

Use/adapt this for a release note, LinkedIn post, or project update.

## Short post draft

Floe v0.3 is focused on production-readiness and operator clarity for ingestion pipelines.

Highlights:
- Partitioned writes for both Delta (`partition_by`) and Iceberg (`partition_spec`)
- Richer accepted-output report metadata, including write-time file metrics (Delta / Iceberg / Parquet)
- Iceberg support expanded with AWS Glue catalog integration (S3-backed tables)
- Better bootstrap UX with `floe add-entity` (config creation + name/format inference)
- Stronger orchestrator readiness with common manifest generation plus Dagster/Airflow integrations

Design boundary (intentional): Floe handles ingestion + validation + write-time reporting.
Table optimization/maintenance (Delta optimize/vacuum, Iceberg compaction/maintenance) stays external and should run as separate platform jobs.

## Slightly longer release summary

This release brings Floe's lakehouse sinks and orchestration workflow closer to day-2 operational use:

- Delta and Iceberg partitioning are now runtime-wired (not just config scaffolding), so teams can define partition layout directly in Floe config.
- Accepted-output reports now surface more useful write metadata:
  - data files written
  - table version / snapshot metadata (format-dependent)
  - file sizing metrics (where available)
- Delta remote metrics (S3/GCS/ADLS) are collected from the committed Delta log via object_store on a best-effort basis, with nullable fallback so write success is never downgraded by post-write metrics parsing.
- Iceberg supports filesystem-catalog semantics (local/S3/GCS) plus AWS Glue catalog registration for S3-backed tables, with documented scope/limitations.
- `floe add-entity` improves onboarding by bootstrapping missing config files and inferring entity name/format from input paths.
- Manifest generation and structured logging continue to support orchestrator integration patterns (Dagster/Airflow) with a common manifest contract.

What Floe still does not do (by design):
- table compaction/optimization/maintenance
- schema evolution workflows for Delta/Iceberg sinks
- target-aware uniqueness checks against existing sink tables in append mode

Those remain separate responsibilities for platform jobs and governance workflows.
