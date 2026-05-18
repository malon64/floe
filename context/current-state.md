# Floe — Current State

## Version

`0.3.10` (see `crates/floe-core/Cargo.toml`)

## What is stable and well-tested

- **CSV / JSON / Parquet / ORC / Avro ingestion** — full pipeline (resolve → check → write)
- **Parquet and Delta Lake accepted sinks** — overwrite and append write modes
- **Iceberg accepted sink** — local, S3, GCS; Glue and REST (Polaris, Nessie) catalog registration
- **Unique constraint checks** — seeded from existing accepted data, CAS-safe
- **Incremental state** — CAS-based file claim/promote/release with heartbeat renewal
- **PII masking** — hash, redact, partial mask, format-preserving strategies per column
- **OpenLineage observer** — circuit breaker + retry on lineage emission failures
- **Variable resolution** — `{{var}}` interpolation in config paths and values
- **Profiles** — per-environment config overlays
- **Run reports** — structured JSON per entity + summary

## What was recently added or changed

- **Unity Catalog / Databricks token flow hardening** (branch `feat/unity-catalog-hardening`) — improved HTTP token exchange and error handling for Unity Catalog Delta registration
- **Remote incremental state** (PR #297) — state.json can live in S3/GCS/ADLS; CAS uses ETags/generations
- **SinkFormat trait refactor** (PR #303) — unified `AcceptedSinkAdapter` + `FormatSeeder` into one trait; `CatalogRegistration` enum replaces 7 flat optional fields; data-driven config validation
- **State module refactor** (PR #304) — removed duplicated load/persist/claim patterns

## Known limitations

- **Single-node only.** No parallelism across entities within one run (entities are processed sequentially).
- **XLSX and XML inputs** are supported but have limited test coverage for edge cases.
- **ADLS conditional writes** use ETags; ADLS Gen2 does not natively support `If-Match` on all operations — workarounds are in place but less battle-tested than S3.
- **Iceberg on ADLS** is not yet supported (supported_storages returns `["local", "s3", "gcs"]`).
- **No schema evolution for Parquet sink** — schema evolution is implemented for Delta and Iceberg only.

## Active branches

| Branch | Purpose |
|---|---|
| `feat/unity-catalog-hardening` | Unity Catalog HTTP flow fixes and tests |
| `refactor/state-module` | State module deduplication (PR #304, pending merge) |

## Test suite

491 tests, all passing on main. `cargo clippy -- -D warnings` clean.
