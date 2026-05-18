# Floe — Roadmap

This is inferred from recent commit history, open branches, code TODOs, and the direction of recent refactors. It is not a formal commitment.

## Near-term (in progress or next)

### Unity Catalog hardening
Branch `feat/unity-catalog-hardening` is active. The HTTP token exchange for Databricks Unity Catalog delta registration has been improved; more test coverage for the full OAuth → token → registration flow is being added.

### Multi-format rejected output
Currently rejected rows can be written as CSV or Parquet. There is existing infrastructure (`RejectedSinkAdapter`) for adding more formats. NDJSON is a natural next rejected format for teams that prefer it over CSV.

### ADLS Iceberg support
Iceberg on ADLS is explicitly excluded in `IcebergSinkFormat::supported_storages()`. Unblocking it requires validating the object-store path handling for `abfs://` URIs in the Iceberg writer.

### Schema evolution for Parquet sink
Delta and Iceberg both support schema evolution (adding columns to existing tables). The Parquet sink returns `schema_evolution.enabled: false`. Adding evolution for Parquet append mode would make the three sinks behaviorally consistent.

## Medium-term

### Parallel entity execution
Entities within a run are currently processed sequentially. A `rayon`-based or `tokio`-based parallel executor could reduce wall-clock time for configs with many independent entities. The main constraint is mutable `CloudClient` state — clients would need to be per-thread or wrapped in `Arc<Mutex<...>>`.

### More input formats
The input format reader (implementing `ReadFormat`) is extensible. Delta and Iceberg as input formats (reading from an existing table) would enable incremental extraction patterns, not just ingestion.

### Write-back / upsert for Parquet
Delta supports `MergeScd1` and `MergeScd2` write modes. Parquet does not. Adding a merge/upsert mode for Parquet (rewrite affected part files) would close a gap for teams that don't want Delta's transaction log overhead.

### Improved CLI output
The CLI currently prints a minimal summary. A structured, coloured table output (similar to `dbt run`) showing per-entity accepted/rejected/total rows in real time would improve operator experience.

## Architectural direction

The `SinkFormat` trait refactor (PR #303) established the pattern for adding formats without touching dispatch code. Future sink formats (`Lance`, `DuckDB`, `ClickHouse`) should implement `SinkFormat` and add one line to `SINK_FORMATS`. No changes to `validate.rs`, `unique_seed/`, or `run/`.

The storage `StorageClient` trait is similarly extensible. A new backend is one file implementing the trait + one match arm in `CloudClient::client_for_context`.
