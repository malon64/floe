# Floe — Architecture

## Crate layout

```
floe/
├── crates/
│   ├── floe-core/     ← library crate — all business logic
│   └── floe-cli/      ← binary crate — thin CLI wrapper (clap)
└── orchestrators/
    ├── dagster-floe/  ← Dagster integration (Python)
    └── airflow-floe/  ← Airflow integration (Python)
```

`floe-cli` depends on `floe-core` and does nothing except parse CLI arguments and call into the library. All testable logic lives in `floe-core`.

## Key modules in `floe-core/src/`

```
src/
├── config/            ← YAML parsing, validation, variable resolution, profiles
├── run/               ← pipeline orchestration (entity loop, file loop, output routing)
│   └── entity/        ← per-entity execution: resolve → check → write
├── io/
│   ├── storage/       ← cloud + local storage backends (S3, GCS, ADLS, local)
│   │   └── providers/ ← one file per backend implementing StorageClient trait
│   ├── read/          ← format readers (CSV, JSON, Parquet, ORC, Avro, XLSX, XML, FWF)
│   └── write/         ← format writers and sink dispatch
│       ├── sink_format.rs  ← unified SinkFormat trait + static registry
│       ├── delta.rs        ← Delta Lake sink (also seeds unique tracker)
│       ├── parquet.rs      ← Parquet sink
│       └── iceberg.rs      ← Iceberg sink (Glue + REST paths)
├── check/             ← schema checks, null checks, unique constraint engine
├── state/             ← incremental state management (CAS-based claim/promote/release)
├── report/            ← run report structs + JSON serialisation
├── pii/               ← PII masking strategies (hash, redact, partial, etc.)
├── lineage/           ← OpenLineage observer with circuit breaker
└── errors.rs          ← error type definitions
```

## Data flow per entity

```
1. Resolve inputs
   config.source.path → storage list → filter by incremental state → InputFile list

2. File-level checks (per file)
   read raw → check schema structure / headers / format
   → reject_file: route all rows to rejected sink, continue to next file

3. Row-level checks (per file)
   read typed (cast) → apply not_null checks → accumulate errors per row
   → rows with errors: add __floe_row_index + __floe_errors columns → rejected sink
   → rows without errors: collected into accepted DataFrame

4. Entity-level checks (once, across all files)
   unique constraint check: accepted rows + existing accepted data (seeded from sink)
   → duplicates: routed to rejected sink (or warned, depending on severity)

5. Write outputs
   accepted DataFrame → SinkFormat::write() → Delta / Parquet / Iceberg
   rejected DataFrame → RejectedSinkAdapter::write() → CSV / Parquet
   error report → JSON file alongside rejected output

6. Promote incremental state
   claimed URIs → files map in state.json (CAS write)
```

## Key abstractions

### `SinkFormat` trait (`io/write/sink_format.rs`)

Unified trait implemented by `DeltaSinkFormat`, `ParquetSinkFormat`, `IcebergSinkFormat`. Each format lives in one file and implements:
- `format_name()` — string identifier
- `supported_modes()` — which write modes are valid
- `supported_storages()` — which storage backends are compatible
- `write(req)` — perform the write, return `AcceptedWriteOutput`
- `seed_unique_tracker(tracker, ctx)` — load existing accepted data for uniqueness seeding

A static `SINK_FORMATS` registry replaces all match-on-format-string dispatch. Adding a new format = implement the trait + add one line to the registry.

### `StorageClient` trait (`io/storage/mod.rs`)

Implemented by `S3Client`, `GcsClient`, `AdlsClient`, `LocalClient`. All storage operations (list, download, upload, conditional read/write/delete) go through this interface. Cloud clients are created lazily and cached in `CloudClient`.

### `CatalogRegistration` enum (`io/format.rs`)

Typed representation of catalog metadata returned from a write:
- `UnityDelta { catalog_name, schema, table }`
- `IcebergGlue { catalog_name, database, namespace, table }`
- `IcebergRest { catalog_name, namespace, table }`

Replaces 7 flat `Option<String>` fields that would otherwise multiply with every new catalog.

### Incremental state (`state/mod.rs`)

CAS (compare-and-swap) based locking over `state.json` in cloud or local storage. A claim is a time-bounded reservation (`acquired_at` + `expires_at`, TTL 1 hour). The `ClaimHeartbeat` background thread renews expiry every TTL/3 seconds. On process exit (normal or panic), `PendingEntityState::drop()` releases claims automatically.

## Config validation

`config/validate.rs` validates entity configs before any I/O. Write mode and storage compat checks are data-driven: they call `SinkFormat::supported_modes()` and `SinkFormat::supported_storages()` instead of hardcoding format names. Adding a new format with restricted capabilities requires no changes to validate.rs.

## Error handling

All fallible functions return `FloeResult<T>` = `Result<T, Box<dyn std::error::Error + Send + Sync>>`. Errors are boxed at the call site and propagated upward. The top-level CLI runner catches and formats them. There are no panics in production paths.
