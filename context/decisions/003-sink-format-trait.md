# ADR-003 — Unified SinkFormat trait

## Status
Accepted (merged PR #303)

## Context
Before this change, adding a new output sink format required touching four separate locations:

1. `io/format.rs` — `AcceptedSinkAdapter` trait with a 9-argument `write_accepted` method
2. `io/unique_seed/` — a separate `FormatSeeder` trait in a parallel module with its own match dispatch
3. `config/validate.rs` — hardcoded `if format == "delta"` and `if format == "iceberg"` capability checks
4. `AcceptedWriteOutput` — 7 flat `Option<String>` fields for catalog metadata (`iceberg_catalog_name`, `iceberg_database`, `delta_catalog_name`, etc.) that every new catalog multiplied

## Decision
Consolidate into a single **`SinkFormat` trait** (`io/write/sink_format.rs`) and a **`CatalogRegistration` enum** (`io/format.rs`).

```rust
pub trait SinkFormat: Send + Sync {
    fn format_name(&self) -> &'static str;
    fn supported_modes(&self) -> &'static [WriteMode];
    fn supported_storages(&self) -> &'static [&'static str];
    fn write(&self, req: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput>;
    fn seed_unique_tracker(&self, tracker, ctx) -> FloeResult<()> { Ok(()) }
}

pub static SINK_FORMATS: &[&dyn SinkFormat] = &[
    &DELTA_SINK_FORMAT, &PARQUET_SINK_FORMAT, &ICEBERG_SINK_FORMAT,
];
```

Config validation is now data-driven: `validate_sink_write_mode` calls `fmt.supported_modes().contains(...)` and `validate_sink_storage_compat` calls `fmt.supported_storages().contains(...)`.

`CatalogRegistration` replaces 7 flat fields with typed variants:
```rust
pub enum CatalogRegistration {
    UnityDelta { catalog_name, schema, table },
    IcebergGlue { catalog_name, database, namespace, table },
    IcebergRest { catalog_name, namespace, table },
}
```

## Rationale

- **Single location per format.** Each format file (`delta.rs`, `parquet.rs`, `iceberg.rs`) now owns its write logic AND its unique-seeding logic. No parallel modules to keep in sync.
- **Adding a format = one file + one line.** Creating a new format requires implementing `SinkFormat` in a new file and adding `&MySinkFormat` to `SINK_FORMATS`. No changes to `validate.rs`, `unique_seed/`, or `run/`.
- **Typed catalog metadata.** `CatalogRegistration` makes it impossible to accidentally access an Iceberg field on a Delta write. Adding a new catalog type is a new enum variant — exhaustive match catches missed cases at compile time.

## Trade-offs

- `SinkFormat` must be `pub` (not `pub(crate)`) because it appears in the `Runtime` trait's return type, which is `pub`. This leaks the abstraction slightly but is unavoidable without an additional wrapper type.
- The static `SINK_FORMATS` slice requires that all formats are known at compile time. A dynamic plugin system (dlopen) would need a different registration mechanism.

## Consequences
`io/unique_seed/delta.rs`, `parquet.rs`, `iceberg.rs` were deleted — their logic moved into the format files. `AcceptedSinkAdapter` trait was deleted. `FormatSeeder` trait was deleted.
