# ADR-001 — Polars as the DataFrame engine

## Status
Accepted

## Context
Floe needs to read, validate, and write structured data in multiple formats (CSV, JSON, Parquet, etc.). A DataFrame engine is required to apply column-level type casting, null checks, and unique constraint evaluation across rows.

The main candidates were Apache Arrow (raw), Polars, and DataFusion.

## Decision
Use **Polars** as the single DataFrame engine throughout `floe-core`.

## Rationale

- **Zero JVM.** Polars is a Rust-native library. No JVM startup, no GC pauses, no classpath management. This is non-negotiable for a tool that targets sub-second startup.
- **Arrow-native.** Polars uses Apache Arrow as its memory format, which means zero-copy interop with the `object_store` and `deltalake` crates that also operate on Arrow arrays.
- **Rich format support.** Polars has first-class readers for CSV, JSON, Parquet, ORC, Avro, and IPC. Floe wraps these readers with its own validation layer rather than re-implementing format parsing.
- **LazyFrame API.** The `LazyFrame` API enables predicate pushdown when scanning Parquet/Delta for unique-constraint seeding — only the columns needed for the check are read.

## Trade-offs

- Polars has an opinionated API that occasionally changes across minor versions. Pinning the version carefully and testing on upgrade is required.
- DataFusion would provide SQL-level query capability, which Floe does not need. Polars is simpler for column-oriented validation.

## Consequences
All data flowing through the pipeline is represented as `polars::DataFrame` or `polars::LazyFrame`. Format readers return `DataFrame`. Format writers accept `&mut DataFrame`. Checks operate on `DataFrame` columns.
