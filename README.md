# Floe

Floe is a YAML-driven technical ingestion tool for single-node, medium-sized datasets. It targets data engineering use-cases where raw files (initially CSV/Parquet) are ingested into a clean, typed dataset with simple technical rules:

- Schema enforcement (types + nullable)
- Basic data quality checks (not null, uniqueness)
- Quarantine/rejection strategy (row-level by default)
- Run report (JSON) with counts and top violations

Floe is intentionally not a distributed engine and is not meant to replace Spark. This repository is a learning project in Rust; no ingestion or validation logic is implemented yet.

## Scope for v0.1

- Parse a YAML config describing source, sink, schema, and policy
- Run a local CSV ingestion pipeline with schema enforcement and basic checks
- Write accepted/rejected outputs and a JSON run report
- Provide a minimal CLI with `validate` and `run`

## Out of scope for v0.1

- Distributed execution, scheduling, or orchestration
- Advanced rule engines or custom UDFs
- Incremental state management beyond a single run
- Polars integration details (planned later)

## Repository layout

- `crates/floe-core/`: core library crate (placeholder)
- `crates/floe-cli/`: CLI crate (placeholder)
- `docs/`: design notes, spec, and CLI contract
- `example/`: sample configs and input data

## Quick start

No application logic is implemented yet. Use this repo to study the layout and write the Rust code yourself.

- Config example: `example/config.yml`
- Minimal config: `example/config_minimal.yml`
- Sample inputs: `example/in/customer/`

## License

MIT
