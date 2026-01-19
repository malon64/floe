# Floe

Floe is a YAML-driven technical ingestion tool for single-node, medium-sized datasets. It targets data engineering workflows where raw files (initially CSV) are ingested into a clean, typed dataset with simple technical rules:

- Schema enforcement (types + nullable)
- Data quality checks (not_null, unique, cast mismatch)
- Row-level rejection by default
- Run report (JSON) with counts and aggregated violations

Floe is intentionally not a distributed engine and is not meant to replace Spark. This repository is a learning project in Rust, with a working core pipeline that is intentionally small and readable.

## What you can do today

- Validate configs with `floe validate`
- Run local CSV ingestion with `floe run`
- Emit accepted/rejected outputs
- Generate per-entity run reports

## Non-goals (for now)

- Distributed execution or orchestration
- Advanced rule engines or UDFs
- Multi-format IO beyond CSV input + parquet output
- Incremental state beyond a single run

## Repository layout

- `crates/floe-core/`: core library (config parsing, checks, IO, reporting)
- `crates/floe-cli/`: CLI interface
- `docs/`: docs for checks, reports, CLI, and features
- `example/`: sample configs and input data

## Quick start

```bash
floe validate -c example/config.yml --entities customer
floe run -c example/config.yml --entities customer
```

## Docs

- Checks: `docs/checks.md`
- Reports: `docs/report.md`
- CLI: `docs/cli.md`
- Features: `docs/features.md`
- Docs index: see the list above for the current, version-agnostic docs set.

## License

MIT
