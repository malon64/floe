# Changelog

All notable changes to Floe are documented in this file.

## v0.1.4

- Added delta accepted sink support with overwrite semantics (local filesystem only).
- Added iceberg accepted sink configuration stub with a clear not-implemented error.
- Added `sink.accepted.options` for parquet compression and row group sizing.

## v0.1.3

- Added a format registry with adapter traits for inputs and sinks.
- Centralized file extension handling for local/S3 resolution.
- Expanded input format scaffolding (Parquet/JSON readers) and IO dispatch.
- Run pipeline refactor to use format adapters and shared IO helpers.

## v0.1.2

- Config validation now rejects unknown fields and adds broader negative test coverage.
- CLI and report examples use build-time version strings (no hardcoded 0.1.0).
- Release pipeline hardening: portable version bumping, allow-dirty publish, and tap checkout fixes.

## v0.1.1

- Global report layout with run-scoped directories and per-entity reports.
- CI/CD release pipeline (crate publish, binaries, checksums, GitHub Release, Homebrew tap).
- CLI output improvements and documentation refreshes for reports/checks.

## v0.1.0

- Initial CLI (`validate`, `run`) and YAML configuration schema.
- CSV ingestion with Parquet accepted output and CSV rejected output.
- Core checks: casting, not-null, unique; row-level rejection and run reports.
- Multi-entity support with run-scoped reporting.
