# Changelog

All notable changes to Floe are documented in this file.

## v0.2.4

- Added new source input formats: `tsv`, `xlsx`, `fixed`, `orc`, `avro`, and `xml`.
- Added nested JSON selector extraction from `schema.columns[].source` with top-level-only mismatch checks.
- Added source-column mapping behavior (`source` as input selector, `name` as output column) in validation and run surfaces.
- Added XML selector support (`.` or `/` paths and terminal `@attribute`) with required `source.options.row_tag`.
- Dry-run now resolves inputs before execution, shows resolved file lists (with capped preview), and uses the same resolved plan as real runs.
- Refactored runtime/storage boundaries to reduce format- and storage-specific coupling in run orchestration.

## v0.2.3

- Added append/overwrite write modes for accepted parquet and delta outputs.
- Rejected outputs now use dataset-style part files with write modes.
- Append mode now seeds unique checks from existing accepted data (parquet + delta).
- Part naming improvements:
  - UUID parts for append
  - sequential parts for overwrite
  - unified part listing/cleanup helpers.
- Delta append fixes:
  - enforce schema nullability correctly
  - respect `normalize_columns` schema names.
- Dagster Docker runner now writes outputs correctly.

## v0.2.2

- Added Dagster orchestrator connector (`orchestrators/dagster-floe`) with:
  - validate plan ingestion
  - NDJSON run event parsing
  - local and Docker runner integration examples/tests.
- Introduced accepted write-mode scaffolding and design groundwork:
  - `WriteMode` in config model (default overwrite behavior retained)
  - accepted writer abstraction/module split for future append support
  - design note: `docs/design/write_modes.md`.
- Release workflow hardening:
  - build Linux AMD64 artifacts with `cross` to improve glibc compatibility.

## v0.2.1

- Added official Docker image packaging and GHCR publishing on version tags.
- Documentation: Docker usage added to installation guide and summary.
- Added machine-readable run event logs (json/text) behind `--log-format`.
- Fixes: `metadata.project` optional, CSV empty fields treated as null by default, default `cast_mode=strict`.

## v0.2.0

- Cloud storage registry with local/S3/ADLS/GCS support (canonical URIs).
- Cloud IO for CSV/JSON/Parquet inputs via temp download + local read.
- Accepted outputs: parquet (multipart with `max_size_per_file`) and delta (transactional object_store).
- Reports and rejected outputs can be written to cloud via temp upload.
- Schema mismatch policy (missing/extra columns) with file-level rejection handling.
- Deterministic run pipeline (file precheck → row validation → entity-level unique).
- JSON input modes: array (default) and NDJSON.
- Config templating (`env` + `domains`) and improved validation errors.

## v0.1.7

- Added ADLS and GCS storage definitions with URI resolution and validation.
- Implemented ADLS and GCS storage clients (list/download/upload) for file IO.
- Added Delta transactional writes via object_store for S3, ADLS, and GCS.
- Parquet accepted output now supports multipart chunking via `max_size_per_file`.
- Added config templating (`env` + `domains`) and duplicate domain validation.
- Refactored storage interface and reorganized test binaries/modules for faster CI.
- CI: path-based filters, shared rust-cache across branches, and controlled test threading.

## v0.1.6

- Added JSON array input mode alongside NDJSON, with config validation updates.
- Added entity-level accepted output with part files and cleared stale S3 outputs.
- Enforced validation order (file → row → entity) and fixed unique checks by rechunking.
- Expanded and reorganized core test suites (grouped modules) and improved CI test caching.

## v0.1.5

- Refactored storage configuration and IO to use a shared storage client abstraction.
- Split the entity runner into focused modules and centralized warning handling.
- Added configurable row error formatting (json/csv/text) in reports.
- Unified input resolution logic and reorganized run/config tests.
- Improved warn-mode performance and added lazy scan paths for CSV/Parquet.
- Added benchmark harness docs/scripts and example reports for v0.1.5.

## v0.1.4

- Added delta accepted sink support with overwrite semantics (local storage only).
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
