# Changelog

All notable changes to Floe are documented in this file.

## v0.3.6

- Environment profile support (`floe run --profile`):
  - new `--profile <path>` flag on `floe run` to inject environment-specific variables into `{{VAR}}` placeholders in the config at run time
  - profile variables support `${VAR}` cross-references (e.g. `OUT: ${BASE}/data`) resolved before config substitution
  - variable precedence chain: `env.vars` in config > `env.file` > profile variables
  - `env.file` values are included when resolving `${VAR}` references inside profile variables so all config-level overrides are respected
  - validation and run context use the same resolved variable map (no mismatch between what is validated and what executes)
  - `validate_profile` rejects syntactically malformed placeholders (`${}`, unclosed `${`) while allowing valid `${KEY}` cross-references
  - updated `profile.schema.yaml` with correct runner types (`local`, `kubernetes_job`, `databricks_job`) and accurate variable description
- Codebase cleanup:
  - extracted shared `write_temp_config` test helper into `tests/unit/common.rs` (removed four identical per-file copies)
  - consolidated duplicated observer dispatch in `errors::emit` / `warnings::emit` into a shared `log::emit_log`
  - removed 8 redundant unit tests (trivial version-string variants, cycle-length duplicates, cross-file duplicates)
- Documentation:
  - rewrote README with architecture diagram, 4-stage pipeline table, `--profile` usage example, and orchestration section
  - bumped `floe-core` and `floe-cli` to `0.3.6`


- Incremental ingestion release finalization:
  - documented `incremental_mode: file`, entity `state.path`, and the `floe state inspect/reset` CLI flow
  - refreshed the repository example config to show file-based incremental ingestion state explicitly
  - clarified incremental planning and state-commit behavior in the docs summary/reference set
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.5`

## v0.3.4

- Databricks runner foundation for orchestrator connectors:
  - added `databricks_job` runner contract support in Floe profile/manifest flow
  - added connector-side Databricks job client flow for Airflow and Dagster
  - standardized Databricks run status/result mapping with backend metadata and failure reason handling
  - added mocked integration coverage for success, failure, timeout, canceled, and infra failure paths
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.4`
  - bumped `airflow-floe` and `dagster-floe` Python packages to `0.1.4`

## v0.3.3

- Runner contract and architecture cleanup:
  - standardized Kubernetes runner naming to `kubernetes_job` across core/CLI/orchestrator-facing manifest surfaces
  - removed legacy runner alias handling for `kubernetes`
  - removed legacy Airflow manifest bridge from `floe.plan.v1` to `floe.manifest.v1` (manifest-v1 only)
- Connector reliability/refactor:
  - improved Kubernetes status normalization and failure-reason surfacing for orchestrator runner execution paths
  - reduced Airflow/Dagster drift in k8 runner handling and metadata surfaces
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.3`
  - bumped `airflow-floe` and `dagster-floe` Python packages to `0.1.3`

## v0.3.2

- Delta schema evolution rollout:
  - added additive schema evolution support for Delta accepted writes across `append`, `overwrite`, `merge_scd1`, and `merge_scd2`
  - enforced rollout guardrails: additive-only changes, partitioned-table limitations, and merge-key protection against schema-evolved columns
  - added explicit observability via the entity report `schema_evolution` block and the `schema_evolution_applied` lifecycle event

## v0.3.1

- Delta merge write modes:
  - added `merge_scd1` support for keyed upserts into accepted Delta outputs
  - added `merge_scd2` support for history-preserving merges with managed current/validity columns
- Schema/config validation:
  - added `schema.primary_key` as the explicit merge key surface, including validation for required, known, unique, non-nullable key columns
- Merge/reporting refinements:
  - added configurable Delta merge options and SCD2 closed/unchanged reporting metrics

## v0.3.0

- Core performance and observability:
  - added internal phase timing instrumentation (gated by `FLOE_PERF_PHASE_TIMINGS`) for run- and entity-level phases
  - reduced CSV read overhead by reusing prechecked input columns in the read path
  - optimized JSON `read_parse` path by reducing intermediate row allocations during selector extraction.
- Core refactoring (no intended behavior change):
  - split `run/entity` validation + split/reject flow into dedicated modules
  - split accepted write phase/report-state plumbing out of `run/entity/mod.rs`
  - further modularized writer internals (Delta/Iceberg) and report plumbing for maintainability.
- Documentation:
  - refreshed v0.3 release communication draft and docs updates for the new core organization/perf instrumentation.

## v0.2.8

- Iceberg accepted sink:
  - runtime execution of `sink.accepted.partition_spec` for supported transforms (`identity`, `year`, `month`, `day`, `hour`)
  - write-time file sizing metrics in entity reports (`files_written`, `total_bytes_written`, `avg_file_size_mb`, `small_files_count`)
- Delta accepted sink:
  - exact remote commit-log write metrics (S3/GCS/ADLS) collected via object store commit entry parsing (best-effort fallback keeps metrics nullable)
- `dagster-floe` connector package version bumped to `0.1.2` in the same release prep.

## v0.2.7

- Added Iceberg S3 accepted sink support (filesystem catalog, no data catalog / Glue yet).
- Improved `floe add-entity` bootstrap UX:
  - create a new config when target `-c` file does not exist
  - infer default entity name from input filename stem when `--name` is omitted
  - infer default format from input file extension when `--format` is omitted.
- Hardened archive mode safety:
  - archive only resolved/processed inputs
  - collision-safe archive behavior (no silent overwrite on repeated filenames).

## v0.2.6

- Added local Iceberg accepted sink MVP (`sink.accepted.format: iceberg`) with filesystem table layout and append/overwrite snapshot behavior.
- Added cloud source glob pattern support (e.g. `s3://.../sales_*.csv`) with prefix-derived listing + glob filtering.
- Added `floe add-entity` CLI command to infer a schema from an input file and append an entity definition to a Floe config.

## v0.2.5

- Added `floe manifest generate` as the common manifest surface for orchestrators (`floe.manifest.v1`).
- Airflow connector updates:
  - manifest-first operator flow (`FloeRunOperator` / `FloeManifestHook`)
  - multi-manifest DAG registration (`FLOE_MANIFEST_DIR`, one DAG per manifest)
  - streamed Floe stdout/stderr in task logs
  - enriched asset event metadata with per-entity report references.
- Dagster connector updates:
  - migration to common manifest contract
  - strict manifest schema validation
  - multi-manifest loading with one Dagster job per manifest and collision guards.
- CLI cleanup:
  - removed `floe validate --output json` (orchestrator automation now relies on generated manifests).
- Documentation updates for local orchestrator workflows and manifest-driven integration specs.

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
