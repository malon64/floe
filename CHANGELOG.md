# Changelog

All notable changes to Floe are documented in this file.

## Unreleased

- **Stream Parquet writes through the Polars `new_streaming` engine** (completes #332):
  - The accepted Parquet writer in `io/write/parquet.rs::write_parquet_to_path` now calls `LazyFrame::sink_parquet(...).with_new_streaming(true).collect()` instead of the eager `ParquetWriter::new(file).finish(&mut df)`. The outer per-chunk loop in `ParquetSinkFormat::write` (which slices the input DataFrame to keep each call ≤ `sink.accepted.options.max_size_per_file`) is unchanged, so the existing `part-NNNNN.parquet` / `part-<uuid>.parquet` naming, the `PartNameAllocator`, `small_files_count`, and the run-report shape are all preserved.
  - Within each chunk's write, the streaming engine emits one row group at a time and drops it before the next, replacing the previous "buffer the chunk plus full Arrow encoding state" footprint with "one row group + encoding buffer". Marginal versus the per-entity cap delivered earlier in this release, but real, especially on wide schemas or large `row_group_size` values.
  - Polars' `new_streaming` cargo feature is enabled on `floe-core`'s existing `polars = "0.52.0"` dependency. No version bump, no `df-interchange` churn, no Cargo.lock noise. Reader paths (`io/read/parquet.rs::ParquetInputAdapter::read_inputs`, `io/write/parquet.rs::seed_from_parquet_path`) already use `LazyFrame::scan_parquet` plans and pick up the new engine implicitly through Polars' default-collect dispatch — no code change there.
  - New integration test `streaming_parquet_writes_preserve_row_counts_and_size_bound` in `tests/integration/local_run.rs` forces ≥ 2 chunked writes through `sink_parquet` (5000 rows × `max_size_per_file: 4096`) and verifies the round-trip row count plus the per-part size bound.

- **Soft-buffered accepted writes — cap per-entity memory at `max_size_per_file`** (addresses #332):
  - Floe previously held every accepted `DataFrame` from every input file in `accepted_accum: Vec<DataFrame>` until the entire entity was processed, then concatenated and wrote once. Peak RAM scaled as `O(n_files × rows_per_file)`, capping practical batch size.
  - A new `AcceptedBuffer` flushes to the configured sink whenever the running estimated in-memory size meets `sink.accepted.options.max_size_per_file` (default 256 MB) and once at the end of the entity. Peak accepted-side memory is bounded by one configured output-file budget regardless of input fanout.
  - The first flush uses the entity's configured `write_mode` (`overwrite` or `append`); subsequent flushes within the same run are forced to `append`, mirroring the existing `rejected_overwrite_used` pattern. This preserves the existing first-flush-clears-then-appends behaviour for `overwrite` mode.
  - Catalog registration, schema evolution, and table-root reporting come from the first flush; Delta `table_version` and Iceberg `snapshot_id` track the latest commit; counters (`parts_written`, `total_bytes_written`, `small_files_count`) sum across flushes via a new `AcceptedWriteOutput::merge_in` reducer.
  - Merge modes (`merge_scd1`, `merge_scd2`) keep the previous accumulate-then-write path unchanged — they require the full per-entity dataset to compute upsert/close decisions.
  - **Behavioural note (applies to all non-merge sinks)**: because flushes commit synchronously while the file loop is still iterating, a failure on a *later* file in the same batch (read / validation / rejected-write / archive) can leave *earlier* files' accepted rows already committed — and in `overwrite` mode the first flush may already have replaced the previous dataset. The run report and incremental state are not committed in that case, so the same inputs are re-processed on the next run. For Parquet that committed prefix is non-transactional part files in the accepted directory. For Delta and Iceberg each individual flush is transactional, but a later-file failure does not roll back an earlier flush's commit, so readers can still observe a committed prefix between the failure and the next run; cross-run protection comes from `unique_keys` or merge modes, not from the sink's per-commit atomicity. Each file's archive call always runs before its rows enter the buffer. Documented under `sink.accepted.options.max_size_per_file` in `docs/config.md`.

## v0.4.5

- **Iceberg: propagate S3 endpoint and path-style-access into FileIO** (fixes #352, PR #353):
  - For S3-compatible storage (SeaweedFS, MinIO), Iceberg/OpenDAL requires an explicit `endpoint` and `path_style_access` in addition to region. Without these, OpenDAL picks up the wrong `AWS_ENDPOINT_URL` and fails at Parquet file close.
  - Added optional `endpoint` and `path_style_access` fields to `StorageDefinition`; `iceberg_store_config()` now forwards them as `s3.endpoint` and `s3.path-style-access` when present.
  - The metadata `S3Client` is also wired: `build_client()` passes `endpoint_url` and `force_path_style` from `StorageDefinition`, so listing and uniqueness-seed calls to MinIO/SeaweedFS use the correct addressing style.

- **Manifest: write remote summary and enrich `run_finished` event** (fixes #354, PR #356):
  - In manifest mode with a remote `report_base_uri`, `RunContext::from_config()` was skipping `Target` construction and the run summary was never uploaded; `run_finished.summary_uri` was always `null`.
  - `StorageResolver` gains `find_definition_name_for_uri` / `register_definition` so a synthetic storage definition is created for remote report URIs even when the bucket is not listed in the config `storages` block.
  - `RunFinished` is extended with `report_base` and `entity_report_uris` (both `skip_serializing_if`-guarded, so existing parsers see identical JSON when no report is configured).
  - `dagster-floe`: `FloeRunFinished` + `parse_run_finished` + `assets.py` consume `entity_report_uris` directly when available.
  - `airflow-floe`: `operators.py` and `kubernetes_runner.py` pass the new fields through to the XCom payload.

- **Manifest: deterministic generation, source checksums, and content revision** (fixes #355, PR #357):
  - New `--deterministic` flag: sets `generated_at_ts_ms=0` so the same inputs produce byte-identical output — clean `diff` in CI without stripping the timestamp.
  - New `--manifest-name` flag: stores a stable logical name (e.g. `sales.prod`) in the manifest JSON.
  - `config_checksum` (SHA-256) is computed on every generation; `profile_checksum` is added when a profile is supplied.
  - `manifest_revision`: SHA-256 of canonical manifest content (excluding `generated_at_ts_ms`) provides a stable content fingerprint.
  - `manifest_id` now incorporates `config_checksum` so the ID changes when source config content changes.
  - All `HashMap` fields in manifest structs (`exit_codes`, `env`, `definitions`, `tags`, `env_parameters`) replaced with `BTreeMap` for stable alphabetical key ordering in serialised JSON.
  - `dagster-floe` and `airflow-floe` manifest dataclasses parse and surface the four new optional fields (`manifest_name`, `manifest_revision`, `profile_uri`, `profile_checksum`).
  - JSON Schema updated to declare the new fields, preventing `additionalProperties: false` rejections in the Dagster manifest validator.

- **`floe manifest generate` — deployment URI overrides** (fixes #358, PR #359):
  - `-c` and `-p` now accept remote URIs (`s3://`, `gs://`, `abfs://`), downloading transparently and recording the URI as `config_uri` / `profile_uri` in the manifest.
  - `--output` accepts remote URIs; when a remote URI is given the manifest is uploaded and that URI is automatically baked into `execution.base_args` (replacing the `{manifest_uri}` placeholder), eliminating post-generation JSON patching.
  - `--default-domain <domain>` sets `domain`, `group_name`, and `asset_key` prefix for entities that do not specify a domain.
  - `--manifest-path-mode resolved-uri` sets each entity's `source.path` and sink `path` fields to the fully resolved URI (stripping `local://` for local paths), making the manifest a standalone replay contract without a local config directory.
  - Unknown `--manifest-path-mode` values now produce an immediate CLI error instead of silently defaulting.

- **`floe` 0.4.5, `dagster-floe` 0.1.8, `airflow-floe` 0.1.5**: version bumps for this release.

## v0.4.4

- **REST catalog: correct storage factory for S3 and GCS** (fixes #348 #349):
  - `build_rest_catalog()` previously always used `LocalFsStorageFactory` regardless of warehouse scheme, causing Parquet data file reads/writes to fail with OS-level errors on any cloud-backed REST catalog (Polaris, Snowflake Open Catalog, Nessie with S3).
  - The factory is now selected based on the concrete table location URI (`s3://`, `s3a://` → `OpenDalStorageFactory::S3`; `gs://` → `OpenDalStorageFactory::Gcs`; everything else → `LocalFsStorageFactory`).
  - When the warehouse field is a catalog name rather than a storage URI (e.g. Polaris `"lakehouse"`), the table location from `warehouse_location` / `table_root_uri` is used as the primary dispatch key, with `warehouse` as fallback.
  - Both write and uniqueness-seed (Append dedup) paths are fixed.

- **`floe` 0.4.4**: version bump for this release.

## v0.4.3

- **Remote manifest support for `floe run --manifest`** (`docs/cli.md`, fixes #342):
  - `floe run --manifest` now accepts `s3://`, `gs://`, and `abfs://` URIs, downloading the manifest to a temporary directory using the same `resolve_config_location` infrastructure used by `floe run --config`.

- **OpenLineage: stable job name, versioned `_producer`, column lineage, and Dagster job injection** (`docs/lineage.md`, fixes #335 #336 #337 #338):
  - Top-level `RunStarted`/`RunFinished` events now use a **stable job name** derived from the config file stem (e.g. `orders.yml` → job name `orders`) instead of the ephemeral run UUID, so run history accumulates on a single job node in Marquez.
    - An optional `lineage.job_name` field in the config overrides the derived name.
  - The `_producer` URI is now **versioned** via `env!("CARGO_PKG_VERSION")` at compile time (e.g. `https://github.com/malon64/floe/releases/tag/v0.4.3`) instead of a static string, enabling consumers to correlate lineage events with the exact release.
  - **`ColumnLineageDatasetFacet`** is emitted on accepted output datasets, mapping each output column to its source field (or itself when no `source:` rename is configured). The `inputFields` dataset identity uses the same `(namespace, name)` tuple as the input dataset in the same event, so column-level edges are correctly joined in OpenLineage consumers.
  - `dagster-floe` now injects **`DAGSTER_JOB_NAME`** into the floe subprocess environment, so the `ParentRunFacet` carries the correct Dagster job name rather than an empty string.

- **OpenLineage: Marquez-compatible dataset naming and facet placement** (`docs/lineage.md`, fixes #340):
  - Source input datasets use a `.source` sub-namespace (e.g. `myns.source`) to avoid colliding with the logical entity name used for accepted outputs.
  - `SchemaDatasetFacet`, `DataQualityMetricsOutputDatasetFacet`, and `FloeQualityRunFacet` are placed on the accepted output dataset (the write side), matching OpenLineage's semantic: output facets describe what was written.
  - Rejected output datasets use a `.rejected` sub-namespace and carry their own `DataQualityMetricsOutputDatasetFacet` with the rejected row counts.

- **`dagster-floe`: `SourceAsset` registration and enriched rejected metadata** (fixes #339):
  - Each entity now registers a companion `SourceAsset` (key `<entity_key>_source`) so Dagster's asset graph shows the upstream data source as a node. `register_source_assets=False` opts out for pipelines that manage their own upstream declarations.
  - Rejected output metadata now includes `dominant_rejection_reason` and `files_with_rejections` computed from the entity report, giving Dagster run pages actionable context without opening the floe report file.

- **`dagster-floe` 0.1.7**, **`floe` 0.4.3**: version bumps for this release.

## v0.4.2

- **OpenLineage: source and sink datasets emitted separately** (`docs/lineage.md`, fixes #317):
  - Entity `COMPLETE`/`FAIL` events now set `inputs` to the **source dataset** (named after `source.path`) and `outputs` to the **accepted sink dataset** (named after `sink.accepted.path`).
  - When a rejected sink is configured, a second output dataset (named after `sink.rejected.path`) is appended to `outputs`.
  - Schema (`SchemaDataset`), `DataQualityMetrics`, and `FloeQualityRun` facets are attached to the source input dataset, which is the correct OpenLineage placement for input facets.
  - Previously, `inputs` contained a single dataset named after the entity (not the path) and `outputs` was always empty, causing Marquez to show a dead-end lineage graph.

- **OpenLineage observer now installed from profile `lineage` block** (fixes #316):
  - `floe run --profile` now uses `load_config_with_profile_overrides` for the early config load that wires up the OpenLineage observer. Previously it used `load_config_with_profile_vars`, which substituted `{{VAR}}` placeholders but did not merge the `lineage` section from the profile. Users who declared `lineage:` only in their profile file saw no lineage events emitted.

- **Profile support for `floe state inspect` and `floe state reset`** (fixes #320):
  - Both state subcommands now accept `-p / --profile <path>`. Without a profile, configs that reference `{{VAR}}` placeholders would fail with "unknown variable" errors. The same profile-loading path used by `validate` and `run` (variable resolution + storages/catalogs/lineage merging) is now applied before reading or resetting entity state.

- **Incremental state committed for rejected terminal outcomes** (fixes #327):
  - In `incremental_mode: file`, files whose rows are fully or partially rejected under `severity: reject` are now committed to state after the run. Previously, any `RunStatus::Rejected` caused state to be released, making Floe reprocess the same file on every subsequent run and duplicate accepted/rejected sink outputs. `Aborted` and `Failed` outcomes still release state, as those represent incomplete processing where reprocessing is safe.

- **`dagster-floe` 0.1.6**: version bump accompanying the floe 0.4.2 release.

## v0.4.1

- **Profile `storages` and `lineage` overrides** (`docs/profiles.md`):
  - Profile files can now declare a `storages` section and a `lineage` section alongside the existing `catalogs` override. Both are applied as wholesale config replacements before validation and run, following the same pattern as `catalogs`.
  - `floe validate --profile`, `floe run --profile`, and `floe manifest generate --profile` all honour the new sections.
  - Python bindings (`floe.validate`) pass storages and lineage through to the merged config.
  - Profile validation now enforces the same semantic constraints as config validation:
    - `storages.default` is required whenever `storages` is declared.
    - Storage definitions must use a supported type (`local`, `s3`, `adls`, `gcs`) and supply type-required fields (`s3`: bucket + region; `adls`: account + container; `gcs`: bucket).
    - `lineage.namespace` must not be empty; `lineage.max_failures` must be ≥ 1 when set.

## v0.4.0

- **OpenLineage circuit breaker and retry** (`docs/lineage.md`):
  - Transient failures (connection errors, HTTP 5xx, 429) are retried up to 3 times with 0 / 100 / 500 ms backoff before counting as a failure.
  - Non-retryable 4xx responses (e.g. 401 bad API key) are counted immediately without retrying and emit a `lineage_http_error` warning.
  - After `max_failures` consecutive failures (default 3, configurable via `lineage.max_failures`), the circuit opens and all subsequent events in the run are skipped with no HTTP calls.
  - A single `lineage_circuit_open` warning is emitted when the circuit trips. The circuit resets at `RunStarted` so a recovered endpoint is retried in the next pipeline execution without restarting the process.
  - A `lineage_http_error` warning is emitted whenever any event is dropped so silent event loss is always visible in operator logs.

- **Hardened remote incremental state** (`docs/how-it-works.md`):
  - State schema bumped to v2 — a `claims` map sits alongside the existing `files` map, enabling CAS-based concurrent-writer safety on S3, GCS, and ADLS via conditional writes (`If-Match` / generation numbers).
  - Local storage uses `FileLock` (O_CREAT|O_EXCL) to serialize version-check + write, preventing two concurrent local processes from both returning `Written`.
  - Claim scope is restricted to the owning runner: promotion, release, and renewal only touch URIs held by the current `ClaimedEntityState`, preventing cross-runner interference when multiple processes share a `run_id`.
  - Unexpired claims from the same `run_id` now block re-claiming — two processes sharing a `run_id` can no longer process the same files concurrently.
  - Deletes remain conditional even when `expected_version` is `None`, preserving state written by a concurrent runner.
  - V1 state files (no `claims` field) are silently upgraded to V2 on read.

- **Kubernetes manifest runner fields** (`context/orchestrators.md`):
  - `floe manifest generate` with a Kubernetes profile now emits full runner fields in the manifest: `image`, `namespace`, `service_account`, `resources` (cpu / memory_mb), `env`, and structured `secrets`.
  - `secrets` changed from `Vec<String>` to `Vec<{name, secret_name, key}>` to match the shape expected by `kubernetes_runner.py`.
  - `dagster-floe[kubernetes]` optional extra added to express the Kubernetes client dependency explicitly.

- **Internal hardening** (no config or API changes):
  - `PolicySeverity` is now a typed enum parsed at config load time — string comparison in mismatch, precheck, and run/entity paths replaced with enum match.
  - `AcceptedWriteReportState` removed — `run_accepted_write_phase` returns `AcceptedWriteOutput` directly, eliminating a 24-field manual field-by-field copy.
  - Error types (`ConfigError`, `RunError`, `StorageError`, `IoError`) adopt `thiserror` — removes ~40 lines of boilerplate `Display + Error` impls.
  - `SinkFormat` trait centralises write, seed, and data-driven config validation — adding a new format no longer requires changes to validate.rs or dispatch code.
  - State module eliminates duplicated load / persist / claim patterns (76 fewer lines, same behaviour).

- **macOS wheels**: universal2 wheel replaces separate aarch64 + x86_64 artifacts — a single `.whl` runs natively on both Apple Silicon and Intel Macs.

- **`dagster-floe` 0.1.5**: adds `dagster-floe[kubernetes]` optional extra so consumers can declare the `kubernetes>=28` dependency explicitly when using the Kubernetes runner.

## v0.3.9

- **Column-level PII masking** (`docs/pii.md`):
  - New `pii:` entity block with per-column transform strategies: `hash` (SHA-256), `drop`, `nullify`, `redact`, and `mask`.
  - `mask` strategy supports a pattern string with `{firstN}` / `{lastN}` reveal tokens (e.g. `{first4}****{last4}` for card numbers).
  - `redact` strategy replaces every non-null value with a configurable string (default `[REDACTED]`).
  - Masking runs after schema validation and data-quality checks — rejected rows are never masked.
  - Compatible with `normalize_columns`: `pii.columns[].name` matches schema names, not post-normalization runtime names.
  - Full config-validation coverage: duplicate columns, unknown strategies, missing `mask_pattern`, and `strategy: drop` on primary-key columns are all rejected at load time.

- **OpenLineage integration** (`docs/lineage.md`):
  - New optional `lineage:` top-level config block that posts run and entity lifecycle events to any OpenLineage-compatible HTTP endpoint (Marquez, Atlan, OpenMetadata, Astronomer, etc.).
  - Emits four facets per entity `COMPLETE` event: `DataQualityMetrics`, `FloeQualityRun`, `SchemaDataset`, and `ParentRun`.
  - `ParentRun` facet auto-detected from Airflow (`AIRFLOW_CTX_*`) and Dagster (`DAGSTER_*`) environment variables.
  - Bearer auth via `api_key` field; supports `{{VAR}}` placeholder expansion and nested `${HOST}/path` variable resolution via `--profile`.
  - Configurable `timeout_secs` (default 5); fail-silent — HTTP errors emit warnings and do not affect run outcome.
  - Lineage emission is independent of `--log-format`; events are always posted when configured.
  - Lineage HTTP error warnings (401, timeouts) are always surfaced to stderr, even with `--log-format off`.
  - Lifecycle correctness: every run produces a well-formed `START → COMPLETE/FAIL` sequence; pre-execution failures (config validation, context construction) emit a paired `START` before `FAIL`; post-start errors do not re-emit `START`.

- **Profile validation and catalog hardening** (v0.3.8 patch):
  - Profile variable resolution errors now produce explicit config errors instead of silently falling back to unresolved placeholders.
  - Catalog override validation tightened for Unity and REST catalog blocks.

## v0.3.8

- Delta Lake + Unity Catalog registration:
  - new `unity` catalog type — after writing a Delta table to cloud storage, Floe
    optionally registers (or confirms) it as an EXTERNAL DELTA table in Databricks
    Unity Catalog via the REST API (`POST /api/2.1/unity-catalog/tables`).
  - existing-table safety: location mismatch and missing `storage_location`
    (managed table / view collision) are returned as explicit errors.
  - ADLS: `abfs://` URIs are normalised to `abfss://` before registration.
  - new `sink.accepted.delta` block: `catalog`, `schema`, `table` overrides.
  - `create_schema_if_missing` flag on the catalog definition.
  - Validation: `sink.accepted.delta` rejected on non-delta formats; Unity type
    rejected for Iceberg sinks; local storage rejected for Unity targets.
  - Three new report fields: `delta_catalog_name`, `delta_catalog_schema`,
    `delta_catalog_table`.
  - `token` accepts a literal PAT or a single `${ENV_VAR}` reference resolved from the OS environment at run time.

## v0.3.7

- Windows binary now distributed via [Scoop](https://github.com/malon64/scoop-floe): `scoop bucket add floe https://github.com/malon64/scoop-floe` then `scoop install floe`.
- Release pipeline extended with a `x86_64-pc-windows-msvc` build target; artifact packaged as `.zip`.
- GitHub Actions upgraded to Node.js 24 runtime across all workflows.

## v0.3.6

- Added `--profile <path>` flag to `floe run` for environment-specific variable injection into `{{VAR}}` config placeholders.
- Profile variables support `${KEY}` cross-references; precedence chain: `env.vars` > `env.file` > profile variables.
- Updated `profile.schema.yaml` with `kubernetes_job`/`databricks_job` runner types and accurate variable semantics.
- Internal cleanup: shared test helper, consolidated `emit_log`, removed 8 redundant unit tests.
- README rewrite: architecture diagram, pipeline stage table, `--profile` example.

## v0.3.5

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
