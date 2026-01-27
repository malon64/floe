# Refactoring Plan (Sequential, Unified)

This plan consolidates all refactoring ideas into one ordered roadmap. Each phase is designed to be reviewable and testable on its own.

---

## Phase 0 — Ground rules & targets

- Keep behavior stable; refactor without changing outputs.
- Every phase should leave the codebase compiling + tests passing.
- Add/adjust tests when new abstractions are introduced.

---

## Phase 1 — Rename “filesystem” → “storage” (config + code)

**Goal:** make naming align with actual storage clients.

- Rename config block:
  - `storages:` with `default` + `definitions`
- Replace all `*.filesystem` fields with `*.storage`:
  - `source.storage`
  - `sink.accepted.storage`
  - `sink.rejected.storage`
- Definitions describe **storage clients**:
  - `type: local | s3 | adls | gcs`
  - `bucket`, `region`, `prefix` (for s3/adls/gcs)
- Keep **bucket + prefix** semantics.
- (Optional) support backward‑compat alias `filesystems → storages` for old configs.

**Exit criteria**
- Config parses with new `storages` structure.
- Validation errors reference `storage` not `filesystem`.

---

## Phase 2 — StorageClient + Target abstraction

**Goal:** abstract I/O behind a single interface and remove S3-specific plumbing from run.

- Create `io/storage/` module:
  - `trait StorageClient { list, download, upload, delete, exists, mkdirs, move }`
  - Implementations: `LocalClient`, `S3Client`
  - Placeholders: `AdlsClient`, `GcsClient`
- Introduce `Target`:
  - `Target { client: Arc<dyn StorageClient>, path/uri, local_path? }`
  - Methods: `resolve()`, `read_to_temp()`, `write_from_temp()`, `delete()`, `mkdirs()`
- Replace raw paths in run with `InputTarget` / `OutputTarget`.
- Replace `HashMap<String, S3Client>` with **CloudClient** (global registry).

**Exit criteria**
- No S3 client references outside `io/storage/`.
- Run flow depends on `Target` + `CloudClient`.

---

## Phase 3 — Entity runner split into modules

**Goal:** reduce parameter explosion and make `run_entity` readable.

Target layout:
```
run/entity/
  mod.rs        // run_entity orchestration
  context.rs    // EntityContext builder (targets, columns, tempdir, etc.)
  resolve.rs    // resolve storage + input listing
  process.rs    // per-file processing loop
  outputs.rs    // accepted/rejected/raw/archive writes via Target
  report.rs     // build RunReport / FileReport helpers
```

Add:
- `EntityContext` to hold common data and remove long argument lists.
- `ResolvedInputs`, `FileOutcome`, `OutputPaths` value structs.

**Exit criteria**
- `run/entity.rs` becomes a small orchestration module.
- All major functions take `EntityContext` instead of 8–10 parameters.

---

## Phase 4 — Configurable RowErrorFormatter

**Goal:** allow error output format to be configurable.

- Add config field:
  - `report.formatter: json | csv | text` (default: json)
- Define trait:
  - `trait RowErrorFormatter { fn format(&[RowError]) -> String }`
- Implement:
  - `JsonFormatter` (current behavior)
  - `CsvFormatter` and/or `TextFormatter`
- Use formatter in output helpers (no hardcoded JSON).

**Exit criteria**
- Output error format selectable via config.
- Tests cover at least json + one alternative.

---

## Phase 5 — Storage resolution cleanup

**Goal:** unify resolution logic and remove S3‑specific helpers from run.

- Replace `FilesystemResolver` → `StorageResolver`.
- Resolve storage definitions into `Target`.
- Move output key building into storage‑agnostic helper (`io/storage/paths.rs`).

**Exit criteria**
- No S3 path builders used outside storage layer.
- Run code deals only with `Target`.

---

## Phase 6 — Error handling & validation alignment

**Goal:** clean error taxonomy and consistent validation.

- Replace runtime `ConfigError` usage with:
  - `RunError`, `IoError`, or `StorageError`
- Ensure validation errors always mention:
  - `entity.name=...`
  - `storage=...`
- Move all format support checks to format registry (one source of truth).
- Add `format::options_validator(format, options)` to centralize sink option checks.

**Exit criteria**
- Error types clearly identify config vs runtime failures.
- Validation and runtime errors are consistent.

---

## Phase 7 — Naming & warnings cleanup

**Goal:** improve clarity and remove ad‑hoc warning handling.

- Standardize naming:
  - `source_uri`, `source_local_path`
  - `target_uri`, `target_local_path`
- Centralize warnings:
  - `warnings::emit(message)`
  - `warnings::record(message)`
- Remove direct `eprintln!` in run.

**Exit criteria**
- All warnings flow through one helper.
- Report warnings consistent with CLI output.

---

## Phase 8 — Input resolution & test cleanup

**Goal:** unify listing logic and clarify tests.

- Create `resolve_inputs(client, source, options)` (local + S3).
- Reorganize tests:
  - `tests/config_*`
  - `tests/io_*`
  - `tests/run_*`
- Add unit tests for:
  - `Target`
  - `StorageClient` (local + s3 mock)

**Exit criteria**
- Input listing logic no longer duplicated.
- Test structure reflects subsystems.

---

## Suggested execution order

1) Phase 1 (rename to storage)  
2) Phase 2 (StorageClient + Target + CloudClient)  
3) Phase 3 (entity split)  
4) Phase 4 (formatter)  
5) Phase 5–8 (cleanups + tests)  

Each phase should be its own PR.
