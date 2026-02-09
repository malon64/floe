# Runtime Refactor Plan (Ports + Adapters)

This document captures the refactor scope to keep `floe-cli` focused on printing and make
`floe-core/run` format‑agnostic and storage‑agnostic. It also defines the execution plan
and guardrails for the refactor.

## Current Pain Points

- `run` depends on concrete I/O details (`io::format`, `io::storage::CloudClient`,
  `Target::{S3,Adls,Gcs}` matching).
- Adapter selection happens inside `run` and `run/output` using format strings.
- Cloud/local handling and temp‑file behavior are wired directly into `run/entity`.
- CLI owns minor orchestration logic instead of being a pure printer.

These couplings make it costly to add formats (Avro/XML) or add new connectors
(Airflow/Dagster) without touching core run logic.

## Desired Properties

1. **CLI only prints** (parses args + prints output). All orchestration lives in core.
2. **`run` is format‑agnostic** (no `match` on format strings).
3. **`run` is storage‑agnostic** (no `S3/ADLS/GCS` checks or CloudClient construction).
4. **I/O and storage specifics live in adapters** (encapsulated in `io`).

## Proposed Architecture

### Ports (core interfaces)

Introduce a `Runtime`/`IoRegistry` port that supplies:

- `input_adapter(format) -> InputAdapter`
- `accepted_sink_adapter(format) -> AcceptedSinkAdapter`
- `rejected_sink_adapter(format) -> RejectedSinkAdapter`
- `storage() -> StorageClient` (cloud/local abstraction)

### Adapters (existing io module)

Keep CSV/JSON/Parquet readers and Parquet/Delta writers in `io`, but expose them
through the runtime port so `run` never selects adapters directly.

### Storage

`Target::is_remote()` should be the only storage‑specific check in run logic.
All other storage resolution, listing, download/upload remains in `io::storage`.

## Implementation Plan

### Phase 1 — Runtime Port + Default Runtime

- Add `runtime` module in `floe-core`:
  - `trait Runtime` with adapter lookup + storage handle.
  - `DefaultRuntime` that wires current `io` adapters + `CloudClient`.
- Update `run::run_with_base` to create `DefaultRuntime` and call `run_with_runtime`.
- Add `run_with_runtime` API for tests and for future orchestration.

### Phase 2 — Remove Adapter Selection From `run`

- Replace calls to `io::format::input_adapter(...)` in `run/entity` with `runtime.input_adapter(...)`.
- Replace `io::format::accepted_sink_adapter(...)` and `io::format::rejected_sink_adapter(...)`
  in `run/output` with runtime lookups.
- Update `validate_rejected_target(...)` to use runtime rather than `io::format` directly.

### Phase 3 — Storage Awareness Cleanup

- Add `Target::is_remote()` and use it in `run/entity` for temp‑dir logic.
- Stop matching on `Target::{S3,Adls,Gcs}` inside `run`.
- Keep storage resolution and transfer logic inside `io::storage`.

### Phase 4 — CLI Boundary

- Ensure `floe-cli` only does:
  - argument parsing
  - printing/output formatting
  - no runtime wiring or adapter logic

### Phase 5 — Test Strategy

- Add a fake runtime for unit tests (optional) to validate run logic in isolation.
- Ensure integration tests continue to use `DefaultRuntime`.

## Risks & Mitigations

- **Runtime API churn**: keep `DefaultRuntime` minimal and stable.
- **Borrowing / lifetimes**: runtime uses `'static` adapters to avoid lifetime plumbing.
- **Behavior drift**: run full test suite; maintain existing behavior.

## Out of Scope (for this refactor)

- Changing accepted/rejected write semantics.
- New formats (Avro/XML) or orchestrator connectors (Airflow).
- Storage implementation changes (S3/GCS/ADLS behavior).

