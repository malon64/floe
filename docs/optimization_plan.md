# Optimization Plan (Derived from Performance Assessment)

This plan turns the performance assessment into a **reviewable PR sequence**. Each phase is scoped to be low risk and should keep behavior unchanged unless explicitly stated.

## Phase 0 — Baseline + guardrails (no behavior change)
**Goal:** ensure we can measure impact and avoid regressions.

- Add a tiny benchmark helper (optional) or timing logs around:
  - entity-level accepted accumulation
  - row-level error collection
- Add a unit test to guard against ADLS/GCS temp filename collision.
- Add a note in `docs/performance_assessment.md` linking to this plan.

**Exit criteria**
- All tests pass.
- Baseline timings captured (optional).

---

## Phase 1 — Accepted accumulation O(n²) → single concat
**Goal:** avoid repeated `vstack_mut` for large multi-file entities.

- Replace incremental `append_accepted` with:
  - `Vec<DataFrame>` accumulation
  - single concat at end (Polars `concat` / `vstack` once)
- Keep deterministic ordering (input file order).

**Files**
- `crates/floe-core/src/run/entity/mod.rs`

**Exit criteria**
- Existing tests pass.
- Memory peak reduced for multi-file entities (manual check OK).

---

## Phase 2 — Sparse row-error collection
**Goal:** avoid allocating `Vec<Vec<RowError>>` for every row when few errors exist.

- Replace full per-row error vector with:
  - `Vec<(row_idx, Vec<RowError>)>` or
  - error bitmask + compact error store
- Derive `accept_rows` from error bitmask.
- Only build formatted error output for **rejected** rows.

**Files**
- `crates/floe-core/src/checks/mod.rs`
- `crates/floe-core/src/run/entity/mod.rs`
- `crates/floe-core/src/checks/cast.rs`
- `crates/floe-core/src/checks/not_null.rs`
- `crates/floe-core/src/checks/unique.rs`

**Exit criteria**
- Tests for row-level behavior still pass.
- Error report output unchanged.

---

## Phase 3 — Unique check hashing without `to_string`
**Goal:** reduce per-row string allocations for unique checks.

- Replace `HashSet<String>` with:
  - a hash of `AnyValue` / `Series` values
  - or use Polars hashing utilities if available
- Ensure stable behavior across types.

**Files**
- `crates/floe-core/src/checks/unique.rs`

**Exit criteria**
- Unique tests pass.
- No behavior change in duplicate detection.

---

## Phase 4 — Temp file collision protection (ADLS/GCS)
**Goal:** avoid filename collisions for temp downloads.

- Use hash-based temp path (same approach as S3):
  - include hash prefix + sanitized basename
- Apply to ADLS/GCS download paths.

**Files**
- `crates/floe-core/src/io/storage/adls.rs`
- `crates/floe-core/src/io/storage/gcs.rs`

**Exit criteria**
- New unit test for collision passes.
- No behavior change for existing flows.

---

## Phase 5 — Reduce per-file mismatch cloning
**Goal:** avoid cloning `missing_columns` / `extra_columns` for each file.

- Store shared mismatch vectors once; reuse references where possible.
- Keep report format unchanged.

**Files**
- `crates/floe-core/src/run/entity/mod.rs`
- `crates/floe-core/src/report/*`

**Exit criteria**
- Reports remain identical.

---

## Phase 6 — Optional: IO concurrency cap & tuning
**Goal:** avoid WSL OOMs during local testing.

- Add configurable limits:
  - `FLOE_IO_CONCURRENCY` or config option
- Default to a safe value (2–4).

**Exit criteria**
- Local runs are stable.

---

## Suggested PR breakdown

1) Phase 1 (concat optimization)  
2) Phase 2 (sparse row errors)  
3) Phase 3 (unique hashing)  
4) Phase 4 (temp collisions)  
5) Phase 5 (mismatch cloning)  
6) Phase 6 (IO concurrency cap)

Each phase should be its own PR to simplify review.
