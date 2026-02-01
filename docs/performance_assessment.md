# Performance & Complexity Assessment (v0.2 readiness)

This document lists **observed performance risks** and **concrete optimization targets** after scanning the current codebase. It focuses on hot paths (I/O, allocations, concurrency) and highlights areas where behavior is correct but can become expensive on large inputs or cloud workloads.

See also: `docs/optimization_plan.md` for the phased implementation plan.

## Executive summary

Primary risks today are:
- **Row-level error collection** allocates `Vec<Vec<RowError>>` sized to the full row count, even when most rows are valid.
- **Entity-level accepted accumulation** uses repeated `vstack_mut`, which can be **O(n²)** on large multi-file entities.
- **Cloud temp downloads** for ADLS/GCS use filename-only temp paths, which can **collide** for same filenames across different prefixes.
- **Unique checks** convert values to strings for hashing, which is expensive for large datasets.

## Hot path findings & targets

### 1) Entity pipeline: row-level validation
**Location:** `crates/floe-core/src/run/entity/mod.rs`, `checks/cast.rs`, `checks/not_null.rs`, `checks/unique.rs`

**Current behavior**
- Builds `Vec<Vec<RowError>>` of size `row_count` even when there are no errors.
- `cast_mismatch_errors` and `not_null_errors` run per-row loops with string allocations in some cases.
- Unique uses `value.to_string()` for every row; HashSet is `HashSet<String>`.

**Optimization targets**
1) **Sparse error collection**: store only error rows and keep a parallel bitmask; derive accepted rows from mask.  
2) **Avoid string materialization in unique**: use `Series` hashing or `AnyValue` hashing instead of `to_string()`.  
3) **Short-circuit not_null/cast**: avoid per-row iteration when counts are zero (partially done).  

### 2) Entity-level accepted accumulation
**Location:** `crates/floe-core/src/run/entity/mod.rs` (`append_accepted`)

**Current behavior**
- For N input files, accepted rows are appended with `vstack_mut`, which can become **O(n²)**.

**Optimization targets**
1) Accumulate `Vec<DataFrame>` then **single concat** at end using Polars `concat`/`vstack` once.  
2) For very large datasets, consider `LazyFrame` concat and write without fully materializing.  

### 3) Cloud temp download name collisions (correctness + perf)
**Location:** `crates/floe-core/src/io/storage/adls.rs`, `gcs.rs`

**Current behavior**
- ADLS/GCS `download_to_temp` uses `file_name()` as the temp filename.
- If two keys share the same basename (e.g., `a/part.parquet` and `b/part.parquet`), downloads can **collide**.

**Optimization targets**
1) **Use a hash-based temp filename** (same approach as S3) to avoid collisions.  
2) Include sanitized basename for readability if desired.  

### 4) Cloud listing + download
**Location:** `crates/floe-core/src/io/storage/*`

**Current behavior**
- Lists entire prefix and filters suffixes in-memory.
- Uses a single-threaded tokio runtime per client, with blocking `block_on`.

**Optimization targets**
1) **Optional pagination window or max objects cap** for very large prefixes.  
2) **Limit download concurrency** (already implicit) but provide a tunable cap to avoid memory spikes.  

### 5) Report + rejected error generation
**Location:** `crates/floe-core/src/checks/mod.rs`, `run/entity/mod.rs`

**Current behavior**
- Builds formatted error strings for every row (uses `build_errors_formatted`).
- Errors JSON generation is proportional to row count even if only a few errors.

**Optimization targets**
1) **Generate errors only for rejected rows** to avoid formatting large accepted sets.  
2) If in warn mode, consider lazy error string creation.  

### 6) Repeated per-file schema normalization
**Location:** `crates/floe-core/src/run/entity/mod.rs`, `checks/mismatch.rs`

**Current behavior**
- Normalized schema columns are computed once (good), but mismatch reporting clones vectors per file.

**Optimization targets**
1) Avoid cloning `missing_columns`/`extra_columns` vectors when unchanged across files.  
2) Store references or indices where possible in report assembly.  

### 7) Repeated DataFrame column indexing
**Location:** `checks/not_null.rs`, `checks/cast.rs`

**Current behavior**
- Uses `select_at_idx` in loops per column.  

**Optimization targets**
1) Cache `Series` references once per check.  
2) Use `DataFrame::select` or `column` once and reuse.  

## Recommended priority order

1) **Accepted accumulation O(n²)** → concat once.  
2) **Sparse error collection** for row-level validation.  
3) **Hash-based temp filenames** for ADLS/GCS.  
4) **Unique hashing without to_string**.  
5) **Reduce error formatting** to rejected rows only.  

## Notes on concurrency

Current execution is mostly single-threaded per entity, which is safer for memory. If multi-threading is enabled, cap it (2–4) to avoid WSL OOM. Consider **separating IO and compute concurrency limits**.

## Suggested follow-up tasks (no behavior change)

- Add simple micro-bench for `append_accepted` with N files.  
- Add a test for ADLS/GCS temp filename collision.  
- Add a synthetic test for `unique` hashing performance if needed.  
