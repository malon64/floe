# Floe YAML Spec v0.1 (Draft)

This document describes the YAML fields used in `example/config.yml`. It is a focused subset intended for v0.1.

## Top-level

- `version`: Spec version string (e.g., "0.1").
- `metadata`: Project-level metadata.
- `report`: Global report settings.
- `entities`: List of entity configs. v0.1 is expected to run a single entity entry.

## metadata (project)

- `project`: Project or repo name.
- `description`: Human-readable description.
- `owner`: Owning team.
- `tags`: List of tags for cataloging.

## entities[].name

- `name`: Logical dataset name (e.g., "customer").

## entities[].metadata

- `data_product`: Product or domain dataset name.
- `domain`: Business domain (e.g., sales, finance).
- `owner`: Owning team.
- `description`: Human-readable description.
- `tags`: List of tags for cataloging.

## entities[].source

- `format`: Input file format. v0.1: `csv` (parquet later).
- `path`: Input path or glob, msut be absolute.
- `options`:
  - `header`: Whether the CSV has a header row.
  - `separator`: Field delimiter.
  - `encoding`: Text encoding.
  - `null_values`: Strings treated as null (e.g., "", "NULL", "null").
- `cast_mode`: Type casting behavior.
  - `strict`: reject values that cannot be parsed.
  - `coerce`: coerce invalid values to null (then apply nullable rules).

## read plan (implementation note)

Floe uses a simple read plan for CSV to make validation reliable:
- `raw` read: strict read into a string-typed schema (no casting).
- `typed` read: permissive read into the declared schema (invalid casts become nulls).
- `cast` errors are detected when a raw value is present but the typed value is null.
- If `cast_mode` is `coerce`, cast errors are ignored; otherwise they become validation errors.

## entities[].sink

- `accepted`: Output location for accepted rows.
  - `format`: `parquet` or `delta`.
  - `path`: Directory path for accepted output.
- `rejected`: Output location for rejected rows.
  - `format`: `csv` (keeps raw values on type issues).
  - `path`: Directory path for rejected output.
- `archive`:
  - `path`: Directory path for archived input files.
    - If omitted, archiving is disabled.

## report (top-level)

- `path`: Base directory for all run reports.
- Report layout (v0.1):
  - `<report.path>/run_<run_id>/<entity.name>/run.json`
  - `<report.path>/run_<run_id>/run.summary.json` (optional run-level summary)
  - `<report.path>/run_<run_id>/<entity.name>/files/<source_stem>.json` (reserved)

This layout avoids per-entity duplication, prevents collisions across entities,
and makes it easy to locate all reports for a single run.

## entities[].policy

- `severity`: Default action when a rule fails.
  - `warn`: log errors, keep rows, accepted output only.
  - `reject`: split accepted vs rejected rows.
  - `abort`: reject the entire file on any error.

## rejection output (csv)

Rejected rows are written as CSV with the original columns plus:
- `__floe_row_index`: 0-based row index in the input file.
- `__floe_errors`: JSON array string of error objects.

Example error object:
```json
{"rule":"not_null","column":"customer_id","message":"required value missing"}
```

Example `__floe_errors` value (normalized JSON array string):
```json
[
  {"rule":"not_null","column":"customer_id","message":"required value missing"},
  {"rule":"cast_error","column":"created_at","message":"invalid value for target type"}
]
```

For `abort`, the entire file is rejected and a JSON report is also written next to
the rejected CSV:
- `<source_stem>_reject_errors.json` containing per-row error details.
In `abort` mode, the rejected CSV is a copy of the original input file (no extra columns).

## entities[].schema

- `normalize_columns`:
  - `enabled`: Whether to normalize input column names.
  - `strategy`: `snake_case`, `lower`, or `none`.
- `columns`: List of column definitions.
  - `name`: Column name.
  - `type`: Logical type (e.g., `string`, `datetime`, `number`).
    - `number` maps to float64 and accepts decimals (aliases: `float`, `decimal`).
  - `nullable`: Whether nulls are allowed.
  - `unique`: Whether values must be unique across dataset.
    - When `true`, duplicates are handled according to `policy.severity`:
      - `warn`: log duplicates and keep all rows.
      - `reject`: keep first occurrence, reject duplicates.
      - `abort`: abort if duplicates are found.

# Post v0.1 Feature Ideas + Delivery Git Structure

This note captures candidate features to prioritize after the v0.1 milestone and a lightweight Git structure for delivering them.

## Potential Features After v0.1

### 1) Broader Input/Output Support
- **Parquet input/output** alongside CSV.
- **NDJSON/JSONL ingestion** for log-style datasets.
- **Compression**: gzip/zstd for CSV and parquet files.
- **Partitioned output** (e.g., by date or key columns).

### 2) Multi-Entity and Dependency Graphs
- Support **multiple entities** in a single run.
- **Dependency ordering** between entities (e.g., lookups first).
- Reusable **entity templates** to reduce YAML repetition.

### 3) Expanded Data Quality Rules
- **Value domain checks**: in-list, regex, range, and length.
- **Cross-column rules** (e.g., start_date <= end_date).
- **Aggregates checks** (row counts, uniqueness per group).
- **Row-level error metadata** including rule IDs and columns.

### 4) Schema Evolution and Type Handling
- **Type inference mode** with warnings in the report.
- **Schema evolution policy** (allow new columns, drop extras).
- **Casting strategies per column** (strict/coerce/nullable-only).

### 5) Quarantine and Rejection Modes
- **Column-level quarantine** (keep row, null offending fields).
- **Multi-output buckets** by violation severity.
- **Rule weighting** to prioritize error types.

### 6) Reporting, Observability, and DX
- **HTML report** in addition to JSON.
- **Summary metrics** output for CI (pass/fail gates).
- **Structured logs** with run identifiers.
- **CLI UX**: config validation, dry-run, and verbose modes.

### 7) Performance and Execution
- **Polars integration** for vectorized operations.
- **Streaming mode** for large files.
- **Threaded execution** with resource limits.

### 8) Extensibility
- **Custom rule plugins** via Rust traits.
- **Reusable rule packs** distributed as crates.

## Git Structure for Feature Delivery

### Branching Model (lightweight, trunk-based)
- **`main`**: always green; contains released or release-ready work.
- **`release/v0.x`**: optional stabilization branch if needed near a release.
- **`feature/<short-name>`**: short-lived branches for single features.
- **`fix/<short-name>`**: short-lived branches for bugfixes.

### Merge & Release Practices
- **PR-based merges** into `main` with code review.
- **Small, focused PRs** tied to a single feature or refactor.
- **Tags** for releases: `v0.1.0`, `v0.1.1`, etc.
- **Backporting**: cherry-pick fixes into `release/v0.x` if a patch is needed.

### Suggested Milestone Buckets
- **v0.2: Format + multi-entity**
- **v0.3: DQ rule expansion + richer reporting**
- **v0.4: Polars + performance improvements**

### Example Workflow
1. Create branch: `feature/parquet-input`.
2. Implement, test, and open PR.
3. Merge into `main` after review.
4. Tag release when ready.

This keeps history simple and ensures each release is a small, verifiable step forward.
