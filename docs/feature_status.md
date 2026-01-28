# Floe Feature Status (as implemented)

This document summarizes what is currently implemented in the codebase and what is not, to help plan future specs. It reflects the behavior in `main` as of the current refactor branch, not aspirational docs.

---

## CLI

**Implemented**
- `floe validate -c <config> [--entities ...]`
- `floe run -c <config> [--run-id ...] [--entities ...]`
- Run output modes:
  - `--quiet` (only final summary/errors)
  - `--verbose` (extra details)
  - default concise mode

**Not implemented / gaps**
- No `run.summary.json` file (only per-entity `run.json`).

---

## Config & validation

**Implemented**
- YAML config parsing with unknown-key rejection (strict schema at config level).
- `storages` registry:
  - `storages.default`
  - `storages.definitions[]` with `type`, `bucket`, `region`, `prefix`
- Per-entity overrides:
  - `source.storage`, `sink.accepted.storage`, `sink.rejected.storage`
- Validation errors include `entity.name=...` context.

**Not implemented / gaps**
- Storage types beyond `local` and `s3` are rejected.

---

## Input formats

| Format | Local | S3 | Notes |
|---|---|---|---|
| CSV | ✅ | ✅ | S3 uses prefix listing + suffix filter. |
| Parquet | ✅ | ❌ | **Blocked by validation**: `source.format=parquet` only allowed on local. |
| JSON (NDJSON) | ✅ | ✅ | Requires `source.options.ndjson=true`. JSON array mode not supported. |

**JSON constraints**
- NDJSON only (one object per line).
- Nested objects/arrays are rejected.
- Invalid JSON line = file-level rejection.

---

## Output formats (sinks)

**Accepted output**
- `parquet` ✅ (local + S3)
- `delta` ✅ (local only)
- `iceberg` ❌ (recognized, fails fast with clear error)

**Rejected output**
- `csv` ✅ (local + S3)

**Notes**
- Rejected output includes `__floe_row_index` and `__floe_errors`.
- Error report JSON is written for abort paths (per file).

---

## Storage / cloud support

**Implemented**
- `local` storage
- `s3` storage (MVP)
  - list objects under a prefix
  - download to temp file
  - upload from temp file
  - resolved URIs: `s3://bucket/prefix/path`

**Limitations / not implemented**
- No S3 glob patterns (prefix only).
- No explicit recursion control for S3 listing (list is effectively recursive).
- No S3 archive target (archive is local-only).
- No delete/move/exists/mkdirs APIs on StorageClient yet (only list/download/upload).
- ADLS/GCS not implemented.

---

## Input resolution

**Implemented**
- Local `source.path` can be:
  - file
  - directory
  - glob pattern
- Options:
  - `source.options.glob` (override glob for directory inputs)
  - `source.options.recursive` (recursive listing)
- Stable lexicographic sorting of resolved inputs.
- Default glob per format:
  - csv: `*.csv`
  - parquet: `*.parquet`
  - json: `*.json` + related suffixes (`.jsonl`, `.ndjson`, `.djson`)
- Uppercase extension handling supported.

---

## Schema & data quality checks

**Implemented**
- Cast enforcement with `cast_mode: strict | coerce`.
- Checks:
  - `cast_error`
  - `not_null`
  - `unique`
- Schema mismatch policy (file-level):
  - `schema.mismatch.missing_columns: fill_nulls | reject_file`
  - `schema.mismatch.extra_columns: ignore | reject_file`
  - Defaults: `fill_nulls` + `ignore`
  - `policy.severity=warn` never aborts; rejection request becomes warning.
- Column normalization (`schema.normalize_columns`):
  - `snake_case`, `lower`, `camel_case`, `none`

**Not documented yet**
- `schema.mismatch` is implemented but not described in `docs/config.md`.

---

## Policy behavior

**Implemented**
- `warn`: keep rows, log violations.
- `reject`: reject rows with violations.
- `abort`: abort file on first violation.

---

## Reporting

**Implemented**
- Per-entity report: `<report.path>/run_<run_id>/<entity>/run.json`.
- Report includes:
  - run metadata
  - resolved inputs
  - per-file status and counts
  - validation summaries and examples
  - mismatch summary (missing/extra columns + action)
- Row error formatter is configurable:
  - `report.formatter: json | csv | text`

**Not implemented / gaps**
- No run-level summary JSON (`run.summary.json`).
- No report output format other than JSON (only row error payload changes).

---

## Outputs & artifacts

**Implemented**
- Accepted output written to `sink.accepted.path`.
- Rejected output written to `sink.rejected.path`.
- Error report JSON written for abort paths.
- Archive support: `sink.archive.path` (local only).

---

## Testing coverage (not exhaustive)

**Implemented**
- Unit tests for:
  - format registry
  - storage/target behavior
  - S3 key filtering
  - mismatch policy
  - CSV/Parquet/NDJSON flows
- Config validation tests for misconfigurations.

**Gaps**
- No real S3 integration tests (unit-only).

---

## Known intentional restrictions

- Parquet input is local-only (validation rejects S3).
- Delta sink is local-only.
- Iceberg sink is not implemented (fails fast).
- JSON array mode is not supported.
- Archive is local-only.

---

## Suggested follow-ups (planning prompts)

- Decide S3 support matrix for parquet + NDJSON (validation + docs).
- Implement run-level summary report file (if required).
- Add StorageClient operations for delete/move/exists/mkdirs.
- Extend storages beyond S3 (ADLS/GCS) or formalize plugin approach.
- Document `schema.mismatch` in config docs.
