# Floe YAML Spec v0.1 (Draft)

This document describes the YAML fields used in `example/config.yml`. It is a focused subset intended for v0.1.

## Top-level

- `version`: Spec version string (e.g., "0.1").
- `metadata`: Project-level metadata.
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
- `path`: Input path or glob, relative to config directory.
- `options`:
  - `header`: Whether the CSV has a header row.
  - `separator`: Field delimiter.
  - `encoding`: Text encoding.
  - `null_values`: Strings treated as null (e.g., "", "NULL", "null").
- `cast_mode`: Type casting behavior.
  - `strict`: reject values that cannot be parsed.
  - `coerce`: coerce invalid values to null (then apply nullable rules).

## entities[].sink

- `accepted`: Output location for accepted rows.
  - `format`: `parquet` or `delta`.
  - `path`: Directory path for accepted output.
- `rejected`: Output location for rejected rows.
  - `format`: `csv` (keeps raw values on type issues).
  - `path`: Directory path for rejected output.
- `report`:
  - `path`: Directory path for the JSON run report.
- `archive`:
  - `path`: Directory path for archived input files.
    - If omitted, defaults to `<sink.accepted.path>/archived`.

## entities[].policy

- `severity`: Default action when a rule fails.
  - `warn`: log errors, keep rows, accepted output only.
  - `reject`: split accepted vs rejected rows.
  - `abort`: reject the entire file on any error.
- `thresholds`:
  - `max_reject_rate`: Abort if rejected/total exceeds this ratio.
  - `max_reject_count`: Abort if rejected rows exceed this count.

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
  - `type`: Logical type (e.g., `string`, `datetime`).
  - `nullable`: Whether nulls are allowed.
  - `unique`: Whether values must be unique across dataset.
    - `warn`: log duplicates and keep all rows.
    - `reject`: keep first occurrence, reject duplicates.
    - `abort`: abort if duplicates are found.
