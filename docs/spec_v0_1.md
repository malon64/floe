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

## entities[].policy

- `severity`: Default action when a rule fails.
  - `warn`: keep row, record warning.
  - `reject`: quarantine row (default).
  - `abort`: stop the run.
- `quarantine`:
  - `mode`: `row` (default) or `file`.
    - `row`: split accepted vs rejected rows.
    - `file`: reject the entire file if any row fails.
  - `add_reason_columns`: include rule metadata in rejected outputs.
  - `reason_columns`: column names for rule metadata.
- `thresholds`:
  - `max_reject_rate`: Abort if rejected/total exceeds this ratio.
  - `max_reject_count`: Abort if rejected rows exceed this count.

## entities[].schema

- `normalize_columns`:
  - `enabled`: Whether to normalize input column names.
  - `strategy`: `snake_case`, `lower`, or `none`.
- `columns`: List of column definitions.
  - `name`: Column name.
  - `type`: Logical type (e.g., `string`, `datetime`).
  - `nullable`: Whether nulls are allowed.
  - `unique`: Whether values must be unique across dataset.
  - `unique_strategy`: Planned handling for duplicates (v0.1 uses `reject_all`).
