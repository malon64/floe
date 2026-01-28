# Config Reference

This document describes the Floe YAML configuration format and the meaning of
all supported options. See `example/config.yml` for a full working example.

## Structure at a glance

```yaml
version: "0.1"
metadata: { ... }
report:
  path: "/abs/or/relative/report/dir"
entities:
  - name: "customer"
    metadata: { ... }
    source:
      format: "csv"
      path: "/path/to/input/or/dir"
      options:
        header: true
        separator: ","
        encoding: "utf-8"
        null_values: ["", "NULL", "null"]
        recursive: false
        glob: "*.csv"
      cast_mode: "strict"
    sink:
      accepted:
        format: "parquet"
        path: "/path/to/accepted/"
      rejected:
        format: "csv"
        path: "/path/to/rejected/"
      archive:
        path: "/path/to/archive/"
    policy:
      severity: "reject"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
```

## Top-level fields

- `version` (required)
  - String used to validate config compatibility (example: `"0.1"`).
- `metadata` (optional)
  - Free-form project metadata. Keys supported in schema: `project`,
    `description`, `owner`, `tags`.
- `report` (required)
  - `report.path` is the base directory where run reports are written.
  - Reports are written under:
    `report.path/run_<run_id>/run.summary.json` and
    `report.path/run_<run_id>/<entity.name>/run.json`.
- `storages` (optional)
  - Defines named storage clients for `source.storage` and `sink.*.storage`.
  - If omitted, `local` is assumed.
- `entities` (required)
  - Array of entity definitions (datasets). A single CLI run may process
    multiple entities.

## Entity fields

### `name` (required)
Logical dataset name. Used in report and output paths.

### `metadata` (optional)
Free-form entity metadata. Supported keys: `data_product`, `domain`, `owner`,
`description`, `tags`.

### `source` (required)

- `format` (required)
  - Supported: `csv` and `parquet` (local). `json` is supported only in NDJSON
    mode via `source.options.ndjson: true`.
- `path` (required)
  - Input location. Can be a file, a directory, or a glob pattern
    (example: `/data/in/*.csv`).
  - If a directory is provided, a glob is applied to select files.
  - Relative paths resolve against the config file directory.
- `storage` (optional)
  - Name of the storage client to use for this source.
  - Defaults to `storages.default` when defined, otherwise `local`.
- `options` (optional)
  - CSV/JSON options.
  - Defaults if omitted:
    - `header`: `true`
    - `separator`: `";"`
    - `encoding`: `"UTF8"`
    - `null_values`: `[]`
    - `recursive`: `false`
    - `glob`: (none; default is based on `source.format`)
    - `ndjson`: `false`
  - `glob` (optional)
    - Used only when `source.path` is a directory.
    - Overrides the default file pattern for the source format:
      - `csv`: `*.csv`
      - `parquet`: `*.parquet`
      - `json`: `*.json`
    - If `source.path` itself contains a glob pattern, this option is ignored.
  - `recursive` (optional)
    - If `true`, directory globs include subdirectories (via `**/`).
  - `ndjson` (optional)
    - Set to `true` to enable NDJSON ingestion when `source.format: json`.
    - If `false` or omitted, JSON array mode is not supported yet.
    - NDJSON lines must be flat objects; nested objects/arrays are rejected.
- `cast_mode` (optional)
  - `strict` (default): invalid values produce cast errors.
  - `coerce`: invalid values become null (and may still fail `not_null`).

### `sink` (required)

- `accepted` (required)
  - `format`: `parquet` or `delta` (local). `iceberg` is recognized but not
    implemented yet.
  - `path`: output directory for accepted records.
  - `storage` (optional)
    - Name of the storage client to use for this sink target.
    - Defaults to `storages.default` when defined, otherwise `local`.
  - `options` (optional)
    - Parquet-only sink options. When provided for other formats, Floe logs a
      warning and records it in the run report.
    - `compression`: `snappy`, `gzip`, `zstd`, `uncompressed`
    - `row_group_size`: positive integer (rows per row group)
- `rejected` (required when `policy.severity: reject`)
  - `format`: `csv` (v0.1).
  - `path`: output directory for rejected rows.
- `archive` (optional)
  - `path`: directory where raw input files are archived after ingestion.
  - If omitted, archiving is disabled.

### `policy` (required)

- `severity` (required)
  - `warn`: keep rows, log violations.
  - `reject`: reject rows with violations (first unique kept, duplicates rejected).
  - `abort`: abort the entire file on first violation.

### `schema` (required)

- `normalize_columns` (optional)
  - `enabled`: boolean toggle.
  - `strategy`: `snake_case`, `lower`, `camel_case`, `none`.
  - When enabled, both schema column names and input column names are normalized
    before checks. If normalization causes a name collision, the run fails.
- `columns` (required)
  - Array of column definitions.
  - `name` (required): column name in the input file.
  - `type` (required): logical type. Accepted values are case-insensitive and
    normalized by removing `-` and `_`.
  - `nullable` (optional): default `true`.
  - `unique` (optional): default `false`.

## Supported column types

The parser accepts the following type names (case-insensitive, `-` and `_`
ignored):

- `string`: `string`, `str`, `text`
- `boolean`: `boolean`, `bool`
- `integer`: `int8`, `int16`, `int32`, `int64`, `int`, `integer`, `long`
- `unsigned integer`: `uint8`, `uint16`, `uint32`, `uint64`
- `number` (float64): `number`, `float64`, `float`, `double`, `decimal`
- `float32`: `float32`
- `date`: `date`
- `datetime`: `datetime`, `timestamp`
- `time`: `time`

For more details about checks and severity behavior, see `docs/checks.md`.
