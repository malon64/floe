# Features

This document tracks notable features by release.

## v0.1

- YAML config with project metadata, global report path, and multiple entities.
- CSV input with configurable header, delimiter, encoding, and null values.
- Schema enforcement with `cast_mode` strict/coerce.
- Checks: `not_null`, `cast_error`, `unique`.
- Severity handling: `warn`, `reject`, `abort`.
- Accepted/rejected outputs (parquet + csv) with row-level error columns.
- Abort mode writes the original file plus a JSON reject error report.
- Run report per entity with rule aggregations and examples.
- Optional archiving of input files when `sink.archive` is set.
- CLI: `validate` and `run` with `--entities` and `--run-id`.
