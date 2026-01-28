# Floe Run Report

This document describes the JSON run report emitted by `floe run`
and the status/exit code rules.

## Goals

- Provide a deterministic, machine-readable summary of each run.
- Avoid logging raw row values (PII risk).
- Support multiple input files and entities under the same run.

Reports only include aggregate counts and row indices for examples; raw row
values are excluded.

## Report directory layout

Reports are written under the configured report directory:

```
<report.path>/run_<run_id>/run.summary.json
<report.path>/run_<run_id>/<entity.name>/run.json
```

`run_id` is a filename-safe ISO-like string: `YYYY-MM-DDTHH-MM-SSZ`
(colons replaced by dashes). The report JSON must include the full
`report_file` path.

Each entity produces its own `run.json`. The run summary aggregates all entities.

Golden example files live under `example/report/run_2026-01-19T10-23-45Z/`.

## Top-level sections

### Run summary (`run.summary.json`)

- `spec_version`: Config version string.
- `tool`: Tool name/version and optional git info.
- `run`: Run ID, timestamps, duration, status, and exit code.
- `config`: Config path, version, and metadata (free-form).
- `report`: Report base path and the full summary report file path.
- `results`: Totals for files, rows, accepted/rejected rows, warnings, errors.
- `entities`: Per-entity totals + entity report file paths.

### Entity report (`<entity>/run.json`)

- `spec_version`: Config version string.
- `entity`: Entity name and metadata (free-form).
- `source`: Input format, path, options, cast mode, read plan, and resolved inputs.
- `sink`: Accepted/rejected/archive paths and formats.
- `policy`: Severity.
- `results`: Totals for files, rows, accepted/rejected rows, warnings, errors.
- `files`: Per-file outcomes and validation summary.


## Status and exit code rules

Per-file status:
- `success`: No errors for the file.
- `rejected`: Rows were rejected and written to the rejected output.
- `aborted`: The file triggered abort behavior.
- `failed`: Technical error (I/O, parsing, unexpected failure).

Run status:
- `success`: All files succeeded.
- `success_with_warnings`: No rejected/aborted/failed files, but warnings were emitted.
- `rejected`: At least one file was rejected and none failed/aborted.
- `aborted`: At least one file aborted and none failed.
- `failed`: Any file failed.

Exit codes:
- `0`: success, success_with_warnings, or rejected.
- `2`: aborted (policy).
- `1`: failed (technical error).

## Validation payloads

Validation summaries report aggregated counts only (no row-level examples).

Aggregations:
- `validation.errors`/`warnings` are totals for the file.
- `validation.rules[]` aggregates violations by rule and column.

Example rule summary:

```json
{
  "rule": "not_null",
  "severity": "reject",
  "violations": 12,
  "columns": [
    {"column": "customer_id", "violations": 12}
  ]
}
```

Row-level error details are written to the rejected error report JSON
(`*_reject_errors.json`) when available; reports do not include examples.
