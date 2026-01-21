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
<report.path>/run_<run_id>/<entity.name>/run.json
<report.path>/run_<run_id>/run.summary.json
```

`run_id` is a filename-safe ISO-like string: `YYYY-MM-DDTHH-MM-SSZ`
(colons replaced by dashes). The report JSON must include the full
`report_file` path.

Each entity produces its own `run.json`. The `run.summary.json` file
represents the run as a whole and aggregates results across entities.

Golden example files live under `example/report/run_2026-01-19T10-23-45Z/`.

## Top-level sections

- `spec_version`: Config version string.
- `tool`: Tool name/version and optional git info.
- `run`: Run ID, timestamps, duration, status, and exit code.
- `config`: Config path, version, and metadata (free-form).
- `entity`: Entity name and metadata (free-form).
- `source`: Input format, path, options, cast mode, read plan, and resolved inputs.
- `sink`: Accepted/rejected/archive paths and formats.
- `report`: Report base path and the full report file path.
- `policy`: Severity.
- `results`: Totals for files, rows, accepted/rejected rows, warnings, errors.
- `files`: Per-file outcomes and validation summary.

`run.summary.json` uses the same top-level keys and aggregates `results` and
`files` across all entities in the run.

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

Validation summaries report aggregated counts and examples only. Examples
include rule, column, row index, and message (no raw values).

Aggregations:
- `validation.errors`/`warnings` are totals for the file.
- `validation.rules[]` aggregates violations by rule and column.
- `validation.examples.items[]` contains up to `max_examples_per_rule`
  examples per rule (ordered by row index).

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

Example item:

```json
{
  "rule": "cast_error",
  "column": "created_at",
  "row_index": 17,
  "message": "invalid value for target type"
}
```
