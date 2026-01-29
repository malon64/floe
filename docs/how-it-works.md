# How Floe Works

This document describes the canonical ingestion pipeline Floe uses for each
entity. The order is deterministic and is reflected in reports.

## Pipeline phases

### A) Entity planning (entity-level)

1. Resolve input files/objects (local directory/glob or storage prefix).
2. Resolve storage targets for accepted/rejected/report outputs.
3. Prepare output directories if needed.

### B) File-level prechecks (per file)

Floe inspects only the file header/schema before reading full data:

- **CSV**: header row (or the first row for headerless CSV).
- **Parquet**: schema metadata.
- **NDJSON**: the first valid JSON object line.

Then it applies the schema mismatch policy:

- Missing columns: `fill_nulls` or `reject_file`
- Extra columns: `ignore` or `reject_file`

If `reject_file` is triggered, the file is rejected/aborted according to
`policy.severity`, and **row-level validation does not run** for that file.

### C) Row-level validation (per file)

For files that passed prechecks:

1. Read full data into a dataframe.
2. Apply casts (`cast_mode`).
3. Apply `not_null` checks.
4. Build per-row error lists and counters.

### D) Entity-level uniqueness (across files)

After row-level validation, uniqueness is evaluated across the **entire entity**
in file order. The first occurrence is accepted; later duplicates are flagged.
This is the only entity-level check in v0.1/v0.2.

### E) Entity-level accepted output (across files)

Accepted rows from all input files are concatenated in file order and written
once to the accepted sink. For parquet sinks, Floe writes a dataset directory
containing `part-00000.parquet` (and additional parts in future chunking).

## Severity behavior

- **warn**: keep rows, emit warnings and error reports.
- **reject**: reject rows with violations; write rejected output.
- **abort**: abort the file when violations occur.

## Report mapping

- `files[].mismatch`: file-level precheck outcome (missing/extra + action).
- `files[].validation`: row-level + entity-level checks (cast/not_null/unique).
- `accepted_output`: entity-level accepted dataset summary (path + parts).
- `results`: totals aggregated across files and checks.

See `docs/report.md` for the report schema and `docs/checks.md` for rule details.
