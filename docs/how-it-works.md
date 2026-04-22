# How Floe Works

This document describes the canonical ingestion pipeline Floe uses for each
entity. The order is deterministic and is reflected in reports.

## Pipeline phases

### A) Entity planning (entity-level)

1. Resolve input files/objects (local directory/glob or storage prefix).
   - In normal runs, resolved cloud objects are then downloaded to temp files.
   - In dry-run, resolution is list-only for cloud inputs (no downloads, no writes).
2. Resolve storage targets for accepted/rejected/report outputs.
3. If `incremental_mode: file` is enabled, load the entity state file and skip any
   file URI already recorded as previously ingested.
   - If the current size/mtime still matches the recorded state, Floe skips the file silently.
   - If the size/mtime changed, Floe still skips the file and emits an
     `incremental_file_changed` warning instead of reprocessing it.
4. Prepare output directories if needed.

### B) File-level prechecks (per file)

Floe inspects only the file header/schema before reading full data:

- **CSV/TSV**: header row (or the first row for headerless CSV/TSV).
- **Fixed-width**: declared schema widths and inferred columns from row slices.
- **Parquet**: schema metadata.
- **ORC**: schema metadata.
- **NDJSON**: the first valid JSON object line.
- **Avro**: writer schema fields.
- **XLSX**: configured sheet header row.
- **XML**: fields from the first matching `row_tag` element.

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
containing `part-00000.parquet` (and additional parts when chunking is enabled
via `sink.accepted.options.max_size_per_file`).

### F) Incremental state commit

When `incremental_mode: file` is enabled and the entity finishes successfully,
Floe updates the entity state file with the newly processed file URIs plus
observed size/mtime metadata. Later runs reuse that state to skip any file URI
already present in the state file.

The recorded metadata is used only to detect that a previously ingested file has
changed since the earlier run. In that case Floe emits an
`incremental_file_changed` warning, but it still does not reprocess the file
automatically.

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
