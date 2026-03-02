# Write Modes

Floe resolves accepted/rejected write behavior from `sink.write_mode`.

Supported values:
- `overwrite` (default): existing output parts are replaced before writing new output.
- `append`: new output parts are added without deleting existing ones.
- `merge_scd1` (Delta accepted sink only): SCD1 upsert keyed by `schema.primary_key`.

## `merge_scd1` semantics

- Accepted sink format must be `delta`.
- `schema.primary_key` is required and is used as merge key.
- Source rows are validated for merge-key uniqueness during row checks.
  In `policy.severity=warn`, duplicate merge-key rows are rejected before merge so only unambiguous rows are merged.
  A writer-level safeguard still errors if ambiguous duplicates reach the Delta merge stage.
- On key match: update non-key columns from source.
- On missing key: insert source row.
- Current v1 behavior validates strict schema compatibility and does not perform schema evolution.
- Merge execution uses Delta native merge (`MERGE INTO`) through delta-rs/DataFusion.
- Single-writer assumption: Delta commit conflicts are returned as clear write errors.

## Rejected output behavior

`sink.write_mode` is shared with rejected output:
- `overwrite`: first rejected write in a run starts fresh, subsequent rejected files append.
- `append`: rejected files append.
- `merge_scd1`: rejected files append (merge logic is accepted-Delta specific only).
