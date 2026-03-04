# Write Modes

Floe resolves accepted/rejected write behavior from `sink.write_mode`.

Supported values:
- `overwrite` (default): existing output parts are replaced before writing new output.
- `append`: new output parts are added without deleting existing ones.
- `merge_scd1` (Delta accepted sink only): SCD1 upsert keyed by `schema.primary_key`.
- `merge_scd2` (Delta accepted sink only): SCD2 merge keyed by `schema.primary_key`.

## `merge_scd1` semantics

- Accepted sink format must be `delta`.
- `schema.primary_key` is required and is used as merge key.
- Source rows are validated for merge-key uniqueness during row checks.
  In `policy.severity=warn`, duplicate merge-key rows are rejected before merge so only unambiguous rows are merged.
- On key match: update non-key columns from source.
- On missing key: insert source row.
- Current v1 behavior validates strict schema compatibility and does not perform schema evolution.
- Merge execution uses Delta native merge (`MERGE INTO`) through delta-rs/DataFusion.
- Single-writer assumption: Delta commit conflicts are returned as clear write errors.

## `merge_scd2` semantics

- Accepted sink format must be `delta`.
- `schema.primary_key` is required and is used as merge key.
- Source rows are validated for merge-key uniqueness during row checks.
  In `policy.severity=warn`, duplicate merge-key rows are rejected before merge so only unambiguous rows are merged.
- Floe manages SCD2 system columns in Delta tables:
  - `__floe_is_current`
  - `__floe_valid_from`
  - `__floe_valid_to`
- Changed current rows are closed (`is_current=false`, `valid_to=current_timestamp()`),
  then new current versions are inserted.
- Missing keys are inserted as new current rows.
- Unchanged current rows are left as-is.
- Current v1 behavior validates strict schema compatibility and does not perform schema evolution.
- Single-writer assumption: Delta commit conflicts are returned as clear write errors.

## Rejected output behavior

`sink.write_mode` is shared with rejected output:
- `overwrite`: first rejected write in a run starts fresh, subsequent rejected files append.
- `append`: rejected files append.
- `merge_scd1`: rejected files append (merge logic is accepted-Delta specific only).
- `merge_scd2`: rejected files append (merge logic is accepted-Delta specific only).
