# Write Modes

Floe resolves accepted/rejected write behavior from `sink.write_mode`.

Supported values:
- `overwrite` (default): existing output parts are replaced before writing new output.
- `append`: new output parts are added without deleting existing ones.
- `merge_scd1` (Delta accepted sink only): SCD1 upsert keyed by `schema.primary_key`.

## `merge_scd1` semantics

- Accepted sink format must be `delta`.
- `schema.primary_key` is required and is used as merge key.
- Source rows must be unique on the merge key; duplicate source keys abort the entity merge.
- On key match: update non-key columns from source.
- On missing key: insert source row.
- Current v1 behavior validates strict schema compatibility and does not perform schema evolution.
- Current fallback implementation computes merged rows in-memory and commits through a Delta overwrite transaction.
- Single-writer assumption: Delta commit conflicts are returned as clear write errors.

## Rejected output behavior

`sink.write_mode` is shared with rejected output:
- `overwrite`: first rejected write in a run starts fresh, subsequent rejected files append.
- `append`: rejected files append.
- `merge_scd1`: rejected files append (merge logic is accepted-Delta specific only).
