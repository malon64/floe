# Merge Strategy Architecture

This note summarizes how merge write modes are organized in Floe.

## Runtime layering

- `io/write/delta.rs` routes accepted Delta writes by `sink.write_mode`.
- `io/write/strategy/merge/mod.rs` defines merge strategy interfaces.
- `io/write/strategy/merge/scd1.rs` and `scd2.rs` implement Delta-backed merge execution.
- `io/write/strategy/merge/shared.rs` centralizes shared merge utilities
  (merge key resolution, schema compatibility checks, SQL predicate helpers, and common metrics mapping).

## Merge options (`sink.accepted.merge`)

Merge options are connector configuration only; report/output contracts are unchanged.

- `ignore_columns`
  - optional list of business schema columns ignored by merge update/compare logic
- `compare_columns`
  - optional explicit SCD2 change-detection columns
  - if unset, SCD2 compares all non-key business columns minus `ignore_columns`
- `scd2` system column overrides
  - `current_flag_column` (default `__floe_is_current`)
  - `valid_from_column` (default `__floe_valid_from`)
  - `valid_to_column` (default `__floe_valid_to`)

Validation constraints:

- allowed only when `sink.accepted.format=delta` and `sink.write_mode` is `merge_scd1` or `merge_scd2`
- `ignore_columns` / `compare_columns` must reference schema columns
- merge keys (`schema.primary_key`) cannot appear in `ignore_columns` or `compare_columns`
- SCD2 custom system column names must be non-empty, unique, and non-colliding with business schema columns
