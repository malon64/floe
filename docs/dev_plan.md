# Floe v0.1 Dev Plan (Short)

1) Parse YAML config and validate required fields.
2) Read CSV with header, delimiter, and null handling.
3) Enforce schema types with `cast_mode` (strict/coerce).
4) Implement checks: `not_null`, `unique`.
5) Quarantine output: accepted vs rejected (row mode).
6) Emit JSON run report with counts and top violations.
7) CLI UX: `validate` and `run` commands with basic flags.
