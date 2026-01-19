# Design Decisions

- Row-level rejection is the default because it preserves good data while isolating bad rows.
- `unique` is dataset-level and enforced by `policy.severity`:
  - `warn`: log duplicates and keep all rows.
  - `reject`: keep the first occurrence, reject duplicates.
  - `abort`: abort the file if duplicates are found.
- `cast_mode` defines parsing behavior:
  - `strict`: invalid values cause rejection (or abort per policy).
  - `coerce`: invalid values become null; nullable rules decide acceptance.
