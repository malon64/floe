# Design Decisions

- Row-level rejection is the default because it preserves good data while isolating bad rows.
- `unique` is dataset-level. Future strategies may include:
  - `reject_all`: reject all rows with duplicate keys (v0.1 default).
  - `keep_first`: keep the first occurrence and reject the rest.
  - `keep_last`: keep the last occurrence and reject the rest.
- `cast_mode` defines parsing behavior:
  - `strict`: invalid values cause rejection (or abort per policy).
  - `coerce`: invalid values become null; nullable rules decide acceptance.
