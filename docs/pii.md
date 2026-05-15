# PII Masking

Floe supports column-level PII masking transforms applied to accepted output
before it is written to the sink. Transforms run after schema validation and
data quality checks, so rejected rows are never masked.

## Configuration

Add a `pii` block to any entity:

```yaml
entities:
  - name: customer
    # ...
    pii:
      columns:
        - name: email
          strategy: hash
        - name: credit_card
          strategy: mask
          mask_pattern: "{first4}****{last4}"
        - name: ssn
          strategy: redact
          redact_value: "***"
        - name: ip_address
          strategy: nullify
        - name: notes
          strategy: drop
```

## Strategies

| Strategy  | Description                                                                                    | Extra fields           |
|-----------|------------------------------------------------------------------------------------------------|------------------------|
| `hash`    | Replaces each value with its lowercase hex SHA-256 digest. Null values remain null.            | —                      |
| `drop`    | Removes the column from the accepted output entirely.                                          | —                      |
| `nullify` | Replaces every value with null, preserving the column and its type.                            | —                      |
| `redact`  | Replaces every non-null value with a fixed string (default `[REDACTED]`).                      | `redact_value` (opt.)  |
| `mask`    | Applies a pattern string with optional `{firstN}` / `{lastN}` reveal tokens (see below).      | `mask_pattern` (req.)  |
| `tokenize`| Reserved for a future lookup-table tokenization strategy; rejected at config validation now.   | —                      |

### `mask` pattern syntax

The `mask_pattern` field is a literal string placed verbatim as the output,
with two optional tokens substituted:

- `{firstN}` — replaced by the first N Unicode characters of the original value
- `{lastN}` — replaced by the last N Unicode characters of the original value

```yaml
mask_pattern: "{first4}****{last4}"   # 4111222233334444 → 4111****4444
mask_pattern: "***-**-{last4}"         # 123-45-6789       → ***-**-6789
mask_pattern: "****"                   # any value         → ****
```

Rules:
- If `firstN` and `lastN` would overlap (value shorter than N+M chars), the
  last-N characters take priority and first-N is clamped so they never overlap.
- The mask is always applied — a zero-character value still produces the pattern
  without any substitution tokens.
- Null values remain null.

## Interaction with `normalize_columns`

When `schema.normalize_columns` is enabled, the `pii.columns[].name` field
refers to the **schema column name** (as declared in `schema.columns`), not the
post-normalization runtime name. Floe resolves the mapping automatically.

## Interaction with `strategy: drop`

Dropped columns are removed from the accepted DataFrame before the sink write.
They are not present in the Parquet/Delta/Iceberg output schema. If the column
is part of `schema.primary_key` or a `schema.unique_keys` entry, validation
will fail at config-load time.

## Validation rules

- `name` must match a column declared in `schema.columns`.
- `strategy: mask` requires a non-empty `mask_pattern`.
- `strategy: tokenize` is rejected at config validation (not yet implemented).
- `redact_value` and `mask_pattern` are ignored for strategies that do not use them.
- Duplicate column names within `pii.columns` are rejected.
