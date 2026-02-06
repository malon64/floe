# Configuration

This page is a practical summary. The full reference is in `docs/config.md` and the example config is `example/config.yml`.

## Minimal example

```yaml
version: "0.1"
report:
  path: "report"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "example/in/customer"
    sink:
      accepted:
        format: "parquet"
        path: "example/out/accepted/customer"
      rejected:
        format: "csv"
        path: "example/out/rejected/customer"
    policy:
      severity: "reject"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
        - name: "full_name"
          type: "string"
```

## Key sections

- `source`: input format, path, and options.
- `sink`: accepted/rejected outputs and write mode.
- `policy`: `warn`, `reject`, or `abort`.
- `schema`: column types, nullability, unique checks, optional normalization.

## Write mode

`write_mode` controls how accepted and rejected datasets are written:

- `overwrite` (default): replaces existing dataset parts.
- `append`: adds new dataset parts.

The mode is defined at `sink.write_mode` and applies to both accepted and rejected outputs.

## Storage

Cloud storage definitions live under `storages`. Use `source.storage` and `sink.*.storage` to select a client. See `docs/support-matrix.md` for supported formats and cloud behavior.
