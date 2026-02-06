# Configuration

This page is a practical summary. The full reference is in `docs/config.md` and the example config is `example/config.yml`.

## Minimal example (animal)

A tiny, readable example for onboarding:

```yaml
version: "0.1"
report:
  path: "report"
entities:
  - name: "animal"
    source:
      format: "csv"
      path: "example/in/animal"
    sink:
      write_mode: "append"
      accepted:
        format: "parquet"
        path: "example/out/accepted/animal"
      rejected:
        format: "csv"
        path: "example/out/rejected/animal"
    policy:
      severity: "reject"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "animal_id"
          type: "string"
          nullable: false
          unique: true
        - name: "species"
          type: "string"
        - name: "age"
          type: "int64"
```

## Full example (all features)

For a complete, commented config that covers domains, storages, cloud paths, write modes, mismatch policies, and multiple formats, see:

- `docs/wiki/Full-Config-Example.md`

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
