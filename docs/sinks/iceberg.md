# Iceberg sink (accepted output)

Floe writes Iceberg tables for `sink.accepted.format: iceberg` using a local
filesystem catalog layout (table root with `metadata/` and `data/`).

## Storage catalog model

- Table identity: `sink.accepted.path` (table root directory).
- Metadata lives under `<path>/metadata/` (storage catalog layout).
- Local filesystem only in v0.2.
- No external catalog integration in v0.2.

## Example config

```yaml
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      accepted:
        format: "iceberg"
        path: "/data/out/customer_iceberg"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

## Supported scope (v0.2 local MVP)

- Accepted sink only (`sink.accepted.format: iceberg`)
- Local storage only (`sink.accepted.storage` must resolve to local)
- `sink.write_mode`: `overwrite` and `append`
- Scalar column types only (for example: string, bool, signed ints, floats, date/time/datetime)
- Append creates a new Iceberg snapshot/metadata version
- Overwrite replaces logical table contents by recreating table metadata at the same root (no cleanup/GC)
- Run report includes sink format, table root URI, write mode, files written, and snapshot id (when present)

## Limitations / non-goals (v0.2)

- No schema evolution
- No merge/upsert
- No cleanup/garbage collection of orphaned files/metadata
- No cloud/object-store catalogs yet (S3/GCS/Glue follow-ups)

## Empty accepted dataset behavior

- Overwrite/create with an empty accepted dataframe creates the table metadata layout
  (no data files, no snapshot yet).
- Append with an empty accepted dataframe is a no-op for table contents.
