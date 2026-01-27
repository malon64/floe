# Iceberg sink (accepted output)

Floe recognizes `sink.accepted.format: iceberg`, but it does not yet write
Iceberg tables. The CLI fails fast with a clear error so configs remain
forward-compatible.

## Storage catalog model

- Table identity: `sink.accepted.path` (table root directory).
- Metadata lives under `<path>/metadata/` (storage catalog layout).
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

## Limitations (v0.2)

- Writer not implemented; `floe validate` and `floe run` return a clear error.
- Only local storage catalog is planned; cloud catalogs are not supported yet.

## Next steps

- Implement a storage catalog writer (metadata + data files).
- Map Floe schema types to Iceberg types and handle overwrite semantics.
