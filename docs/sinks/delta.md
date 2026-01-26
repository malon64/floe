# Delta Sink (Accepted Output)

Floe can write accepted output as a Delta Lake table on the local filesystem.

Example:
```yaml
entities:
  - name: customers
    source:
      format: csv
      path: /data/in/customers.csv
    sink:
      accepted:
        format: delta
        path: /data/out/customers_delta
```

Semantics:
- `sink.accepted.format: delta` writes a Delta table at `sink.accepted.path`.
- Write mode is `overwrite` (the table at the path is replaced on each run).
- Local filesystem only (S3 is not supported yet for delta output).
