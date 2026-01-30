# Delta Sink (Accepted Output)

Floe can write accepted output as a Delta Lake table on local or S3 storage.

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
- Write mode is `overwrite` via Delta transactions (a new `_delta_log` version is
  committed; history is preserved).
- Local and S3 storage are supported for delta output.

S3 notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Credentials come from the standard AWS environment/provider chain.
