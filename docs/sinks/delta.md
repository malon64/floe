# Delta Sink (Accepted Output)

Floe can write accepted output as a Delta Lake table on local, S3, ADLS, or GCS storage.

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
- Write mode comes from `sink.write_mode`:
  - `overwrite`: replace data via a new Delta transaction (history preserved).
  - `append`: add new files via a new Delta transaction (history preserved).
- Local, S3, ADLS, and GCS storage are supported for delta output.

## Partition config (Phase A scaffolding)

Delta accepted sinks can now declare identity partition columns in config:

```yaml
sink:
  accepted:
    format: delta
    path: /data/out/orders_delta
    partition_by: ["order_date", "country"]
```

Current status:
- `floe validate` checks that partition columns exist in `schema.columns`.
- Execution wiring for partitioned Delta writes is pending follow-up PRs.
- Compaction/optimization remains external to Floe.

S3 notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Credentials come from the standard AWS environment/provider chain.

ADLS notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Authentication uses Azure env-based credentials (e.g., `AZURE_STORAGE_ACCOUNT_KEY`,
  `AZURE_STORAGE_SAS_KEY`, or Azure AD token variables). The storage account and
  container are taken from the storage definition and table URI.

GCS notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Authentication uses Application Default Credentials via
  `GOOGLE_APPLICATION_CREDENTIALS`.
