# Delta Sink (Accepted Output)

Floe can write accepted output as a Delta Lake table on local, S3, or ADLS storage.

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
- Local, S3, and ADLS storage are supported for delta output.

S3 notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Credentials come from the standard AWS environment/provider chain.

ADLS notes:
- Delta writes go directly through the object_store backend (no temp download/upload).
- Authentication uses Azure env-based credentials (e.g., `AZURE_STORAGE_ACCOUNT_KEY`,
  `AZURE_STORAGE_SAS_KEY`, or Azure AD token variables). The storage account and
  container are taken from the storage definition and table URI.
