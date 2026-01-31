# ADLS Storage (MVP)

ADLS (Gen2) storage is supported for list/download/upload using Azure's default
credential chain. This MVP uses temp downloads/uploads for file IO.

## Config fields

```yaml
storages:
  default: adls_raw
  definitions:
    - name: adls_raw
      type: adls
      account: myaccount
      container: raw
      prefix: ingest
```

Required:
- `account`
- `container`

Optional:
- `prefix` (default empty)

## Canonical URI format

```
abfs://<container>@<account>.dfs.core.windows.net/<prefix>/<path>
```

Examples:
- `abfs://raw@myaccount.dfs.core.windows.net/ingest/customers/customers.csv`
- `abfs://raw@myaccount.dfs.core.windows.net/customers/customers.csv` (no prefix)

## Authentication

Floe relies on Azure's default credential chain. Set one of the supported env
combinations, for example:

```bash
export AZURE_CLIENT_ID=...
export AZURE_TENANT_ID=...
export AZURE_CLIENT_SECRET=...
```

Managed identity, Azure CLI, and other default credential sources are also supported.

## Supported behavior

- Inputs: CSV and JSON (array/ndjson) via prefix listing + suffix filtering.
- Outputs:
  - Accepted parquet: temp local write then upload.
  - Accepted delta: direct object_store writes (no temp upload).
  - Rejected CSV: temp local write then upload.

## Limitations

- Source path is treated as a prefix (no glob patterns).
- Parquet input is local-only.
