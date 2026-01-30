# ADLS Storage (planned)

Floe supports configuring ADLS storage definitions but the backend is not implemented yet.
This file documents the intended configuration and canonical URI format.

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

## Status

MVP is implemented with temp download/upload (list/get/put) using Azure default credentials.
