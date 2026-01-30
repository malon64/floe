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

## Status

The ADLS storage backend is **not implemented yet** (no list/get/put). Any config that
references `type: adls` will fail fast with a clear error until the backend is added.
