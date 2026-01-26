# Sink options

Floe supports `sink.accepted.options` for tuning accepted output writers. For now, only Parquet options are applied; other formats accept the options block
but emit a warning and record it in the run report.

## Parquet options

- `compression`: `snappy`, `gzip`, `zstd`, `uncompressed`
- `row_group_size`: positive integer (rows per row group)

Example:

```yaml
sink:
  accepted:
    format: "parquet"
    path: "/data/out/customers"
    options:
      compression: "zstd"
      row_group_size: 50000
```
