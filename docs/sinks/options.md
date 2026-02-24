# Sink options

Floe supports `sink.accepted.options` for tuning accepted output writers.

- Parquet applies `compression`, `row_group_size`, and `max_size_per_file`.
- Delta currently applies `max_size_per_file` (mapped to the Delta writer target file size).
- Other options on unsupported formats are ignored with a warning recorded in the run report.

## Parquet options

- `compression`: `snappy`, `gzip`, `zstd`, `uncompressed`
- `row_group_size`: positive integer (rows per row group)
- `max_size_per_file`: positive integer bytes (default: 256MB; split accepted parquet into parts)

Notes:

- For Parquet, this controls write-time file splitting.
- For Delta, `max_size_per_file` maps to the Delta writer target file size.
- Table compaction/optimization (Delta/Iceberg/Parquet datasets) is still an
  external operation in the current Floe releases.

Example:

```yaml
sink:
  accepted:
    format: "parquet"
    path: "/data/out/customers"
    options:
      compression: "zstd"
      row_group_size: 50000
      max_size_per_file: 268435456
```
