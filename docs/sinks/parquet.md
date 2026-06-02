# Parquet Sink (Accepted Output)

Floe can write accepted output as Parquet files on local, S3, ADLS, or GCS storage.

Example:
```yaml
entities:
  - name: customers
    source:
      format: csv
      path: /data/in/customers.csv
    sink:
      accepted:
        format: parquet
        path: /data/out/customers
```

Semantics:
- `sink.accepted.format: parquet` writes one or more part files under `sink.accepted.path`.
- Write mode comes from `sink.write_mode`:
  - `overwrite`: clear the output directory, then write. The first flush of a run removes existing files; subsequent flushes append.
  - `append`: add new part files alongside existing ones.
  - `merge_scd1` / `merge_scd2`: not supported for Parquet — use Delta or Iceberg for merge modes.

## File splitting

Large accepted datasets are split into multiple part files. The split threshold is controlled by
`sink.accepted.options.max_size_per_file` (default 256 MB). Floe flushes rows to disk
incrementally as memory reaches this threshold, so peak accepted-side RAM is bounded regardless
of how many input files the entity processes.

```yaml
sink:
  accepted:
    format: parquet
    path: /data/out/orders
    options:
      compression: zstd
      row_group_size: 50000
      max_size_per_file: 268435456   # 256 MB
```

Available options:
- `compression`: `snappy` (default), `gzip`, `zstd`, `uncompressed`
- `row_group_size`: rows per row group (positive integer)
- `max_size_per_file`: flush threshold in bytes (default: 268435456 / 256 MB)

## Limitations

### Parquet is not transactional

Parquet has no built-in transaction or atomic-commit semantics. Floe does not implement a
write-ahead log or two-phase commit for Parquet output. This has two practical consequences:

**Incomplete files on write failure**

If a failure occurs during a Parquet write (process crash, out-of-disk, network error on remote
storage), the in-progress part file is left on disk in an incomplete or corrupt state. There is no
rollback — incomplete files must be cleaned up manually before the next run or they will coexist
with valid output files in the accepted directory.

**Partial commits with soft-buffered writes**

Because Floe flushes accepted rows incrementally (see [File splitting](#file-splitting) above),
earlier flushes are already committed to the accepted path before the entity finishes processing.
If a failure occurs on a *later* input file in the same run:

- Rows from *earlier* input files that were already flushed remain on disk — there is no mechanism
  to roll them back.
- The run report and incremental state are **not** committed, so Floe will re-process the same
  inputs on the next run. This means the already-committed rows will be written again.
- In `overwrite` mode the first flush already cleared the previous dataset, so the directory will
  contain only the partial output from the failed run until it is re-processed.

For `append` mode, re-processing will produce duplicate part files in the accepted directory.
Use `unique_keys` deduplication or a downstream dedup step if idempotency is required.

Merge modes (`merge_scd1`, `merge_scd2`) are not affected because they accumulate the full
dataset before writing and never perform intermediate flushes.

**If you need transactional accepted writes, use the [Delta](delta.md) or [Iceberg](iceberg.md)
sink instead.** Each Delta/Iceberg flush is an individually atomic commit; earlier commits are
still not rolled back on a later-file failure within the same run, but readers never observe
a corrupt or partial file.
