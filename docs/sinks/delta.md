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
  - `merge_scd1`: upsert source rows using `schema.primary_key` as merge key.
    - matched keys: update non-key columns from source
    - missing keys: insert source rows
  - `merge_scd2`: close changed current rows and insert new current versions.
    - requires SCD2 system columns in the Delta table:
      `__floe_is_current`, `__floe_valid_from`, `__floe_valid_to`
- Local, S3, ADLS, and GCS storage are supported for delta output.

Schema evolution for standard Delta writes:
- Default behavior remains strict.
- `schema.schema_evolution.mode: add_columns` enables additive-only schema evolution for
  accepted Delta `append`, `overwrite`, `merge_scd1`, and `merge_scd2` writes.
- Existing columns must remain compatible; incompatible non-additive changes fail before commit.
- Unsupported changes in this phase include drops, renames, type changes, nullable widening of existing non-nullable columns, and additive merge-key changes.
- Adding columns to already partitioned Delta tables is rejected in this phase.
- When evolution is applied, Floe emits a structured `schema_evolution_applied` event and
  populates the entity report `schema_evolution` block with `applied=true` and `added_columns`.
- When evolution is enabled but no Delta schema change is needed, the entity report still
  includes `schema_evolution` with `applied=false`.

Merge mode notes (`merge_scd1`, `merge_scd2`):
- Requires `schema.primary_key` in config (non-empty, non-nullable columns).
- Only supported on Delta accepted sinks.
- Strict remains the default behavior.
- `schema.schema_evolution.mode: add_columns` enables additive-only evolution for merge modes too.
  - New non-key business columns may be added to the target Delta table before merge.
  - Merge-key columns cannot be introduced by evolution.
  - SCD2 system columns stay managed by Floe and are not treated as user-additive columns.
- Source duplicates on merge key are handled during row checks before write.
- Single-writer assumption applies; Delta commit conflicts surface as write errors.

## Partition config

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
- Partitioned Delta writes are runtime-wired for local/S3/ADLS/GCS accepted sinks.
- `sink.accepted.options.max_size_per_file` is mapped to the Delta writer target file size when set.
- Accepted output reports include file sizing metrics derived from committed Delta log `add` actions:
  `files_written`, `total_bytes_written`, `avg_file_size_mb`, `small_files_count`.
- Local and remote Delta targets (S3/GCS/ADLS) use the committed version file
  `_delta_log/<version>.json` for metrics collection.
- Remote metrics collection is best-effort after a successful write:
  - if commit-log read/parse succeeds, metrics are exact for the committed version
  - if commit-log read/parse fails, the write still succeeds and the report falls back to
    `files_written=null`, `part_files=[]`, and nullable size metrics
- `files_written` counts `add` actions in the committed version file (not Delta log files).
- `part_files` in the report is a capped list of data-file basenames from `add.path`.
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

Operational note:
- Floe writes data and reports write-time metrics. Table optimization/maintenance
  (for example `OPTIMIZE`/compaction and `VACUUM`) should run as separate jobs.
