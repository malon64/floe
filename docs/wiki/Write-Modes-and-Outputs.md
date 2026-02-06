# Write Modes and Outputs

Floe writes accepted data (Parquet or Delta), rejected rows (CSV), and reports (JSON). Write mode is configured at `sink.write_mode` and applies to accepted and rejected outputs.

## Write modes

- `overwrite`: remove existing dataset parts and write new parts.
- `append`: add new parts without deleting existing ones.

## Accepted outputs

- Parquet: multipart output (`part-...parquet`) with optional chunking via `sink.accepted.options.max_size_per_file`.
- Delta: transactional table (`_delta_log`) using object_store for cloud backends.

## Rejected outputs

- CSV parts are written as dataset-style parts (`part-...csv`).
- Rejected rows include `__floe_row_index` and `__floe_errors` columns.

## Reporting

Reports are written under `report.path/run_<run_id>/`.

- `run.summary.json`
- `<entity>/run.json`

See `docs/report.md` and `docs/logging.md` for details.
