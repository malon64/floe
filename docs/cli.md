# CLI Usage

## Commands

- `floe validate -c <config> [--entities <name[,name...]>]`
  - Validate YAML config and exit with non-zero code on errors.

- `floe run -c <config> [--run-id <id>] [--entities <name[,name...]>] [--log-format json|text]`
  - Execute ingestion using the config.
  - Run reports are written under `<report.path>/run_<run_id>/...` as defined in the config.
  - `-c` accepts local paths or cloud URIs (`s3://`, `gs://`, `abfs://`).
  - `--log-format json` emits NDJSON events to stdout (useful for orchestrators).
  - `--dry-run` resolves entity inputs and output targets without running ingestion.
  - `--quiet` reduces console output to run totals.
  - `--verbose` expands run output details.

- `floe manifest generate -c <config> --output <path|-> [--entities <name[,name...]>]`
  - Validate config and generate a common orchestrator manifest JSON (`schema=floe.manifest.v1`).
  - `--output -` writes manifest JSON to stdout.

- `floe add-entity -c <config> --input <path|file://...> [--format csv|json|parquet] [--name <entity>] [--domain <domain>]`
  - Infer a schema from an input file and append a new entity to `entities[]`.
  - If `-c` points to a missing file, Floe creates a minimal config and adds the entity.
  - If `--format` is omitted, Floe infers it from the input file extension (`.csv`, `.json`, `.parquet`).
  - If `--name` is omitted, Floe infers it from the input filename stem.
  - Generates defaults for `sink`, `policy`, and `schema.mismatch`.
  - JSON inference is v0.3 bootstrap-focused: top-level keys only; nested objects/arrays are inferred as `string`.
  - `--dry-run` prints the updated YAML without writing.
  - `--output <new.yml>` writes the updated config to a new file instead of overwriting `-c`.

- `floe state inspect -c <config> --entity <name>`
  - Print the resolved incremental state path for an entity and the current state JSON when present.
  - Helpful for confirming what Floe has already seen in `incremental_mode: file`.

- `floe state reset -c <config> --entity <name> --yes`
  - Remove the local state file for an entity.
  - Requires `--yes` because the next incremental run may reprocess previously tracked files.

### Dry-run behavior

- Dry-run resolves input files/objects using the same planning path as real runs.
- For cloud sources, dry-run lists matching objects but does not download them.
- Console preview prints resolved file count and a capped list (`50` entries by default).
- Use `--verbose` to print the full resolved file list.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entities customer`

- Run with default paths from the config:
  - `floe run -c example/config.yml --entities customer`
- Run with JSON logs:
  - `floe run -c example/config.yml --entities customer --log-format json`
- Dry-run preview:
  - `floe run -c example/config.yml --entities customer --dry-run`
- Generate common manifest:
  - `floe manifest generate -c example/config.yml --output orchestrators/airflow-floe/example/manifest.airflow.json`
- Print common manifest to stdout:
  - `floe manifest generate -c example/config.yml --output -`
- Add an entity from a CSV file:
  - `floe add-entity -c example/config.yml --input ./in/customers.csv --format csv --name customers`
- Bootstrap a new config file and infer name/format:
  - `floe add-entity -c new-config.yml --input ./in/orders.csv`
- Inspect incremental state:
  - `floe state inspect -c example/config.yml --entity customer`
- Reset incremental state:
  - `floe state reset -c example/config.yml --entity customer --yes`
- Report output:
  - `example/report/run_<run_id>/run.summary.json`
  - `example/report/run_<run_id>/customer/run.json`

- Set a run id:
  - `floe run -c example/config.yml --entities customer --run-id dev-001`
