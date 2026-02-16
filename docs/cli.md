# CLI Usage

## Commands

- `floe validate -c <config> [--entities <name[,name...]>] [--output text|json]`
  - Validate YAML config and exit with non-zero code on errors.
  - `--output json` prints a single JSON object to stdout (for orchestrators / automation).

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

### Dry-run behavior

- Dry-run resolves input files/objects using the same planning path as real runs.
- For cloud sources, dry-run lists matching objects but does not download them.
- Console preview prints resolved file count and a capped list (`50` entries by default).
- Use `--verbose` to print the full resolved file list.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entities customer`
  - `floe validate -c example/config.yml --output json`

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
- Report output:
    - `example/report/run_<run_id>/run.summary.json`
    - `example/report/run_<run_id>/customer/run.json`

- Set a run id:
  - `floe run -c example/config.yml --entities customer --run-id dev-001`
