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

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entities customer`
  - `floe validate -c example/config.yml --output json`

- Run with default paths from the config:
  - `floe run -c example/config.yml --entities customer`
- Run with JSON logs:
  - `floe run -c example/config.yml --entities customer --log-format json`
- Report output:
    - `example/report/run_<run_id>/run.summary.json`
    - `example/report/run_<run_id>/customer/run.json`

- Set a run id:
  - `floe run -c example/config.yml --entities customer --run-id dev-001`
