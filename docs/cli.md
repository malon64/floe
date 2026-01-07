# CLI Contract (Draft)

## Commands

- `floe validate -c <config> [--entity <name>]`
  - Validate YAML config and exit with non-zero code on errors.

- `floe run -c <config> [--in <path>] [--out <path>] [--run-id <id>] [--entity <name>]`
  - Execute ingestion using the config, optionally overriding input/output paths.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entity customer`

- Run with default paths from the config:
  - `floe run -c example/config.yml --entity customer`

- Override input/output paths and set a run id:
  - `floe run -c example/config.yml --entity customer --in example/in/customer --out example/out --run-id dev-001`
