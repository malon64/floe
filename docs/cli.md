# CLI Contract (Draft)

## Commands

- `floe validate -c <config>`
  - Validate YAML config and exit with non-zero code on errors.

- `floe run -c <config> [--in <path>] [--out <path>] [--run-id <id>]`
  - Execute ingestion using the config, optionally overriding input/output paths.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml`

- Run with default paths from the config:
  - `floe run -c example/config.yml`

- Override input/output paths and set a run id:
  - `floe run -c example/config.yml --in example/in/customer --out example/out --run-id dev-001`
