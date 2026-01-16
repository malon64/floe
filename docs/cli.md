# CLI Contract (Draft)

## Commands

- `floe validate -c <config> [--entities <name[,name...]>]`
  - Validate YAML config and exit with non-zero code on errors.

- `floe run -c <config> [--run-id <id>] [--entities <name[,name...]>]`
  - Execute ingestion using the config.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entities customer`

- Run with default paths from the config:
  - `floe run -c example/config.yml --entities customer`

- Set a run id:
  - `floe run -c example/config.yml --entities customer --run-id dev-001`
