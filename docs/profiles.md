# Environment Profiles

Environment Profiles let you define per-environment configuration values
(variables, runner settings, validation strictness) in a standalone YAML file.
Profile variables feed into the same `{{VAR}}` placeholder substitution used in
Floe config files, giving you a clean separation between logic (the config) and
environment-specific values (the profile).

## Machine-readable schema

A JSON-Schema-compatible YAML schema is provided at [`profile.schema.yaml`](../profile.schema.yaml)
in the repository root.  It covers every field defined in this document with
types, `required` / `additionalProperties` constraints, and inline descriptions.

**AI-assisted authoring** — paste the contents of `profile.schema.yaml` into
your AI assistant (or point it at the file) and ask it to generate a valid
profile for your environment.  The schema enumerates all allowed values (e.g.
`runner.type: local`) and marks required fields, so generated output can be
validated immediately with `cargo test` or a JSON Schema validator.

## Schema v1

```yaml
apiVersion: floe/v1          # required; must be exactly "floe/v1"
kind: EnvironmentProfile     # required; must be exactly "EnvironmentProfile"
metadata:                    # required
  name: dev                  # required; non-empty string identifier
  description: "..."         # optional
  env: dev                   # optional; free-form environment label
  tags: [development, local] # optional; string list
execution:                   # optional
  runner:
    type: local              # required when execution is present; currently only "local"
variables:                   # optional; flat string → string map
  KEY: value
validation:                  # optional
  strict: false              # optional bool; default behaviour is determined by the config
```

### Required fields

| Field | Notes |
|-------|-------|
| `apiVersion` | Must equal `floe/v1`. |
| `kind` | Must equal `EnvironmentProfile`. |
| `metadata.name` | Non-empty string. |

All other fields are optional.

---

## Variable precedence

When variable sources are merged at runtime the following precedence applies
(highest wins):

```
config variables  >  CLI overrides  >  profile variables
```

- **Profile variables** – defined in `variables:` of the profile file.
- **CLI overrides** – supplied via `--var KEY=VALUE` (future CLI flag).
- **Config variables** – defined in `env.vars:` of the main Floe YAML config.

Unresolved `${VAR}` placeholders in variable values are detected at parse/
validation time and produce an immediate error.

---

## Examples

### dev.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
  env: dev
execution:
  runner:
    type: local
variables:
  CATALOG: dev_catalog
  SCHEMA: dev_bronze
  TABLE_PREFIX: dev
  INCOMING_DIR: /tmp/floe/incoming
  OUTPUT_DIR: /tmp/floe/output
  REPORT_DIR: /tmp/floe/reports
validation:
  strict: false
```

### uat.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: uat
  env: uat
execution:
  runner:
    type: local
variables:
  CATALOG: uat_catalog
  SCHEMA: uat_bronze
  TABLE_PREFIX: uat
  INCOMING_DIR: /data/uat/incoming
  OUTPUT_DIR: /data/uat/output
  REPORT_DIR: /data/uat/reports
validation:
  strict: true
```

### prod.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
  env: prod
execution:
  runner:
    type: local
variables:
  CATALOG: prod_catalog
  SCHEMA: prod_bronze
  TABLE_PREFIX: prod
  INCOMING_DIR: /data/prod/incoming
  OUTPUT_DIR: /data/prod/output
  REPORT_DIR: /data/prod/reports
validation:
  strict: true
```

---

## Using profile variables in a Floe config

Profile variables are consumed by the standard `{{VAR}}` placeholder syntax in
the main config file.  The example below shows how catalog and schema values
defined in a profile can drive path construction.

```yaml
version: "0.1"
report:
  path: "{{REPORT_DIR}}"
entities:
  - name: customers
    source:
      format: csv
      path: "{{INCOMING_DIR}}/customers.csv"
    sink:
      accepted:
        format: parquet
        path: "{{OUTPUT_DIR}}/{{TABLE_PREFIX}}_customers.parquet"
    policy:
      severity: warn
    schema:
      columns:
        - name: customer_id
          type: string
          nullable: false
```

---

## Validation errors

The parser and validator return actionable errors for common problems:

| Situation | Error message (example) |
|-----------|------------------------|
| Wrong `apiVersion` | `profile.apiVersion: expected "floe/v1", got "floe/v2"` |
| Wrong `kind` | `profile.kind: expected "EnvironmentProfile", got "SomethingElse"` |
| Missing `metadata` | `profile.metadata is required` |
| Missing `metadata.name` | `profile.metadata.name is required` |
| Unknown field | `unknown field profile.unknownField` |
| Unknown runner | `profile.execution.runner.type: unknown runner "kubernetes"; known runners: local` |
| Unresolved variable | `profile variable "PATH_VAR" contains unresolved placeholder: ${UNDEFINED}` |

---

## File layout convention

Place profiles alongside the main config or in a dedicated `profiles/`
directory:

```
my-project/
├── config.yml          # main Floe config
└── profiles/
    ├── dev.yml
    ├── uat.yml
    └── prod.yml
```

See `example/profiles/` in this repository for ready-to-use templates.
The machine-readable field reference is in `profile.schema.yaml` at the repo root.
