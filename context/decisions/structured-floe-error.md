# Decision: structured `FloeError` enum (incremental migration)

## Context

`floe-core` historically returned `FloeResult<T> = Result<T, Box<dyn Error + Send + Sync>>`
over four stringly-typed wrappers (`ConfigError`, `RunError`, `StorageError`,
`IoError` — each a `struct(String)`). Consumers (floe-python, the CLI logger,
orchestrators) could only classify failures by downcasting to those concrete
wrapper types or by inspecting message strings, and structured context (entity,
path, rule) was baked into formatted strings rather than carried as fields.
See issue #395.

## Decision

Introduce a `thiserror`-based `FloeError` **enum** in `crates/floe-core/src/errors.rs`
with typed variants (`Config`, `Validation`, `Storage`, `Sink`, `State`, `Run`,
`Io`) that carry structured fields (`entity`, `path`, `rule`, `message`). Each
variant's `Display` is exactly `{message}`, so output stays byte-compatible with
the legacy wrappers and existing exact-string test assertions still hold.

Migration is **incremental**, by design (a single 600+ site rewrite would be an
unreviewable rebase magnet):

- `FloeResult<T>` stays the boxed alias. `FloeError` is `Error + Send + Sync`, so
  it flows into `FloeResult` for free via `?`.
- The legacy wrapper structs remain for not-yet-migrated modules and convert into
  `FloeError` via `From` impls.
- Consumers recover the structured error with `err.downcast_ref::<FloeError>()`
  and match on `.kind()` (a `FloeErrorKind` discriminant). `to_py_err`
  (floe-python) and `error_code_for` (floe-cli) map the kind to the typed Python
  exception / CLI log code.

The four legacy wrappers (`ConfigError`/`RunError`/`StorageError`/`IoError`) have
been **removed**; every floe-core failure is constructed as a `FloeError` variant.
The mapping used during the full migration was: `ConfigError → FloeError::config`,
`RunError → FloeError::run`, `IoError → FloeError::io`, `StorageError →
FloeError::storage` (storage carries a structured `path` via `storage_at`). This is
behaviour-preserving — all four `config`/`validation`/`run`/`sink`/`state` and
`storage`/`io` kinds map to the same Python exception / log code the old wrappers
did, and `Display` is unchanged.

## Conventions for new error sites

1. Construct the matching variant: `FloeError::config(msg)`,
   `FloeError::storage_at(path, msg)`, `FloeError::run(msg)`, etc. Populate the
   structured fields (`entity` / `path` / `rule`) when the value is in scope.
2. **Return a bare `FloeError`, never `Box::new(FloeError)`.** In a function
   returning `FloeResult<T>`, `?` boxes the bare `FloeError` once (concrete type
   `FloeError`, so `downcast_ref` works). `Box::new(FloeError)` flowing through `?`
   double-boxes and defeats classification.
3. For a *tail-position* `map_err`/`ok_or_else` closure (no `?`) whose function
   returns `FloeResult<T>`, append `.into()` to the `FloeError` so it boxes into
   the `Box<dyn Error + Send + Sync>` return type.
4. To refine a `Run` site to `Sink` or `State` (both already map to the same
   consumer output), just use the more specific constructor.

## Status

Complete — full migration landed in one pass (no customers / no back-compat
constraint). `FloeResult<T>` remains the boxed alias so foreign errors keep
flowing through `?`; flipping it to a non-boxed `Result<T, FloeError>` would
require `From` impls for every foreign error type and is intentionally out of
scope.
