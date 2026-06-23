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
  (floe-python) and `error_code_for` (floe-cli) check the enum first, then fall
  back to the legacy wrapper checks — so behaviour is identical whether a module
  has been migrated or not.

The **storage subsystem** (`io/storage/providers/*`, `io/storage/ops/output.rs`,
the storage path of `io/write/parquet.rs`) is the first migrated slice: it is
cohesive, has zero exact-string test coupling, and is exactly the kind of failure
(not-found / conflict / auth) consumers most want to classify.

## How to migrate the next module

1. Replace `Box::new(XError(msg))` constructions with the matching `FloeError`
   constructor (`FloeError::config(msg)`, `FloeError::storage_at(path, msg)`,
   etc.). Keep the message string identical to preserve `Display`.
2. Populate the structured fields (`entity` / `path` / `rule`) where the value is
   already in scope — that is the whole point.
3. Drop the now-unused legacy wrapper import; add `use crate::errors::FloeError;`.
4. If a consumer needs to classify the new failures specially, the enum mapping in
   `to_py_err` / `error_code_for` already routes by `kind()` — no change needed
   unless a new `FloeErrorKind` arm is added.
5. Once a wrapper type has no remaining construction sites anywhere, delete the
   struct and its `From` impl.

## Status

- Foundation + storage subsystem migrated (this PR).
- Remaining: `config` (the largest, ~350 sites — string-assertion sensitive, do
  carefully), `run`, `state`, and the read/write `IoError` sites. One module per
  follow-up PR.
