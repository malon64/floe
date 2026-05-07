# New Catalog / Storage Integration Checklist

Use this skill when implementing a new catalog type or bumping a catalog library version in Floe.
Run through every section before opening a PR.

---

## 1. Shared builder — write path AND seed path

Both paths build the catalog independently. Any new option or bug fix must be applied to **both**:

- Write path: `crates/floe-core/src/io/write/iceberg/<type>.rs`
- Seed path:  `crates/floe-core/src/io/unique_seed/iceberg.rs`

If the same builder config appears in more than one place, extract a shared `build_<type>_catalog(...)` helper first.

---

## 2. Library internals — read the source before writing code

Before coding against a new or upgraded crate, skim its `tests/` or `examples/` for:

- Required builder calls (e.g. `with_storage_factory` in `iceberg-catalog-rest` 0.9.x)
- Property name conventions (e.g. `"token"` vs `"credential"` for bearer vs OAuth2)
- Any prefix stripping the caller must do (e.g. `client_credentials:` before inserting `"credential"`)

Document findings in a comment next to the builder so the next developer doesn't have to rediscover them.

---

## 3. Auth credential mapping — all prefix forms

When a config field encodes auth type via a prefix (e.g. `token:<pat>`, `client_credentials:<id>:<secret>`), verify all forms are handled:

- `token:` → library `"token"` property (bearer path)
- `client_credentials:` → stripped `"credential"` property (OAuth2 path)
- bare value → raw `"credential"` (fallback)

Cross-check: write path and seed path must map identically.

---

## 4. Validation audit — `src/config/validate.rs`

After any new catalog type or new config field, audit every gate that touches storage or catalog:

| Check | Glue | REST (no warehouse_storage) | REST (with warehouse_storage) |
|---|---|---|---|
| accepted_storage must be s3/gcs | yes | **no** — local allowed (Nessie/dev) | **no** — warehouse_storage is the table root |
| warehouse_storage must be s3/gcs | s3 only | n/a | yes |

The accepted_storage cloud check applies **only to Glue**. REST catalogs support local storage.
When `warehouse_storage` is set on a REST catalog the accepted_storage check is irrelevant.

---

## 5. StorageFactory — required for iceberg-catalog-rest ≥ 0.9

Every `RestCatalogBuilder` must call `.with_storage_factory(...)` before `.load()`.
Use `storage_factory_for_location` to pick local vs cloud based on the resolved table path.
Both the write builder and the seed builder need this.

---

## 6. Test coverage

For each new catalog variant add:

- Config parse test: valid YAML round-trips through `load_config`
- Validation positive test: accepted edge-case configs (local storage + REST, warehouse_storage + local accepted, etc.)
- Validation negative test: invalid combinations still return the right error
- Catalog resolver test: `resolve_iceberg_target` returns correct namespace / location

Run:
```
cargo fmt
cargo clippy -- -D warnings
cargo test -p floe-core --test unit -- unit::config
```
All must pass before pushing.
