# Floe — How to do a version bump PR

This document describes the exact steps to bump Floe to a new version and open a release PR. Follow them in order.

---

## 1. Decide the new version

Floe follows semantic versioning. The config schema version (`version: "0.x"` in YAML) is versioned independently and only changes when the config surface changes in a user-visible way.

| Change | Version bump |
|---|---|
| Breaking config or API change | minor (`0.X.0`) |
| New feature, backward-compatible | minor (`0.X.0`) or patch (`0.x.Y`) |
| Bug fix, internal refactor, test, doc | patch (`0.x.Y`) |

When in doubt: if a user upgrading the binary needs to change their config YAML or manifest JSON, it's a minor bump.

---

## 2. Create the branch

```bash
git checkout main && git pull
git checkout -b chore/bump-v<NEW_VERSION>
```

---

## 3. Bump versions in all three `Cargo.toml` files and `pyproject.toml`

Files to update — change `version = "..."` **and** the `floe-core` dependency version in the crates that reference it:

| File | Fields |
|---|---|
| `crates/floe-core/Cargo.toml` | `version` |
| `crates/floe-cli/Cargo.toml` | `version`, `floe-core` dependency version |
| `crates/floe-python/Cargo.toml` | `version`, `floe-core` dependency version |
| `crates/floe-python/pyproject.toml` | `version` (the `[project]` version — controls the PyPI wheel version) |

There is no workspace-level `version` field — each crate is bumped independently but all four files must stay in sync.

---

## 4. Update `Cargo.lock`

```bash
cargo check -p floe-core -p floe-cli -p floe-python
```

`cargo check` is sufficient — it resolves dependencies and rewrites `Cargo.lock` without a full compile. Do not run `cargo generate-lockfile` alone as it does not validate crate graph consistency.

---

## 5. Write the `CHANGELOG.md` entry

Add a new `## vX.Y.Z` section **at the top** of `CHANGELOG.md` (below the file header, above the previous version). Structure:

- One bullet per user-visible change group (feature, fix, behaviour change).
- Sub-bullets for details and caveats.
- Reference the relevant doc file in parentheses where applicable.
- "Internal hardening" section at the end for refactors with no config/API changes — still worth documenting so the diff is traceable.

Source material: `git log <previous-tag>..HEAD --oneline` gives the full list of commits since the last release. Read the commit bodies (not just summaries) for the detailed changes.

---

## 6. Update documentation

For each user-visible change, check whether the matching doc file needs updating:

| Change | Doc file |
|---|---|
| New config field | `docs/config.md` |
| New sink behaviour | `docs/sinks/<format>.md` |
| Lineage changes | `docs/lineage.md` |
| Incremental state changes | `docs/how-it-works.md` |
| PII changes | `docs/pii.md` |
| Manifest / orchestrator changes | `context/orchestrators.md` |
| Profile/variable changes | `docs/profiles.md`, `docs/variables.md` |
| CLI changes | `docs/cli.md` |

Also update `context/architecture.md` if a new key abstraction was added or removed, and `context/project.md` if the supported formats or storages table changed.

---

## 7. Run CI checks locally

All three must pass before pushing:

```bash
cargo fmt --all
cargo clippy -p floe-core -p floe-cli -p floe-python -- -D warnings
cargo test -p floe-core
```

---

## 8. Commit and push

```bash
git add crates/floe-core/Cargo.toml \
        crates/floe-cli/Cargo.toml \
        crates/floe-python/Cargo.toml \
        crates/floe-python/pyproject.toml \
        Cargo.lock \
        CHANGELOG.md \
        docs/ \
        context/
git commit -m "chore: bump all crates to <NEW_VERSION> and update docs"
git push -u origin chore/bump-v<NEW_VERSION>
```

---

## 9. Open the PR

Title: `chore: bump all crates to v<NEW_VERSION>`

PR body template:

```markdown
## Summary
- Bump `floe-core`, `floe-cli`, `floe-python` from `<OLD>` to `<NEW>`.
- Update `CHANGELOG.md` with v<NEW_VERSION> entry.
- Update docs: <list doc files touched>.

## Checklist
- [ ] All three `Cargo.toml` versions updated (floe-core, floe-cli, floe-python)
- [ ] `crates/floe-python/pyproject.toml` version updated
- [ ] `Cargo.lock` updated via `cargo check`
- [ ] `CHANGELOG.md` entry written
- [ ] Docs updated for all user-visible changes
- [ ] `cargo fmt`, `clippy`, `cargo test -p floe-core` all pass
```

Merge target: `main`. No feature flags needed — this is a pure version bump.

---

## Notes

- The Python orchestrator packages (`dagster-floe`, `airflow-floe`) have their own `pyproject.toml` versions and are bumped separately when they have changes. A Rust-only bump does not require bumping the Python packages.
- The `floe-python` PyPI wheel version is derived from its `Cargo.toml` version — bumping it here also bumps the published Python package.
- GitHub releases and PyPI publishes are triggered by CI on merge to `main` — no manual tagging required.
