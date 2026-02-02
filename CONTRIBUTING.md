# Contributing to Floe

Thanks for helping improve Floe! This guide keeps collaboration smooth and PRs easy to review.

## Quick start

- Fork the repo and create a branch from `main`.
- Keep PRs small and focused.
- Follow the testing expectations below.

## Development workflow

1) Create a branch:

```bash
git switch -c feature/your-change
```

2) Make changes with minimal scope.

3) Run checks (required):

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

4) Run tests (required):
- Unit tests for the modules you touched
- Add a simple local integration test if behavior crosses modules

Example:

```bash
cargo test -p floe-core --test unit io::storage::
cargo test -p floe-core --test integration
```

5) Open a PR with a clear summary and test list.

## Coding guidelines

- Prefer small, composable modules over large functions.
- Add short comments only where logic is non-obvious.
- Avoid breaking changes without a migration note.
- Update docs when behavior/config changes.

## PR expectations

- Summary + scope + risks
- Tests run
- Docs updated
- No secrets in logs or fixtures

## Reporting issues

Please use the issue templates:
- Feature request
- Bug report

They capture required details and success criteria.

## Release notes

If a change is user-facing, update `CHANGELOG.md`.

## Code owners (optional)

Some repos use CODEOWNERS to require specific reviewers for certain areas.
If we add it later, you may see auto-assigned reviewers based on files changed.
