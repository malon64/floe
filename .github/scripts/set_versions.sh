#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <version>" >&2
  exit 1
fi

version="$1"

VERSION="${version}" python - <<'PY'
import os
import re
from pathlib import Path

version = os.environ["VERSION"]

def replace_package_version(path: Path) -> None:
    content = path.read_text(encoding="utf-8")
    def repl(match: re.Match) -> str:
        return f'version = "{version}"'
    updated, count = re.subn(r'^version = ".*"$', repl, content, count=1, flags=re.M)
    if count == 0:
        raise SystemExit(f"no package version line found in {path}")
    path.write_text(updated, encoding="utf-8")

def replace_dep_version(path: Path) -> None:
    content = path.read_text(encoding="utf-8")
    pattern = r'(floe-core\\s*=\\s*\\{[^}]*version\\s*=\\s*\")([^\"]+)(\")'
    updated, count = re.subn(pattern, lambda m: f"{m.group(1)}{version}{m.group(3)}", content, count=1)
    if count == 0:
        raise SystemExit(f"no floe-core dependency version found in {path}")
    path.write_text(updated, encoding="utf-8")

replace_package_version(Path("crates/floe-core/Cargo.toml"))
replace_package_version(Path("crates/floe-cli/Cargo.toml"))
replace_dep_version(Path("crates/floe-cli/Cargo.toml"))
PY
