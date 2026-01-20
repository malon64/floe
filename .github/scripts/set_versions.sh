#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <version>" >&2
  exit 1
fi

version="$1"

sed -i -E "s/^version = \".*\"/version = \"${version}\"/" crates/floe-core/Cargo.toml
sed -i -E "s/^version = \".*\"/version = \"${version}\"/" crates/floe-cli/Cargo.toml
sed -i -E "s/(floe-core = \\{[^}]*version = \")[^\"]+\"/\\1${version}\"/" crates/floe-cli/Cargo.toml
