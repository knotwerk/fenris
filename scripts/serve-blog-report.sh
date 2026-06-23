#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
port="${1:-8765}"

cd "${repo_root}/target/carbon/report"
python3 -m http.server "${port}" --bind 127.0.0.1
