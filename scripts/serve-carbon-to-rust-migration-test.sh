#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
port="${1:-8765}"
selected_port=""

cd "${repo_root}"
python3 scripts/render-carbon-to-rust-migration-test.py
report_file="carbon-to-rust-migration-test.html"
guide_file="carbon-to-rust-reporting-guide.html"

for candidate in $(seq "${port}" "$((port + 50))"); do
  if python3 - "${candidate}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(1)
PY
  then
    selected_port="${candidate}"
    break
  fi
done

if [[ -z "${selected_port}" ]]; then
  echo "No free local port found from ${port} to $((port + 50))." >&2
  exit 1
fi

echo "Serving Carbon to Rust migration test at http://127.0.0.1:${selected_port}/${report_file}"
echo "Reporting guide at http://127.0.0.1:${selected_port}/${guide_file}"
cd "${repo_root}/target/carbon/report"
python3 -m http.server "${selected_port}" --bind 127.0.0.1
