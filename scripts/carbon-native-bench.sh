#!/usr/bin/env bash
set -euo pipefail

if [[ " ${RUSTFLAGS:-} " != *" -C target-cpu=native "* ]]; then
  export RUSTFLAGS="${RUSTFLAGS:-} -C target-cpu=native"
fi
export CARBON_NATIVE_BENCH=1

# Optional legacy baseline overrides for optimized C++ resource comparisons:
#   CARBON_LEGACY_RESOURCES_CLI=/path/to/release/cli/resources
#   CARBON_LEGACY_RESOURCES_DEV_CLI=/path/to/devfeatures/release/cli/resources
# xtask records the selected CMake build type and keeps rows observed-only until
# the selected legacy binaries are known non-debug baselines.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_resources_cli="${repo_root}/carbonengine/resources/.cmake-build-linux-vcpkg-release/cli/resources"
default_resources_dev_cli="${repo_root}/carbonengine/resources/.cmake-build-linux-vcpkg-release-devfeatures/cli/resources"
if [[ -z "${CARBON_LEGACY_RESOURCES_CLI:-}" && -x "${default_resources_cli}" ]]; then
  export CARBON_LEGACY_RESOURCES_CLI="${default_resources_cli}"
fi
if [[ -z "${CARBON_LEGACY_RESOURCES_DEV_CLI:-}" && -x "${default_resources_dev_cli}" ]]; then
  export CARBON_LEGACY_RESOURCES_DEV_CLI="${default_resources_dev_cli}"
fi

command="${1:-all}"
shift || true

run_xtask() {
  cargo run --profile release-native -p xtask -- "$@"
}

case "${command}" in
  all | native-evidence)
    run_xtask bench "$@"
    run_xtask bench-scheduler-comparison --workload-set all --tier quick --samples 10
    run_xtask bench-scalability --tier quick --families scheduler,io,data --samples 5
    run_xtask io-workloads "$@"
    run_xtask report-progress
    ;;
  *)
    run_xtask "${command}" "$@"
    ;;
esac
