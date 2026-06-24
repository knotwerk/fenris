# CarbonEngine Test Harness Status

Date: 2026-06-22

## Summary

Local repo pull and first harness pass are complete.

- `resources`: builds on Linux with vcpkg and passes `121/121` CTest tests.
- `scheduler`: native Linux source build now imports `_scheduler` and runs the unchanged Python unittest suite, `210/210` with `7` expected skips; C API CTest still needs GTest coverage before Linux is a complete upstream gate.
- `core`: native Linux source build succeeds for the scheduler baseline with tests/docs/telemetry/memory tracking disabled; full standalone test packaging still needs GTest integration.
- `io`: cloned for dependency classification; not promoted into the first migration gate yet.

## Resources Gate

Status: green on this Linux host.

Configure:

```sh
cmake -S . -B .cmake-build-linux-vcpkg-probe -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON \
  -DBUILD_DOCUMENTATION=OFF \
  -DCMAKE_TOOLCHAIN_FILE=vendor/github.com/microsoft/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_OVERLAY_TRIPLETS=vendor/github.com/carbonengine/vcpkg-registry/triplets \
  -DVCPKG_USE_HOST_TOOLS=ON
```

Build:

```sh
cmake --build .cmake-build-linux-vcpkg-probe --target resources-test resources-cli --parallel
cp .cmake-build-linux-vcpkg-probe/cli/resources_debug .cmake-build-linux-vcpkg-probe/tests/resources_debug
```

Test:

```sh
ctest --test-dir .cmake-build-linux-vcpkg-probe --output-on-failure
```

Final result:

```text
100% tests passed, 0 tests failed out of 121
Total Test time (real) = 35.20 sec
```

Local fixes made to reach the green gate:

- Added missing standard-library includes required by GCC.
- Enabled Linux `stat`-based `BinaryOperation` capture.
- Added Linux resource-group goldens for `100664` mode values.
- Fixed case-sensitive fixture path references in tests.
- Added case-insensitive local path resolution fallback for reads/writes/removes where fixture metadata depends on the macOS/Windows behavior.
- Hardened `FileDataStreamIn::StartRead` so missing paths, directories, and invalid file sizes fail cleanly.
- Made test file comparisons use the same local file-loading behavior as the library.
- Added a regression test for `CreateBundle` failing when resource source files are missing.
- Fixed `CreateBundle` so a failed resource stream open returns the real error instead of producing an empty successful bundle.

## Scheduler Gate

Status: Python baseline green on native Linux; full C API CTest baseline still open.

Findings:

- `CMakePresets.json` only exposes Windows/macOS presets.
- Main target builds the CPython extension `_scheduler`.
- C API gate is `SchedulerCapiTest`.
- Python tests are under `tests/python/scheduler/tests`.
- `python3 discover.py` in `tests/python/scheduler` passes and finds `210` tests.
- `python3 -m unittest discover -v` fails without the built `_scheduler` extension.
- `cargo run -p xtask -- legacy-scheduler native-linux` builds `carbonengine/core` and `carbonengine/scheduler` directly against the host Python/greenlet package, then runs the unchanged Python unittest suite.
- vcpkg Linux configure remains blocked by `carbon-core` unsupported or broken on `x64-linux`.
- Direct scheduler CMake `BUILD_TESTING=ON` still needs a GTest package before C API CTest can be included.

Scheduler C API migration gate must therefore run on Windows/macOS CI first, or the Linux native path needs GTest-backed C API CTest coverage.

## Core Gate

Status: partially classified.

Plain configure with local CMake reaches:

```text
tests/CMakeLists.txt:5 find_package(GTest)
```

Next step is to run `core` with vcpkg on a supported preset runner or deliberately add a Linux probe gate.

## Rust Migration Readiness

Ready now:

- Use `resources` CTest as a parity gate for Rust spikes.
- Extract fixture-level parity lists from the now-green `resources` test suite.
- Start Rust `resources-tools` prototypes for checksum, gzip, patch, chunking, bundle, and resource-group import/export.

Blocked or classified:

- Scheduler Rust parity requires Windows/macOS extension builds and the `_scheduler` module.
- Scheduler C API capsule compatibility must be tested before replacing internals.
- Performance dashboard can use static fixture JSON immediately, but live C++/Rust comparisons need repeatable benchmark runners.
