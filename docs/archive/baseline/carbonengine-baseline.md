# CarbonEngine Baseline

Generated from local checkouts in `/data/repos/fenris/carbonengine`.

## Host Toolchain

- OS: Linux `x86_64`, Ubuntu kernel `6.8.0-111-generic`
- Git: `2.34.1`
- System CMake: `3.22.1`
- Active local CMake: `3.31.10` from `/home/helgi/.local/bin/cmake`
- Ninja: `1.11.1.git.kitware.jobserver-1`
- Python: `3.12.4`
- Rust: `rustc 1.86.0`, `cargo 1.86.0`
- C/C++ compiler: GCC `11.4.0`

Important constraint:

- `resources`, `scheduler`, and `io` require CMake `3.30+` or `3.31+`.
- The shipped CMake presets target Windows and macOS only.
- Linux should be treated as an exploration host until explicitly supported.

## Repository Pins

| Repo | Origin | Branch | Commit | Latest commit summary |
| --- | --- | --- | --- | --- |
| `resources` | `https://github.com/carbonengine/resources.git` | `main` | `77d0867388370a31a2f78b9f2ddbcd23deec8bc1` | `2026-06-18 Merge pull request #24 from CCPCookies/CreateBundleFix` |
| `scheduler` | `https://github.com/carbonengine/scheduler.git` | `main` | `d1fa83bac1908cab78143642a2253a832e3ccb5d` | `2026-03-16 Merge pull request #36 from ccptoebeans/kotlin_token` |
| `core` | `https://github.com/carbonengine/core.git` | `main` | `b3774d4881d7459f07f31f2e21f80cd292793c36` | `2026-06-15 Merge pull request #23 from carbonengine/tracy-test-client-improvements` |
| `vcpkg-registry` | `https://github.com/carbonengine/vcpkg-registry.git` | `main` | `3310df1918db850480f42433337d24d97ee5c165` | `2026-06-22 Add rules to build libogg and libvorbis statically. (#94)` |
| `io` | `https://github.com/carbonengine/io.git` | `main` | `5c4c669f6ebbda56996f1326315222dae9bf281e` | `2026-05-21 Merge pull request #16 from carbonengine/clean-license-split` |

## Submodule Pins

| Parent repo | Submodule path | Commit |
| --- | --- | --- |
| `resources` | `vendor/github.com/carbonengine/vcpkg-registry` | `de86dcad60458ef170911adb5c42a053fc5d9117` |
| `resources` | `vendor/github.com/microsoft/vcpkg` | `a7d06b3a72d5ec48353bacb84152bd027ee9999b` |
| `scheduler` | `vendor/github.com/carbonengine/vcpkg-registry` | `de86dcad60458ef170911adb5c42a053fc5d9117` |
| `scheduler` | `vendor/github.com/microsoft/vcpkg` | `11be7f536538ace43f867c8efb3d624c43b6af54` |
| `core` | `vendor/github.com/carbonengine/vcpkg-registry` | `de86dcad60458ef170911adb5c42a053fc5d9117` |
| `core` | `vendor/github.com/microsoft/vcpkg` | `b2c74683ecfd6a8e7d27ffb0df077f66a9339509` |
| `io` | `vendor/github.com/carbonengine/vcpkg-registry` | `de86dcad60458ef170911adb5c42a053fc5d9117` |
| `io` | `vendor/github.com/microsoft/vcpkg` | `460551b0ec06be1ba6b918448bf3b0f44add813d` |

Submodules were initialized with an HTTPS rewrite for `git@github.com:` URLs.

## Build And Test Entry Points

### `core`

- CMake root: `carbonengine/core/CMakeLists.txt`
- Minimum CMake in root: `3.19`
- Presets file requires: `3.31`
- Project: `CcpCore`
- Tests: `tests/CMakeLists.txt`, target `CcpCoreTest`
- vcpkg dependencies: `tracy`, `gtest`, `lz4`, `python3`

### `resources`

- CMake root: `carbonengine/resources/CMakeLists.txt`
- Minimum CMake: `3.31.0`
- Project: `resources 4.3.1`
- Tests: `tests/CMakeLists.txt`, target `resources-test`
- CLI: `resources-cli`, output name `resources`
- vcpkg dependencies: `argparse`, `bsdiff-drake127`, `cryptopp`, `curl`, `inih`, `yaml-cpp`, `zlib`
- Test features: `gtest`, `tiny-process-library`

### `scheduler`

- CMake root: `carbonengine/scheduler/CMakeLists.txt`
- Minimum CMake: `3.30.1`
- Project: `Scheduler`
- Tests: `tests/capiTest/CMakeLists.txt`, target `SchedulerCapiTest`
- Python tests: `tests/python/scheduler/tests`
- vcpkg dependencies: `python3`, `greenlet`, `carbon-core`, `gtest`

### `io`

- CMake root: `carbonengine/io/CMakeLists.txt`
- Minimum CMake: `3.30`
- Project: `io 1.0.0`
- Tests: C++ socket unit tests and Python `carboniotests`
- vcpkg dependencies: `gtest`, `carbon-scheduler`, `libuv`, `python3`, `carbon-core`, `openssl`
- Role in first pass: classify dependency and scheduler coupling; do not migrate unless required.

## Upstream Preset Shape

Primary presets are platform-gated:

- Windows: `x64-windows-internal`, `x64-windows-release`, `x64-windows-debug`, `x64-windows-trinitydev`
- macOS arm64: `arm64-osx-internal`, `arm64-osx-release`, `arm64-osx-debug`, `arm64-osx-trinitydev`
- macOS x64: `x64-osx-internal`, `x64-osx-release`, `x64-osx-debug`, `x64-osx-trinitydev`

`resources` also has `x64-windows-release-with-dev-features`.

## Immediate Implications

- Local Linux can inspect and prototype Rust code immediately.
- `resources` now has a repeatable Linux vcpkg probe and green CTest gate: `121/121` tests passing.
- Faithful upstream C++ test execution likely needs either:
  - a Windows/macOS runner matching the presets, or
  - a deliberate Linux build adaptation spike.
- For migration credibility, the first dashboard should display "baseline unavailable/classified" rather than hiding missing C++ runs when the host cannot execute an upstream harness.
