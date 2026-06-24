# CarbonEngine Rust Migration Kickoff Plan

## Summary

This is the live kickoff plan for migrating selected CarbonEngine components toward Rust while preserving existing behavior and public interfaces where that is the right engineering tradeoff.

Initial repository scope:

- Primary migration targets: `resources`, `scheduler`
- Required baseline dependencies: `core`, `vcpkg-registry`
- Early classification target: `io`
- Deferred: other CarbonEngine repositories unless dependency discovery pulls them into the first migration path

Default migration stance:

- Preserve current C++/CLI/Python interfaces where practical.
- Prefer an idiomatic Rust core plus a compatibility adapter when direct interface preservation would make the Rust implementation materially worse.
- Decide compatibility case by case through short research spikes.
- Do not count performance wins unless the corresponding correctness/parity tests pass.

## Two-Week Gates

### Gate 1: Repositories Pulled And Pinned

Clone targeted repositories under `carbonengine/`:

- `resources`
- `scheduler`
- `core`
- `vcpkg-registry`
- `io`

Record:

- Clone URL
- Default branch
- Checked-out commit SHA
- Submodule URLs and SHAs
- vcpkg baseline and custom registry baseline
- Important build/test entry points

Gate passes when the repositories are present locally and the baseline metadata is recorded in this workspace.

### Gate 2: Existing Tests Passing Or Classified

Run the upstream harness before doing Rust replacement work.

Preferred order:

- `core`: configure/build, then `CcpCoreTest` through CTest.
- `resources`: GTest/CTest suite covering library, CLI, bundle, patch, checksum, compression, filters, and fixtures.
- `scheduler`: C API GTest suite plus Python scheduler tests.
- `io`: classify socket/SSL/scheduler coupling; do not migrate unless it blocks scheduler work.

Known initial build constraint:

- This Linux workstation has CMake 3.22 from `/usr/bin/cmake`.
- The CarbonEngine repos declare CMake 3.30 or 3.31 and ship Windows/macOS presets.
- The first harness pass should either use an upgraded CMake locally, a container/toolchain image, or a Windows/macOS runner that matches upstream assumptions.

Gate passes when each primary repo has a repeatable command sequence and failures are either fixed or explicitly classified as environment, dependency, platform, or product behavior.

Current status on 2026-06-22:

- `resources`: green on Linux with vcpkg, `121/121` CTest tests passing.
- `scheduler`: native Linux source build now produces `_scheduler` and runs the unchanged Python unittest suite, `210/210` with `7` expected skips; upstream vcpkg presets remain Windows/macOS oriented and C API CTest still needs GTest.
- `core`: native Linux source build succeeds for the scheduler baseline with tests/docs/telemetry/memory tracking disabled; full test packaging still needs GTest integration.
- `io`: cloned for dependency classification; not in the first green gate.

### Gate 3: Rust Prototype Spikes

Prototype narrowly, with evidence.

- `resources` format spike: YAML/CSV resource group import/export, result codes, and fixture parity.
- `resources-tools` spike: MD5, FNV, rolling checksum, gzip, BSDIFF/BSPATCH, chunk index, and bundle streams.
- `scheduler` spike: tasklet/channel semantics, CPython/greenlet behavior, C API capsule compatibility, and callback semantics.
- build integration spike: CMake plus Cargo strategy, vcpkg packaging impact, and Python extension naming.

Each spike must end with one of these decisions:

- Keep the current interface directly.
- Build an idiomatic Rust core behind the current interface.
- Introduce a new Rust-facing API with a legacy compatibility adapter.

### Gate 4: CEO-Facing Performance Dashboard

Build a visual dashboard that compares the original C++ implementation against Rust prototypes under original-use stress workloads.

Dashboard goals:

- Make correctness status visible before speed claims.
- Show where Rust improves throughput, latency, memory behavior, and scaling.
- Make the remaining bottleneck obvious.
- Be polished enough for CCP/Fenris executive review without turning into a marketing page.

Hard rule:

- No chart is allowed to imply a Rust win unless the matching parity or golden fixture check passes.

## Parallel Agent Lanes

### Agent A: Repo And Harness

Responsibilities:

- Clone targeted repositories and initialize required submodules.
- Record pinned revisions and build metadata.
- Establish repeatable build/test commands.
- Own the "existing tests passing or classified" gate.

Deliverables:

- `docs/archive/baseline/carbonengine-baseline.md`
- `docs/archive/baseline/test-harness-status.md`
- Shell command transcript summaries with blockers and next actions

### Agent B: Resources Migration Research

Responsibilities:

- Map resource group YAML/CSV schemas.
- Map public result codes and error behavior.
- Inventory CLI operations and fixture expectations.
- Identify which interfaces should be preserved directly.

Candidate Rust crates:

- `serde`
- `serde_yaml`
- `csv`
- `thiserror`
- `camino`
- `rayon`
- `memmap2`
- `ahash`
- `bitvec`

Deliverables:

- `resources-migration-spike.md`
- First parity fixture list for Rust implementation

### Agent C: Resource Tools Spike

Responsibilities:

- Prototype checksums, compression, patching, chunk indexing, and stream behavior.
- Verify Carbon's custom BSDIFF header behavior before choosing a crate.
- Compare memory and streaming behavior against the C++ implementation.

Candidate Rust crates:

- `md-5`
- `flate2`
- `qbsdiff` or `bsdiff`
- `rayon`
- `memmap2`

Deliverables:

- `resources-tools-spike.md`
- Rust benchmark JSON schema for tools workloads

### Agent D: Scheduler Spike

Responsibilities:

- Research tasklet, channel, scheduler, callback, and exception semantics.
- Determine whether PyO3 alone is sufficient or whether direct `pyo3-ffi`/CPython API work is required.
- Preserve the C API capsule where practical.
- Identify where preserving Python compatibility would make Rust worse and propose adapter boundaries.

Candidate Rust crates:

- `pyo3`
- `pyo3-ffi`
- `bindgen`
- `cbindgen`
- `parking_lot`
- `crossbeam`
- `tokio` only if async IO integration requires it
- `intrusive-collections`
- `slotmap`

Deliverables:

- `scheduler-migration-spike.md`
- Scheduler parity scenario list

### Agent E: Dashboard And Benchmarks

Responsibilities:

- Define stress workloads.
- Build benchmark runners that emit JSON evidence.
- Build the Vite/React dashboard using local patterns from `duckdash` and `gridflow`.
- Make the dashboard useful for both engineers and executive review.

Dashboard metrics:

- Correctness/parity status
- Throughput
- p50/p95/p99 latency
- Peak and steady-state memory
- CPU utilization
- Speedup ratio
- Dataset scale
- Commit SHA and build profile

Dashboard workloads:

- `resources`: import/export, create bundle, unpack bundle, create patch, apply patch, chunk index, checksum, gzip, CLI end-to-end.
- `scheduler`: tasklet switch throughput, channel send/receive, blocking/unblocking, exception delivery, callbacks, multi-thread cleanup.
- `io`: optional later socket/SSL stress if scheduler migration touches tasklet-aware IO.

Deliverables:

- `docs/archive/planning/performance-dashboard-plan.md`
- benchmark JSON schema
- first static dashboard mock using fixture JSON

### Agent F: Build, Packaging, And CI

Responsibilities:

- Decide CMake/Cargo integration strategy.
- Preserve vcpkg packaging where current consumers depend on it.
- Define Windows/macOS CI gates once local commands are stable.
- Keep Linux support explicit rather than accidental.

Candidate approaches:

- `corrosion`
- `cargo-c`
- `maturin` for Python-extension delivery where appropriate
- CMake custom targets wrapping Cargo
- separate Rust workspace with C ABI artifacts consumed by CMake

Deliverables:

- `build-packaging-ci-plan.md`
- first CI matrix proposal

## Dashboard Visual Design

The first dashboard should be an operational benchmark console, not a landing page.

First viewport:

- Compact executive strip: tests passing, parity passing, best speedup, largest bottleneck.
- Workload selector tabs: `resources`, `scheduler`, later `io`.
- Side-by-side C++ vs Rust comparison for the selected workload.
- Stress controls: file count, file size, chunk size, patch delta size, tasklet count, channel contention.

Core views:

- Speedup bars per workload.
- Latency distribution with p50/p95/p99.
- Throughput over stress scale.
- Memory and CPU comparison.
- Correctness panel linking every chart to a parity test result.
- Run metadata: repo SHAs, build profile, host, compiler, rustc, CMake, command line.

Acceptance criteria:

- Dashboard can load static benchmark JSON without services.
- Dashboard can compare at least one C++ baseline run and one Rust prototype run.
- Dashboard clearly marks missing or failed parity evidence.
- Dashboard has Playwright screenshot coverage for desktop and compact widths.

## After Gates Pass

Plan the full migration in detail:

- Rust workspace layout and crate boundaries.
- Public API/ABI preservation list.
- Compatibility adapter boundaries.
- Golden fixture inventory and property tests.
- Performance budgets and regression thresholds.
- Packaging and CI rollout for Windows/macOS first.
- Whether Linux should become a supported platform or remain a local exploration environment.
- Replacement order for production code paths.

## Current Assumptions

- First planning horizon is two weeks.
- `resources` and `scheduler` are the primary migration targets.
- `core` is required for baseline builds.
- `io` is cloned and classified early but not migrated unless it blocks scheduler or dashboard stress realism.
- Compatibility is the default, but Rust code quality can justify adapters or a new Rust-facing API.
- Performance dashboard claims must be reproducible and backed by passing parity tests.

## Reference Inputs

- CarbonEngine org: https://github.com/carbonengine
- `resources`: https://github.com/carbonengine/resources
- `scheduler`: https://github.com/carbonengine/scheduler
- `core`: https://github.com/carbonengine/core
- `io`: https://github.com/carbonengine/io
- `vcpkg-registry`: https://github.com/carbonengine/vcpkg-registry
- Local Rust references: `/data/repos/trade/trade-ranker`, `/data/repos/trade/trade-tools/tools/fast_llm`, `/data/repos/trade/duckdelta`, `/data/repos/trade/gridflow`, `/data/repos/trade/duckdash`
