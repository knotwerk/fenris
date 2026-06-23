# CarbonEngine Performance Dashboard Plan

## Purpose

Build an evidence dashboard that compares current CarbonEngine C++ behavior against Rust prototypes under original-use stress workloads. The dashboard must make correctness visible first; speedups are only valid when the matching parity gate passes.

## First View

The first viewport should be a dense benchmark console:

- Executive strip: tests passing, parity passing, best speedup, biggest bottleneck.
- Workload tabs: `resources`, `scheduler`, later `io`.
- Side-by-side C++ vs Rust bars for throughput, latency, memory, and CPU.
- Stress controls: file count, total bytes, chunk size, patch delta size, tasklet count, channel contention.
- Run metadata: repo SHA, build profile, compiler, rustc, CMake, host, command.

## Workloads

`resources`:

- resource group YAML import/export
- legacy CSV import/export
- create bundle
- unpack bundle
- create patch
- apply patch
- chunk index generation
- MD5/FNV checksums
- gzip compress/decompress
- legacy filter matching
- create resource group from directory
- CLI end-to-end flows

`scheduler`:

- tasklet switch throughput
- channel send/receive latency
- blocking/unblocking
- exception delivery
- callback dispatch
- multi-thread cleanup
- C API capsule smoke tests

## Benchmark JSON

Each benchmark runner should emit one JSON document per run:

```json
{
  "schema_version": 1,
  "component": "resources",
  "workload": "create_patch",
  "implementation": "cpp",
  "repo": "carbonengine/resources",
  "commit": "77d0867388370a31a2f78b9f2ddbcd23deec8bc1",
  "build_profile": "debug",
  "host": {
    "os": "linux",
    "cpu": "unknown",
    "ram_bytes": 0
  },
  "parameters": {
    "file_count": 1000,
    "total_bytes": 1073741824,
    "chunk_size": 1048576,
    "patch_delta_percent": 5
  },
  "correctness": {
    "parity_status": "pass",
    "test_command": "ctest --test-dir .cmake-build-linux-vcpkg-probe --output-on-failure",
    "tests_passed": 121,
    "tests_failed": 0
  },
  "metrics": {
    "duration_ms": 0,
    "throughput_bytes_per_sec": 0,
    "latency_p50_ms": 0,
    "latency_p95_ms": 0,
    "latency_p99_ms": 0,
    "peak_rss_bytes": 0,
    "cpu_user_ms": 0,
    "cpu_system_ms": 0
  }
}
```

Dashboard rule:

- If `correctness.parity_status != "pass"`, render charts in a warning state and suppress speedup badges.

## Implementation Phases

Phase 1: static dashboard.

- Add fixture JSON for one C++ `resources` run and placeholder Rust prototype runs.
- Build a Vite/React app that loads local JSON only.
- Add Playwright screenshots for desktop and compact widths.

Phase 2: benchmark runners.

- Initial samples are available from `cargo run -p xtask -- bench`, but they are not final broad speedup evidence. Current rows cover scheduler fixture execution, resource MD5, resource gzip, resource filter matching, and preliminary parity-checked create-group, create-group-from-filter, merge-group, diff-group, and remove-resources legacy CLI/Rust process comparisons.
- Wrap existing C++ CLI/library flows with timing and resource collection.
- Emit JSON from repeatable commands.
- Add Rust prototype runners with the same schema.

Phase 3: executive comparison.

- Add historical run comparison by commit.
- Add stress scale curves.
- Add bottleneck callouts generated from p95 latency, peak RSS, and throughput regressions.

## Parallel Workstreams

- Agent A: benchmark schema and C++ `resources` runner.
- Agent B: Rust prototype runner for matching `resources` workloads.
- Agent C: dashboard UI and static fixture ingestion.
- Agent D: scheduler workload definitions and Windows/macOS runner plan.
- Agent E: CI artifact publishing for JSON and screenshots.

## Acceptance Gates

- Dashboard loads without a backend.
- A failed parity run visibly blocks speedup claims.
- At least one `resources` workload compares C++ and Rust JSON.
- Playwright validates no blank charts and no overlapping UI at desktop and compact widths.
- Every chart links back to the command and commit that produced the data.
