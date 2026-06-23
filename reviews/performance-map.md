# Performance Map

Performance claims must be attached to parity status. If parity fails or has not run for the same workload, store the benchmark but mark it `not comparable`.

## Required Metadata

Every benchmark sample must record:

- component, workload, implementation, commit, build profile;
- host OS, CPU, RAM, compiler, CMake, rustc/cargo;
- Rust build mode, `RUSTFLAGS`, and whether `target-cpu=native` was active;
- selected legacy binary path, CMake build type, and non-debug detector status for every process-level speedup candidate;
- workload parameters;
- parity status and test command;
- repeated sample count and variance;
- throughput, latency, CPU, memory, and allocation metrics where available.

## Scheduler Workloads

| Workload | Legacy source | Rust target | Primary metric | Supporting metrics | Comparable gate |
| --- | --- | --- | --- | --- | --- |
| tasklet FIFO run | `test_scheduler.py::test_scheduler_run_order` | fixture runner | completed tasklets/sec | switches/sec, p95 dispatch latency | `run_order.json` passes both implementations. |
| targeted `tasklet.run()` | `test_scheduler.py::test_tasklet_run_order*` | fixture runner | completed tasklets/sec | queue depth, switches/sec | Targeted run fixtures pass. |
| schedule/yield/remove | `TestSchedule*` | fixture runner | dispatches/sec | p50/p95 yield latency | Schedule fixtures pass. |
| channel blocking | `test_blocking_send`, `test_blocking_receive` | fixture runner | channel ops/sec | blocked queue mutation latency | Blocking fixtures pass. |
| channel ping-pong | send/receive matching tests | benchmark runner | round trips/sec | p50/p95/p99 wake latency | Transfer and preference fixtures pass. |
| QueueChannel buffered ops | `test_queuechannel.py` | fixture/bridge runner | buffered enqueue/dequeue ops/sec | p95 enqueue latency, allocation count | QueueChannel fixture or bridge tests pass. |
| deadlock/block trap rejection | deadlock and trap tests | fixture runner | rejected ops/sec | state mutation count must be zero | Deadlock/trap fixtures pass. |
| C ABI calls | `SchedulerCapiTest` | FFI benchmark | ABI calls/sec | invalid-handle cost, panic containment overhead | C API tests pass. |
| IO request cycles | `carbonengine/io` socket/SSL tests | workload runner | request cycles/sec | wake latency, loopback throughput | IO semantic traces pass. |

## Resources Workloads

| Workload | Legacy source | Rust target | Primary metric | Supporting metrics | Comparable gate |
| --- | --- | --- | --- | --- | --- |
| directory discovery/import | `ResourceGroupImpl::CreateFromDirectory` | resource model/pipeline | files discovered/sec | peak RSS, rows/sec | YAML/CSV fixtures pass. |
| YAML/CSV import/export | library and CLI tests | compat crate | manifest rows/sec | parse latency, allocations | Output compatibility checks pass. |
| MD5/FNV/rolling checksum | `ResourceTools.cpp`, checksum streams | tools crate | bytes hashed/sec | CPU scaling, allocations | Digest identity tests pass. |
| gzip compress/decompress | gzip streams | tools crate | bytes compressed/sec | compressed size, p95 chunk latency | Compression identity tests pass. |
| create bundle | bundle CLI/library tests | bundle pipeline | bytes bundled/sec | chunk throughput, peak RSS | Bundle output parity passes. |
| unpack bundle | bundle fixtures | bundle pipeline | bytes unpacked/sec | checksum validation time | Unpacked file parity passes. |
| create patch | patch CLI/library tests | patch pipeline | bytes patched/sec | patch size, CPU scaling | Patch generation compatibility passes. |
| apply patch | patch fixtures | patch pipeline | bytes applied/sec | temp disk use, failure cleanup latency | Patch application parity passes. |
| filter evaluation | filter tests | filter engine | paths matched/sec | compiled filter build time | Filter parity passes. |
| local object-store remote simulation | downloader tests to be added | remote crate | upload/download bytes/sec | retry/cancel latency | Local remote parity tests pass. |

## Benchmark Tiers

| Tier | Status | Required before |
| --- | --- | --- |
| Tier 1 local baseline | partial | Any local throughput/speedup claim. |
| Tier 1 native release baseline | partial | Native/profile/LTO context for current narrow comparable rows; optimized legacy baseline detector must be green before speedup claims; SIMD/Rayon/Tokio claims still need implementation and stage-specific evidence. |
| Tier 2 local network simulation | open | IO realism claims involving loopback sockets or containers. |
| Tier 3 multi-host network | future | Cross-network production gain claims. |

## Current Claim State

Initial benchmark samples exist in `target/carbon/evidence/bench-tier-local.json` for scheduler fixture execution, a process-measured Rust scheduler fixture row, resource MD5, resource gzip, resource filter matching, create-group directory export, create-group-from-filter YAML export, merge-group YAML additive export, diff-group CSV additions export, remove-resources YAML export, create-bundle local-CDN output, create-patch local-CDN output, unpack-bundle local-CDN output, and apply-patch local-CDN output. The scheduler process row records native-build wall time, p50/p95/p99 fixture latency, effective CPU burn, CPU percent, peak RSS, throughput, and a conservative 100k-unit linear estimate, but is marked `rust_scheduler_process_not_legacy_comparable` because there is no matched legacy scheduler process baseline. The create-group, create-group-from-filter, merge-group, diff-group, remove-resources, create-bundle, create-patch, unpack-bundle, and apply-patch rows verify byte-for-byte parity before timing legacy CLI and Rust process runs, record raw samples plus min/mean/p50/p95/max summaries, and are marked `comparable_process_to_process`; those comparable rows include throughput, wall time, p50/p95 latency, effective CPU burn, CPU percent, peak RSS, and conservative 100k-unit linear estimates. Current comparable resource rows use selected legacy `resources_debug` binaries against Rust `release-native`; `optimization_readiness.speedup_claim_eligible_comparisons` remains `0`, so treat their ratios as observed local process/resource-consumption evidence, not optimized-baseline speedup claims, until an optimized legacy C++ baseline is captured. `target/carbon/evidence/io-workloads.json` adds sampled TCP and TLS loopback request-cycle stats for a Python stdlib baseline and the Rust scheduler Python bridge, including aggregated request latency, five process samples per implementation by default, throughput, effective CPU burn, CPU percent, peak RSS, and 100k-request linear estimates; these rows are explicitly marked not legacy Carbon IO comparable. The same IO gate now validates the fixture-only `fixtures/io` normalized semantic trace corpus for socket recv/send wake, SSL read/write wake, and SSL send_throw error wake. Those fixtures improve semantic evidence quality but are not performance rows and do not make the loopback stats legacy Carbon IO comparable. The other rows are Rust-only or wall-time-only. Only scheduler resource-only observations and narrow preliminary local catalog, bundle, and patch workflow observations are allowed.

The workspace now has a `release-native` Cargo profile and `scripts/carbon-native-bench.sh`; the latest `bench-tier-local.json` records `host.rust_build.target_cpu_native=true`, `build_profile=release-native`, fat LTO, `optimization_readiness`, and selected legacy resources baseline detection for the current narrow comparable rows. The latest `io-workloads.json` also records `release-native` and `target_cpu_native=true` for TCP/TLS loopback baseline-vs-scheduler-bridge rows, but those rows are not legacy Carbon IO comparable. SIMD, Rayon/Tokio, bitset, optimized-index, and optimized-baseline speedup claims still require implementation plus new evidence under the same parity status rules above.
