# Carbon Rust Migration Review Queue

This queue is the ordered source of work for pairwise migration reviews. Reviewers inspect exactly one legacy/Rust pair and return structured findings only. Implementation code is changed only from consolidated tasks, not during pair review.

Status values:

- `done`: reviewed and consolidated into `reviews/findings.jsonl`.
- `partial`: seeded from existing source/test review, still needs a dedicated reviewer pass.
- `queued`: ready for one reviewer.
- `blocked`: cannot be reviewed against a concrete Rust target yet.

## Wave 1: Scheduler Core Review

| Order | Pair | Reviewer scope | Status | Pair note |
| --- | --- | --- | --- | --- |
| 1 | `scheduler/src/ScheduleManager.cpp` -> `carbon-scheduler-core` | run queue, yielding, run limits, deadlock, callbacks | done | `reviews/pairs/wave1-schedule-manager.md` |
| 2 | `scheduler/src/Tasklet.cpp` -> `carbon-scheduler-core` | lifecycle, nested tasklets, kill/remove, switch/trap state | done | `reviews/pairs/wave1-tasklet.md` |
| 3 | `scheduler/src/Channel.cpp` -> `carbon-scheduler-core` | unbuffered channel queues, preference, close/open, exception transfer | done | `reviews/pairs/wave1-channel.md` |
| 4 | `scheduler/tests/python/scheduler/tests/test_scheduler.py` -> scheduler fixtures | run ordering, nested tasklets, schedule/remove, switch trap | done | `reviews/pairs/wave1-test-scheduler.md` |
| 5 | `scheduler/tests/python/scheduler/tests/test_channel.py` -> scheduler fixtures | send/receive, block trap, deadlock, preference, close/open, exceptions | done | `reviews/pairs/wave1-test-channel.md` |
| 6 | `scheduler/tests/python/scheduler/tests/test_tasklet.py` -> scheduler fixtures | tasklet API, args/kwargs, kill, pause, thread boundaries, exceptions | done | `reviews/pairs/wave1-test-tasklet.md` |
| 7 | `scheduler/tests/python/scheduler/tests/test_queuechannel.py` -> scheduler fixtures | buffered QueueChannel wrapper semantics | done | `reviews/pairs/wave1-test-queuechannel.md` |
| 8 | `scheduler/tests/python/scheduler/tests/test_utils.py` -> scheduler fixtures | teardown invariants, active counters, nested-tasklet mode reset | done | `reviews/pairs/wave1-test-utils.md` |

## Wave 2: Scheduler FFI And Python Boundary

| Order | Pair | Reviewer scope | Status | Pair note |
| --- | --- | --- | --- | --- |
| 9 | `scheduler/include/Scheduler.h` -> planned Rust C ABI | capsule layout, function pointer compatibility, ABI versioning | partial | `reviews/pairs/wave2-scheduler-h.md` |
| 10 | `scheduler/src/PyTasklet.cpp` -> Python tasklet bridge | Python type, lifecycle properties, args/kwargs, refcount/GC | queued | `reviews/pairs/wave2-py-tasklet.md` |
| 11 | `scheduler/src/PyChannel.cpp` -> Python channel bridge | Python type, send/receive API, exceptions, iterator/close state | queued | `reviews/pairs/wave2-py-channel.md` |
| 12 | `scheduler/src/PyScheduleManager.cpp` -> Python scheduler bridge | Python-owned schedule manager object and lifetime | queued | `reviews/pairs/wave2-py-schedule-manager.md` |
| 13 | `scheduler/src/SchedulerModule.cpp` -> module init/export compatibility | module exports, `_C_API`, exceptions, Python package shape | queued | `reviews/pairs/wave2-scheduler-module.md` |
| 14 | `scheduler/tests/capiTest/Channel.cpp` -> FFI tests | channel C API behavior and invalid handles | queued | `reviews/pairs/wave2-capi-channel.md` |
| 15 | `scheduler/tests/capiTest/Scheduler.cpp` -> FFI tests | scheduler C API behavior, counters, callbacks | queued | `reviews/pairs/wave2-capi-scheduler.md` |
| 16 | `scheduler/tests/capiTest/Tasklet.cpp` -> FFI tests | tasklet C API behavior and lifecycle | queued | `reviews/pairs/wave2-capi-tasklet.md` |
| 17 | `scheduler/tests/capiTest/InterpreterWithSchedulerModule.cpp` -> FFI harness | embedded interpreter import/setup behavior | queued | `reviews/pairs/wave2-capi-interpreter.md` |

## Wave 3: Realistic Scheduler Consumer Review

| Order | Pair | Reviewer scope | Status | Pair note |
| --- | --- | --- | --- | --- |
| 18 | `carbonengine/io/src/carbonio.cpp` -> scheduler bridge/workloads | scheduler-aware IO event loop usage | queued | `reviews/pairs/wave3-carbonio.md` |
| 19 | `carbonengine/io/src/socketmodule.cpp` -> socket workload fixtures | socket accept/connect/send/receive wakeups | queued | `reviews/pairs/wave3-socketmodule.md` |
| 20 | `carbonengine/io/src/_ssl.c` -> SSL workload fixtures | SSL read/write wakeups and error propagation | queued | `reviews/pairs/wave3-ssl.md` |
| 21 | `carbonengine/io/tests/python/carboniotests/test/test_socket.py` -> realistic socket tests | Python socket parity workloads | queued | `reviews/pairs/wave3-test-socket.md` |
| 22 | `carbonengine/io/tests/python/carboniotests/test/test_ssl.py` -> realistic SSL tests | Python SSL parity workloads | queued | `reviews/pairs/wave3-test-ssl.md` |
| 23 | `carbonengine/io/src/c_channel.*` -> C channel compatibility | `PyChannel_GetBalance`, `SendThrow`, wake decisions | queued | `reviews/pairs/wave3-c-channel.md` |

## Wave 4: Resources Review

| Order | Pair | Reviewer scope | Status | Pair note |
| --- | --- | --- | --- | --- |
| 24 | `ResourceGroup*.cpp/.h` -> Rust resource catalog/model | YAML/CSV compatibility and resource model | partial | `reviews/pairs/wave4-resource-group.md` |
| 25 | `BundleResourceGroup*.cpp/.h` -> Rust bundle pipeline | bundle manifest and chunk byte parity | partial | `reviews/pairs/wave4-bundle-resource-group.md` |
| 26 | `PatchResourceGroup*.cpp/.h` -> Rust patch pipeline | patch generation/application parity | partial | `reviews/pairs/wave4-patch-resource-group.md` |
| 27 | `ResourceTools.cpp` -> Rust IO/hash/compression toolkit | MD5, FNV, rolling checksum, path handling | partial | `reviews/pairs/wave4-resource-tools.md` |
| 28 | `Downloader.cpp` -> Tokio/object-store remote IO | remote probe/download behavior | queued | `reviews/pairs/wave4-downloader.md` |
| 29 | `GzipCompressionStream.cpp` -> gzip compatibility | compressed byte compatibility | queued | `reviews/pairs/wave4-gzip-compression.md` |
| 30 | `Md5ChecksumStream.cpp` -> digest compatibility | digest identity | queued | `reviews/pairs/wave4-md5-checksum.md` |
| 31 | `ResourceFilter.cpp` -> globset/bitmap filter engine | filter matching and mapping files | queued | `reviews/pairs/wave4-resource-filter.md` |
| 32 | `ThreadPool`/pipeline code -> Rayon/Tokio runtime split | CPU/IO concurrency boundary | queued | `reviews/pairs/wave4-threadpool-pipeline.md` |
| 33 | resources GTest/CLI tests -> Rust parity fixtures | fixture corpus, CLI behavior, byte outputs | partial | `reviews/pairs/wave4-resources-tests.md` |

## Current Gate Notes

- Local `resources` legacy gate is green: `121/121` CTest tests pass according to `docs/archive/baseline/test-harness-status.md`.
- Local `scheduler` legacy gate is green on the native Linux evidence path: the unchanged legacy Python unittest suite passes with `210` tests and `7` skips, and the C API CTest slice passes `36/36`.
- Rust migration crates are present as submodules: `carbon-scheduler-rs` and `carbon-resources-rs`. Their broader parity and report-readiness statuses remain governed by `reviews/report-readiness.md` and the evidence JSON under `target/carbon/evidence/`.
