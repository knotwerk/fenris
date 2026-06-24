# Scheduler V2 Rust Rewrite Plan

## Executive Position

Scheduler V2 is not a Rust migration for its own sake. It is a performance,
scalability, cleanup, and memory-safety rewrite. The project should continue
only while it proves measurable value against the existing scheduler.

The rewrite must preserve the Carbon programming model long enough to migrate
safely, but the design target is not a line-for-line port. The design target is
a fast, compact, single-writer scheduler kernel in Rust, with Python and
Greenlet kept at the compatibility boundary and with multicore scale achieved
through isolated domains and explicit messages.

The hard rule:

- No performance claim counts unless the matching parity gate passes.
- No broad rewrite phase starts unless the prior phase shows concrete wins.
- No architecture choice is accepted because it is fashionable; it must improve
  latency, memory, throughput, scalability, operability, or safety.

## Relationship To Current Parity Work

This plan has two stages:

1. **Report-gate completion.** Finish enough scheduler parity, realistic IO
   evidence, and benchmark comparability to generate the requested evidence-gated
   HTML report.
2. **V2 experiments.** Start aggressive Rust architecture work only after the
   current baseline is committed, tagged, and measurable.

As of 2026-06-23, the final report is not yet gate-ready. The progress report is
useful, but `cargo run -p xtask -- report` still blocks because several
scheduler and benchmark gates are explicitly not report-ready. V2 work must not
hide or bypass those blockers.

V2 should not destabilize the parity baseline. The operating sequence is:

1. Close or explicitly import the report-gate evidence needed for the final
   report.
2. Commit the known-good parity baseline in a real repository boundary.
3. Tag or otherwise record the baseline commit used for benchmark comparison.
4. Start V2 experiments behind explicit backend, policy, and feature flags.
5. Merge only experiments that preserve the declared compatibility commitment and
   improve measured performance, scalability, memory behavior, or safety.

The parity baseline is the measuring stick. V2 is allowed to be aggressive, but
it must be aggressive in small, measurable increments.

## Current Repository Snapshot

As of 2026-06-23, the scheduler Rust work is no longer just a sketch. The local
workspace has these useful pieces:

- `carbon-scheduler-core` has a pure Rust scheduler handle layer with tasklet
  IDs, channel IDs, run queue IDs, runnable queues, per-run-queue switch-trap
  state, channel wait queues, same-domain send/receive matching, snapshots, and
  basic close/clear behavior.
- `carbon-scheduler-trace` runs scheduler fixtures and invariant checks.
- `carbon-scheduler-ffi` has ABI versioning, C capsule layout checks, and panic
  containment.
- `carbon-scheduler-python` mirrors live Python tasklet/channel operations into
  the Rust core, uses core IDs for selected channel/run-queue ordering paths,
  and reads public tasklet/channel lifecycle and public state properties from
  core snapshots. Current-thread runnable PyObject registry storage is now
  thread-local compatibility state while CoreScheduler remains the runnable
  ordering authority.
- The Python bridge test suite is locally green at `57/57` tests.
- The core/trace/FFI tests are locally green at `14/14`, `1/1`, and `3/3`
  respectively.
- `cargo run -p xtask -- rust-scheduler-python` records the PyO3 bridge as a
  compatibility boundary, not the final scheduler architecture.
- `target/carbon/evidence/scheduler-fixtures.json` passes `60/60` semantic
  fixtures, including trace-expectation bounded-pump schedule-order fixtures
  invalid direct tasklet run/switch no-mutation coverage, `scheduler.schedule`
  BACK reschedule ordering, FRONT_PLUS_ONE targeted-run boundary coverage, and
  switch-trap operation rejection without mutating schedule/schedule_remove
  events, nested parent/depth tasklet-chain coverage, and is
  `report_ready=true` for the scheduler fixture gate.
- `target/carbon/evidence/legacy-scheduler.json` now passes the native Linux
  source-build path with the unchanged legacy Python unittest suite (`210`
  tests, `7` skipped) and `36/36` C API CTest cases, with `report_ready=true`.
- `target/carbon/evidence/io-workloads.json` passes loopback and fixture-only
  semantic checks but still lacks captured legacy Carbon IO traces.
- `target/carbon/evidence/scheduler-comparison.json` now has matched legacy C++
  scheduler vs Rust scheduler bridge pressure rows with semantic checksums,
  throughput, p99/p99.9, CPU p95, peak RSS p95, and throughput CV. It is lab
  evidence; real game-environment validation remains the production gate.

This is a strong parity foundation, but it is not yet the V2 performance
architecture. It should be treated as the baseline to commit before the more
aggressive rewrite work begins.

## Why PyO3 Still Exists

The PyO3 crate is not the destination architecture. It exists because the legacy
Carbon scheduler is a Python extension with a public `_scheduler` module,
Greenlet behavior, and a `scheduler._C_API` capsule used by C/C++ consumers.

During the migration, PyO3 is the compatibility shell that lets the unchanged
legacy Python tests, C API smoke tests, and IO-facing `Scheduler.h` consumers run
against the Rust implementation. Without that shell, the tests would not prove
that existing game code can still import and call the scheduler.

The V2 target is different:

- `carbon-scheduler-core` owns tasklet/channel/scheduler state and decisions.
- `carbon-scheduler-python` owns only Python compatibility payloads: callables,
  args, exceptions, Greenlet continuations, refcounts, and legacy object
  identity.
- PyO3 is allowed at the boundary only. It should not own scheduler ordering,
  lifecycle, channel matching, or run-queue authority.

So the answer is: yes, PyO3 bindings are still needed to run the legacy Python
and C API tests during migration, but they are a bridge, not the rewrite target.

## Current Gaps

The most important gaps are not missing Rust syntax; they are ownership,
layout, measurement, and deployment gaps.

### Core Data Structure Gaps

Current state:

- `CoreScheduler` uses `BTreeMap` for tasklets, channels, and run queues.
- Run queues and channel wait queues use `VecDeque`.
- `CoreTaskletId`, `CoreChannelId`, and `CoreRunQueueId` are monotonic `u64`
  values, not generational arena keys.
- `CoreTaskletState` still combines a lifecycle enum with independent
  `alive`, `scheduled`, and `paused` booleans.
- O(1) removal of a known tasklet from a queue is not yet guaranteed; some paths
  still scan and retain.

V2 gap:

- Replace map/deque state with dense generational arenas and ID-linked intrusive
  queues.
- Make invalid lifecycle combinations unrepresentable in the hot core state.
- Split hot scheduler state from cold Python/debug state.
- Add debug-only invariant validation for queue membership and cached counts.

### Ownership And Domain Gaps

Current state:

- The Python bridge uses `BRIDGE_CORE_SCHEDULER: OnceLock<Mutex<CoreScheduler>>`.
- Current-thread runnable PyObject queues are thread-local compatibility state;
  the global `Mutex<Vec<ThreadRunQueue>>` remains for foreign-thread handoff.
- Several counters, callbacks, and registries remain process-global statics.
- Python tasklet/channel objects still carry authoritative Greenlet/PyObject
  payloads and mirrored queue lists.

V2 gap:

- Replace the global bridge mirror with per-domain owned `SchedulerCore`.
- Make `SchedulerCore` `!Send` and `!Sync`.
- Move all runnable and channel wait queue authority into the owner domain.
- Keep Python references in owner-domain payload wrappers only.
- Remove direct foreign-domain state mutation.

### Cross-Domain Protocol Gaps

Current state:

- The existing parity bridge can mirror same-process behavior, but there is no
  bounded domain inbox.
- There are no operation tokens for cross-domain send/receive/cancel/close
  races.
- There are no Loom tests for wake/cancel/shutdown interleavings.

V2 gap:

- Define domain handles and bounded inboxes.
- Define channel home-domain ownership.
- Add operation IDs and terminal operation states.
- Model-test cross-domain rendezvous, cancellation, close, and shutdown.

### Performance Evidence Gaps

Current state:

- Scheduler fixture tests provide correctness evidence.
- There is not yet a scheduler-specific benchmark suite proving tasklet,
  channel, memory, or scaling wins.
- There is no committed benchmark evidence schema tied to parity status for
  scheduler workloads.

V2 gap:

- Add microbenchmarks before changing data structures.
- Add memory-per-tasklet and memory-per-channel measurements.
- Add large-scale blocked/runnable tasklet workloads.
- Add same-domain and future cross-domain channel latency benchmarks.
- Require parity status in every benchmark output.

### Compatibility Gaps

Current state:

- Python bridge tests are green locally.
- The plan still needs supported-platform confirmation for the upstream C++
  scheduler C API and Python tests where Linux is not the authoritative gate.
- Current Rust core behavior is still partly a mirror of Python behavior rather
  than the single source of truth.

V2 gap:

- Commit the green parity state.
- Record exact test commands, platform, and SHAs.
- Keep a `legacy` backend and `rust-nested` compatibility policy during V2.
- Treat behavior changes as explicit exceptions, not incidental rewrites.

### Fast-Lane Technology Gaps

Current state:

- There is no scheduler domain crate.
- There is no reactor abstraction.
- There is no Rayon scheduler integration.
- There are no SIMD kernels.
- There is no timer wheel.
- There is no trace ring in the dispatch hot path.

V2 gap:

- Add these only after the core data layout and benchmark harness prove where
  they matter.
- Keep each technology behind a trait, feature flag, or isolated module.
- Delete experiments that do not beat the scalar/simple baseline on real
  workloads.

## Report Gate Burn-Down Before V2

The next scheduler work should burn down the report blockers in this order. This
is the shortest path from the current progress report to the final HTML report.

| Gate | Current evidence | Blocker | Next proof |
| --- | --- | --- | --- |
| Scheduler semantic fixtures | `scheduler-fixtures.json` passes 60/60 fixtures, including limited `run_n_tasklets(1)` schedule-order fixtures, invalid direct tasklet run/switch no-mutation coverage, `scheduler.schedule` BACK requeue ordering, FRONT_PLUS_ONE targeted-run boundary trace expectations, switch-trap trapped-operation counts with zero mutating schedule/schedule_remove events, scheduler callback previous/next switch-point trace expectations, nested tasklet parent/depth chain coverage, single blocked-queue membership, fixture-level blocked-channel teardown cleanup, and scheduler-level `unblock_all_channels` cleanup with active-channel count invariants, `report_ready=true` | Closed for the current deterministic core fixture gate | Keep this gate green while core ownership moves out of the Python bridge; add new fixtures when ownership changes touch tasklet, channel, timeout, switch-trap, callback, or cleanup semantics. |
| Legacy scheduler baseline | `legacy-scheduler.json` passes `cargo run -p xtask -- legacy-scheduler native-linux` on this host with `210` Python tests, `7` skips, and `36/36` C API CTest cases, `report_ready=true` | Closed for this host | Keep this gate green before publishing scheduler comparison evidence. |
| Rust scheduler Python/C API | `rust-scheduler-python.json` passes, `core_ownership_status.status=partial`; covered channel transfers now use core-owned payload handoff tokens, blocked send/receive tasklet state projection, live channel-continuation projection, current-tasklet channel handoff requeue projection, `schedule_remove` pause projection, and blocked throw cleanup read core snapshots, public tasklet call/setup/insert/run/switch/bind/unbind/dont_raise guards plus C API block-trap reads consult core snapshots, and schedule-manager refcount/weakref/thread-cache teardown is covered while PyO3 stores the actual Python value/exception objects | Queue identity adapters, remaining lifecycle decisions, callbacks, broader refcount/GC, Python payload object storage, and in-process C API coverage are not final | Make `CoreScheduler` snapshots authoritative for the remaining lifecycle/channel decisions while PyO3 holds only compatibility payload objects; keep unchanged Python tests and C API source slices green. |
| Realistic IO workloads | `io-workloads.json` passes loopback and fixture-only traces; `io-workloads` can now import timing-free normalized trace artifacts from `CARBON_LEGACY_CARBONIO_TRACE_JSON` and `CARBON_RUST_CARBONIO_TRACE_JSON` | No captured legacy `carbonio`/`_socket`/`_ssl` semantic trace comparison from supported-platform artifacts | Capture or import supported-platform legacy Carbon IO traces and compare normalized wake/send/throw events against the Rust bridge until `legacy_carbonio_trace_status=pass`. |
| Scheduler benchmark comparability | `scheduler-comparison.json` has matched legacy C++ scheduler vs Rust scheduler bridge pressure rows with semantic checksums, throughput, p99/p99.9, CPU p95, peak RSS p95, and throughput CV | Real game-environment validation is still not captured | Use the lab rows for clearly labeled scheduler comparison, then add a real game trace or harness before production scheduler claims. |
| Final report | `report-progress` writes HTML; `report` exits `1` | Multiple evidence files have `report_ready=false` | Every feature/performance claim in the final report links to passing, report-ready evidence. |

Do not start invasive V2 core rewrites until this table is either complete or
the remaining blockers are intentionally scoped out of the final report. The
work allowed before then is narrow and useful: fixture promotion, benchmark
schema improvements, evidence imports, and additional bridge ownership slices
that move existing tests toward `report_ready=true`.

## Pre-V2 Commit Checklist

Before starting the V2 experiment sequence, commit the current parity baseline
with enough evidence that future performance work can be judged cleanly. Fenris
is the integration repo; scheduler implementation commits belong in
`carbon-scheduler-rs`.

Required before the baseline commit:

- `cargo test --manifest-path carbon-scheduler-rs/Cargo.toml -p
  carbon-scheduler-core -p carbon-scheduler-trace -p carbon-scheduler-ffi`
  passes.
- `cargo test --manifest-path carbon-scheduler-rs/Cargo.toml -p
  carbon-scheduler-python` passes.
- `cargo run -p xtask -- scheduler-fixtures` passes.
- `cargo run -p xtask -- rust-scheduler-python` passes.
- `scripts/carbon-native-bench.sh bench-scheduler-comparison --workload-set
  all --tier quick --samples 10` passes with zero rejected semantic
  mismatches.
- `cargo run -p xtask -- io-workloads` passes or its unsupported legacy trace
  blocker is explicitly recorded.
- Fixture coverage and known unsupported cases are recorded.
- The Python bridge core mirror behavior is documented by tests.
- C API capsule layout and function coverage are documented by tests.
- Any unsupported upstream platform gate is classified, not hidden.
- Benchmark/dashboard output clearly separates parity status from speed claims.
- The baseline commit SHA or repository SHAs are recorded in the benchmark
  metadata.

Suggested commit message shape:

```text
scheduler: commit Rust parity baseline before V2 experiments

- records semantic fixture parity
- mirrors Python tasklet/channel state into Rust core
- keeps C capsule ABI layout checked
- leaves V2 performance ownership work behind follow-up experiments
```

After this commit, V2 branches should be small and evidence-driven. A branch that
cannot beat the committed baseline should either be reverted, kept as a research
note, or narrowed until it has a measurable payoff.

## Operating Model

V2 development should use an experiment ledger. Each experiment states:

- hypothesis;
- compatibility surface affected;
- benchmark workloads;
- expected win;
- rollback path;
- production flag or build feature;
- parity evidence required before merge.

Examples:

```text
Experiment: Dense tasklet arena
Hypothesis: Replacing PyObject queue ownership with Rust hot/cold arena state
reduces memory per scheduled tasklet by 30 percent and improves runnable
insert/remove throughput by 3x.

Compatibility affected: none at Python API level.
Required parity: scheduler semantic fixtures, Python tasklet lifecycle tests,
C API tasklet insert/remove tests.
Required benchmarks: 1k/100k runnable tasklets, insert/remove, run_n_tasklets.
Rollback: feature flag returns bridge to compatibility queue implementation.
```

Experiments should be short enough to land or kill quickly. Do not let a large
rewrite branch become the only place performance exists.

## Compatibility Commitment

The rewrite should preserve full compatibility by default. A compatibility break
is allowed only when there is a strong, documented reason and the team accepts
the tradeoff explicitly.

Valid reasons to consider a break:

- the legacy behavior prevents a major measured performance or scalability win;
- the legacy behavior is unsafe or cannot be made reliable under the V2 ownership
  model;
- telemetry shows the behavior is unused in production game code;
- the behavior is an undocumented internal quirk rather than a public contract;
- retaining the behavior would force the V2 implementation back into the legacy
  architecture.

Every proposed break needs:

- parity evidence showing exactly what changes;
- production telemetry or test evidence showing impact;
- a compatibility mode or migration shim where practical;
- a rollback path;
- an explicit entry in release notes and migration docs;
- benchmark evidence if the reason is performance.

Compatibility tiers:

- **Tier 0: Must preserve.** Public Python API, C capsule ABI, Greenlet stackful
  blocking, FIFO runnable behavior, same-domain channel behavior, callbacks,
  traps, exception delivery, tasklet kill and throw behavior.
- **Tier 1: Must preserve in `rust-nested` compatibility mode.** Nested
  `tasklet.run` ordering, legacy channel preference quirks, direct switch
  behavior, introspection details, and legacy shutdown semantics.
- **Tier 2: Additive V2 APIs and policies.** Flat scheduling, typed cross-domain
  channels, deterministic tick-stamped messages, classed runnable queues, modern
  task groups, and bounded mailbox APIs. These should be opt-in until proven.
- **Tier 3: Break only with strong evidence.** Rare compatibility quirks that are
  unused in production and materially block performance, scalability, or safety.

The default production path should be:

```text
legacy
-> rust-nested compatibility
-> rust-flat on controlled shards
-> v2 domain-scaled mode
```

Compatibility is the default contract. V2 can push beyond legacy behavior only
through explicit modes, additive APIs, or approved breaks with strong evidence.

## Success Criteria

The rewrite is justified only if it can demonstrate several of these outcomes on
representative workloads:

- At least 1.5x better scheduler throughput on Python compatibility workloads.
- At least 30 percent lower scheduler-owned memory for large blocked/runnable
  tasklet sets.
- At least 10x faster pure Rust state-machine scheduling operations compared
  with legacy-equivalent bookkeeping, excluding Greenlet/Python execution.
- Near-linear scaling to 4 cores on partitionable game-domain workloads.
- Materially lower p95/p99 tick overshoot under scheduler pressure.
- Bounded cross-domain message latency under overload.
- Faster and safer shutdown with fewer leaked or stale tasklet states.
- SIMD/Rayon lanes showing multiple-x wins on real game data workloads, not toy
  microbenchmarks.

If these gates do not materialize, the project should stop at a smaller
compatibility cleanup rather than continue as a full rewrite.

## Non-Goals

- Do not convert legacy tasklets into Tokio tasks.
- Do not work-steal suspended Python or Greenlet stacks.
- Do not place the scheduler core behind `Arc<Mutex<_>>`.
- Do not make free-threaded Python a first production dependency.
- Do not put Arrow, JSON, Protobuf, or OpenTelemetry on the dispatch hot path.
- Do not optimize for synthetic benchmarks that do not resemble game workloads.
- Do not hide overload with unbounded queues.

## Aggressive Optimization Policy

V2 should push the envelope where the evidence supports it. The following are
allowed after the parity baseline exists:

- custom dense arenas instead of general-purpose containers;
- hot/cold state splitting;
- SoA layouts for dense game-side data;
- bounded lock-free or wait-free queues where model-tested;
- hand-written `std::arch` SIMD kernels with scalar fallbacks;
- architecture-specific AVX2, AVX-512, and NEON paths where benchmarked;
- Linux `io_uring` backend experiments;
- Rayon-based CPU lanes for pure Rust workloads;
- flat scheduling mode that intentionally differs from nested compatibility;
- typed cross-domain messages instead of arbitrary PyObject sharing;
- carefully encapsulated `unsafe` in narrow modules with tests and invariants.

Aggressive does not mean uncontrolled. Any unsafe or architecture-specific
optimization needs:

- scalar or safe baseline implementation;
- correctness tests;
- benchmark evidence;
- CPU feature gating;
- documented invariants;
- rollback flag or isolated module boundary.

## Current Problems To Solve

The existing scheduler design has several properties that block significant
performance and scalability improvements:

- Tasklet lifecycle is represented by overlapping booleans and raw links. Invalid
  combinations are representable.
- Runnable queues and channel wait queues are pointer-heavy and difficult to
  audit.
- Channel rendezvous can directly schedule tasklets on another thread's manager.
- Cross-thread behavior relies too much on the GIL and legacy assumptions.
- Process-global and thread-global state makes isolation, subinterpreters, and
  parallel tests harder.
- Nested tasklet execution complicates budget enforcement and scheduler state.
- Python references and Greenlet handles are mixed into scheduling state.
- Current Rust/Python bridge has useful core mirroring and green local tests, but
  still uses global scheduler and queue state rather than owner-domain state.

The rewrite should address these as performance problems as much as safety
problems. Cleaner ownership should reduce branches, heap churn, refcount churn,
cache misses, and scheduler tail latency.

## Performance Thesis

The scheduler hot path is not primarily vector math. It is ordered state-machine
work. On the compatibility bridge, Python/Greenlet/PyO3 cost is real and limits
what a micro-optimization can prove. That is a parity benchmark, not the final
architecture benchmark.

The final Rust architecture must also be measured without Python in the hot
path. That native lane is valid even when it is not legacy same-API comparable:
it answers whether Rust-owned domains, tasklets, channels, and game-workload
kernels become cheaper when the scheduler no longer manages Python objects,
Greenlets, or refcounts.

The speedup comes from:

- dense hot state in Rust arrays;
- generational IDs instead of object pointers;
- O(1) ID-linked queues;
- fewer Python object touches;
- fewer allocations and refcount operations;
- single-writer ownership;
- bounded batched domain messages;
- predictable budgets;
- better cache locality;
- flat scheduling mode after compatibility parity is proven.

Vectorization still matters, but mostly around the scheduler:

- timer-wheel occupancy scans;
- ready-priority masks;
- domain-ready masks;
- entity interest sets;
- visibility/reachability sets;
- permission and faction filters;
- spatial candidate filtering;
- routing-table scans;
- batch serialization/deserialization;
- compression and checksum paths;
- bulk cancellation and diagnostic snapshots.

Do not try to SIMD-optimize the runnable queue or channel FIFO itself. Those are
ordered control-flow structures. Make them compact and branch-light instead.

## Benchmark Architecture Lanes

Use three separate benchmark lanes and label them separately in reports:

1. **Compatibility bridge parity:** legacy C++ scheduler extension versus the
   Rust bridge through the same Python tasklet/channel API. This proves behavior
   and exposes bridge overhead. It should not be used to dismiss native Rust
   architecture upside, because Python/Greenlet/PyO3 remains in the hot path.
2. **Native Rust scheduler kernel:** Rust-owned tasklets, queues, channels,
   wakeups, domains, and budgets with no Python objects or Greenlets in the hot
   path. This is the right benchmark for scheduler dispatch cost, tail latency,
   memory per tasklet/channel, overload behavior, and cross-domain wakeup cost.
3. **Native Rust game-workload kernels:** pure Rust data snapshots or owned
   domain data processed with scalar Rust first, then Rayon/SIMD/bitsets where
   profiling shows dense regular work. This is the valid 10x-20x lane.

The SIMD/Rayon note therefore has a narrow meaning: SIMD is premature for the
current Python bridge pressure rows because those rows are dominated by bridge
and control-flow costs. It is not premature for native Rust lanes once a dense
workload exists and a scalar Rust baseline is green.

Current implementation: `bench-scheduler-architecture` records the native Rust
scheduler kernel lane as a first-class architecture test. It samples native
runnable queue drain and channel rendezvous at the same pressure points as the
bridge parity rows, and it now also samples Rust-owned fanout-pipeline and
zone-tick-study work where tasklet bodies, worker accounting, entity updates,
channel handoffs, and aggregation stay in Rust. The command joins matching rows
to `scheduler-comparison.json` and writes `scheduler-architecture.json` with the
bridge-versus-native comparison, `no_python_hot_path` metadata, and explicit
non-claims. `bench-scalability` with `--families native-scheduler` still records
the broader native matrix, including larger queue/domain wakeup probes. These
rows are target-architecture probes; they should be shown beside the bridge
parity rows, not merged into legacy speedup claims.

## Post-Parity Experiment Sequence

After the parity baseline is committed, V2 should proceed through two parallel
lanes:

- **Scheduler kernel lane:** make the scheduler itself compact, safe, and faster.
- **20x workload lane:** find dense game-side workloads where Rust, Rayon, SIMD,
  better memory layout, and batching can produce order-of-magnitude wins.

The 20x workload lane should move up the agenda. It is the best candidate for
headline performance gains. The scheduler kernel lane is still required because
it provides compatibility, bounded latency, and the wake/block integration those
accelerated workloads need.

## 20x Workload Lane

The likely 20x wins are not Greenlet context switches. They are dense workloads
currently expressed through Python object iteration, repeated scheduler waits, or
branch-heavy per-entity loops that can be recast as Rust data kernels.

Candidate workloads:

- visibility and interest-set computation;
- spatial candidate filtering;
- broad-phase proximity queries;
- pathfinding or graph expansion batches;
- market/economy batch recalculation;
- inventory/asset search and filtering;
- permission, faction, standings, and access-mask evaluation;
- routing table scans;
- network packet validation and decode;
- serialization/deserialization batches;
- compression/checksum/encryption lanes;
- large notification fanout with coalescing;
- deadlock/wait-for graph diagnostics;
- bulk cancellation and wake selection.

What makes a workload a strong 20x candidate:

- it touches thousands to millions of homogeneous records;
- it currently crosses Python object boundaries per item;
- it has stable input snapshots;
- it can run without Python objects on worker threads;
- it can use SoA layout, bitsets, sorted integer sets, or SIMD-friendly arrays;
- it can batch scheduler wakeups rather than wake one tasklet per tiny result;
- it has a correctness oracle or replay trace.

The V2 plan should actively hunt these workloads before the full scheduler
rewrite is complete. The scheduler only needs enough integration to submit a
pure Rust job, block the tasklet, and wake it with an owned result.

### 20x Experiment A: Workload Inventory

Hypothesis:

- The largest wins are in Python-side dense game loops adjacent to scheduler
  waits, not in the scheduler dispatch loop itself.

Work:

- Inventory production/game workloads that wake many tasklets or scan many
  entities.
- Classify each as scalar-control, dense-data, graph, IO-bound, or Python-call
  dominated.
- Estimate item counts, current latency, allocation rate, and scheduler wake
  pattern.
- Pick the first three candidates with the highest expected payoff.

Merge gate:

- At least three real candidate workloads are documented with input shape,
  expected payoff, and correctness source.

### 20x Experiment B: Scalar Rust Snapshot Baseline

Hypothesis:

- Moving a dense Python loop to plain scalar Rust can already produce a large
  win by eliminating Python object iteration and improving memory locality.

Work:

- Define immutable snapshot input types.
- Convert Python/game input into owned Rust arrays.
- Implement scalar Rust baseline.
- Return owned Rust result to the scheduler domain.

Merge gate:

- Scalar Rust is correct against the existing implementation.
- Scalar Rust beats the existing path materially before Rayon/SIMD is added.

### 20x Experiment C: Data Layout Rewrite

Hypothesis:

- Struct-of-arrays, packed IDs, bitsets, and sorted integer sets produce larger
  gains than thread parallelism alone.

Work:

- Convert hot fields to SoA arrays.
- Use dense IDs rather than object references.
- Use `u64` masks, `bitvec`, Roaring bitmaps, or sorted `Vec<u32>` based on
  density and access pattern.
- Add cache and allocation metrics.

Merge gate:

- Data layout beats scalar object-shaped Rust.
- Memory bandwidth and cache behavior improve measurably.

### 20x Experiment D: Rayon Parallelism

Hypothesis:

- Once data is in Rust-owned snapshots, Rayon can scale dense workloads across
  cores without touching Python or scheduler state.

Work:

- Add explicit bounded Rayon pool.
- Split workloads by shard/entity ranges.
- Return one coalesced completion to the scheduler domain.
- Compare 1, 2, 4, and 8 worker configurations.

Merge gate:

- No Python objects enter Rayon workers.
- Workload scales on real hardware.
- Scheduler wakeup overhead does not erase the gain.

### 20x Experiment E: SIMD Kernels

Hypothesis:

- Selected inner loops can gain another multiple from AVX2/AVX-512/NEON kernels
  after data layout is fixed.

Work:

- Keep scalar implementation as the reference.
- Add `std::arch` SIMD kernels behind runtime CPU feature detection.
- Benchmark AVX2 first; add AVX-512 only where the deployment fleet benefits.
- Keep nightly portable SIMD experimental only.

Merge gate:

- SIMD path is correct against scalar reference.
- Runtime dispatch is tested.
- The kernel improves real workload time, not only isolated microbenchmarks.

### 20x Experiment F: Scheduler Integration

Hypothesis:

- Batching result delivery and tasklet wakeups is required to preserve 20x
  compute wins at the application level.

Work:

- Submit snapshot job from tasklet.
- Block tasklet on scheduler-native completion.
- Coalesce result wakeups by entity/session/tick.
- Apply backpressure when CPU queue is full.

Merge gate:

- End-to-end workflow preserves most of the compute speedup.
- Queue saturation is visible and bounded.
- Compatibility mode can still fall back to the legacy path.

## Scheduler Kernel Experiment Sequence

The scheduler lane order matters: measure first, then replace data layout, then
replace ownership, then push multicore. The 20x workload lane can run in parallel
after the parity baseline is committed.

### Experiment 1: Scheduler Benchmark Harness

Hypothesis:

- The current parity implementation cannot guide V2 without repeatable
  scheduler-specific benchmarks and memory measurements.

Work:

- Add benchmark JSON for scheduler workloads.
- Add microbenchmarks for runnable queue operations, channel rendezvous,
  blocking, unblocking, and `run_n_tasklets`.
- Add scale benchmarks for 1k and 100k runnable/blocked tasklets.
- Record parity status and command metadata in every result.

Merge gate:

- Benchmarks run locally and in the intended CI environment.
- Dashboard refuses to display speedup badges when parity fails.

### Experiment 2: Dense Generational Arena

Hypothesis:

- Replacing `BTreeMap` state and monotonic IDs with dense generational arenas
  improves cache locality, prevents stale ID reuse, and reduces memory.

Work:

- Add `TaskletId`/`ChannelId` generational keys.
- Replace tasklet/channel maps with dense arenas.
- Keep compatibility snapshots stable.
- Add stale-ID tests.

Merge gate:

- Fixture and bridge parity remain green.
- Memory per tasklet/channel drops materially.
- Core queue/channel microbenchmarks improve by at least 2x before continuing
  deeper into this path.

### Experiment 3: ID-Linked Intrusive Queues

Hypothesis:

- Queue links stored in tasklet hot state make removal and run-next insertion
  faster and more predictable than `VecDeque` plus scans.

Work:

- Store previous/next queue links by tasklet ID.
- Track exact queue membership.
- Support FIFO, remove-known-tasklet, run-next, and insert-after-current.
- Add invariant checks for symmetric links and cached lengths.

Merge gate:

- O(1) removal path is covered by tests.
- Runnable and channel wait queue benchmarks improve.
- No Python-visible ordering regression.

### Experiment 4: Exclusive Lifecycle State

Hypothesis:

- Removing overlapping `alive`/`scheduled`/`blocked` combinations from the hot
  state reduces bugs and simplifies dispatch branches.

Work:

- Replace hot lifecycle booleans with a richer enum.
- Keep legacy Python properties as derived compatibility views.
- Validate every transition in debug/test builds.

Merge gate:

- Python API properties still report legacy-compatible values.
- Invalid combinations cannot be constructed by core APIs.
- Branch and invariant checks show simpler hot-path behavior.

### Experiment 5: Per-Domain Scheduler Ownership

Hypothesis:

- Replacing the global bridge `Mutex<CoreScheduler>` and global thread-run-queue
  registry with owner-domain state is required for scalability and will reduce
  contention.

Work:

- Introduce `SchedulerDomain`.
- Move current thread queue ownership into a domain-local core.
- Make `SchedulerCore` `!Send` and `!Sync`.
- Keep cross-thread Python compatibility through domain handles.

Merge gate:

- Local bridge tests stay green.
- Multi-thread scheduler tests stay green.
- Contention on scheduler-global mutexes is eliminated from the hot path.

### Experiment 6: Cross-Domain Channel Protocol

Hypothesis:

- Bounded owner-domain messages can preserve compatibility while enabling real
  parallel domains and removing foreign queue mutation.

Work:

- Add bounded inboxes and control-lane capacity.
- Add channel home-domain routing.
- Add operation IDs and terminal states.
- Add Loom tests for send/receive/cancel/close/shutdown races.

Merge gate:

- Same-domain fast path remains at least as fast as before.
- Cross-domain behavior is deterministic and documented.
- Overload is visible and bounded.

### Experiment 7: Flat Scheduling Policy

Hypothesis:

- Flat scheduling reduces nested-control-flow overhead and improves tick budget
  enforcement while `rust-nested` preserves full compatibility.

Work:

- Keep `rust-nested` as default compatibility mode.
- Add `rust-flat` behind explicit policy flag.
- Compare semantic traces and replayed workloads.
- Instrument direct `tasklet.run` and nested handoff usage.

Merge gate:

- `rust-nested` remains fully compatible.
- `rust-flat` is opt-in.
- p95/p99 tick overshoot improves on representative workloads.

## Benchmark Gate Matrix

| Area | Minimum benchmark before rewrite | Desired V2 result |
| --- | --- | --- |
| Core runnable insert/remove | `BTreeMap`/`VecDeque` parity baseline | 2x improvement after arena and intrusive queues |
| Same-domain channel rendezvous | Python bridge plus core mirror baseline | 1.5x improvement without ordering regressions |
| 100k blocked tasklets | RSS and cleanup duration baseline | 30 percent lower scheduler-owned memory |
| Run budget behavior | p95/p99 tick overshoot baseline | Material p95/p99 reduction in flat mode |
| Cross-domain channel | First bounded inbox baseline | Bounded p95 latency under overload |
| Pure Rust state machine | Legacy-equivalent bookkeeping baseline | 10x improvement excluding Python/Greenlet |
| 20x workload scalar Rust | Current Python/game implementation | Large win from removing Python object iteration |
| 20x workload data layout | Scalar object-shaped Rust baseline | Clear cache/allocation improvement |
| 20x workload Rayon/SIMD | Scalar SoA Rust baseline | 10x-20x on at least one real game-shaped workload |

These are not permanent product guarantees. They are gates for deciding whether a
specific rewrite path has earned more investment.

## Target Architecture

```text
Game node
|
+-- Shard router / ownership table
|
+-- Scheduler domain 0, pinned owner thread
|   +-- CPython thread state or interpreter
|   +-- Greenlet stacks, compatibility mode
|   +-- Rust SchedulerCore, !Send and !Sync
|   +-- Dense tasklet/channel arenas
|   +-- Runnable queues and wait queues
|   +-- Timer wheel
|   +-- Bounded command inbox
|   +-- Local reactor
|   +-- Fixed-size trace ring
|
+-- Scheduler domain 1, pinned owner thread
|   +-- same structure
|
+-- Scheduler domain N
|
+-- Typed cross-domain message fabric
|
+-- Bounded Rayon CPU pools
|   +-- no Python objects
|   +-- no Greenlet access
|   +-- immutable snapshots in, owned Rust results out
|
+-- Telemetry and trace exporters
    +-- OTLP for coarse operational events
    +-- Arrow/Parquet for batched offline trace analysis
```

The key invariant:

Only a scheduler domain's owner thread may mutate that domain's tasklets,
runnable queues, channel wait queues, timers, Python handles, or Greenlets.

Foreign domains communicate through bounded commands. This is both the safety
model and the scalability model.

## Crate Layout

```text
carbon-scheduler-core
    Pure deterministic scheduler state machine.
    No Python, no Tokio, no Rayon, no Arrow.

carbon-scheduler-arena
    Dense generational arenas and compact queue links.
    Can start inside core, then split when mature.

carbon-scheduler-domain
    Owner-thread loop, inbox draining, timers, shutdown, domain handles.

carbon-scheduler-channel-protocol
    Cross-domain rendezvous protocol, operation IDs, cancellation races.

carbon-scheduler-reactor
    Local IO reactor trait and implementations.
    Tokio-current-thread first, io_uring backend for Linux performance.

carbon-scheduler-cpu
    Rayon integration for pure Rust work submitted by tasklets.

carbon-scheduler-simd
    Stable architecture-specific kernels and optional nightly portable SIMD
    experiments for dense data workloads.

carbon-scheduler-ffi
    C ABI, panic containment, opaque handles, versioning.

carbon-scheduler-python
    Python module, Greenlet switching, PyObject ownership, capsule API.

carbon-scheduler-trace
    Fixed event schema, binary ring export, JSON/Arrow test artifacts.

carbon-scheduler-model
    Simple reference model, property tests, differential tests, Loom tests.

carbon-scheduler-bench
    Microbenchmarks, game-shaped stress workloads, evidence JSON.
```

The core crate must remain small. It is the critical path and the part that
needs the strongest test coverage.

## Core Data Model

### IDs

Use generational IDs everywhere the legacy code uses object pointers or raw queue
links.

```rust
#[repr(transparent)]
struct TaskletId(u64);

#[repr(transparent)]
struct ChannelId(u64);

#[repr(transparent)]
struct OperationId(u64);
```

Suggested bit layout:

```text
u64 id
bits  0..31  slot
bits 32..55  generation
bits 56..63  domain/local type tag, optional
```

The prototype can use `slotmap`; the production hot path should move to a custom
dense arena if profiling shows measurable overhead.

### Tasklet State

Replace overlapping lifecycle booleans with an explicit enum.

```rust
enum TaskletState {
    New,
    Runnable {
        class: ReadyClass,
    },
    Running,
    BlockedSend {
        channel: ChannelId,
        operation: OperationId,
    },
    BlockedReceive {
        channel: ChannelId,
        operation: OperationId,
    },
    Suspended,
    Cancelling {
        reason: CancelReason,
    },
    Dead,
}
```

Keep optional flags separate only when they are truly independent:

- block trap;
- switch trap;
- callback enabled;
- tracing enabled;
- compatibility quirks.

### Hot And Cold Split

Tasklets should be split by access frequency.

```rust
struct TaskletHot {
    state: TaskletState,
    next: Option<TaskletId>,
    prev: Option<TaskletId>,
    queue: QueueMembership,
    budget: TaskletBudget,
    generation: u32,
    flags: TaskletFlags,
    switch_count: u32,
}

struct TaskletCold {
    py_payload: PyTaskletPayload,
    debug_name: InternedString,
    context: Option<ContextId>,
    parent: Option<TaskletId>,
    stats: TaskletStats,
}
```

The scheduler loop should mostly touch `TaskletHot`.

### Channel State

```rust
struct ChannelHot {
    preference: ChannelPreference,
    close_state: ChannelCloseState,
    balance: i32,
    send_head: Option<TaskletId>,
    send_tail: Option<TaskletId>,
    recv_head: Option<TaskletId>,
    recv_tail: Option<TaskletId>,
    operation_generation: u32,
}
```

The channel owns matching decisions. Payloads and Python exception objects live
outside the pure core.

### Queues

Use ID-linked intrusive queues stored inside tasklet hot state.

```rust
struct QueueLinks {
    previous: Option<TaskletId>,
    next: Option<TaskletId>,
    membership: QueueMembership,
}
```

Required operations:

- FIFO push back;
- pop front;
- O(1) remove known tasklet;
- insert after current tasklet;
- insert run-next;
- validate cached length against traversal in debug/test builds.

No payload-bearing queue should be represented as a bitset.

## Scheduler Loop

The owner-domain loop should be explicit and budgeted.

```text
drain control commands
drain bounded domain inbox batch
poll local IO completions
expire simulation timers
expire operational timers
run tasklets within budget
submit/collect CPU job completions
flush trace/metrics batch
park until next event or deadline
```

Budget shape:

```rust
struct RunBudget {
    max_switches: u32,
    max_wall_time_ns: u64,
    max_inbox_commands: u32,
    max_io_completions: u32,
    max_timer_callbacks: u32,
    max_cpu_completions: u32,
}
```

The scheduler cannot preempt arbitrary Python code. Long-running loops need
explicit checkpoints:

```text
scheduler.checkpoint(logical_cost)
```

Checkpoint responsibilities:

- test cancellation;
- charge logical work;
- yield if the budget is exhausted;
- keep the normal path cheap.

CPU-heavy loops that cannot checkpoint should be moved to Rust/Rayon.

## Compatibility Modes

### Mode 1: Nested Compatibility

This is the first correctness target. Preserve existing tasklet.run behavior,
channel preference behavior, callbacks, traps, exceptions, and C/Python APIs as
closely as possible.

Use this mode to pass parity, collect semantic traces, and discover which legacy
features are actually used by game code.

### Mode 2: Flat Scheduler

This is the later performance mode. It should remove the nested execution model
from the hot path and make budget accounting simpler.

Flat mode must be opt-in and replay-tested. It is likely one of the largest
cleanup and latency wins, but it is a behavior change and should not be merged
with the initial safety rewrite.

## Cross-Domain Communication

### Domain Handles

`SchedulerCore` is not `Send` and not `Sync`.

```rust
struct SchedulerCore {
    _not_send_or_sync: PhantomData<Rc<()>>,
    // arenas, queues, timers
}

#[derive(Clone)]
struct SchedulerHandle {
    domain_id: DomainId,
    inbox: Arc<ArrayQueue<DomainCommand>>,
}
```

Only `SchedulerHandle` crosses threads.

### Bounded Inboxes

Every domain inbox must be bounded. Overflow is a first-class event, not a hidden
allocation problem.

Overflow policies:

- reject;
- backpressure sender cooperatively;
- reserve control-lane capacity;
- drop oldest only for explicitly lossy telemetry;
- coalesce by entity/key when semantics allow it;
- disconnect overloaded clients where appropriate.

### Channel Home Domain

Each channel has a home domain.

```text
sender domain
    -> SendRequest(operation_id, sender, payload)
channel home domain
    -> FIFO match
    -> wake/ack preferred side
receiver domain
```

Each operation reaches exactly one terminal state:

```rust
enum OperationState {
    Pending,
    Matched,
    Cancelled,
    Closed,
    Failed,
}
```

Races to model:

- send vs receive;
- send vs cancel;
- receive vs close;
- match vs shutdown;
- wake vs tasklet kill;
- domain overload vs control command.

Use Loom for these protocols before production rollout.

## IO Strategy

Do not make the initial scheduler correctness rewrite depend on the most
experimental IO backend.

Use a trait:

```rust
trait DomainReactor {
    fn poll(&mut self, budget: usize) -> ReactorBatch;
    fn register(&mut self, operation: IoOperation) -> IoToken;
    fn cancel(&mut self, token: IoToken);
    fn next_deadline(&self) -> Option<Instant>;
}
```

Implementation sequence:

1. Tokio current-thread or direct `mio` backend for portability and integration
   speed.
2. Linux `io_uring` backend for high-throughput production experiments.
3. Platform-specific backend if Windows/macOS production needs it.

Tokio belongs beside the scheduler. It should not become the tasklet scheduler.

## CPU And SIMD Strategy

### Rayon CPU Lane

Use Rayon for pure Rust, data-parallel jobs:

- pathfinding;
- visibility;
- graph ranking;
- spatial queries;
- compression;
- checksums;
- crypto;
- large validation;
- batch economic calculations;
- serialization/deserialization.

Contract:

```text
tasklet freezes input
-> submits pure Rust job
-> blocks on scheduler-native completion
Rayon worker computes without Python
-> returns owned Rust result
domain inbox wakes tasklet
```

Rayon workers must never:

- touch Python objects;
- switch Greenlets;
- mutate domain game state directly;
- mutate scheduler queues.

### SIMD Lane

Use SIMD where data is dense and operations are regular:

- bitset intersections;
- mask scans;
- prefix/count operations;
- entity set filtering;
- spatial candidate filtering;
- packet validation;
- checksums and compression kernels.

Production-safe approach:

- use stable scalar baseline first;
- use stable `std::arch` kernels for x86_64/AVX2/AVX-512 and Arm NEON where
  justified;
- use runtime CPU feature detection;
- keep nightly portable SIMD behind an experimental feature flag until stable
  enough for production;
- require correctness and perf comparisons for each kernel.

Data structures:

- fixed `u64` masks for up to 64 ready classes or buckets;
- `bitvec` for compact dynamic bitfields;
- Roaring bitmaps for large sparse entity sets;
- dense `Vec<u32>` sorted sets when iteration dominates intersection;
- custom SoA arrays for hot entity attributes.

## Python And Greenlet Strategy

### Initial Production Target

Keep Greenlet for compatibility. Move state ownership, queues, channels, budgets,
and lifecycle to Rust.

Python layer owns:

- Greenlet handles;
- Python callable and arguments;
- exception state;
- PyObject reference counts;
- C API capsule surface;
- conversion between Rust errors and Python exceptions.

Rust core owns:

- tasklet IDs;
- lifecycle state;
- queue membership;
- channel matching;
- operation state;
- budget accounting;
- timer state;
- invariant checks.

### Free-Threaded Python

Treat free-threaded Python as a research track. Python 3.14 improves the
free-threaded build materially, but Greenlet compatibility and the full native
extension ecosystem must be proven before depending on it.

Even if free-threaded Python becomes viable, keep scheduler domains single-writer.
Free-threading should allow multiple domains to execute at once. It should not
allow many threads to mutate the same scheduler state.

### Subinterpreters

Subinterpreters are a later deployment option, not the initial rewrite.

Prerequisites:

- no process-global scheduler policy;
- per-interpreter module state;
- no cross-interpreter PyObject sharing;
- audited native extensions;
- validated Greenlet lifecycle per interpreter;
- typed portable payloads for cross-interpreter messages.

## Trace And Telemetry

The dispatch hot path writes fixed-size events into a preallocated ring.

```rust
#[repr(C)]
struct SchedulerEvent {
    sequence: u64,
    tick: u64,
    monotonic_ns: u64,
    tasklet: u64,
    channel: u64,
    operation: u64,
    domain: u16,
    event: u8,
    state_before: u8,
    state_after: u8,
    queue_depth: u32,
    channel_balance: i32,
}
```

Use:

- binary ring for hot-path events;
- Arrow IPC for parity and offline trace batches;
- Parquet/Zstd for long-term storage;
- OTLP for coarse operational events only.

Do not emit an OpenTelemetry span for every tasklet switch.

Emit spans for:

- game ticks;
- budget overruns;
- cross-domain requests;
- IO waits;
- task-group failures;
- domain migration;
- shutdown.

## Testing Strategy

### Parity Tests

Use semantic traces, not timestamps.

Compare:

- tasklet lifecycle transitions;
- runnable order;
- channel wait queue order;
- channel balance;
- transfer outcomes;
- exception delivery;
- callback events;
- C API results;
- shutdown cleanup results.

### Invariant Tests

Continuously assert:

- one owner domain per tasklet;
- at most one runnable queue membership;
- exactly one wait queue for a blocked tasklet;
- no queue membership for a dead tasklet;
- symmetric queue links;
- cached queue count equals calculated count;
- channel balance matches wait queues;
- only owner domain mutates local state;
- every channel operation resolves exactly once.

### Property Tests

Generate workloads covering:

- send/receive races;
- cancellation;
- close/open/clear;
- nested runs;
- flat mode;
- timeout budgets;
- shutdown;
- inbox saturation;
- large tasklet counts;
- random channel preferences.

### Loom Tests

Use Loom for:

- domain inbox handoff;
- cross-domain channel match;
- cancellation vs wake;
- shutdown control-lane reservation;
- operation token terminal-state transitions.

## Benchmark Strategy

Benchmarks must emit machine-readable evidence JSON with:

- implementation;
- commit;
- build profile;
- host;
- CPU features;
- workload parameters;
- parity status;
- metrics;
- command line.

### Microbenchmarks

- create tasklet;
- insert/remove runnable tasklet;
- pop/push ready queue;
- same-domain channel ping-pong;
- blocked sender wake;
- blocked receiver wake;
- kill blocked tasklet;
- close channel with N waiters;
- run N tasklets;
- timer insert/expire;
- inbox push/drain;
- trace event write.

### Scale Benchmarks

- 1,000 runnable tasklets;
- 100,000 runnable tasklets;
- 1,000 blocked tasklets;
- 100,000 blocked tasklets;
- 10,000 channels;
- high-contention channel;
- many mostly idle channels;
- large timer wheel;
- domain inbox saturation and recovery.

### Game-Shaped Benchmarks

- session wake storm;
- chat/message fanout;
- solar-system or zone tick;
- market/economy batch;
- spatial visibility update;
- pathfinding batch;
- network IO completion burst;
- shutdown under blocked tasklets;
- replayed production trace.

### Metrics

- ns per scheduler state transition;
- tasklet switches per second;
- same-domain channel latency;
- cross-domain channel latency;
- p50/p95/p99 tick duration;
- max tick overshoot;
- oldest runnable age;
- memory per tasklet;
- memory per channel;
- CPU utilization per domain;
- cache misses;
- branch misses;
- allocations per tick;
- PyObject refcount operations per scheduler operation;
- inbox saturation rate;
- shutdown duration;
- leaked tasklets.

## Rollout Plan

### Phase -2: Final Report Gate

This phase is the immediate work. It is intentionally separate from V2 and is
the path to the requested HTML report.

Deliverables:

- promoted scheduler semantic fixtures or an explicit final-report scope cut;
- native Linux legacy scheduler baseline stays green with Python and C API CTest
  coverage;
- Rust scheduler Python/C API evidence with the remaining core-ownership blocker
  narrowed or cleared;
- captured/imported legacy Carbon IO semantic traces, or final-report wording
  that limits IO claims to loopback/resource observations;
- scheduler pressure comparison rows stay clearly labeled as lab evidence until
  a real game-environment workload exists;
- `cargo run -p xtask -- report` succeeds only after all included claims are
  evidence-backed.

Exit gate:

- `cargo run -p xtask -- report-readiness` shows no blocker for any claim that
  will appear in the final report;
- the generated HTML contains feature parity, realistic performance/resource
  data, and architecture-improvement tables with linked evidence.

### Phase -1: Commit Current Parity Baseline

This phase snapshots the current migration state before risky V2 experiments.

Deliverables:

- parity fixtures and semantic trace vocabulary;
- current Python bridge behavior documented by green tests;
- live Python tasklet/channel behavior mirrored into Rust core snapshots;
- C API capsule compatibility checks;
- known failures classified;
- benchmark/dashboard gates able to separate parity from speed;
- committed baseline.

Exit gate:

- local core/trace/FFI/Python tests are green;
- supported-platform C++/Python parity status is green, imported, or explicitly
  classified as excluded from V2 baseline claims;
- current parity-agent work is committed;
- the baseline commit is recorded;
- the V2 branch starts from a known comparison point.

### Phase 0: Baseline And Evidence Harness

Deliverables:

- legacy scheduler benchmark runner;
- Rust benchmark runner;
- dashboard JSON schema;
- fixture/parity runner;
- perf counter collection;
- memory measurement for tasklet/channel scale tests;
- CPU feature reporting for SIMD decisions;
- local and CI command documentation.

Exit gate:

- existing behavior is measured;
- missing upstream platform gates are explicit;
- dashboard blocks speedup claims when parity fails.

### Phase 1A: 20x Workload Discovery And Baselines

Build:

- inventory of dense game-side workloads adjacent to scheduler waits;
- first three candidate workload briefs;
- immutable Rust snapshot type for the top candidate;
- scalar Rust baseline for the top candidate;
- correctness oracle or trace comparison for the top candidate.

Exit gate:

- at least one real workload has a scalar Rust baseline;
- the baseline proves whether Python object iteration or data layout is a major
  bottleneck;
- the candidate is either promoted to Rayon/SIMD work or killed quickly.

### Phase 1B: Pure Rust Core Data-Structure Prototype

Build:

- generational tasklet/channel IDs;
- explicit lifecycle enum;
- dense arenas;
- runnable queues;
- channel wait queues;
- same-domain send/receive;
- budgets;
- trace events;
- invariant checks.

Exit gate:

- semantic fixtures pass;
- core microbenchmarks prove the data-structure change is worth keeping;
- memory model is visibly smaller than pointer-heavy legacy state;
- core has no Python dependency.

### Phase 1C: 20x Workload Acceleration

Build:

- SoA or other dense layout for the selected workload;
- Rayon parallel implementation;
- optional `std::arch` SIMD kernels after data layout is proven;
- scheduler-native job submission and completion wakeup;
- fallback to compatibility path.

Exit gate:

- no Python objects enter worker threads;
- scalar, Rayon, and SIMD variants are correctness-tested;
- one real workload shows a 10x-20x improvement or the lane is re-targeted to a
  better workload;
- scheduler integration preserves most of the compute win end to end.

### Phase 2: Python Compatibility Bridge

Build:

- existing Python module API compatibility;
- existing C capsule compatibility;
- Greenlet switching boundary;
- Rust-owned scheduler state;
- Python payload wrappers;
- panic containment;
- owner-thread drop rules.

Exit gate:

- Python scheduler tests pass on supported upstream platform;
- C API tests pass;
- Python-visible compatibility remains full by default;
- global bridge scheduler state is removed from the hot path;
- Python compatibility path shows measurable memory or throughput improvement.

### Phase 3: Cross-Domain Protocol

Build:

- domain handles;
- bounded inbox;
- control lane;
- channel home-domain protocol;
- operation tokens;
- cancellation/close/shutdown races;
- Loom tests.

Exit gate:

- no foreign scheduler mutation remains;
- cross-domain channel tests pass;
- Loom protocol tests cover cancellation, close, wake, and shutdown races;
- overload behavior is deterministic;
- p95 cross-domain latency is bounded in stress tests.

### Phase 4: CPU And SIMD Acceleration

Build:

- Rayon job submission/completion;
- immutable snapshot contract;
- first real game-data workload;
- stable SIMD kernels for selected dense operations;
- benchmark feature flags.

Exit gate:

- at least one real workload shows multiple-x speedup;
- no Python objects cross into Rayon workers;
- SIMD kernels have scalar fallback and correctness tests.

### Phase 5: Flat Scheduling Mode

Build:

- compatibility nested mode retained;
- flat mode policy;
- replay comparison tooling;
- budget enforcement improvements;
- production-use instrumentation for direct `tasklet.run`.

Exit gate:

- replayed workloads match accepted behavior;
- p95/p99 tick overshoot improves materially;
- scheduler code path is simpler and faster in flat mode.

### Phase 6: Multicore Domain Scaling

Build:

- shard ownership table;
- entity/session/domain routing;
- deterministic tick-stamped messages;
- domain pinning;
- NUMA-aware process/domain experiments;
- optional subinterpreter spike.

Exit gate:

- partitionable workload scales near-linearly to 4 cores;
- no suspended Python stack migration;
- cross-domain message overhead remains below target;
- operational shutdown works under load.

### Phase 7: Production Hardening

Build:

- fault injection;
- leak detection;
- crash containment;
- rollback switch;
- dual backend deployment;
- trace sampling;
- dashboard release view.

Exit gate:

- production can choose `legacy`, `rust-nested`, or `rust-flat`;
- rollback is simple;
- dashboard shows parity, performance, and safety status per build.

## Technology Decisions

### Use Now

- Rust dense arenas and generational IDs.
- ID-linked queues.
- Bounded inboxes.
- `crossbeam_queue::ArrayQueue` for initial bounded MPMC queues.
- `bitvec` for compact bitfields.
- `Rayon` for pure Rust CPU lanes.
- Tokio current-thread or direct `mio` for first reactor.
- Loom for concurrent protocol tests.
- PyO3 plus pyo3-ffi at the Python boundary.
- Criterion or similar for Rust microbenchmarks.
- `perf`, heap profiling, and CPU feature reporting for evidence.

### Use After Proof

- custom lock-free SPSC/MPSC queues for hot domain lanes;
- Linux `io_uring` reactor;
- stable `std::arch` SIMD kernels;
- Roaring bitmaps for sparse large entity sets;
- Arrow/Parquet export for trace analytics;
- isolated subinterpreters.

### Keep Experimental

- nightly portable SIMD;
- free-threaded Python with Greenlet;
- AVX-512-only designs;
- io_uring-only architecture;
- work-conserving cross-domain policies that weaken determinism.

## Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Greenlet cost dominates compatibility path | Rust state rewrite shows limited speedup | Separate pure core wins from compatibility wins; use flat mode and Rust CPU lanes for larger gains |
| Legacy nested semantics block simplification | Budget overruns and complexity remain | Preserve nested first; instrument usage; introduce flat mode later |
| Cross-domain channels change ordering | Gameplay regressions | Semantic traces and replay tests; define documented cross-domain ordering |
| Python refs leak or drop on wrong thread | Crashes/leaks | Strict owner-domain PyObject wrappers and drop assertions |
| Free-threaded Python is not viable with extensions | No in-process multicore Python | Scale first with processes or GIL-isolated domains; keep free-threading research-only |
| SIMD adds complexity without real wins | Maintenance cost | Require scalar baseline and real-workload speedups |
| Unbounded queues hide overload | Memory growth and latency spikes | Bounded queues and explicit overflow policies |
| Benchmarks become synthetic | Misleading decisions | Game-shaped workloads and replayed traces required |

## Go / No-Go Gates

### Continue To Full Rewrite If

- core state-machine benchmarks show large wins;
- memory footprint drops on large tasklet/channel sets;
- compatibility tests can pass through the Rust-owned state model;
- cross-domain protocol is model-tested;
- at least one multicore or Rayon/SIMD workload shows business-relevant speedup.

### Stop Or Reduce Scope If

- Greenlet/Python overhead erases most scheduler-state wins;
- compatibility requires recreating the legacy design without simplification;
- cross-domain ordering breaks important gameplay semantics;
- no representative workload improves materially;
- maintenance complexity grows faster than performance evidence.

## Immediate Next Actions

1. Decide the repository boundary for the combined migration workspace, then
   commit the current green progress baseline.
2. Import a supported Windows/macOS legacy scheduler CTest artifact, or
   explicitly remove scheduler speedup claims from the final report scope.
3. Continue the core-ownership drain in `carbon-scheduler-python` until
   `rust-scheduler-python.json` can move from `core_ownership_status=partial` to
   complete for the claimed surface.
4. Capture or import normalized legacy Carbon IO traces for `carbonio`,
   `_socket`, and `_ssl`, then rerun `cargo run --profile release-native -p
   xtask -- io-workloads` with `CARBON_LEGACY_CARBONIO_TRACE_JSON` and
   `CARBON_RUST_CARBONIO_TRACE_JSON` set. The evidence must record
   `legacy_carbonio_trace_status=pass` and zero mismatches before Carbon IO
   parity is claimable.
5. Keep `scheduler-comparison.json` green as the matched legacy scheduler vs
   Rust scheduler bridge pressure benchmark; use it for lab comparison only
   until a real game-environment workload is captured.
6. Regenerate `cargo run -p xtask -- report-progress` after each evidence slice
   and keep `cargo run -p xtask -- report` blocked until every included claim is
   report-ready.
7. After the final report gate is clean, record the baseline SHA, local test
   commands, and green test counts.
8. Add scheduler-specific microbenchmarks for runnable queues, channel
   rendezvous, `run_n_tasklets`, memory per tasklet, and memory per channel.
9. Inventory dense game-side workloads and select the first three 20x
    candidates.
10. Build a scalar Rust snapshot baseline for the top 20x candidate before
    Rayon, SIMD, or a domain rewrite.
11. Prototype dense generational arenas behind the existing fixture runner only
    after benchmark evidence shows the baseline bottleneck.
13. Replace boolean lifecycle in the Rust model with an explicit hot-state enum
    after the fixture/bridge parity gates cover the affected lifecycle cases.
14. Add Rayon and SIMD variants only after the scalar 20x candidate baseline is
    correct and measured.
15. Add a cross-domain channel protocol model and Loom tests when same-domain
    ownership is already Rust-authoritative.

## References

- Python 3.14 free-threaded mode improvements:
  https://docs.python.org/3/whatsnew/3.14.html
- Greenlet change notes and free-threading caveats:
  https://greenlet.readthedocs.io/en/latest/changes.html
- Tokio `LocalSet`:
  https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html
- Rayon:
  https://docs.rs/rayon/latest/rayon/
- Linux `io_uring`:
  https://man7.org/linux/man-pages/man7/io_uring.7.html
- Crossbeam `ArrayQueue`:
  https://docs.rs/crossbeam-queue/latest/crossbeam_queue/struct.ArrayQueue.html
- Slotmap:
  https://docs.rs/slotmap/latest/slotmap/
- Bitvec:
  https://docs.rs/bitvec/latest/bitvec/
- Rust portable SIMD:
  https://doc.rust-lang.org/std/simd/index.html
