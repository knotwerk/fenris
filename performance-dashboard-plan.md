# Carbon Performance Dashboard And Optimization Plan

## Purpose

Build an evidence dashboard and benchmark loop that make correctness visible
before performance claims. The scheduler rewrite must robustly beat the legacy
C++ scheduler on matched workloads, while the resource pipeline gets a second
native data path based on Arrow record batches, Arrow IPC, and Parquet/Zstd
instead of moving YAML/CSV/JSON-like manifests through the hot path.

## Current Evidence Snapshot

- Scheduler semantic parity is green for the current lab rows, but performance
  is not: the Rust scheduler bridge is slower on every matched quick workload.
- The first optimization targets are fanout pipeline, fake zone tick tail
  latency, channel rendezvous, and runnable queue pressure.
- Resource CLI rows are useful local evidence, but speedup language is gated by
  optimized legacy-baseline selection and release-native Rust metadata.
- The native resource format lane now has concrete `ResourceCatalog` Arrow IPC
  and Parquet/Zstd round-trip support; broader bundle/patch metadata migration
  remains follow-up work.

## Dashboard Requirements

- Show scheduler parity, current speed ratio, p99 tail, CPU, and RSS first.
- Show a regression table sorted worst-first, including the likely first
  optimization target for each scheduler row.
- Show the performance loop: parity, hypothesis, implementation, benchmark,
  decision, and rollback.
- Show resource results separately from scheduler results.
- Show native resource format rows separately from legacy YAML/CSV-compatible
  CLI rows.
- Suppress broad speedup claims unless each row has passing parity, release-
  native Rust, debug assertions off, target-cpu native, and a known non-debug
  legacy baseline.

## Scheduler Optimization Loop

The robust-win gate is:

- zero semantic mismatches;
- at least `1.20x` median scheduler throughput on quick matched rows;
- no quick scheduler row below `1.0x`;
- Rust p99 no worse than legacy p99;
- at least ten native samples per row for shareable scheduler comparison evidence.

Optimize in this order:

1. Fanout pipeline: batch wakeups, reduce Python bridge touches, and profile
   allocation/refcount churn.
2. Zone tick: separate dense entity work from scheduler dispatch; test scalar
   Rust snapshots before Rayon/SIMD.
3. Channel rendezvous: replace queue scans with ID-linked O(1) wait queues.
4. Runnable tasklets: replace `BTreeMap`/`VecDeque` scans with dense tasklet
   storage and known-tasklet O(1) removal.

Each experiment must name the affected compatibility surface, benchmark rows,
expected win, rollback path, and required parity command.

## Native Resource Data Path

The compatibility path remains legacy YAML/CSV at the boundary. The native path
must not use YAML or JSON as intermediate data movement.

Implemented first:

- `ResourceCatalog` to Arrow `RecordBatch`;
- Arrow IPC byte round-trip;
- Parquet/Zstd byte round-trip;
- scalability rows for `catalog-arrow-ipc-roundtrip` and
  `catalog-parquet-roundtrip`.

Next resource work:

- extend the same columnar model to bundle metadata and patch metadata;
- add file-backed CLI flags for `--resource-backend legacy|arrow-ipc|parquet`;
- benchmark legacy path, one-boundary-conversion path, and fully native
  columnar path separately;
- compare row throughput, bytes/sec, p99 latency, CPU burn, peak RSS, and
  semantic parity.

## Technology Fit

- Arrow IPC: native resource transport, parity batches, offline trace batches.
- Parquet/Zstd: persisted resource catalog snapshots and long-lived evidence.
- Rayon: pure Rust dense-data work only, after scalar Rust baselines pass.
- SIMD: profiled dense kernels only; not runnable FIFO or channel control flow.
- Tokio: future local reactor experiments only; not tasklet scheduling.
- Proto: compact control frames only if Arrow IPC is the wrong fit.

## Acceptance Gates

- `cargo test -p carbon-resources-core resource_catalog_` passes.
- `cargo run -p xtask -- bench-scalability-worker data catalog-arrow-ipc-roundtrip 10 2` passes.
- `cargo run -p xtask -- bench-scalability-worker data catalog-parquet-roundtrip 10 2` passes.
- `scripts/render-blog-report.py` renders the scheduler regression, native
  resource format, optimization loop, and technology-fit sections.
