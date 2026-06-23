# Optimization Map

Status: optimized local resource comparisons are now measurable; scheduler and
distributed orchestration are the next comparison surface.

## Measured Baseline

The current comparable evidence is in
`target/carbon/evidence/bench-tier-local.json`.

| Metric | Current Result |
| --- | --- |
| Comparable old-vs-Rust resource workloads | 9 |
| Legacy baseline | Optimized C++ `Release` resources CLI |
| Rust baseline | `release-native`, `target-cpu=native`, debug assertions off |
| Parity for measured resource rows | Pass |
| Median wall-latency uplift | 1.52x |
| Best wall-latency uplift | 2.32x |
| Equal or faster rows | 7 of 9 |
| Median p99 reduction | 43% lower |
| Median peak-memory reduction | 57% lower |
| Median CPU-burn reduction | 34% lower |

The two weak rows are `create_bundle_local_cdn` at 0.86x and
`create_patch_local_cdn` at 0.90x. `unpack_bundle_local_cdn` is effectively
flat at 1.00x. These are the first resource workloads to profile before making
any broader resource-pipeline claim.

## Scheduler Story

The scheduler claim should not be "Rust is faster." The stronger and more
accurate claim is:

> Tasklet scheduling becomes cheaper, more predictable, and easier to scale
> across domains.

For scheduler and distributed work, raw file throughput is not the central
question. The central question is orchestration overhead: how much latency,
CPU, memory, and tail risk the scheduler adds between useful units of work.

The scheduler measurement map lives in the Knotwerk scheduler port:

`carbon-scheduler-rs/docs/distributed-orchestration-metrics.md`

## Scheduler Metrics To Add

| Area | Metrics |
| --- | --- |
| Tasklet lifecycle | create/enqueue/yield/resume/complete ops/sec, dispatch latency, CPU ns/op, memory per live tasklet |
| Channel handoff | send-to-receive latency, blocked-to-runnable latency, fan-in/fan-out throughput, p99 under waiter pressure |
| Queue pressure | runnable depth, blocked depth, queue operation latency, fairness drift, starvation count |
| Tail stability | p50, p95, p99, p99.9, max latency, spike frequency under steady load and burst load |
| Reliability | lost wakeups, duplicate completions, leaked tasklets/channels, cancellation latency, timeout accuracy, shutdown completeness |
| Distributed overhead | raw transport RTT, scheduled RTT, scheduler overhead over raw transport, messages/sec/core, control bytes/message |
| Backpressure | queue-depth stability, reject/defer rate, overload recovery time, producer stall latency |
| Observability | trace-off overhead, trace-on overhead, events/sec, trace bytes/sec |

## Larger-Win Opportunities

A 20x claim should only attach to a named workload and metric. It is plausible
where the current path pays heavy object, lock, bridge, global-state, or
linear-scan costs. It is not a blanket claim for all resource or network work.

| Opportunity | Most Relevant Surface | Expected Metric Movement |
| --- | --- | --- |
| Rust-owned scheduler core | Tasklet and channel authority | Lower handoff latency, lower p99, lower CPU/op |
| Single-writer scheduler domains | Cross-domain coordination | Higher messages/sec/core, less lock contention |
| Dense generational IDs | Tasklet/channel storage | Lower memory per tasklet/channel, better cache locality |
| Indexed or intrusive queues | Wake, cancel, timeout, priority paths | Lower queue-pressure p99 and cancellation latency |
| Bitsets and priority masks | Runnable and waiter membership | Faster priority selection and readiness checks |
| Bounded domain inboxes | Distributed task handoff | Measurable backpressure and safer overload behavior |
| Batched wakeups | Fan-in/fan-out bursts | Lower per-message overhead and better throughput |
| Compact control frames | Distributed scheduler metadata | Lower scheduler bytes/message and scheduled RTT |
| Arrow IPC | Trace, telemetry, replay, large batched metadata | Faster report ingestion and lower trace overhead |
| Timer wheel/deadline queues | Timeout and cancellation | Lower timeout scan cost and tighter timeout accuracy |
| SIMD/bit operations | Masks, counters, filters, checksum-like hot paths | Lower CPU/op in specific measured kernels |

## Resource Pipeline Next Steps

The measured resource results are useful, but they are not the scheduler story.
They should be used as proof that the comparison harness can produce optimized,
parity-gated old-vs-Rust evidence on this Linux host.

The next resource optimization pass should focus on:

- `create_bundle_local_cdn`: profile hashing, compression, chunk metadata, file
  IO, and process startup contribution;
- `create_patch_local_cdn`: profile diff generation, patch payload writing,
  checksum work, and allocation pressure;
- `unpack_bundle_local_cdn`: profile decompression, chunk reads, output writes,
  and small-file overhead.

Each change should report old baseline, current Rust baseline, optimized Rust,
p50, p95, p99, throughput, CPU burn, peak RSS, and parity status.

## Game-Environment Gate

The next important step is to run the scheduler in a real game environment.
That is where parity and optimization become meaningful, because scheduler wins
depend on the actual task graph: fan-in/fan-out shape, payload sizes, network
topology, cancellation behavior, timeout patterns, burst load, and failure
modes.

The game-environment run should produce:

- old scheduler vs Rust scheduler traces for the same gameplay/server workload;
- raw transport vs scheduled transport measurements;
- tasklet handoff and channel handoff latency;
- p99 and p99.9 tail behavior under steady load and burst load;
- CPU and memory per live tasklet/channel;
- failure-path behavior for cancellation, disconnect, retry, timeout, and
  shutdown;
- enough trace data to explain any final-state mismatch.
