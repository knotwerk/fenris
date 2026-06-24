# Scheduler C++ Baseline Note

The legacy C++ scheduler should be treated as an optimized baseline, not as a
design to mechanically copy.

The current same-API Rust/PyO3 bridge rows are below legacy on the matched
scheduler pressure workloads. That is not surprising: the C++ implementation is
already highly direct in the hot path, with tasklet and channel state held close
to the Python objects, intrusive queue links, direct transfer slots, and minimal
callback checks when callbacks are disabled.

This should change the performance plan in two ways:

- Do not present "Rust is faster" as the scheduler claim while the compatibility
  bridge is below the C++ baseline.
- Do not spend the next pass trying to reproduce the C++ fast path one-for-one.
  Use it to understand the cost floor of the Python-compatible API, then focus
  Rust work on the architecture change: Rust-owned scheduler state, cheaper and
  more predictable tasklet orchestration, clearer domain boundaries, and a
  no-Python/native execution lane where real upside can exist.

For reporting, split the evidence into two tracks:

- Compatibility track: old C++ scheduler versus Rust bridge for the same Python
  API and fixtures. This proves parity and shows the cost of preserving the
  existing interface.
- Architecture track: native Rust scheduler workloads with no Python in the hot
  path. This is where claims about cheaper scheduling, lower tail latency, and
  scaling across domains belong, but only after reconciliation and game-shaped
  workload evidence exist.

The C++ baseline remains valuable because it prevents weak claims. If a Rust
compatibility row is slower, say so. The useful question is whether moving more
tasklet work into Rust changes the orchestration cost model enough to matter for
the game workload.
