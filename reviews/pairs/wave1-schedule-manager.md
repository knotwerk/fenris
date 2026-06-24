# Wave 1 Pair: ScheduleManager.cpp -> carbon-scheduler-core

Status: done.

Legacy source:

- `carbonengine/scheduler/src/ScheduleManager.cpp`
- `carbonengine/scheduler/src/ScheduleManager.h`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-trace`

## Consolidated Findings

- `SCH-CORE-001`: done; the scheduler state model and core fixture gate cover main/current identity, runnable order, insert/remove, switch counts, and run-count parity for the current core slice.
- `SCH-CORE-002`: `BACK` reschedule and the non-nested `FRONT_PLUS_ONE` targeted-run boundary now have fixture coverage; broader targeted-run edge cases, callback identity, and final core ownership still need implementation and fixtures.
- `SCH-CORE-003`: done; immediate receive/send deadlocks and receive/send after runnable-child drain are covered by event-checked scheduler fixtures.
- `SCH-CORE-004`: partial; parent links now have a pure core model and
  fixture coverage, while parent-chain yield/switch traversal and Greenlet
  lifecycle authority remain bridge work.
- `SCH-CORE-005`: partial; bounded `run_n_tasklets` fixtures and timeout counters exist, while real monotonic timeout policy remains open.
- `SCH-CORE-006`: schedule callback points should be trace events before FFI callback execution.
- `SCH-CORE-007`: switch trap must be an integer level, not a boolean; this is now covered by the nested-level fixture.
- `SCH-CORE-008`: exceptional tasklet outcomes need symbolic core states.
- `SCH-CORE-009`: per-thread manager ownership and teardown require an FFI/core contract.
- `SCH-CORE-010`: scheduler benchmarks are blocked until parity fixtures pass.

## Required Fixtures

- `run_order` runner support is covered.
- schedule/remove and reschedule-position fixtures.
- immediate send/receive deadlock and drain-before-deadlock fixtures are covered.
- nested parent-chain yield/switch fixtures beyond the current parent/depth
  metadata fixture.
- real monotonic timeout-policy fixtures beyond the existing bounded `run_n_tasklets(1)` and timeout-counter coverage.
- callback-point trace fixtures.
- promoted switch-trap no-mutation and nested-level fixtures.
