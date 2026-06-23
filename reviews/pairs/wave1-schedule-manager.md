# Wave 1 Pair: ScheduleManager.cpp -> carbon-scheduler-core

Status: done.

Legacy source:

- `carbonengine/scheduler/src/ScheduleManager.cpp`
- `carbonengine/scheduler/src/ScheduleManager.h`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-trace`

## Consolidated Findings

- `SCH-CORE-001`: no Rust scheduler state machine exists for main/current tasklet identity, runnable queue, insert/remove, or run-count parity.
- `SCH-CORE-002`: `BACK`, `FRONT_PLUS_ONE`, `schedule_remove`, and targeted-run boundary behavior need implementation and fixtures.
- `SCH-CORE-003`: main-tasklet deadlock handling must drain runnable children before raising when applicable.
- `SCH-CORE-004`: nested parent links need a pure core model independent of Greenlet.
- `SCH-CORE-005`: partial; bounded `run_n_tasklets` fixtures and timeout counters exist, while real monotonic timeout policy remains open.
- `SCH-CORE-006`: schedule callback points should be trace events before FFI callback execution.
- `SCH-CORE-007`: switch trap must be an integer level, not a boolean; this is now covered by the nested-level fixture.
- `SCH-CORE-008`: exceptional tasklet outcomes need symbolic core states.
- `SCH-CORE-009`: per-thread manager ownership and teardown require an FFI/core contract.
- `SCH-CORE-010`: scheduler benchmarks are blocked until parity fixtures pass.

## Required Fixtures

- `run_order` runner support.
- schedule/remove and reschedule-position fixtures.
- immediate send/receive deadlock fixtures.
- nested parent/yield fixtures.
- real monotonic timeout-policy fixtures beyond the existing bounded `run_n_tasklets(1)` and timeout-counter coverage.
- callback-point trace fixtures.
- promoted switch-trap no-mutation and nested-level fixtures.
