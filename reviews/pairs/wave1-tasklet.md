# Wave 1 Pair: Tasklet.cpp -> carbon-scheduler-core

Status: done.

Legacy source:

- `carbonengine/scheduler/src/Tasklet.cpp`
- `carbonengine/scheduler/src/Tasklet.h`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-TASKLET-001`: tasklet lifecycle needs explicit Rust state transitions.
- `SCH-TASKLET-002`: targeted `run`, nested mode, non-nested `FRONT_PLUS_ONE` boundary, direct switch success, switch-trap rejection, and blocked/dead direct run/switch no-mutation are covered by current fixtures; wrong-thread behavior and Python object/API details remain outside the pure-core fixture slice.
- `SCH-TASKLET-003`: done; CoreScheduler rejects second blocking membership and the fixture gate enforces exactly one blocked wait queue per tasklet.
- `SCH-TASKLET-004`: done; CoreScheduler now exposes kill/pending-exit state, blocked-channel cleanup, and dead-tasklet no-op behavior at the handle boundary.
- `SCH-TASKLET-005`: partial; symbolic exception delivery now covers self-raised, caught, unhandled injected exceptions, immediate throw delivery, pending throw delivery, and catchable pending `TaskletExit`, while exact FFI traceback/value conversion and broader Python/Greenlet TaskletExit edge cases remain open.
- `SCH-TASKLET-006`: partial; current fixtures assert `times_switched_to` receive/schedule/rebind reset behavior plus active/all-time tasklet counts, and the bridge now reconciles start/end/run-time metrics through `CoreScheduler` snapshots. Broader counter transitions under exception, timeout, and cross-thread paths remain outside pure-core coverage.
- `SCH-TASKLET-007`: callable validation, context managers, and handlers remain in the bridge; `dont_raise`, exception-suppression decisions, `highlighted`, `context`, and method/module/file/line/parent-callsite metadata now come from core snapshots.

## Required Fixtures

- tasklet lifecycle pack for bind/setup/run/remove/switch/kill.
- wrong-thread direct run/switch and remaining bridge-level Python object/API checks.
- remaining wrong-thread and Python/Greenlet TaskletExit delivery checks in the bridge.
- keep `tasklet_exception_delivery_cleanup` and `tasklet_throw_pending_immediate_delivery` green while finishing bridge traceback/value and remaining Python/Greenlet TaskletExit work.
- remaining timing/counter fixtures for exception, timeout, and cross-thread paths.
