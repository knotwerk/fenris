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
- `SCH-TASKLET-005`: throw/exception transfer needs symbolic payloads before FFI traceback work.
- `SCH-TASKLET-006`: counters and `times_switched_to` need fixture assertions.
- `SCH-TASKLET-007`: callable validation, metadata, `dont_raise`, context managers, and handlers belong in the bridge.

## Required Fixtures

- tasklet lifecycle pack for bind/setup/run/remove/switch/kill.
- wrong-thread direct run/switch and remaining bridge-level Python object/API checks.
- remaining wrong-thread and Python/Greenlet TaskletExit delivery checks in the bridge.
- symbolic exception-delivery fixtures.
- counter and switch-count fixtures.
