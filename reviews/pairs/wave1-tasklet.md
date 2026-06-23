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
- `SCH-TASKLET-002`: targeted `run`, nested mode, direct switch success, switch-trap rejection, and blocked/dead direct run/switch no-mutation are covered by current fixtures; wrong-thread behavior, Python object/API details, and `FRONT_PLUS_ONE` remain outside the pure-core fixture slice.
- `SCH-TASKLET-003`: blocked membership must be a single scheduler-owned invariant.
- `SCH-TASKLET-004`: kill needs symbolic immediate/pending core events.
- `SCH-TASKLET-005`: throw/exception transfer needs symbolic payloads before FFI traceback work.
- `SCH-TASKLET-006`: counters and `times_switched_to` need fixture assertions.
- `SCH-TASKLET-007`: callable validation, metadata, `dont_raise`, context managers, and handlers belong in the bridge.

## Required Fixtures

- tasklet lifecycle pack for bind/setup/run/remove/switch/kill.
- wrong-thread direct run/switch and remaining bridge-level Python object/API checks.
- kill while blocked and pending kill fixtures.
- symbolic exception-delivery fixtures.
- counter and switch-count fixtures.
