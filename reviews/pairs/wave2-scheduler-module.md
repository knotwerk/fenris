# Wave 2 Pair: SchedulerModule.cpp -> module init/export compatibility

Status: queued.

Legacy source:

- `carbonengine/scheduler/src/SchedulerModule.cpp`

Rust target:

- `carbon-scheduler-bridge`

## Review Scope

Module exports, exception objects, `_C_API` capsule creation, build-flavor extension names, `scheduler` package re-export behavior, and public API compatibility.

