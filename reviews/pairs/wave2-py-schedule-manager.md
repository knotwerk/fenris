# Wave 2 Pair: PyScheduleManager.cpp -> Python scheduler bridge

Status: queued.

Legacy source:

- `carbonengine/scheduler/src/PyScheduleManager.cpp`

Rust target:

- `carbon-scheduler-bridge`

Seed finding: `SCH-FFI-004`.

## Review Scope

Python wrapper lifetime, weakrefs, thread-local ownership, and active schedule-manager counters.

