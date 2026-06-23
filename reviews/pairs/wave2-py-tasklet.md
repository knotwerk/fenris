# Wave 2 Pair: PyTasklet.cpp -> Python tasklet bridge

Status: queued.

Legacy source:

- `carbonengine/scheduler/src/PyTasklet.cpp`

Rust target:

- `carbon-scheduler-bridge`

Seed finding: `SCH-FFI-002`.

## Review Scope

Python type allocation/deallocation, weakrefs, properties, args/kwargs, subclass edge cases, exact Python errors, refcount/GC behavior, and metadata exposure.

