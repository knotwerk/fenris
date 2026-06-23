# Wave 2 Pair: PyChannel.cpp -> Python channel bridge

Status: queued.

Legacy source:

- `carbonengine/scheduler/src/PyChannel.cpp`

Rust target:

- `carbon-scheduler-bridge`

Seed finding: `SCH-FFI-003`.

## Review Scope

Python channel properties, send/receive parsing, `send_exception`, `send_throw`, iterator behavior, close/open API compatibility, and exception translation.

