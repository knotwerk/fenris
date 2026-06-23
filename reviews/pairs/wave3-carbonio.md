# Wave 3 Pair: carbonio.cpp -> scheduler bridge/workloads

Status: queued.

Legacy source:

- `carbonengine/io/src/carbonio.cpp`

Rust target:

- scheduler bridge realistic workloads

Seed finding: `SCH-IO-001`.

## Review Scope

Scheduler-aware IO loop behavior, blocked receivers woken by IO callbacks, request/send queues, and semantic trace extraction.

