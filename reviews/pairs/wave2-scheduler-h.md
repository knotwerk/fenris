# Wave 2 Pair: Scheduler.h -> planned Rust C ABI

Status: partial.

Legacy source:

- `carbonengine/scheduler/include/Scheduler.h`

Rust target:

- `carbon-scheduler-ffi`

Seed finding: `SCH-FFI-001`.

## Notes

The Rust replacement must either preserve the `SchedulerCAPI` capsule layout or provide a compatibility adapter. The ABI also needs explicit versioning, opaque handles for Rust-owned state, panic containment, and invalid-handle tests.

