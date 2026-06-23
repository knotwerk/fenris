# Wave 1 Pair: test_utils.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_utils.py`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-trace`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-UTILS-001`: fixture teardown must prove cleanup after blocked channels.
- `SCH-UTILS-002`: schedule-manager refcount and active-manager count require FFI tests.
- `SCH-UTILS-003`: done; scheduler traces now carry event-level cached and calculated run counts, and the fixture gate rejects divergence.
- `SCH-UTILS-004`: bounded `run_n_tasklets(1)` requires runner support.
- `SCH-UTILS-005`: nested-tasklet mode needs per-fixture config and reset.
- `SCH-UTILS-006`: build-flavor extension names and package re-export behavior are API gates.

## Required Fixtures And Gates

- fixture runner teardown phase.
- active channel and active manager final assertions.
- event-level cached/calculated run-count invariant is covered; keep it enabled for new fixture operations.
- `run_n_tasklets` fixture op.
- `config.use_nested_tasklets`.
- import smoke tests for `release`, `debug`, `trinitydev`, and `internal`.
