# Wave 1 Pair: test_utils.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_utils.py`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-trace`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-UTILS-001`: partial; fixture-level teardown now proves blocked-channel cleanup, while Python active channel/manager lifetime counters remain FFI/core-registry work.
- `SCH-UTILS-002`: schedule-manager refcount and active-manager count require FFI tests.
- `SCH-UTILS-003`: done; scheduler traces now carry event-level cached and calculated run counts, and the fixture gate rejects divergence.
- `SCH-UTILS-004`: done; bounded `run_n_tasklets(1)` has runner support plus limited schedule-order fixtures for nested and non-nested modes.
- `SCH-UTILS-005`: done; nested-tasklet mode is per-scenario and covered by true/false fixture variants.
- `SCH-UTILS-006`: build-flavor extension names and package re-export behavior are API gates.

## Required Fixtures And Gates

- fixture runner teardown phase for blocked-channel cleanup is covered.
- active channel object lifetime and active manager final assertions remain FFI/core-registry work.
- event-level cached/calculated run-count invariant is covered; keep it enabled for new fixture operations.
- `run_n_tasklets` fixture op and limited schedule-order variants are covered.
- per-scenario `nested_tasklets` true/false coverage is in the fixture corpus.
- import smoke tests for `release`, `debug`, `trinitydev`, and `internal`.
