# Wave 1 Pair: test_tasklet.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_tasklet.py`

Rust target:

- `fixtures/scheduler`
- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-PYTEST-TASKLET-001`: lifecycle fixtures cover the current pure-core slice, including receive/schedule/rebind switch-count behavior; broader Greenlet/object/thread behavior remains outside core fixtures.
- `SCH-PYTEST-TASKLET-002`: partial; symbolic tasklet exception-delivery control flow exists for self-raised, caught, unhandled injected exceptions, pending/immediate throw delivery, and catchable pending `TaskletExit`, while exact Python traceback/value and remaining TaskletExit edge cases remain bridge work.
- `SCH-PYTEST-TASKLET-003`: partial; bind/setup/rebind fixtures exist, while Python callable validation and exact API errors remain bridge work.
- `SCH-PYTEST-TASKLET-004`: weakref, cyclic cleanup, frame, metadata, and timing require Python FFI tests.
- `SCH-PYTEST-TASKLET-005`: cross-thread ownership and cleanup require integration tests.

## Required Fixtures

- bind/setup/run/remove/switch/kill lifecycle fixtures.
- args/kwargs and rebind fixtures.
- remaining timing/counter fixtures for exception, timeout, and cross-thread paths.
- keep `tasklet_exception_delivery_cleanup` and `tasklet_throw_pending_immediate_delivery` green while adding bridge traceback/value and remaining TaskletExit tests.
- FFI tests for Python object lifetime and metadata.
