# Wave 1 Pair: test_tasklet.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_tasklet.py`

Rust target:

- `fixtures/scheduler`
- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-PYTEST-TASKLET-001`: lifecycle fixtures are missing.
- `SCH-PYTEST-TASKLET-002`: symbolic exception-delivery model is missing.
- `SCH-PYTEST-TASKLET-003`: bind/setup needs a core-vs-FFI coverage split.
- `SCH-PYTEST-TASKLET-004`: weakref, cyclic cleanup, frame, metadata, and timing require Python FFI tests.
- `SCH-PYTEST-TASKLET-005`: cross-thread ownership and cleanup require integration tests.

## Required Fixtures

- bind/setup/run/remove/switch/kill lifecycle fixtures.
- args/kwargs and rebind fixtures.
- switch-count fixtures.
- symbolic exception and TaskletExit fixtures.
- FFI tests for Python object lifetime and metadata.

