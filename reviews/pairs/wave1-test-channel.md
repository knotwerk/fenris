# Wave 1 Pair: test_channel.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_channel.py`

Rust target:

- `fixtures/scheduler`
- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-PYTEST-CHANNEL-001`: basic blocking fixtures exist but need a runner.
- `SCH-PYTEST-CHANNEL-002`: non-blocking main-tasklet transfer under `block_trap` is uncovered.
- `SCH-PYTEST-CHANNEL-003`: immediate and send-after-children deadlock cases are missing.
- `SCH-PYTEST-CHANNEL-004`: channel preference matrix is partial; receiver preference, sender preference, and simple neutral preference are covered, while full neutral ordering and receiver-first sender-preference remain open.
- `SCH-PYTEST-CHANNEL-005`: block-trap no-mutation fixtures are missing.
- `SCH-PYTEST-CHANNEL-006`: kill, pending kill, and clear cleanup fixtures are missing.

## Required Fixtures

- main send/receive non-blocking under block trap.
- immediate send/receive deadlocks.
- remaining preference matrix coverage for full neutral ordering and receiver-first sender-preference.
- send/receive queue order.
- close/open and iterator FFI tests.
- symbolic exception transfer.
- cross-thread/refcount integration suite later.
