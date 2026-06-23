# Wave 1 Pair: test_channel.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_channel.py`

Rust target:

- `fixtures/scheduler`
- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-PYTEST-CHANNEL-001`: done; basic blocking send/receive fixtures pass under the scheduler fixture runner.
- `SCH-PYTEST-CHANNEL-002`: done; non-blocking main-tasklet transfer under `block_trap` is covered.
- `SCH-PYTEST-CHANNEL-003`: done; immediate receive/send and receive/send after runnable-child drain deadlocks are covered.
- `SCH-PYTEST-CHANNEL-004`: partial; pure-core preference order is covered for the current matrix, while Python-visible tasklet identity assertions remain bridge work.
- `SCH-PYTEST-CHANNEL-005`: done; block-trap no-mutation fixtures cover main and worker send/receive cases.
- `SCH-PYTEST-CHANNEL-006`: partial; blocked cleanup fixtures exist, while pending-kill completed-transfer and broader Python TaskletExit cleanup remain open.

## Required Fixtures

- main send/receive non-blocking under block trap is covered.
- immediate send/receive and drain-before-deadlock fixtures are covered.
- current pure-core preference matrix and send/receive queue order fixtures are covered; bridge identity assertions remain separate work.
- close/open and iterator FFI tests.
- symbolic exception transfer.
- cross-thread/refcount integration suite later.
