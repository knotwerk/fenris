# Wave 1 Pair: test_queuechannel.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_queuechannel.py`
- `carbonengine/scheduler/python/scheduler/__init__.py`

Rust target:

- `fixtures/scheduler/queuechannel_*.json`
- `carbon-scheduler-core` or `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-QUEUECHANNEL-001`: QueueChannel parity is currently invisible and must be explicitly classified.
- `SCH-QUEUECHANNEL-002`: buffered enqueue/dequeue and QueueChannel balance rules are missing from trace/core.
- `SCH-QUEUECHANNEL-003`: empty receive then later send wakeup needs a hybrid buffered/unbuffered fixture.
- `SCH-QUEUECHANNEL-004`: queued exception payloads need symbolic and FFI coverage.
- `SCH-QUEUECHANNEL-005`: empty receive errors and block-trap no-mutation checks are missing.
- `SCH-QUEUECHANNEL-006`: nested blocked receiver ordering needs a fixture after nested ops exist.
- `SCH-QUEUECHANNEL-007`: Python subclass compatibility is a bridge requirement.
- `SCH-QUEUECHANNEL-008`: main receive drain of buffered tail values needs coverage.

## Decision Needed

QueueChannel can either stay a Python wrapper over a Rust-backed base channel or move into Rust as a buffered channel. The public Python API must remain compatible either way.

