# Wave 1 Pair: test_scheduler.py -> scheduler fixtures

Status: done.

Legacy source:

- `carbonengine/scheduler/tests/python/scheduler/tests/test_scheduler.py`

Rust target:

- `fixtures/scheduler`
- `carbon-scheduler-trace`

## Consolidated Findings

- `SCH-PYTEST-SCHED-001`: `run_order.json` exists but needs a runner.
- `SCH-PYTEST-SCHED-002`: targeted `tasklet.run()` fixtures now cover `t1.run()`, `t2.run()` with nested tasklets, and `t2.run()` without nested tasklets.
- `SCH-PYTEST-SCHED-003`: single-level, multi-level, and blocked-yield nested tasklet fixtures now cover nested and non-nested modes.
- `SCH-PYTEST-SCHED-004`: `scheduler.schedule` versus `schedule_remove` state transitions are missing.
- `SCH-PYTEST-SCHED-005`: `_C_API` exposure and callbacks require FFI gates.

## Required Fixtures

- targeted run fixtures for nested true/false are present and passing.
- bounded `run_n_tasklets` variants.
- switch and switch-trap fixtures.
- callback-point trace fixtures.
