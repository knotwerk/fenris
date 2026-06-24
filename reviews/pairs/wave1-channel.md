# Wave 1 Pair: Channel.cpp -> carbon-scheduler-core

Status: done.

Legacy source:

- `carbonengine/scheduler/src/Channel.cpp`
- `carbonengine/scheduler/src/Channel.h`

Rust target:

- `carbon-scheduler-core`
- `carbon-scheduler-bridge`

## Consolidated Findings

- `SCH-CHANNEL-001`: implement unbuffered rendezvous channel state, queues, balance, matching, and resume actions.
- `SCH-CHANNEL-002`: preference `-1/0/1` has partial compatibility fixtures; full neutral ordering and main-side preference cases remain open.
- `SCH-CHANNEL-003`: close/open must distinguish closing, closed, queue growth rejection, and draining existing peers.
- `SCH-CHANNEL-004`: partial; block-trap no-mutation and externally raised exception re-block cleanup are covered, while broader interrupted transfer paths remain open.
- `SCH-CHANNEL-005`: value/error messages need a symbolic payload model.
- `SCH-CHANNEL-006`: partial; cancellation-safe blocked queue removal, pending-kill completed-transfer cleanup, and raised-exception re-block cleanup are covered for current fixtures, while broader clear/error edge cases remain open.
- `SCH-CHANNEL-007`: queue-head/order introspection is required for `PyChannel_GetQueue`.
- `SCH-CHANNEL-008`: channel callback points should be traced before FFI execution.
- `SCH-CHANNEL-009`: done; `CoreScheduler` now owns active/blocked channel counts and `unblock_all_active_channels`, with fixture-level teardown coverage for scheduler-level unblock-all.

## Required Fixtures

- existing `blocking_send`, `blocking_receive`, `send_receive_match`.
- remaining preference matrix cases for full neutral ordering and main-side preference.
- close/open/drain/reopen fixtures.
- block-trap no-mutation fixtures.
- queue-order fixtures.
- kill/clear cleanup fixtures.
