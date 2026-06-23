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
- `SCH-CHANNEL-004`: block-trap and interrupted operation paths need no-mutation guarantees.
- `SCH-CHANNEL-005`: value/error messages need a symbolic payload model.
- `SCH-CHANNEL-006`: cancellation-safe blocked queue removal is required.
- `SCH-CHANNEL-007`: queue-head/order introspection is required for `PyChannel_GetQueue`.
- `SCH-CHANNEL-008`: channel callback points should be traced before FFI execution.
- `SCH-CHANNEL-009`: active channel registry and `unblock_all_active_channels` need core semantics.

## Required Fixtures

- existing `blocking_send`, `blocking_receive`, `send_receive_match`.
- remaining preference matrix cases for full neutral ordering and main-side preference.
- close/open/drain/reopen fixtures.
- block-trap no-mutation fixtures.
- queue-order fixtures.
- kill/clear cleanup fixtures.
