# Wave 2 Pair: capiTest/Channel.cpp -> FFI tests

Status: queued.

Legacy source:

- `carbonengine/scheduler/tests/capiTest/Channel.cpp`

Rust target:

- `carbon-scheduler-ffi`

## Review Scope

`PyChannel_New`, `PyChannel_Send`, `PyChannel_Receive`, `PyChannel_GetBalance`, `PyChannel_SetPreference`, `PyChannel_GetQueue`, `SendException`, `SendThrow`, invalid handles, and panic containment.

