# Wave 3 Pair: c_channel.* -> C channel compatibility

Status: queued.

Legacy source:

- `carbonengine/io/src/c_channel.cpp`
- `carbonengine/io/src/c_channel.h`

Rust target:

- C channel compatibility

## Review Scope

`PyChannel_GetBalance` wake decisions, `PyChannel_SendThrow` error propagation, and IO-facing use of the scheduler capsule.

