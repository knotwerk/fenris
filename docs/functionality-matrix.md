# Functionality Matrix

This matrix maps the current Carbon scheduler source of truth to the standalone
Rust work lanes.

Source of truth:

- Scheduler implementation: `/data/repos/fenris/carbonengine/scheduler`
- C API header: `/data/repos/fenris/carbonengine/scheduler/include/Scheduler.h`
- Python tests:
  `/data/repos/fenris/carbonengine/scheduler/tests/python/scheduler/tests`
- C API tests: `/data/repos/fenris/carbonengine/scheduler/tests/capiTest`

Lane meanings:

- `core`: pure Rust scheduler/channel/tasklet state machine. No Python,
  Greenlet, C API, or FFI.
- `trace`: semantic trace schema, fixture parser, fixture runner, and trace
  comparison.
- `ffi`: later C ABI or Python capsule compatibility over a green Rust core.
- `deferred`: behavior that depends on Python object lifetime, Greenlet
  mechanics, thread integration, traceback/frame handling, or public API polish
  outside the first milestone.

Bridge ownership rule: `carbon-scheduler-python` exists only to keep the
legacy `_scheduler`, `scheduler`, and `scheduler._C_API` import/API surface
testable while the migration is in progress. It is not the final scheduler
owner. The target architecture is that `carbon-scheduler-core` owns
tasklet/channel/scheduler lifecycle state and scheduling decisions, FFI exposes
opaque handles/status/error translation, and the PyO3 layer stores only Python
callables, exception objects, GIL/refcount/GC glue, and compatibility wrappers.
Current evidence records this as `core_ownership_status=partial`. The core now
has an initial `CoreScheduler` owner-thread handle API for Rust-owned
`CoreTaskletId`/`CoreChannelId`/`CoreRunQueueId` unbuffered channel rendezvous
state, bridge run-queue FIFO/count/remove/pop behavior, scheduled-state authority,
per-run-queue scheduler switch-trap depth, explicit pause/resume lifecycle
transitions, balance, blocked queues, preference clamping, close/open/clear, and
block-trap no-mutation checks. Live PyO3 tasklet/channel objects now carry opaque
`CoreTaskletId`/`CoreChannelId` handles and mirror unbuffered channel balance,
preference, block-trap, and blocked sender/receiver queue transitions through
`CoreScheduler`; covered unbuffered send/receive transfers now consume
`CoreChannelOperationResult` sender/receiver IDs, balance, and immediate
peer-handoff decisions to adapt legacy Python tasklet queues by core ID rather
than by Python queue-front position or local preference checks, the
sender-preferred pre-receive scheduling probe now reads preference and blocked
sender state from `CoreScheduler` snapshots, and `channel.queue` now maps
`CoreScheduler::queue_front` IDs back to legacy Python tasklet objects. Live
PyO3 tasklet objects also mirror
alive/scheduled/paused/times_switched_to through `CoreScheduler` tasklet
snapshots for the setup/run/finish/block/continuation/clear/kill paths covered
by bridge tests, use core pause/resume transitions for bind/remove/insert and
switch pause/resume paths, and consult the core paused snapshot before direct
running a paused tasklet, while live bridge scheduling now uses CoreScheduler run
queues and maps selected `CoreTaskletId` values back to Python objects for
callable execution, and Python scheduler switch-trap entrypoints plus trapped
operation guards now use CoreScheduler per-run-queue state. Python runtime sync
no longer writes the core scheduled flag; Python-visible tasklet
alive/blocked/scheduled/paused/block_trap/times_switched_to properties and the
C API times-switched getter now read CoreScheduler snapshots, including the
legacy transient current-tasklet scheduled flag during `scheduler.schedule()`.
Python-visible channel preference/closing/closed properties also read
CoreScheduler snapshots.
Final report readiness still requires that status to become complete
by making those core snapshots authoritative for remaining lifecycle decisions,
Python payload handoff, queue identity adapters, callbacks, and broader
lifecycle transitions.

Current Python bridge status: `carbon-scheduler-python` now has an initial PyO3
smoke gate for `_scheduler`, `_scheduler_debug`, `_scheduler_trinitydev`, and
`_scheduler_internal` module population/import from `target/carbon/python`,
legacy `scheduler` package import through `PYTHONPATH`, public symbol presence,
named `scheduler._C_API` PyCapsule validation with non-null initial ABI-table
pointer, callable `PyTasklet_New`, `PyChannel_New`,
`PyScheduler_GetCurrent`, and `PyScheduler_GetScheduler` entries, callable
tasklet/channel type/property entries, callable read-only scheduler counter
entries, simple `PyTasklet_Setup` plus `PyScheduler_RunNTasklets` callable
execution, Python-level `scheduler.tasklet(callable)(args)` plus
`scheduler.run_n_tasklets(1)`, direct `tasklet.run()` for simple queued
callables, generated source-slice binaries that compile and run the real
legacy `capiTest/Tasklet.cpp`, `Channel.cpp`, and `Scheduler.cpp` C API tests
one test per child process, an optional in-process source-slice probe that now
records the repeated-interpreter failure after `Py_FinalizeEx()`,
local `maturin` release-wheel build/install smoke from a fresh virtualenv,
all 210 unchanged legacy unittest cases, callback registration get/set,
initial tasklet
kill/invalid construction/weakref/cyclic cleanup/TaskletExit smoke,
switch-trap raised-callable smoke, initial channel
blocking/deadlock accounting, block-trap send/receive errors, closed/open
channel basics, channel iterator smoke, block-trap balance no-mutation checks,
ABI version exposure, `channel` subclass constructor/init shape,
`QueueChannel` buffered value/exception paths, preference setting,
stable-current `block_trap`, and nested-tasklet flag roundtripping. It is
intentionally not a behavior-parity claim; Greenlet switching, resumable Python
object channel transfer through scheduled tasklets, advanced setup/run
lifecycle behavior beyond the passing per-test C API source slices, draining
tasklet/channel/scheduler lifecycle ownership out of PyO3 objects, full
in-process legacy `SchedulerCapiTest`/`capiTest` binary compatibility, broader
`_C_API` failure-mode hardening, captured legacy Carbon IO semantic traces, and
broader wheel flavor/dependency/install matrices remain FFI/report-readiness
work. The current IO gate validates a fixture-only `fixtures/io` normalized
semantic trace corpus for socket recv/send wake, SSL read/write wake, and SSL
send_throw error wake; this is expected trace-shape evidence, not captured
legacy Carbon IO parity.

The real legacy C API source files are currently run one test per child process.
That proves the source-level C API behavior, but not the exact legacy
GoogleTest fixture lifecycle that calls `Py_InitializeFromConfig()` and
`Py_FinalizeEx()` around every test in one process. A local opt-in
`CARBON_CAPI_GTEST_IN_PROCESS=1` probe now runs the same generated source
slices sequentially in one process. It passes the first test in each slice and
then fails on the next embedded `scheduler` import because `_scheduler.channel`
is `None`, causing `QueueChannel(_scheduler.channel)` to raise `TypeError`.
PyO3 0.21 uses process-global module/type caches, so full in-process
reinitialize/finalize parity remains a separate boundary decision rather than a
covered claim.

## SchedulerCAPI Groups

| C API group | Functions | Core lane | Trace lane | FFI lane | Deferred lane |
| --- | --- | --- | --- | --- | --- |
| tasklet lifecycle | `PyTasklet_New`, `PyTasklet_Setup`, `PyTasklet_Insert`, `PyTasklet_Alive`, `PyTasklet_Kill` | Represent tasklet IDs, alive/dead state, bound operation, scheduled/paused/blocked flags, insertion/removal, kill before run, kill while blocked. `CoreScheduler` now exposes tasklet runtime snapshots, explicit pause/resume transitions, schedule-clears-paused behavior, and transient scheduled snapshots for legacy current-tasklet schedule yields; the live PyO3 bridge mirrors alive/scheduled/paused/times_switched_to for covered setup/run/finish/block/continuation/clear/kill paths, uses core paused snapshots for direct-run paused eligibility, and exposes public tasklet lifecycle/scheduling/block-trap/switch-count properties from core snapshots. | Emit `tasklet.new`, `tasklet.setup`, `tasklet.insert`, `tasklet.kill`, `tasklet.complete`, and state snapshots. | Map Rust tasklets to `PyTaskletObject*`, Python callable setup, Python args/kwargs, and return codes; make core snapshots authoritative for scheduling/lifecycle decisions. | Python refcount behavior, weakrefs, cyclic callable cleanup, `TaskletExit` exception injection, tracer/context-manager behavior, and broader Greenlet continuation ownership. |
| tasklet properties | `PyTasklet_GetBlockTrap`, `PyTasklet_SetBlockTrap`, `PyTasklet_IsMain`, `PyTasklet_Check`, `PyTasklet_GetTimesSwitchedTo`, `PyTasklet_GetContext` | Track `is_main`, `block_trap`, and `times_switched_to`; model type checks as API-layer concerns. | Emit switch-count changes and block-trap rejection events. | Expose exact getters/setters, `PyTasklet_Check` semantics, `GetContext` C-string access, and callsite metric/string setter compatibility. | Python frame inspection and invalid subclass construction behavior. |
| channel basics | `PyChannel_New`, `PyChannel_Send`, `PyChannel_Receive`, `PyChannel_GetBalance`, `PyChannel_GetPreference`, `PyChannel_SetPreference`, `PyChannel_Check` | Implement unbuffered channels, blocked sender/receiver queues, balance rules (`+senders`, `-receivers`), preference clamping to `-1/0/1`, transfer matching, close/open/clear, remove-blocked cleanup, and block-trap no-mutation checks. The `CoreScheduler` handle API covers these as Rust-owned state, and the live PyO3 bridge now mirrors unbuffered channel balance/preference/block-trap/blocked-queue transitions through `CoreTaskletId`/`CoreChannelId` handles while using core send/receive results to select matched Python tasklets by core ID, decide immediate peer handoff, select `channel.queue` fronts in covered paths, and expose public channel preference/closing/closed getters from core snapshots. | Emit `channel.new`, `channel.send.attempt`, `channel.receive.attempt`, `channel.block`, `channel.transfer`, `channel.unblock`, `channel.balance`. | Convert Rust channel operations to `PyObject*` payload and error returns over opaque core handles. | Python payload storage, pending payload close-state, broader PyObject queue identity adapters, broader scheduling ownership, and exact exception text outside core semantics still need to move behind adapter boundaries. |
| channel errors | `PyChannel_SendException`, `PyChannel_SendThrow` | Model typed error payloads as channel messages once value transfer is green. | Emit exception-transfer events with symbolic error names and args. | Preserve Python exception type/value/traceback behavior. | Full traceback preservation, `sys.exc_info()`, pending kill interactions with completed transfers. |
| channel queues | `PyChannel_GetQueue` | Expose blocked queue head/order in core introspection. | Add queue snapshots for blocked sender/receiver order fixtures. | Return the exact blocked `PyTaskletObject*`. | Public iterator protocol and Python `QueueChannel` wrapper semantics. |
| scheduler control | `PyScheduler_Schedule`, `PyScheduler_GetRunCount`, `PyScheduler_GetCurrent`, `PyScheduler_RunWithTimeout`, `PyScheduler_RunNTasklets`, `PyScheduler_GetScheduler` | Implement deterministic run queue, current tasklet, `schedule`, `schedule_remove`, full run, limited run, run-count semantics including main tasklet, deadlock detection, CoreScheduler-owned per-run-queue switch-trap level and no-mutation guards, and wall-clock `RunWithTimeout` pumping that always runs at least one queued tasklet for zero/expired timeouts. | Emit `scheduler.run.start`, `scheduler.switch`, `scheduler.schedule`, `scheduler.schedule_remove`, `scheduler.switch_trap`, `scheduler.switch_trap_error`, `scheduler.deadlock`, and final run counters. | Expose the capsule functions and Python `scheduler` object. | Callback reentrancy, advanced Greenlet exception paths, public Python API edge cases. |
| scheduler callbacks | `PyScheduler_SetChannelCallback`, `PyScheduler_GetChannelCallback`, `PyScheduler_SetScheduleCallback`, `PyScheduler_SetScheduleFastCallback` | Invoke Python schedule callbacks, share schedule callbacks across threads, and invoke the fast C schedule callback at switch points covered by bridge parity tests. | Emit callback-point events when useful for parity fixtures and invoke user callbacks in the Python bridge. | Python-level callback set/get storage, schedule callback basic ordering, multi-thread callback visibility, simple channel callback order, and fast C callback smoke are covered. | Callback exceptions, callback reentrancy, broader lifetime/refcount cleanup. |
| scheduler counters | `PyScheduler_GetNumberOfActiveScheduleManagers`, `PyScheduler_GetNumberOfActiveChannels`, `PyScheduler_GetAllTimeTaskletCount`, `PyScheduler_GetActiveTaskletCount`, `PyScheduler_GetTaskletsCompletedLastRunWithTimeout`, `PyScheduler_GetTaskletsSwitchedLastRunWithTimeout` | Track active channel count, active/all-time tasklet count, completed count, and switch count. One schedule manager per core instance. | Include final counter assertions in fixtures where relevant. | Export exact integer-returning C API. | Python GC timing and schedule-manager reference cleanup. |

## Legacy Support File Parity Notes

| Support source | Public behavior affected | Rust guard |
| --- | --- | --- |
| `Utils.cpp`/`Utils.h::StdStringFromPyObject` | `tasklet.context` and `tasklet.parent_callsite` accept only Python `str`, store UTF-8 text, and reject non-strings with `TypeError("value must be a string")`. `Tasklet::SetCallsiteData` also uses the helper internally for method/module/file metrics after coercing `__name__` and `__module__` through `str()`. | `crates/carbon-scheduler-python/src/lib.rs::tasklet_getset_properties_match_legacy_py_tasklet_surface` checks metric extraction, string round-trips, and exact non-string setter error text. |
| `GILRAII.cpp`/`GILRAII.h` | Legacy C API capsule entries always reacquire/release the GIL before touching Python objects. This is not a standalone Python feature, but it is visible as safe C API calls for tasklet/channel/scheduler operations. | Rust C API entries that dereference Python objects use `Python::with_gil`; pure counter/fast-callback slots avoid Python object access. The C API parity tests named `c_api_*_match_legacy_*` exercise the Python-object entries through the capsule. |
| `PythonCppType.cpp`/`PythonCppType.h` | Internal holder for the wrapper `PyObject*`; public effects are object identity and explicit `Incref`/`Decref` use behind `PyTasklet_Setup`, `PyTasklet_Kill`, `PyScheduler_GetCurrent`, `PyScheduler_GetScheduler`, queue heads, and active tasklet/channel lifetime counters. | Guarded by `c_api_tasklet_setup_reference_counts_match_legacy_capi_test`, `c_api_scheduler_identity_run_count_and_run_n_tasklets_match_legacy_capi_paths`, `c_api_channel_constructor_introspection_entries_match_legacy_capi_paths`, `active_channel_count_matches_legacy_teardown_and_capi_refcounts`, `tasklet_resource_counters_match_legacy_capi_lifetime`, and `schedule_manager_wrapper_lifecycle_matches_legacy_thread_cache`. No separate Rust type is needed. |

## Python Test Mapping

| Source tests | Primary lane | Trace coverage | Notes for next agents |
| --- | --- | --- | --- |
| `test_channel.py::test_blocking_send` | core | `fixtures/scheduler/blocking_send.json` | Sender blocks when no receiver exists; tasklet is alive/blocked, unscheduled, channel balance is `1`, run count returns to main only. |
| `test_channel.py::test_blocking_receive` | core | `fixtures/scheduler/blocking_receive.json` | Receiver blocks when no sender exists; channel balance is `-1`. |
| `test_channel.py::test_block_trap_send`, `test_block_trap_recv`, main deadlock, close/open basics | core then ffi | `fixtures/scheduler/channel_block_trap_no_balance_change.json`, `fixtures/scheduler/channel_block_trap_nonblocking_transfer.json`, `fixtures/scheduler/channel_close_open_parity.json`, plus Rust PyO3 bridge unchanged subset | Core fixtures cover block-trap balance preservation, nonblocking block-trap transfer, and close/open state; Python bridge covers object/API exception shape. Resuming blocked Python frames remains open. |
| `test_channel.py::test_non_blocking_send`, `test_non_blocking_receive`, `test_sending_tasklets_rescheduled_by_channel_are_run`, `test_receiving_tasklets_rescheduled_by_channel_are_run` | core | `fixtures/scheduler/send_receive_match.json`, `fixtures/scheduler/channel_preference_sender.json`, `fixtures/scheduler/channel_preference_sender_receiver_first.json`, `fixtures/scheduler/channel_preference_neither_simple.json`, `fixtures/scheduler/channel_preference_neither_tasklet_transfer.json`, `fixtures/scheduler/channel_preference_neither_multi_party_order.json`, `fixtures/scheduler/channel_main_side_preference_matrix.json` | Covers transfer matching, receiver-preferred continuation, sender-preferred continuation, receiver-first sender preference, simple neutral preference wakeup, neutral tasklet transfer, multi-party neutral ordering, and main-side send/receive preference order. |
| `test_scheduler.py::test_scheduler_run_order`, `test_tasklet_run_order`, `test_tasklet_run_order_2`, `capiTest/Scheduler.cpp::PyScheduler_RunNTasklets` | core | `fixtures/scheduler/run_order.json`, `fixtures/scheduler/scheduler_run_n_tasklets_one_fifo_boundary.json`, `fixtures/scheduler/tasklet_run_nested_target_first.json`, `fixtures/scheduler/tasklet_run_nested_target_middle.json`, `fixtures/scheduler/tasklet_run_no_nested_target_middle.json` | FIFO runnable order, bounded one-tasklet scheduler pumping, and targeted `tasklet.run()` ordering for nested and non-nested modes. |
| `test_scheduler.py::test_nested_tasklet_run_order`, `test_nested_tasklet_run_order_with_schedule`, `test_multi_level_nested_tasklet_run_order_with_schedule`, `test_multi_level_nested_tasklet_run_order_with_yield_to_blocked` | core | `fixtures/scheduler/nested_tasklet_run_order.json`, `fixtures/scheduler/nested_tasklet_run_order_no_nested.json`, `fixtures/scheduler/nested_tasklet_schedule_order.json`, `fixtures/scheduler/nested_tasklet_schedule_order_no_nested.json`, `fixtures/scheduler/multi_level_nested_schedule_order.json`, `fixtures/scheduler/multi_level_nested_schedule_order_no_nested.json`, `fixtures/scheduler/nested_yield_to_blocked.json`, `fixtures/scheduler/nested_yield_to_blocked_no_nested.json` | Single-level, multi-level, and blocked-yield nested tasklet variants are covered for nested and non-nested modes. |
| `test_channel.py::test_main_tasklet_receive_deadlock_after_running_child_tasklets`, `test_main_tasklet_send_deadlock_after_running_child_tasklets`, `test_main_tasklet_blocking_without_a_sender`, `test_main_tasklet_blocking_without_receiver` | core | `fixtures/scheduler/deadlock.json`, `fixtures/scheduler/main_receive_immediate_deadlock.json`, `fixtures/scheduler/main_send_immediate_deadlock.json`, `fixtures/scheduler/main_send_deadlock_after_running_child_tasklets.json` | Main tasklet cannot become the final blocked tasklet. Existing runnable children are drained before the deadlock error in the receive/send-after-children tests. |
| `test_channel.py::test_preference_sender`, `test_preference_receiver`, `test_preference_neither_simple`, `test_preference_neither` | core | `fixtures/scheduler/send_receive_match.json`, `fixtures/scheduler/channel_preference_sender.json`, `fixtures/scheduler/channel_preference_sender_receiver_first.json`, `fixtures/scheduler/channel_preference_neither_simple.json`, `fixtures/scheduler/channel_preference_neither_tasklet_transfer.json`, `fixtures/scheduler/channel_preference_neither_multi_party_order.json`, `fixtures/scheduler/channel_main_side_preference_matrix.json` | Preference `-1`, sender preference `1`, simple neutral preference `0`, receiver-first sender preference, neutral tasklet transfer, multi-party neutral ordering, and main-side preference order are covered in core. |
| `test_channel.py::test_channel_receive_queue_order`, `test_channel_send_queue_order` | core | `fixtures/scheduler/channel_queue_introspection_order.json` | Blocked sender/receiver queues are FIFO from the user-visible perspective and trace blocked queue snapshots. |
| `test_channel.py::test_send_exception`, `test_send_throw`, `test_send_throw_prefence_send` | core then ffi | `fixtures/scheduler/channel_exception_payloads.json`, `fixtures/scheduler/channel_send_exception_main_receive.json` | Symbolic exception payloads are covered in core for tasklet and main receive paths; exact Python exception object/value/traceback behavior remains in the bridge. |
| `test_channel.py::test_send_on_closed`, `test_receive_on_closed`, `test_closing`, `test_open`, `test_iterator_on_closed` | core then ffi | `fixtures/scheduler/channel_close_open_parity.json` | Channel open/closing state and closed send/receive errors are covered in core; iterator protocol and exact Python exceptions stay in the bridge. |
| `test_channel.py::test_block_trap_send`, `test_block_trap_recv`, `test_attempting_send_on_block_trapped_tasklet_does_not_change_balance`, `test_attempting_receive_on_block_trapped_tasklet_does_not_change_balance` | core | `fixtures/scheduler/channel_block_trap_no_balance_change.json`, `fixtures/scheduler/channel_block_trap_nonblocking_transfer.json` | Core rejects would-block operations under block trap without mutating balance or queues and allows nonblocking transfers. |
| `test_channel.py::test_set_channel_callback`, `test_channel_callback_with_blocking_send`, `test_channel_callback_with_blocking_receive` | core then ffi/partial | `fixtures/scheduler/channel_callback_will_block.json` plus bridge tests | Core records callback will-block points; Python-level callback storage/invocation/order for blocking send/receive are covered in the bridge tests. Broader callback exception/reentrancy behavior remains open. |
| `test_channel.py::*refcount*`, `test_*cleanup_on_thread_finish`, `test_inter_thread_communication`, `test_nested_channel_with_parent_death_running_fine_and_cleaning_up_correctly` | core/ffi split | `fixtures/scheduler/channel_kill_error_cleanup_while_blocked.json`, `fixtures/scheduler/channel_close_cleanup_blocked_receivers.json`, `fixtures/scheduler/channel_clear_pending_exit_cleanup.json`, plus Python bridge tests | Core now covers blocked sender/receiver close cleanup and pending-exit channel clear teardown; Python object lifetime, GC, threads, and Greenlet parent behavior remain in the bridge. |
| `test_queuechannel.py::*` | bridge | Rust PyO3 unchanged legacy suite | `QueueChannel` stays as the legacy Python wrapper over the Rust-backed base `channel` for the current compatibility path. All 10 unchanged legacy QueueChannel tests pass, covering buffered sends, balance/length, queued data, blocking receive wakeup, queued exception/throw, main-tasklet empty receive error, nested blocked receiver order, block-trap receive rejection, and main receive drain. A separate Rust buffered-channel feature is deferred unless QueueChannel is deliberately moved into core later. |
| `test_tasklet.py::test_run`, `test_run_args`, `test_run_args_kwargs`, `test_remove_and_run`, `test_remove_and_switch`, `test_paused`, `test_kill_tasklet`, `test_times_switched_to*` | core then ffi | `fixtures/scheduler/tasklet_lifecycle_remove_insert_switch_times.json`, `fixtures/scheduler/tasklet_kill_pending_and_immediate.json`, plus unchanged Rust PyO3 bridge subset for direct run/args/kwargs | Core fixtures cover remove/insert/switch counts and kill pending/immediate; Python bridge covers direct run, argument passing, core-owned pause/resume snapshots for bind/remove/insert, and direct-run paused eligibility. Remaining Greenlet continuation semantics are not report-ready. |
| `test_tasklet.py::test_raise_exception`, `TestTaskletThrow*`, `TestKill*`, `TestExceptions*`, `TestTaskletExitException*`, `TestTaskletDontRaise*` | deferred | trace later | Needs exception injection semantics; exact Python exception, tracer, and frame behavior is deferred. |
| `test_tasklet.py::test_invalid_tasklet_when_skipping_init`, `test_invalid_tasklet_when_skipping_new`, `test_weakref_in_tasklet_new`, cyclic cleanup tests, `TestBind*`, metrics tests | core/ffi split | `fixtures/scheduler/tasklet_bind_setup_rebind_run.json`, `fixtures/scheduler/tasklet_unbind_paused.json`, plus Rust PyO3 bridge unit tests/unchanged subset | Core now guards bind(args), setup enqueue, rebind after run, unbind of a paused tasklet, explicit pause/resume state, and times_switched_to reset. Python type/lifetime, weakref/GC, metrics, and exact error paths stay in the bridge tests. |
| `test_tasklet.py::*_from_another_thread`, `test_new_tasklets_cleanup_on_thread_finish`, `test_partially_complete_tasklets_cleanup_on_thread_finish` | deferred | none initially | Thread ownership and cross-thread safety are out of first milestone scope. |
| `test_scheduler.py::test_schedule`, `test_schedule_remove_fail`, `TestRun::test_calling_run_from_non_main_tasklet`, `TestSwitch::*`, `TestSwitchTrap::*` | core then ffi/deferred | `fixtures/scheduler/nested_tasklet_schedule_order.json`, `fixtures/scheduler/nested_tasklet_schedule_order_no_nested.json`, `fixtures/scheduler/schedule_remove_reinsert_paused_tasklet.json`, `fixtures/scheduler/scheduler_switch_trap_no_state_mutation.json` | Schedule order, schedule-remove pause/reinsert, and scheduler switch-trap no-mutation behavior are covered in core; non-main `scheduler.run()` and Python exception object/text details remain bridge coverage. |
| `test_scheduler.py::test_set_schedule_callback`, `test_schedule_callback_basic`, `test_schedule_callback_with_multiple_threads`, `TestCAPIExposure::test_has_capi_attribute` | ffi/partial | trace optional | `_C_API` exposure, callback get/set, schedule callback basic ordering, multi-thread visibility, and fast C callback smoke now pass; callback exception/reentrancy behavior remains open. |

## C API Test Mapping

| Source tests | Primary lane | Notes for next agents |
| --- | --- | --- |
| `capiTest/Tasklet.cpp::PyTasklet_New`, `PyTasklet_Check`, `PyTasklet_IsMain` | ffi | Covered by Rust PyO3 bridge C API tests and by the real legacy `Tasklet.cpp` source slice for constructor/type/main-tasklet behavior. Core supplies tasklet identity and main flag. |
| `capiTest/Tasklet.cpp::PyTasklet_Setup`, `PyTasklet_Insert`, `PyTasklet_Alive`, `PyTasklet_Kill` | core then ffi | Covered by the real legacy `Tasklet.cpp` source slice plus PyO3 bridge tests for simple setup/run callable workload, insert/kill, alive state, run-count updates, and refcount behavior around setup/kill. The optional in-process source-slice probe currently fails after the first test due repeated embedded interpreter reinitialization, so full in-process lifecycle remains a broader C API binary blocker. |
| `capiTest/Tasklet.cpp::PyTasklet_Setup_ReferenceCount` | ffi | Covered by the real legacy `Tasklet.cpp` source slice and `c_api_tasklet_setup_reference_counts_match_legacy_capi_test`; scheduling keeps one queue reference, duplicate setup does not leak, and kill releases it. |
| `capiTest/Tasklet.cpp::PyTasklet_GetBlockTrap` | core then ffi | Covered by the real legacy `Tasklet.cpp` source slice; core owns the flag and FFI exposes Python property compatibility. |
| `capiTest/Channel.cpp::PyChannel_New`, `PyChannel_Check` | ffi | Covered by the real legacy `Channel.cpp` source slice for constructor/type exposure. |
| `capiTest/Channel.cpp::PyChannel_Send`, `PyChannel_Receive`, `PyChannel_GetBalance`, `PyChannel_SetPreference` | core then ffi | Covered by the real legacy `Channel.cpp` source slice for balance, blocking transfer, preference clamping, and receive/send handoff. |
| `capiTest/Channel.cpp::PyChannel_GetQueue` | core then ffi | Covered by the real legacy `Channel.cpp` source slice; FFI returns the exact blocked tasklet object. |
| `capiTest/Channel.cpp::PyChannel_Send_With_Killed_Tasklet`, `PyChannel_Receive_With_Killed_Tasklet` | ffi | Covered by the real legacy `Channel.cpp` source slice for Python C helper state and kill cleanup details. |
| `capiTest/Channel.cpp::PyChannel_SendException`, `PyChannel_SendThrow`, `PyChannel_SendThrow_NoValueOrTb` | ffi | Covered by the real legacy `Channel.cpp` source slice for exception type/value/traceback transfer through C API. |
| `capiTest/Scheduler.cpp::PyScheduler_Schedule`, `PyScheduler_GetRunCount`, `PyScheduler_GetCurrent`, `PyScheduler_RunNTasklets`, `PyScheduler_RunForTime` | core then ffi | Covered by the real legacy `Scheduler.cpp` source slice for current/run-count/run-n behavior and practical timeout pumping semantics; core fixtures now include bounded run-n, zero-timeout one-tasklet progress, remaining scheduled tasklets after an expired timeout, and large-timeout drain counters. |
| `capiTest/Scheduler.cpp::PyScheduler_SetChannelCallback`, `PyScheduler_GetChannelCallback`, `PyScheduler_SetScheduleCallback`, `PyScheduler_SetScheduleFastcallback` | ffi/partial | Covered by the real legacy `Scheduler.cpp` source slice for callback registration, invocation, identity, and fast C callback smoke; broader callback exception/reentrancy behavior remains open. |
| `capiTest/Scheduler.cpp::PyScheduler_GetNumberOfActiveScheduleManagers`, `PyScheduler_GetNumberOfActiveChannels`, `PyScheduler_GetAllTimeTaskletCount`, `PyScheduler_GetActiveTaskletCount`, `PyScheduler_GetTaskletsCompletedLastRunWithTimeout`, `PyScheduler_GetTaskletsSwitchedLastRunWithTimeout` | core then ffi | Covered by the real legacy `Scheduler.cpp` source slice for active manager/channel/tasklet counters and last-timeout completed/switched counters; `scheduler_timeout_large_counts_all_tasklets` asserts the simple 3-completed/6-switched legacy counter case in core. |

## Scheduler Fixture Corpus

The scheduler fixture corpus lives in `fixtures/scheduler/`. It currently has
48 JSON fixtures: 47 with ordered event expectations and one final-state
timeout snapshot. All run trace-gate invariant checks for event sequence order,
run-count/tasklet-count consistency, blocked
tasklet state, channel balance, queue-front consistency, and channel/tasklet
blocked-queue cross-links. These fixtures
intentionally avoid Python object references. Values are encoded as JSON
primitives or tagged objects, tasklets are stable names, and channels are stable
names. The current Rust gate parses this v0 envelope with
`carbon-scheduler-trace`; run it with `cargo run -p xtask -- scheduler-fixtures`.

## Native Performance Evidence

`scripts/carbon-native-bench.sh` records release-native evidence with
`RUSTFLAGS` including `-C target-cpu=native`. Its default `all` mode runs the
Tier 1 benchmark evidence, matched scheduler comparison evidence, scheduler
pressure evidence, IO workload evidence, and progress report refresh. The
scheduler comparison evidence now records matched legacy C++ scheduler vs Rust
scheduler bridge pressure rows with semantic checksums, throughput, p99/p99.9,
CPU p95, peak RSS p95, and throughput CV; those rows are lab evidence until a
real game-environment workload exists.
For resources process comparisons, `bench-tier-local.json` now records
`optimization_readiness`, selected legacy resources CLI paths, surrounding
CMake build type, and `legacy_known_non_debug`. On this checkout the native
benchmark helper selects the local `legacy_resources_release` binaries when
available, so the resource rows are eligible for optimized-baseline speedup
claims while broader report gates still apply.
The IO workload evidence now also records release-native native CPU context and
validates the timing-free `fixtures/io` semantic trace corpus, but it remains a
Python stdlib baseline vs Rust scheduler bridge observation until a captured
legacy Carbon IO semantic trace comparison exists.

Speedup language is gated separately from semantic parity: report rows can show
debug or non-native observed ratios, but a speedup claim requires a comparable
legacy-vs-Rust process row from Rust `release-native`, `target_cpu_native=true`,
debug assertions off, and a selected known non-debug legacy baseline. Use
`CARBON_LEGACY_RESOURCES_CLI` and `CARBON_LEGACY_RESOURCES_DEV_CLI` with
`scripts/carbon-native-bench.sh bench` when a compatible Release/RelWithDebInfo
legacy resources build exists.

## Resources Current Slice

The first Rust `resources` slice lives in `carbon-resources-core` and is
tracked by `cargo run -p xtask -- rust-resources`.

| Feature area | Current Rust proof | Remaining parity work |
| --- | --- | --- |
| checksums/compression/streams/matching chunks/chunk indexes | MD5 string/file checksums, MD5 stream checksums over legacy file chunks, legacy FNV path checksum, rolling Adler checksum formula, FindMatchingChunks string cases, FindMatchingChunk file offset cases, CountMatchingChunks patch fixture offsets, ChunkIndex generation/persisted lookup, ChunkIndex checksum-filtered generation/lookup, gzip fixture decompression, gzip round trip, chunked gzip stream compression/decompression, FileDataStreamIn chunked reads, FileDataStreamOut byte output, and CompressedFileDataStreamOut gzip roundtrip | Broader streaming edge cases and larger legacy corpus checks. |
| ResourceGroup import/export | Legacy YAML/CSV import, v0 CSV prefix paths, malformed YAML/CSV result categories, future-minor version clamping, future-major version rejection, byte-for-byte small YAML/CSV exports including Linux/macOS/Windows create-group YAML, skip-compression YAML, CSV, and prefixed CSV goldens with platform-specific `BinaryOperation` values, large `Indicies` normal and binary-operation v0 CSV to v0.1 YAML export, `Indicies` YAML corpus round-trips, top-level resources CLI help-shape output for no command, invalid command, and `--help`, and operation-specific `--help`/no-argument usage-shape output for all resource commands | Broader fixture corpus coverage and detailed CLI output/error text compatibility. |
| create-group directory discovery | Linux create-group fixture outputs for YAML, skip-compression YAML, sorted legacy CSV, and prefixed legacy CSV; Linux/macOS/Windows create-group goldens round-trip byte-for-byte through import/export with platform `BinaryOperation` values preserved | Export-resources behavior, invalid input result codes, streaming threshold coverage, and cross-host generation of platform binary-operation values. |
| catalog merge | YAML additive, YAML identical, CSV additive, and CSV intersect fixture outputs | CLI error paths, mixed document-version conversion beyond current fixtures, and larger corpus coverage. |
| catalog diff | CSV additions, changes, and subtractions fixture outputs | CLI error paths, YAML diff fixtures if required by legacy behavior, and larger corpus coverage. |
| remove resources | YAML remove-resource fixture output plus missing-resource ignore/error handling | CSV output fixtures, CLI error paths, and larger corpus coverage. |
| BundleGroup manifests and bytes | Create-bundle local CDN, create-bundle remote CDN, and unpack fixture BundleGroup YAML import/export match goldens byte-for-byte; ResourceTools bundle stream splitting now covers many files into many uncompressed chunks and many files into one compressed chunk with exact reconstruction; local bundle create emits manifest/chunk bytes; remote-CDN compressed bundle create emits manifest/payload bytes; local unpack and compressed remote-CDN unpack restore resource bytes for current fixtures; local CLI evidence covers a create-and-unpack roundtrip with stable YAML and payload byte checks, create-bundle zero-chunk and missing-resource-source failure cleanup, plus a 42-chunk unpack fixture with 41 full 1000-byte chunks, one 961-byte tail chunk, resources spanning chunk boundaries, missing-chunk failure cleanup, remote-requested-local chunk failure cleanup, and local-requested-remote compressed chunk failure cleanup; local remote-CDN mirror/cache tests and process CLI evidence cover first-run downloads, second-run cache hits, bad-cache replacement, and checksum-failure behavior without network services | Broader bundle corpus coverage, error paths, network-backed remote source behavior, and remote catalog behavior. |
| PatchGroup manifests and payloads | Create-patch, chunked create-patch, and old patch fixture PatchGroup YAML import/export match goldens byte-for-byte, including removed-resource metadata, max input chunk size, empty patch locations, and optional compressed sizes; local CDN patch payload bytes validate against manifest location, uncompressed size, and checksum; corrupt local patch payloads are rejected before output; low-level BSDIFF corruption rejects short payloads, invalid headers, truncated streams, and target-length mismatches; Rust-generated BSDIFF payloads match normal, chunked, and old-layout local CDN payload bytes; normal/no-change/chunked local create-patch CLI cases, zero-chunk and missing previous/next resource-source create-patch failure cleanup, a synthetic copy-only create/apply CLI case with no generated binary patch payloads, normal/chunked/old-layout apply-patch cases, malformed local apply manifest rejection for zero apply chunk size, target offset overflow, source range overflow, and overlapping copy ranges, and missing previous/next resource input plus missing/corrupt apply-patch payload failure cleanup are covered, including old-layout no-removal semantics | Broader temp-file cleanup modes, larger patch corpus coverage, and final benchmark/report promotion. |
| legacy filters and CreateFromFilter | Prefix maps, `*`/`...` matching, include/exclude rules, local respath rule quirks, multi-prefix matching, generated wildcard/ellipsis path-matrix coverage, generated include/exclude section-property coverage, filter index mapping parsing, CreateFromFilter ResourceGroup output | Broader filter-file corpus and fuzz coverage. |
