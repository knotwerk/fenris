#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use carbon_resources_core::{
    apply_legacy_local_patch_from_directories, create_legacy_local_bundle_from_resource_group,
    create_legacy_local_patch_from_resource_groups_with_options,
    create_legacy_remote_cdn_bundle_from_resource_group,
    create_legacy_resource_group_from_directory, create_legacy_resource_groups_from_filter_mapping,
    diff_legacy_resource_catalogs, export_legacy_csv_resource_group, export_legacy_diff,
    export_legacy_local_relative_resources, export_legacy_yaml_bundle_resource_group,
    export_legacy_yaml_patch_resource_group, export_legacy_yaml_resource_group, gzip_compress,
    md5_hex, merge_legacy_resource_catalogs, parse_legacy_csv_resource_group,
    parse_legacy_filter_index_mapping_yaml, parse_legacy_filter_ini,
    parse_legacy_yaml_bundle_resource_group, parse_legacy_yaml_patch_resource_group,
    parse_legacy_yaml_resource_group, remove_legacy_resources, unpack_legacy_local_bundle_from_cdn,
    unpack_legacy_remote_bundle_from_local_mirror, LegacyBundleDataResource,
    LegacyPatchDataResource, ResourceCatalog, ResourceRecord,
};
use carbon_scheduler_core::{
    run_scenario, ChannelSpec, Entrypoint, Operation, Scenario, TaskletSpec,
};
use carbon_scheduler_trace::{assert_report_pass, run_fixture_dir};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::hint::black_box;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use std::time::Instant;

const EVIDENCE_DIR: &str = "target/carbon/evidence";
const RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET: &[&str] = &[
    "test_scheduler.TestCAPIExposure.test_has_capi_attribute",
    "test_tasklet.TestTasklets.test_run",
    "test_tasklet.TestTasklets.test_run_args",
    "test_tasklet.TestTasklets.test_run_args_kwargs",
    "test_tasklet.TestTasklets.test_kill_tasklet",
    "test_tasklet.TestTasklets.test_paused",
    "test_tasklet.TestTasklets.test_remove_and_run",
    "test_tasklet.TestTasklets.test_invalid_tasklet_when_skipping_init",
    "test_tasklet.TestTasklets.test_invalid_tasklet_when_skipping_new",
    "test_tasklet.TestTasklets.test_weakref_in_tasklet_new",
    "test_tasklet.TestTasklets.test_cyclical_callable_reference_cleans_up",
    "test_tasklet.TestTasklets.test_tasklets_with_cyclical_argument_cleans_up",
    "test_scheduler.TestSwitchTrap.test_run_raising_function",
    "test_scheduler.TestSwitchTrap.test_schedule",
    "test_scheduler.TestSwitchTrap.test_run",
    "test_scheduler.TestSwitchTrap.test_run2",
    "test_scheduler.TestSwitchTrap.test_run_specific",
    "test_scheduler.TestSwitchTrap.test_run_paused",
    "test_scheduler.TestSwitchTrap.test_schedule_remove",
    "test_scheduler.TestSwitchTrap.test_kill",
    "test_scheduler.TestSwitchTrap.test_raise_exception",
    "test_scheduler.TestSchedule.test_schedule",
    "test_scheduler.TestSchedule.test_schedule_remove_fail",
    "test_scheduler.TestSchedule.test_set_schedule_callback",
    "test_scheduler.TestSchedule.test_schedule_callback_basic",
    "test_scheduler.TestTaskletRunOrderBaseWithNestedTasklets.test_tasklet_run_order",
    "test_scheduler.TestTaskletRunOrderBaseWithNestedTasklets.test_tasklet_run_order_2",
    "test_scheduler.TestTaskletRunOrderBaseWithoutNestedTasklets.test_tasklet_run_order",
    "test_scheduler.TestTaskletRunOrderBaseWithoutNestedTasklets.test_tasklet_run_order_2",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_scheduler_run_order",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_scheduler_run_order",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_scheduler_run_order",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_scheduler_run_order",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_nested_tasklet_run_order",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_nested_tasklet_run_order",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_nested_tasklet_run_order",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_nested_tasklet_run_order",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_multi_level_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_multi_level_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_multi_level_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_multi_level_nested_tasklet_run_order_with_schedule",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_multi_level_nested_tasklet_run_order_with_yield_to_blocked",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_multi_level_nested_tasklet_run_order_with_yield_to_blocked",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_multi_level_nested_tasklet_run_order_with_yield_to_blocked",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_multi_level_nested_tasklet_run_order_with_yield_to_blocked",
    "test_scheduler.TestSwitch.test_switch",
    "test_scheduler.TestSwitch.test_switch_paused",
    "test_scheduler.TestSwitch.test_switch_paused_trapped",
    "test_scheduler.TestSwitch.test_switch_trapped",
    "test_channel.TestChannels.test_blocking_send",
    "test_channel.TestChannels.test_blocking_receive",
    "test_channel.TestChannels.test_non_blocking_receive",
    "test_channel.TestChannels.test_block_trap_send",
    "test_channel.TestChannels.test_block_trap_recv",
    "test_channel.TestChannels.test_main_tasklet_blocking_without_a_sender",
    "test_channel.TestChannels.test_main_tasklet_blocking_without_receiver",
    "test_channel.TestChannels.test_main_tasklet_receive_deadlock_after_running_child_tasklets",
    "test_channel.TestChannels.test_main_tasklet_send_deadlock_after_running_child_tasklets",
    "test_channel.TestChannels.test_send_on_closed",
    "test_channel.TestChannels.test_receive_on_closed",
    "test_channel.TestChannels.test_closing",
    "test_channel.TestChannels.test_open",
    "test_channel.TestChannels.test_iterator_on_closed",
    "test_channel.TestChannels.test_channel_iterator_interface",
    "test_channel.TestChannels.test_attempting_send_on_block_trapped_tasklet_does_not_change_balance",
    "test_channel.TestChannels.test_attempting_receive_on_block_trapped_tasklet_does_not_change_balance",
    "test_channel.TestChannels.test_invalid_channel_when_skipping_new",
    "test_channel.TestChannels.test_invalid_channel_when_skipping_init",
    "test_channel.TestChannels.test_blocked_tasklet_next_is_none",
    "test_channel.TestChannels.test_set_channel_callback",
    "test_channel.TestChannels.test_channel_callback_with_blocking_receive",
    "test_channel.TestChannels.test_channel_callback_with_blocking_send",
    "test_channel.TestChannels.test_channel_receive_queue_order",
    "test_channel.TestChannels.test_channel_send_queue_order",
    "test_channel.TestChannels.test_pending_kill_on_completed_transfer_prefer_receiver",
    "test_channel.TestChannels.test_pending_kill_on_completed_transfer_prefer_sender",
    "test_channel.TestChannels.test_preference_neither_simple",
    "test_channel.TestChannels.test_send_exception",
    "test_channel.TestChannels.test_send_throw",
    "test_channel.TestChannels.test_send_throw_prefence_send",
    "test_queuechannel.TestQueueChannels.test_non_blocking_send",
    "test_queuechannel.TestQueueChannels.test_channel_balance",
    "test_queuechannel.TestQueueChannels.test_queue_data",
    "test_queuechannel.TestQueueChannels.test_blocking_receive",
    "test_queuechannel.TestQueueChannels.test_send_exception",
    "test_queuechannel.TestQueueChannels.test_send_throw",
    "test_queuechannel.TestQueueChannels.test_main_tasklet_blocking_without_receiver",
    "test_queuechannel.TestQueueChannels.test_blocked_tasklets_greenlet_is_not_parent",
    "test_queuechannel.TestQueueChannels.test_block_trap_send",
    "test_queuechannel.TestQueueChannels.test_blocking_receive_on_main_tasklet",
    "test_tasklet.TestBind.test_bind",
    "test_tasklet.TestBind.test_bind_fail_not_callable",
    "test_tasklet.TestBind.test_unbind_ok",
    "test_tasklet.TestBind.test_unbind_fail_current",
    "test_tasklet.TestBind.test_unbind_fail_scheduled",
    "test_tasklet.TestBind.test_bind_noargs",
    "test_tasklet.TestBind.test_bind_args",
    "test_tasklet.TestBind.test_bind_kwargs",
    "test_tasklet.TestBind.test_bind_args_kwargs",
    "test_tasklet.TestBind.test_bind_args_kwargs_nofunc",
    "test_tasklet.TestBind.test_bind_args_not_runnable",
    "test_tasklet.TestBind.test_rebind_after_run",
    "test_tasklet.TestTasklets.test_raise_exception",
    "test_tasklet.TestTasklets.test_bind_from_another_thread",
    "test_tasklet.TestTasklets.test_insert_from_another_thread",
    "test_tasklet.TestTasklets.test_kill_from_another_thread",
    "test_tasklet.TestTasklets.test_remove_and_switch",
    "test_tasklet.TestTasklets.test_run_from_another_thread",
    "test_tasklet.TestTasklets.test_switch_from_another_thread",
    "test_tasklet.TestTaskletExitException.test_tasklet_raising_standard_exception",
    "test_tasklet.TestTaskletExitException.test_tasklet_raising_TaskletExit_exception",
    "test_tasklet.TestTaskletExitException.test_tasklet_cannot_accidentally_catch_taskletexit",
    "test_tasklet.TestTaskletExitException.test_tasklet_get_frame",
    "test_tasklet.TestTaskletExitException.test_call_setup_twice",
    "test_tasklet.TestTaskletExitException.test_kill_unbound_tasklet",
    "test_tasklet.TestTaskletMetricsCollection.test_method_name",
    "test_tasklet.TestTaskletMetricsCollection.test_line_number",
    "test_tasklet.TestTaskletMetricsCollection.test_file_name",
    "test_tasklet.TestTaskletMetricsCollection.test_start_end_time",
    "test_tasklet.TestTaskletDontRaise.test_tasklet_dont_raise",
    "test_tasklet.TestTaskletDontRaise.test_tasklet_with_tracer",
    "test_tasklet.TestTaskletDontRaise.test_raising_tasklet_with_tracer",
    "test_tasklet.TestTaskletDontRaise.test_tasklet_with_raising_tracer_enter",
    "test_tasklet.TestTaskletDontRaise.test_tasklet_with_raising_tracer_exit",
    "test_tasklet.TestTaskletDontRaise.test_raising_tasklet_with_raising_tracer_exit",
    "test_tasklet.TestTaskletDontRaise.test_exception_handler",
    "test_tasklet.TestTaskletDontRaise.test_exception_handler_none",
    "test_tasklet.TestTaskletDontRaise.test_exception_handler_raises",
    "test_scheduler.TestRun.test_calling_run_from_non_main_tasklet",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_channel_usage_schedule_order_preference_receiver",
    "test_scheduler.TestScheduleOrderNoLimitWithNestedTasklets.test_schedule_callback_with_multiple_threads",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_channel_usage_schedule_order_preference_receiver",
    "test_scheduler.TestScheduleOrderWithLimitWithNestedTasklets.test_schedule_callback_with_multiple_threads",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_channel_usage_schedule_order_preference_receiver",
    "test_scheduler.TestScheduleOrderWithLimitWithoutNestedTasklets.test_schedule_callback_with_multiple_threads",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_channel_usage_schedule_order_preference_receiver",
    "test_scheduler.TestScheduleOrderWithoutLimitWithoutNestedTasklets.test_schedule_callback_with_multiple_threads",
    "test_scheduler.TestSwitch.test_switch_blocked",
    "test_scheduler.TestSwitch.test_switch_blocked_trapped",
    "test_scheduler.TestSwitch.test_switch_self",
    "test_scheduler.TestSwitch.test_switch_self_trapped",
    "test_scheduler.TestSwitchTrap.test_receive",
    "test_scheduler.TestSwitchTrap.test_receive_throw",
    "test_scheduler.TestSwitchTrap.test_send",
    "test_scheduler.TestSwitchTrap.test_send_exception",
    "test_scheduler.TestSwitchTrap.test_send_throw",
    "test_channel.TestChannels.test_blocked_tasklets_greenlet_is_not_parent",
    "test_channel.TestChannels.test_blocking_receive_on_main_tasklet",
    "test_channel.TestChannels.test_blocking_send_on_main_tasklet",
    "test_channel.TestChannels.test_channel_args_refcount_prefer_receive",
    "test_channel.TestChannels.test_channel_args_refcount_prefer_sender",
    "test_channel.TestChannels.test_channel_test_clear_blocked",
    "test_channel.TestChannels.test_inter_thread_communication",
    "test_channel.TestChannels.test_kill_blocked_on_receive_on_closed",
    "test_channel.TestChannels.test_kill_blocked_on_send_on_closed",
    "test_channel.TestChannels.test_kill_tasklet_blocked_on_channel_receive",
    "test_channel.TestChannels.test_kill_tasklet_blocked_on_channel_send",
    "test_channel.TestChannels.test_nested_channel_with_parent_death_running_fine_and_cleaning_up_correctly",
    "test_channel.TestChannels.test_non_blocking_send",
    "test_channel.TestChannels.test_pending_kill_blocked_receive_tasklet",
    "test_channel.TestChannels.test_pending_kill_blocked_send_tasklet",
    "test_channel.TestChannels.test_preference_neither",
    "test_channel.TestChannels.test_preference_receiver",
    "test_channel.TestChannels.test_preference_sender",
    "test_channel.TestChannels.test_raise_exception_blocked_on_receive_on_closed",
    "test_channel.TestChannels.test_raise_exception_blocked_on_send_on_closed",
    "test_channel.TestChannels.test_receive_on_channel_that_had_previously_been_blocked_and_continued_after_an_exception_is_raised_on_it",
    "test_channel.TestChannels.test_receiving_tasklets_rescheduled_by_channel_are_run",
    "test_channel.TestChannels.test_send_on_channel_that_had_previously_been_blocked_and_continued_after_an_exception_is_raised_on_it",
    "test_channel.TestChannels.test_sending_tasklets_rescheduled_by_channel_are_run",
    "test_channel.TestChannels.test_tasklet_channel_cleanup_on_thread_finish",
    "test_channel.TestChannels.test_yielding_to_blocked_tasklet_yields_to_parent",
    "test_tasklet.TestBind.test_rebind_main",
    "test_tasklet.TestBind.test_rebind_recursion_depth",
    "test_tasklet.TestBind.test_unbind_main",
    "test_tasklet.TestExceptions.test_raise_exception",
    "test_tasklet.TestExceptions.test_throw_exception",
    "test_tasklet.TestKill.test_kill_current",
    "test_tasklet.TestKill.test_kill_pending_False",
    "test_tasklet.TestKill.test_kill_pending_true",
    "test_tasklet.TestTaskletDontRaise.test_new_tasklets_cleanup_on_thread_finish",
    "test_tasklet.TestTaskletDontRaise.test_partially_complete_tasklets_cleanup_on_thread_finish",
    "test_tasklet.TestTaskletThrowImmediate.test_dead",
    "test_tasklet.TestTaskletThrowImmediate.test_kill_dead",
    "test_tasklet.TestTaskletThrowImmediate.test_kill_new",
    "test_tasklet.TestTaskletThrowImmediate.test_new",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_args",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_exc_info",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_inst",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_invalid",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_noargs",
    "test_tasklet.TestTaskletThrowImmediate.test_throw_traceback",
    "test_tasklet.TestTaskletThrowNonImmediate.test_dead",
    "test_tasklet.TestTaskletThrowNonImmediate.test_kill_dead",
    "test_tasklet.TestTaskletThrowNonImmediate.test_kill_new",
    "test_tasklet.TestTaskletThrowNonImmediate.test_new",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_args",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_exc_info",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_inst",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_invalid",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_noargs",
    "test_tasklet.TestTaskletThrowNonImmediate.test_throw_traceback",
    "test_tasklet.TestTasklets.test_remove_from_another_thread",
    "test_tasklet.TestTasklets.test_setup_from_another_thread",
    "test_tasklet.TestTasklets.test_times_switched_to",
    "test_tasklet.TestTasklets.test_times_switched_to_bind_reset",
];

fn rust_scheduler_unchanged_legacy_subset_count() -> usize {
    RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET.len()
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Ok(());
    };

    match command.as_str() {
        "scheduler-fixtures" => {
            let fixture_dir = args
                .next()
                .unwrap_or_else(|| String::from("fixtures/scheduler"));
            scheduler_fixtures(Path::new(&fixture_dir))
        }
        "scheduler-trace" => {
            let Some(fixture_path) = args.next() else {
                bail!("scheduler-trace requires a fixture path");
            };
            scheduler_trace(Path::new(&fixture_path))
        }
        "legacy-scheduler" => legacy_scheduler(args.collect()),
        "rust-scheduler-python" => rust_scheduler_python(),
        "io-workloads" => io_workloads(),
        "legacy-resources" => legacy_resources(),
        "rust-resources" => rust_resources(),
        "bench" => bench_tier_local(),
        "bench-scalability" => bench_scalability(args.collect()),
        "bench-scalability-worker" => bench_scalability_worker(args.collect()),
        "bench-scheduler-core" => bench_scheduler_core(args.collect()),
        "rust-resources-cli" => {
            std::process::exit(rust_resources_legacy_cli(args.collect()));
        }
        "rust-create-group" => rust_create_group(args.collect()),
        "rust-create-group-from-filter" => rust_create_group_from_filter(args.collect()),
        "rust-create-bundle" => rust_create_bundle(args.collect()),
        "rust-unpack-bundle" => rust_unpack_bundle(args.collect()),
        "rust-create-patch" => rust_create_patch(args.collect()),
        "rust-apply-patch" => rust_apply_patch(args.collect()),
        "rust-merge-group" => rust_merge_group(args.collect()),
        "rust-diff-group" => rust_diff_group(args.collect()),
        "rust-remove-resources" => rust_remove_resources(args.collect()),
        "report-readiness" => report_readiness(),
        "report-progress" => progress_report(),
        "report" => final_report(),
        _ => {
            print_usage();
            bail!("unknown xtask command: {command}");
        }
    }
}

fn scheduler_fixtures(fixture_dir: &Path) -> Result<()> {
    let report = run_fixture_dir(fixture_dir)?;
    let evidence_path = evidence_path("scheduler-fixtures.json");
    let mut evidence = serde_json::to_value(&report)?;
    let linked_local_evidence = scheduler_fixture_linked_evidence();
    let legacy_scheduler_report_ready = read_evidence("legacy-scheduler.json")
        .ok()
        .is_some_and(|value| report_ready_blockers("legacy-scheduler.json", &value).is_empty());
    let io_workloads_report_ready = read_evidence("io-workloads.json")
        .ok()
        .is_some_and(|value| report_ready_blockers("io-workloads.json", &value).is_empty());
    let mut remaining_before_report_ready = vec![String::from(
        "promote remaining lifecycle, callback, switch-trap, nested-timeout, teardown, and cleanup fixture coverage beyond the current symbolic scheduler core slice",
    )];
    if !legacy_scheduler_report_ready {
        remaining_before_report_ready.push(String::from(
            "run the supported-platform legacy scheduler baseline gate before final scheduler parity claims",
        ));
    }
    if !io_workloads_report_ready {
        remaining_before_report_ready.push(String::from(
            "run normalized legacy carbonio/_socket/_ssl semantic trace comparison before final IO scheduler parity claims",
        ));
    }
    if let Some(object) = evidence.as_object_mut() {
        object.insert(
            String::from("linked_local_evidence"),
            linked_local_evidence
                .get("linked_local_evidence")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        object.insert(
            String::from("local_python_capi_bridge_link_status"),
            linked_local_evidence
                .get("local_python_capi_bridge_link_status")
                .cloned()
                .unwrap_or_else(|| json!("missing")),
        );
        object.insert(
            String::from("local_io_scheduler_bridge_link_status"),
            linked_local_evidence
                .get("local_io_scheduler_bridge_link_status")
                .cloned()
                .unwrap_or_else(|| json!("missing")),
        );
        object.insert(
            String::from("remaining_before_report_ready"),
            json!(remaining_before_report_ready),
        );
    }
    write_json(&evidence_path, &evidence)?;

    println!(
        "scheduler-fixtures: {:?} ({} passed, {} failed); evidence {}",
        report.status,
        report.passed,
        report.failed,
        evidence_path.display()
    );

    assert_report_pass(&report)
}

fn scheduler_fixture_linked_evidence() -> Value {
    let rust_scheduler_python = read_evidence("rust-scheduler-python.json").ok();
    let io_workloads = read_evidence("io-workloads.json").ok();

    let rust_status = rust_scheduler_python
        .as_ref()
        .and_then(|value| string_at(value, &["status"]))
        .unwrap_or_else(|| String::from("missing"));
    let capi_binary_smoke_status = rust_scheduler_python
        .as_ref()
        .and_then(|value| string_at(value, &["capi_binary_smoke", "status"]))
        .unwrap_or_else(|| String::from("missing"));
    let tasklet_source_slice_status = rust_scheduler_python
        .as_ref()
        .and_then(|value| string_at(value, &["legacy_capi_tasklet_source_slice", "status"]))
        .unwrap_or_else(|| String::from("missing"));
    let channel_source_slice_status = rust_scheduler_python
        .as_ref()
        .and_then(|value| string_at(value, &["legacy_capi_channel_source_slice", "status"]))
        .unwrap_or_else(|| String::from("missing"));
    let scheduler_source_slice_status = rust_scheduler_python
        .as_ref()
        .and_then(|value| string_at(value, &["legacy_capi_scheduler_source_slice", "status"]))
        .unwrap_or_else(|| String::from("missing"));
    let local_python_capi_bridge_link_status = if rust_status == "pass"
        && capi_binary_smoke_status == "pass"
        && tasklet_source_slice_status == "pass"
        && channel_source_slice_status == "pass"
        && scheduler_source_slice_status == "pass"
    {
        "pass"
    } else if rust_status == "missing" {
        "missing"
    } else {
        "not_green"
    };

    let io_status = io_workloads
        .as_ref()
        .and_then(|value| string_at(value, &["status"]))
        .unwrap_or_else(|| String::from("missing"));
    let scheduler_capi_semantic_smoke_status = io_workloads
        .as_ref()
        .and_then(|value| string_at(value, &["scheduler_capi_semantic_smoke", "status"]))
        .unwrap_or_else(|| String::from("missing"));
    let legacy_carbonio_trace_status = io_workloads
        .as_ref()
        .and_then(|value| {
            string_at(
                value,
                &[
                    "legacy_carbonio_semantic_traces",
                    "legacy_carbonio_trace_status",
                ],
            )
        })
        .unwrap_or_else(|| String::from("missing"));
    let local_io_scheduler_bridge_link_status =
        if io_status == "pass" && scheduler_capi_semantic_smoke_status == "pass" {
            "pass"
        } else if io_status == "missing" {
            "missing"
        } else {
            "not_green"
        };

    json!({
        "linked_local_evidence": {
            "rust_scheduler_python": {
                "file": "rust-scheduler-python.json",
                "status": rust_status,
                "report_ready": rust_scheduler_python
                    .as_ref()
                    .and_then(|value| value.get("report_ready"))
                    .and_then(Value::as_bool),
                "unchanged_legacy_subset_count": rust_scheduler_python
                    .as_ref()
                    .and_then(|value| value.get("unchanged_legacy_subset_count"))
                    .and_then(Value::as_u64),
                "queuechannel_unchanged_legacy_subset_count": rust_scheduler_python
                    .as_ref()
                    .and_then(|value| value.get("queuechannel_unchanged_legacy_subset_count"))
                    .and_then(Value::as_u64),
                "capi_binary_smoke_status": capi_binary_smoke_status,
                "legacy_capi_tasklet_source_slice_status": tasklet_source_slice_status,
                "legacy_capi_channel_source_slice_status": channel_source_slice_status,
                "legacy_capi_scheduler_source_slice_status": scheduler_source_slice_status,
                "legacy_capi_in_process_probe_status": rust_scheduler_python
                    .as_ref()
                    .and_then(|value| string_at(value, &["legacy_capi_in_process_probe_status"]))
                    .unwrap_or_else(|| String::from("missing"))
            },
            "io_workloads": {
                "file": "io-workloads.json",
                "status": io_status,
                "report_ready": io_workloads
                    .as_ref()
                    .and_then(|value| value.get("report_ready"))
                    .and_then(Value::as_bool),
                "scheduler_capi_semantic_smoke_status": scheduler_capi_semantic_smoke_status,
                "legacy_carbonio_trace_status": legacy_carbonio_trace_status,
                "io_process_sample_count": io_workloads
                    .as_ref()
                    .and_then(|value| value.get("io_process_sample_count"))
                    .and_then(Value::as_u64)
            }
        },
        "local_python_capi_bridge_link_status": local_python_capi_bridge_link_status,
        "local_io_scheduler_bridge_link_status": local_io_scheduler_bridge_link_status
    })
}

fn scheduler_trace(fixture_path: &Path) -> Result<()> {
    let fixture = carbon_scheduler_trace::load_fixture(fixture_path)?;
    let trace = run_scenario(&fixture.scenario)
        .with_context(|| format!("running fixture {}", fixture.name))?;
    println!("{}", serde_json::to_string_pretty(&trace)?);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LegacySchedulerMode {
    Build,
    Run,
    BuildRun,
}

impl LegacySchedulerMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Run => "run",
            Self::BuildRun => "build-run",
        }
    }

    fn needs_build(self) -> bool {
        matches!(self, Self::Build | Self::BuildRun)
    }

    fn runs_ctest(self) -> bool {
        matches!(self, Self::Run | Self::BuildRun)
    }
}

struct ProbeStepResult {
    evidence: Value,
    success: bool,
    stdout: String,
    stderr: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LegacySchedulerImportArgs {
    artifact_path: PathBuf,
    host_os: Option<String>,
    host_arch: Option<String>,
    source_command: Option<String>,
}

#[derive(Clone, Copy)]
struct LegacySchedulerTriplet {
    target_triplet: &'static str,
    chainload_toolchain: &'static str,
}

fn parse_legacy_scheduler_mode(args: Vec<String>) -> Result<LegacySchedulerMode> {
    match args.as_slice() {
        [] => Ok(LegacySchedulerMode::BuildRun),
        [mode]
            if matches!(
                mode.as_str(),
                "build-run" | "build_and_run" | "build/run" | "all"
            ) =>
        {
            Ok(LegacySchedulerMode::BuildRun)
        }
        [mode] if mode == "build" => Ok(LegacySchedulerMode::Build),
        [mode] if mode == "run" => Ok(LegacySchedulerMode::Run),
        _ => bail!(
            "legacy-scheduler usage: legacy-scheduler [build|run|build-run|native-linux] | legacy-scheduler import <artifact.json|ctest.log> [--host-os <windows|macos>] [--host-arch <x86_64|aarch64>] [--source-command <command>]"
        ),
    }
}

fn parse_legacy_scheduler_import_args(args: &[String]) -> Result<LegacySchedulerImportArgs> {
    let Some(path) = args.first() else {
        bail!("legacy-scheduler import requires an artifact path");
    };
    let mut parsed = LegacySchedulerImportArgs {
        artifact_path: PathBuf::from(path),
        ..Default::default()
    };
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        let (key, inline_value) = arg
            .split_once('=')
            .map(|(key, value)| (key, Some(value.to_string())))
            .unwrap_or((arg.as_str(), None));
        let value = match inline_value {
            Some(value) => value,
            None => {
                index += 1;
                args.get(index)
                    .cloned()
                    .with_context(|| format!("{key} requires a value"))?
            }
        };
        match key {
            "--host-os" => parsed.host_os = Some(value),
            "--host-arch" => parsed.host_arch = Some(value),
            "--source-command" => parsed.source_command = Some(value),
            _ => bail!("unknown legacy-scheduler import option: {key}"),
        }
        index += 1;
    }
    Ok(parsed)
}

fn legacy_scheduler_supported_triplet() -> Option<LegacySchedulerTriplet> {
    match (env::consts::OS, env::consts::ARCH) {
        ("windows", "x86_64") => Some(LegacySchedulerTriplet {
            target_triplet: "x64-windows-release",
            chainload_toolchain: "x64-windows-carbon.cmake",
        }),
        ("macos", "aarch64") => Some(LegacySchedulerTriplet {
            target_triplet: "arm64-osx-release",
            chainload_toolchain: "arm64-osx-carbon.cmake",
        }),
        ("macos", "x86_64") => Some(LegacySchedulerTriplet {
            target_triplet: "x64-osx-release",
            chainload_toolchain: "x64-osx-carbon.cmake",
        }),
        _ => None,
    }
}

fn legacy_scheduler(args: Vec<String>) -> Result<()> {
    if args.first().is_some_and(|arg| arg == "import") {
        return legacy_scheduler_import(&args[1..]);
    }
    if args
        .first()
        .is_some_and(|arg| arg == "native-linux" || arg == "native")
    {
        return legacy_scheduler_native_linux();
    }

    let mode = parse_legacy_scheduler_mode(args)?;
    let command = match mode {
        LegacySchedulerMode::Build => "probe legacy scheduler CMake/vcpkg configure and build",
        LegacySchedulerMode::Run => "run legacy scheduler CTest baseline from an existing build",
        LegacySchedulerMode::BuildRun => "probe legacy scheduler CMake/vcpkg build and ctest",
    };
    let started = Instant::now();
    let source_dir = Path::new("carbonengine/scheduler");
    let build_dir = Path::new("target/carbon/legacy-scheduler-build");
    let vcpkg_root = source_dir.join("vendor/github.com/microsoft/vcpkg");
    let vcpkg_exe = vcpkg_root.join(if cfg!(windows) { "vcpkg.exe" } else { "vcpkg" });
    let bootstrap_script = vcpkg_root.join(if cfg!(windows) {
        "bootstrap-vcpkg.bat"
    } else {
        "bootstrap-vcpkg.sh"
    });
    let toolchain_file = vcpkg_root.join("scripts/buildsystems/vcpkg.cmake");
    let overlay_triplets =
        source_dir.join("vendor/github.com/carbonengine/vcpkg-registry/triplets");

    let supported_triplet = legacy_scheduler_supported_triplet();
    let mut blockers: Vec<Value> = Vec::new();
    let mut steps: Vec<Value> = Vec::new();
    let mut configure_success = None;
    let mut build_success = None;
    let mut ctest_success = None;
    let mut ctest_summary = None;

    if !source_dir.exists() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_source_missing",
            "carbonengine/scheduler source directory is missing",
            "restore or checkout carbonengine/scheduler before running the legacy scheduler baseline",
        ));
    }

    if mode.needs_build() && !toolchain_file.exists() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_vcpkg_toolchain_missing",
            format!(
                "legacy scheduler vcpkg toolchain is missing at {}",
                toolchain_file.display()
            ),
            "restore the vendored vcpkg toolchain under carbonengine/scheduler",
        ));
    }

    if blockers.is_empty() && mode.needs_build() {
        if !vcpkg_exe.exists() {
            if env::var("CARBON_LEGACY_SCHEDULER_BOOTSTRAP")
                .ok()
                .as_deref()
                == Some("1")
            {
                let args: Vec<String> = Vec::new();
                let bootstrap_program = fs::canonicalize(&bootstrap_script)
                    .unwrap_or_else(|_| bootstrap_script.clone());
                let result = run_probe_step(
                    "bootstrap-vcpkg",
                    &bootstrap_program,
                    &args,
                    Some(&vcpkg_root),
                    &[],
                );
                let ok = result.success;
                steps.push(result.evidence);
                if !ok {
                    blockers.push(readiness_blocker(
                        "legacy_scheduler_vcpkg_bootstrap_failed",
                        "vcpkg bootstrap failed",
                        "fix the vendored vcpkg bootstrap failure or provide a prebuilt legacy scheduler vcpkg executable",
                    ));
                }
            } else {
                blockers.push(readiness_blocker(
                    "legacy_scheduler_vcpkg_missing",
                    format!(
                        "vcpkg executable is missing at {}; set CARBON_LEGACY_SCHEDULER_BOOTSTRAP=1 to let this gate bootstrap it",
                        vcpkg_exe.display()
                    ),
                    "bootstrap or provide a usable legacy scheduler vcpkg installation on this host",
                ));
            }
        }

        if blockers.is_empty() && vcpkg_exe.exists() {
            let repo_root = env::current_dir().context("locating repository root")?;
            let source_dir_abs = repo_root.join(source_dir);
            let build_dir_abs = repo_root.join(build_dir);
            let toolchain_file_abs = repo_root.join(&toolchain_file);
            let overlay_triplets_abs = repo_root.join(&overlay_triplets);
            let vcpkg_root_abs = repo_root.join(&vcpkg_root);

            if build_dir.exists() {
                fs::remove_dir_all(build_dir).with_context(|| {
                    format!(
                        "cleaning legacy scheduler build dir {}",
                        build_dir.display()
                    )
                })?;
            }
            if let Some(parent) = build_dir.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("creating legacy scheduler build root {}", parent.display())
                })?;
            }

            let mut configure_args = Vec::new();
            if command_stdout("ninja", &["--version"]).is_ok() {
                configure_args.push(String::from("-G"));
                configure_args.push(String::from("Ninja"));
            }
            configure_args.extend([
                String::from("-S"),
                source_dir_abs.display().to_string(),
                String::from("-B"),
                build_dir_abs.display().to_string(),
                String::from("-DCMAKE_BUILD_TYPE=Release"),
                String::from("-DBUILD_TESTING=ON"),
                format!("-DCMAKE_TOOLCHAIN_FILE={}", toolchain_file_abs.display()),
                format!(
                    "-DVCPKG_OVERLAY_TRIPLETS={}",
                    overlay_triplets_abs.display()
                ),
            ]);
            if let Some(triplet) = supported_triplet {
                configure_args.extend([
                    format!("-DVCPKG_TARGET_TRIPLET={}", triplet.target_triplet),
                    format!("-DVCPKG_HOST_TRIPLET={}", triplet.target_triplet),
                    format!(
                        "-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE={}",
                        source_dir
                            .join("vendor/github.com/carbonengine/vcpkg-registry/toolchains")
                            .join(triplet.chainload_toolchain)
                            .display()
                    ),
                ]);
            } else {
                configure_args.push(String::from("-DVCPKG_INSTALL_OPTIONS=--allow-unsupported"));
            }
            let envs = vec![(
                String::from("VCPKG_ROOT"),
                vcpkg_root_abs.display().to_string(),
            )];
            let configure_result = run_probe_step(
                "cmake-configure",
                Path::new("cmake"),
                &configure_args,
                None,
                &envs,
            );
            let configure_output =
                format!("{}\n{}", configure_result.stdout, configure_result.stderr);
            configure_success = Some(configure_result.success);
            steps.push(configure_result.evidence);
            if configure_success == Some(true) {
                let build_args = vec![
                    String::from("--build"),
                    build_dir_abs.display().to_string(),
                    String::from("--config"),
                    String::from("Release"),
                ];
                let build_result =
                    run_probe_step("cmake-build", Path::new("cmake"), &build_args, None, &envs);
                build_success = Some(build_result.success);
                steps.push(build_result.evidence);
                if build_success == Some(false) {
                    blockers.push(readiness_blocker(
                        "legacy_scheduler_cmake_build_failed",
                        "legacy scheduler CMake build failed",
                        "resolve the legacy scheduler CMake build failure before recording the baseline",
                    ));
                } else {
                    ctest_summary = None;
                }
            } else {
                blockers.extend(legacy_scheduler_configure_blockers(&configure_output));
            }
        }
    }

    if blockers.is_empty() && mode.runs_ctest() {
        let repo_root = env::current_dir().context("locating repository root")?;
        let build_dir_abs = repo_root.join(build_dir);
        if !build_dir.exists() {
            blockers.push(readiness_blocker(
                "legacy_scheduler_build_dir_missing",
                format!(
                    "legacy scheduler build directory is missing at {}",
                    build_dir.display()
                ),
                "run `cargo run -p xtask -- legacy-scheduler build` before running the legacy scheduler CTest baseline",
            ));
        } else {
            let envs = Vec::<(String, String)>::new();
            let test_args = vec![
                String::from("--test-dir"),
                build_dir_abs.display().to_string(),
                String::from("--build-config"),
                String::from("Release"),
                String::from("--output-on-failure"),
            ];
            let test_result = run_probe_step("ctest", Path::new("ctest"), &test_args, None, &envs);
            ctest_success = Some(test_result.success);
            ctest_summary = parse_ctest_summary_from_text(&test_result.stdout);
            steps.push(test_result.evidence);
            if ctest_success == Some(false) {
                blockers.push(readiness_blocker(
                    "legacy_scheduler_ctest_failed",
                    "legacy scheduler CTest failed",
                    "fix the failing legacy scheduler Python/C API tests before recording the baseline",
                ));
            }
        }
    }

    let build_status = legacy_scheduler_build_status(
        mode,
        source_dir.exists(),
        toolchain_file.exists(),
        vcpkg_exe.exists(),
        configure_success,
        build_success,
        ctest_success,
        ctest_summary,
    );
    if mode == LegacySchedulerMode::Build && build_success == Some(true) {
        blockers.push(readiness_blocker(
            "legacy_scheduler_ctest_not_run",
            "legacy scheduler build succeeded, but CTest was not run in build mode",
            "run `cargo run -p xtask -- legacy-scheduler run` or `cargo run -p xtask -- legacy-scheduler build-run` to record the baseline tests",
        ));
    }
    if mode.runs_ctest() && ctest_success == Some(true) {
        match ctest_summary {
            Some(summary) if summary.total > 0 && summary.failed == 0 => {}
            Some(_) => blockers.push(readiness_blocker(
                "legacy_scheduler_ctest_no_passing_baseline",
                "legacy scheduler CTest completed without a non-empty passing test baseline",
                "ensure the legacy scheduler CTest run discovers and passes the Python and C API baseline tests",
            )),
            None => blockers.push(readiness_blocker(
                "legacy_scheduler_ctest_summary_missing",
                "legacy scheduler CTest output did not contain a parseable test summary",
                "rerun CTest with normal output so the evidence gate can record passed and failed test counts",
            )),
        }
    }

    let requested_action_passed = match mode {
        LegacySchedulerMode::Build => {
            configure_success == Some(true) && build_success == Some(true)
        }
        LegacySchedulerMode::Run => ctest_success == Some(true),
        LegacySchedulerMode::BuildRun => {
            configure_success == Some(true)
                && build_success == Some(true)
                && ctest_success == Some(true)
        }
    };
    let status = if requested_action_passed {
        "pass"
    } else {
        "fail"
    };
    let report_ready = status == "pass"
        && mode.runs_ctest()
        && ctest_summary.is_some_and(|summary| summary.total > 0 && summary.failed == 0)
        && blockers.is_empty();
    let step_text = serde_json::to_string(&steps).unwrap_or_default();
    let missing_vcpkg = !vcpkg_exe.exists();
    let unsupported_linux_carbon_core =
        !cfg!(any(windows, target_os = "macos")) && step_text.contains("carbon-core:x64-linux");
    let local_probe_diagnosis = if report_ready {
        Value::Null
    } else if unsupported_linux_carbon_core {
        json!("vcpkg bootstrap succeeds, but the carbon-core vcpkg port declares support only for Windows x64 and macOS; the forced x64-linux configure fails before the legacy scheduler baseline can be built. The immediate CMake error is a Linux case-sensitive source mismatch, CCPDefines.cpp vs CcpDefines.cpp, and carbon-core also contains broader non-Windows/non-Apple platform guards. Import a non-empty passing supported-platform baseline with `cargo run -p xtask -- legacy-scheduler import <artifact.json|ctest.log> --host-os <windows|macos> --host-arch <x86_64|aarch64>`, or add an official Linux-supported carbon-core/vcpkg triplet.")
    } else if missing_vcpkg {
        json!("vcpkg executable is missing; set CARBON_LEGACY_SCHEDULER_BOOTSTRAP=1 or provide a prebuilt legacy scheduler environment")
    } else {
        blockers
            .first()
            .and_then(|blocker| blocker.get("message"))
            .cloned()
            .unwrap_or_else(|| json!("legacy scheduler configure/build/test probe failed before a report-ready legacy baseline was produced"))
    };
    let remaining_before_report_ready = blocker_remediations(&blockers);
    let failures = blocker_codes(&blockers);
    let evidence = json!({
        "schema": "carbon.evidence.legacy_scheduler.v1",
        "gate": "legacy-scheduler",
        "component": "scheduler",
        "implementation": "legacy_cpp_python_extension",
        "status": status,
        "report_ready": report_ready,
        "coverage": if mode.runs_ctest() { "legacy_scheduler_cmake_ctest" } else { "legacy_scheduler_build_probe" },
        "command": command,
        "mode": mode.as_str(),
        "duration_ms": started.elapsed().as_millis() as u64,
        "host": {
            "os": env::consts::OS,
            "arch": env::consts::ARCH,
            "cmake": command_stdout("cmake", &["--version"])
                .map(|text| text.lines().next().unwrap_or("unknown").to_string())
                .unwrap_or_else(|_| String::from("unknown")),
            "ctest": command_stdout("ctest", &["--version"])
                .map(|text| text.lines().next().unwrap_or("unknown").to_string())
                .unwrap_or_else(|_| String::from("unknown")),
            "python": command_stdout("python3", &["--version"]).unwrap_or_else(|_| String::from("unknown"))
        },
        "source_directory": source_dir.display().to_string(),
        "build_directory": build_dir.display().to_string(),
        "vcpkg_executable": vcpkg_exe.display().to_string(),
        "toolchain_file": toolchain_file.display().to_string(),
        "supported_triplet": supported_triplet.map(|triplet| triplet.target_triplet),
        "linux_support_note": "The checked-in scheduler CMake presets cover Windows and macOS; this probe attempts a local Linux configure only when vcpkg is available.",
        "legacy_build_status": build_status,
        "local_probe_diagnosis": local_probe_diagnosis,
        "steps": steps,
        "blockers": blockers,
        "failures": failures,
        "remaining_before_report_ready": remaining_before_report_ready
    });
    let evidence_path = evidence_path("legacy-scheduler.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "legacy-scheduler: {status} (legacy CMake/vcpkg scheduler baseline probe; report_ready={report_ready}); evidence {}",
        evidence_path.display()
    );

    if requested_action_passed {
        Ok(())
    } else {
        bail!(
            "legacy scheduler gate failed; see {}",
            evidence_path.display()
        )
    }
}

fn legacy_scheduler_native_linux() -> Result<()> {
    let started = Instant::now();
    let repo_root = env::current_dir().context("locating repository root")?;
    let core_source_dir = repo_root.join("carbonengine/core");
    let scheduler_source_dir = repo_root.join("carbonengine/scheduler");
    let core_build_dir = repo_root.join("target/carbon/legacy-core-linux-build");
    let core_prefix = repo_root.join("target/carbon/legacy-core-linux-prefix");
    let scheduler_build_dir = repo_root.join("target/carbon/legacy-scheduler-linux-build");
    let greenlet_config_root = repo_root.join("target/carbon/greenlet-linux-cmake");
    let greenlet_config_dir = greenlet_config_root.join("greenlet");
    let tests_dir = scheduler_source_dir.join("tests/python/scheduler/tests");
    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));

    let mut steps = Vec::new();
    let mut blockers = Vec::new();
    let mut configure_success = None;
    let mut build_success = None;
    let mut python_unittest_success = None;
    let mut ctest_success = None;
    let mut ctest_summary = None;
    let mut python_unittest_summary = Value::Null;
    let mut greenlet_probe = Value::Null;

    if env::consts::OS != "linux" {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_host_mismatch",
            "legacy-scheduler native-linux only runs on Linux hosts",
            "use the vcpkg legacy-scheduler build/run flow on supported Windows/macOS hosts",
        ));
    }
    if !core_source_dir.exists() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_core_source_missing",
            "carbonengine/core source directory is missing",
            "restore carbonengine/core before running the native Linux scheduler baseline",
        ));
    }
    if !scheduler_source_dir.exists() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_source_missing",
            "carbonengine/scheduler source directory is missing",
            "restore carbonengine/scheduler before running the native Linux scheduler baseline",
        ));
    }

    let greenlet_info = if blockers.is_empty() {
        match legacy_scheduler_greenlet_info(&python) {
            Ok(info) => {
                greenlet_probe = info.to_json();
                Some(info)
            }
            Err(error) => {
                blockers.push(readiness_blocker(
                    "legacy_scheduler_greenlet_probe_failed",
                    format!("failed to locate host Python greenlet package: {error}"),
                    "install greenlet for the selected PYTHON interpreter before running the native Linux scheduler baseline",
                ));
                None
            }
        }
    } else {
        None
    };
    let gtest_prefix = legacy_scheduler_gtest_prefix(&repo_root);
    let gtest_probe = if let Some(prefix) = &gtest_prefix {
        json!({
            "status": "found",
            "prefix": prefix.display().to_string(),
            "config": prefix.join("share/gtest/GTestConfig.cmake").display().to_string()
        })
    } else {
        json!({
            "status": "missing",
            "searched": "CARBON_GTEST_CMAKE_PREFIX_PATH, resources vcpkg build prefixes, scheduler vcpkg installed prefix, /usr, /usr/local"
        })
    };

    if blockers.is_empty() {
        fs::remove_dir_all(&core_build_dir).ok();
        fs::remove_dir_all(&core_prefix).ok();
        fs::remove_dir_all(&scheduler_build_dir).ok();
        fs::create_dir_all(&greenlet_config_dir)
            .with_context(|| format!("creating {}", greenlet_config_dir.display()))?;
        let greenlet_info = greenlet_info.as_ref().expect("greenlet info is available");
        fs::write(
            greenlet_config_dir.join("greenletConfig.cmake"),
            legacy_scheduler_greenlet_config(greenlet_info),
        )
        .with_context(|| {
            format!(
                "writing {}",
                greenlet_config_dir.join("greenletConfig.cmake").display()
            )
        })?;

        let mut core_configure_args = cmake_configure_args(&core_source_dir, &core_build_dir);
        core_configure_args.extend([
            String::from("-DCMAKE_BUILD_TYPE=Release"),
            format!("-DCMAKE_INSTALL_PREFIX={}", core_prefix.display()),
            String::from("-DBUILD_TESTING=OFF"),
            String::from("-DBUILD_DOCUMENTATION=OFF"),
            String::from("-DWITH_TELEMETRY=OFF"),
            String::from("-DWITH_MEMORY_TRACKING=OFF"),
        ]);
        let core_configure = run_probe_step(
            "native-linux-core-cmake-configure",
            Path::new("cmake"),
            &core_configure_args,
            None,
            &[],
        );
        steps.push(core_configure.evidence.clone());

        let core_build = if core_configure.success {
            let args = vec![
                String::from("--build"),
                core_build_dir.display().to_string(),
                String::from("--target"),
                String::from("install"),
            ];
            run_probe_step(
                "native-linux-core-cmake-build-install",
                Path::new("cmake"),
                &args,
                None,
                &[],
            )
        } else {
            skipped_probe_step("native-linux-core-cmake-build-install")
        };
        steps.push(core_build.evidence.clone());

        let mut scheduler_configure_args =
            cmake_configure_args(&scheduler_source_dir, &scheduler_build_dir);
        let mut cmake_prefix_paths = vec![
            core_prefix.display().to_string(),
            greenlet_config_root.display().to_string(),
        ];
        if let Some(prefix) = &gtest_prefix {
            cmake_prefix_paths.push(prefix.display().to_string());
        }
        scheduler_configure_args.extend([
            String::from("-DCMAKE_BUILD_TYPE=Release"),
            format!(
                "-DBUILD_TESTING={}",
                if gtest_prefix.is_some() { "ON" } else { "OFF" }
            ),
            String::from("-DBUILD_DOCUMENTATION=OFF"),
            format!("-DCMAKE_PREFIX_PATH={}", cmake_prefix_paths.join(";")),
            format!(
                "-DCMAKE_INSTALL_RPATH={};{}",
                core_prefix.join("lib").display(),
                greenlet_info.python_libdir.display()
            ),
        ]);
        let scheduler_configure = if core_build.success {
            run_probe_step(
                "native-linux-scheduler-cmake-configure",
                Path::new("cmake"),
                &scheduler_configure_args,
                None,
                &[],
            )
        } else {
            skipped_probe_step("native-linux-scheduler-cmake-configure")
        };
        steps.push(scheduler_configure.evidence.clone());

        let scheduler_build = if scheduler_configure.success {
            let args = vec![
                String::from("--build"),
                scheduler_build_dir.display().to_string(),
            ];
            run_probe_step(
                "native-linux-scheduler-cmake-build",
                Path::new("cmake"),
                &args,
                None,
                &[],
            )
        } else {
            skipped_probe_step("native-linux-scheduler-cmake-build")
        };
        steps.push(scheduler_build.evidence.clone());

        configure_success = Some(core_configure.success && scheduler_configure.success);
        build_success = Some(core_build.success && scheduler_build.success);

        let python_test = if scheduler_build.success {
            let python_path = env_join_paths([
                scheduler_build_dir.as_path(),
                scheduler_source_dir.join("python").as_path(),
                greenlet_info.python_module_dir.as_path(),
            ])?;
            let ld_library_path = env_join_paths([
                core_prefix.join("lib").as_path(),
                greenlet_info.python_libdir.as_path(),
            ])?;
            let envs = vec![
                (String::from("LD_LIBRARY_PATH"), ld_library_path),
                (String::from("PYTHONPATH"), python_path),
                (String::from("BUILDFLAVOR"), String::from("release")),
            ];
            let args = vec![
                String::from("-m"),
                String::from("unittest"),
                String::from("discover"),
                String::from("-v"),
            ];
            run_probe_step(
                "native-linux-python-unittest-discover",
                Path::new(&python),
                &args,
                Some(&tests_dir),
                &envs,
            )
        } else {
            skipped_probe_step("native-linux-python-unittest-discover")
        };
        python_unittest_summary =
            parse_python_unittest_summary(&python_test.stdout, &python_test.stderr);
        python_unittest_success = Some(
            python_test.success
                && python_unittest_summary
                    .get("ok")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && python_unittest_summary
                    .get("ran")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    > 0,
        );
        steps.push(python_test.evidence);

        let capi_ctest = if scheduler_build.success && gtest_prefix.is_some() {
            let ld_library_path = env_join_paths([
                core_prefix.join("lib").as_path(),
                greenlet_info.python_libdir.as_path(),
            ])?;
            let envs = vec![(String::from("LD_LIBRARY_PATH"), ld_library_path)];
            let args = vec![
                String::from("--test-dir"),
                scheduler_build_dir.display().to_string(),
                String::from("--output-on-failure"),
                String::from("-R"),
                String::from("Capi"),
            ];
            run_probe_step(
                "native-linux-scheduler-capi-ctest",
                Path::new("ctest"),
                &args,
                None,
                &envs,
            )
        } else {
            skipped_probe_step("native-linux-scheduler-capi-ctest")
        };
        ctest_success = if gtest_prefix.is_some() {
            Some(capi_ctest.success)
        } else {
            None
        };
        ctest_summary = parse_ctest_summary_from_text(&format!(
            "{}\n{}",
            capi_ctest.stdout, capi_ctest.stderr
        ));
        steps.push(capi_ctest.evidence);
    }

    if configure_success == Some(false) {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_configure_failed",
            "native Linux legacy scheduler configure failed",
            "fix the direct CMake configure failure for carbonengine/core or carbonengine/scheduler",
        ));
    }
    if build_success == Some(false) {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_build_failed",
            "native Linux legacy scheduler build failed",
            "fix the direct CMake build failure for carbonengine/core or carbonengine/scheduler",
        ));
    }
    if python_unittest_success == Some(false) {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_python_tests_failed",
            "native Linux legacy scheduler Python unittest suite failed",
            "fix the failing scheduler Python parity tests before using this host baseline",
        ));
    }
    if gtest_prefix.is_none() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_gtest_missing",
            "native Linux C API CTest was not configured because no GTest CMake package was found",
            "set CARBON_GTEST_CMAKE_PREFIX_PATH or provide a vcpkg-installed GTest package before running the native Linux scheduler gate",
        ));
    }
    if ctest_success == Some(false) {
        blockers.push(readiness_blocker(
            "legacy_scheduler_native_linux_capi_ctest_failed",
            "native Linux legacy scheduler C API CTest failed",
            "fix the failing SchedulerCapiTest cases before using this host baseline",
        ));
    }
    if ctest_success == Some(true) {
        match ctest_summary {
            Some(summary) if summary.total > 0 && summary.failed == 0 => {}
            Some(_) => blockers.push(readiness_blocker(
                "legacy_scheduler_native_linux_capi_ctest_no_passing_baseline",
                "native Linux C API CTest did not report a non-empty passing baseline",
                "ensure ctest discovers and passes the SchedulerCapiTest C API suite",
            )),
            None => blockers.push(readiness_blocker(
                "legacy_scheduler_native_linux_capi_ctest_summary_missing",
                "native Linux C API CTest output did not include a parseable CTest summary",
                "record a CTest run with the standard summary line before promoting report_ready",
            )),
        }
    }

    let requested_action_passed = configure_success == Some(true)
        && build_success == Some(true)
        && python_unittest_success == Some(true)
        && ctest_success == Some(true)
        && ctest_summary.is_some_and(|summary| summary.total > 0 && summary.failed == 0);
    let status = if requested_action_passed {
        "pass"
    } else {
        "fail"
    };
    let report_ready = requested_action_passed && blockers.is_empty();
    let failures = blocker_codes(&blockers);
    let remaining_before_report_ready = blocker_remediations(&blockers);
    let python_ran = python_unittest_summary
        .get("ran")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let python_skipped = python_unittest_summary
        .get("skipped")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let evidence = json!({
        "schema": "carbon.evidence.legacy_scheduler.v1",
        "gate": "legacy-scheduler",
        "component": "scheduler",
        "implementation": "legacy_cpp_python_extension",
        "status": status,
        "report_ready": report_ready,
        "coverage": if ctest_success == Some(true) { "legacy_scheduler_native_linux_python_unittest_capi_ctest" } else { "legacy_scheduler_native_linux_python_unittest" },
        "command": "build carbonengine/core and carbonengine/scheduler directly on Linux, run the unchanged legacy scheduler Python unittest suite, then run SchedulerCapiTest C API CTest when GTest is available",
        "mode": "native-linux",
        "duration_ms": started.elapsed().as_millis() as u64,
        "host": {
            "os": env::consts::OS,
            "arch": env::consts::ARCH,
            "cmake": command_stdout("cmake", &["--version"])
                .map(|text| text.lines().next().unwrap_or("unknown").to_string())
                .unwrap_or_else(|_| String::from("unknown")),
            "python": command_stdout(&python, &["--version"]).unwrap_or_else(|_| String::from("unknown")),
        },
        "source_directory": scheduler_source_dir.display().to_string(),
        "core_source_directory": core_source_dir.display().to_string(),
        "build_directory": scheduler_build_dir.display().to_string(),
        "core_build_directory": core_build_dir.display().to_string(),
        "core_install_prefix": core_prefix.display().to_string(),
        "greenlet_probe": greenlet_probe,
        "gtest_probe": gtest_probe,
        "vcpkg_executable": Value::Null,
        "toolchain_file": Value::Null,
        "supported_triplet": "native-linux-host",
        "linux_support_note": "This path bypasses the checked-in vcpkg triplet and builds the local carbonengine/core and carbonengine/scheduler sources directly against the host Python and greenlet package.",
        "legacy_build_status": {
            "mode": "native-linux",
            "source_available": core_source_dir.exists() && scheduler_source_dir.exists(),
            "toolchain_available": command_stdout("cmake", &["--version"]).is_ok(),
            "vcpkg_available": Value::Null,
            "configure": probe_status(configure_success),
            "build": probe_status(build_success),
            "ctest": probe_status(ctest_success),
            "tests_passed": ctest_summary.map(|summary| summary.passed),
            "tests_failed": ctest_summary.map(|summary| summary.failed),
            "tests_total": ctest_summary.map(|summary| summary.total),
            "python_unittest": probe_status(python_unittest_success),
            "python_tests_passed": python_ran.saturating_sub(python_skipped),
            "python_tests_skipped": python_skipped,
            "python_tests_total": python_ran,
            "baseline_complete": requested_action_passed
        },
        "python_unittest_summary": python_unittest_summary,
        "local_probe_diagnosis": if report_ready { Value::Null } else { json!("native Linux source build and Python unittest baseline are available on this host, but the full scheduler baseline is not report-ready until SchedulerCapiTest C API CTest discovers a non-empty passing C API suite.") },
        "steps": steps,
        "blockers": blockers,
        "failures": failures,
        "remaining_before_report_ready": remaining_before_report_ready
    });
    let evidence_path = evidence_path("legacy-scheduler.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "legacy-scheduler: {status} (native Linux source build plus Python/C API baseline; report_ready={report_ready}); evidence {}",
        evidence_path.display()
    );

    if requested_action_passed {
        Ok(())
    } else {
        bail!(
            "native Linux legacy scheduler gate failed; see {}",
            evidence_path.display()
        )
    }
}

fn legacy_scheduler_import(args: &[String]) -> Result<()> {
    let import_args = parse_legacy_scheduler_import_args(args)?;
    let started = Instant::now();
    let artifact_text = fs::read_to_string(&import_args.artifact_path).with_context(|| {
        format!(
            "reading legacy scheduler import artifact {}",
            import_args.artifact_path.display()
        )
    })?;
    let evidence = legacy_scheduler_import_evidence(&import_args, &artifact_text, started)?;
    let report_ready = evidence
        .get("report_ready")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let status = evidence
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("fail")
        .to_string();
    let evidence_path = evidence_path("legacy-scheduler.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "legacy-scheduler: {status} (imported supported-platform legacy scheduler baseline; report_ready={report_ready}); evidence {}",
        evidence_path.display()
    );

    if status == "pass" {
        Ok(())
    } else {
        bail!(
            "legacy scheduler import failed validation; see {}",
            evidence_path.display()
        )
    }
}

fn legacy_scheduler_import_evidence(
    import_args: &LegacySchedulerImportArgs,
    artifact_text: &str,
    started: Instant,
) -> Result<Value> {
    let imported_json = serde_json::from_str::<Value>(artifact_text).ok();
    let summary = imported_json
        .as_ref()
        .and_then(legacy_scheduler_imported_ctest_summary)
        .or_else(|| parse_ctest_summary_from_text(artifact_text));
    let json_host_os = imported_json
        .as_ref()
        .and_then(|value| value.pointer("/host/os").and_then(Value::as_str))
        .map(str::to_string);
    let json_host_arch = imported_json
        .as_ref()
        .and_then(|value| value.pointer("/host/arch").and_then(Value::as_str))
        .map(str::to_string);
    let host_os = import_args.host_os.clone().or(json_host_os);
    let host_arch = import_args.host_arch.clone().or(json_host_arch);
    let host_supported =
        legacy_scheduler_import_host_supported(host_os.as_deref(), host_arch.as_deref());

    let mut blockers = Vec::new();
    match summary {
        Some(CTestSummary { total: 0, .. }) => blockers.push(readiness_blocker(
            "legacy_scheduler_import_zero_tests",
            "imported legacy scheduler CTest artifact reports zero tests",
            "import a supported Windows/macOS CTest run that discovers the legacy scheduler Python and C API tests",
        )),
        Some(CTestSummary { failed, .. }) if failed != 0 => blockers.push(readiness_blocker(
            "legacy_scheduler_import_failed_tests",
            "imported legacy scheduler CTest artifact includes failed tests",
            "fix the failing legacy scheduler baseline tests before importing the artifact",
        )),
        Some(_) => {}
        None => blockers.push(readiness_blocker(
            "legacy_scheduler_import_ctest_summary_missing",
            "imported legacy scheduler artifact does not contain parsed CTest test counts",
            "import either a legacy-scheduler evidence JSON file or a raw CTest log containing the standard summary line",
        )),
    }
    if !host_supported {
        blockers.push(readiness_blocker(
            "legacy_scheduler_import_supported_host_missing",
            "imported legacy scheduler artifact is not stamped with a supported Windows x64 or macOS host",
            "rerun import with --host-os/--host-arch from a supported Windows x64 or macOS legacy baseline run, or include those fields in the JSON artifact",
        ));
    }

    let ctest_passed = summary.is_some_and(|summary| summary.total > 0 && summary.failed == 0);
    let baseline_complete = ctest_passed && host_supported;
    let status = if ctest_passed { "pass" } else { "fail" };
    let report_ready = status == "pass" && blockers.is_empty();
    let imported_kind = if imported_json.is_some() {
        "json"
    } else {
        "ctest_log"
    };
    let source_status = imported_json
        .as_ref()
        .and_then(|value| value.get("status").and_then(Value::as_str));
    let source_report_ready = imported_json
        .as_ref()
        .and_then(|value| value.get("report_ready").and_then(Value::as_bool));
    let source_coverage = imported_json
        .as_ref()
        .and_then(|value| value.get("coverage").and_then(Value::as_str));
    let source_gate = imported_json
        .as_ref()
        .and_then(|value| value.get("gate").and_then(Value::as_str));
    let source_command = import_args
        .source_command
        .clone()
        .or_else(|| {
            imported_json
                .as_ref()
                .and_then(|value| value.get("command").and_then(Value::as_str))
                .map(str::to_string)
        })
        .unwrap_or_else(|| String::from("external supported-platform CTest artifact"));
    let remaining_before_report_ready = blocker_remediations(&blockers);
    let failures = blocker_codes(&blockers);

    Ok(json!({
        "schema": "carbon.evidence.legacy_scheduler.v1",
        "gate": "legacy-scheduler",
        "component": "scheduler",
        "implementation": "legacy_cpp_python_extension",
        "status": status,
        "report_ready": report_ready,
        "coverage": "legacy_scheduler_imported_supported_platform_ctest",
        "command": source_command,
        "mode": "import",
        "duration_ms": started.elapsed().as_millis() as u64,
        "host": {
            "os": host_os,
            "arch": host_arch,
            "supported_legacy_platform": host_supported,
        },
        "source_directory": "external-supported-platform-baseline",
        "build_directory": Value::Null,
        "vcpkg_executable": Value::Null,
        "toolchain_file": Value::Null,
        "supported_triplet": legacy_scheduler_import_triplet(host_os.as_deref(), host_arch.as_deref()),
        "linux_support_note": "This evidence was imported from a supported Windows/macOS legacy scheduler run because the checked-in legacy carbon-core/vcpkg configuration is not Linux-supported.",
        "legacy_build_status": {
            "mode": "import",
            "source_available": Value::Null,
            "toolchain_available": Value::Null,
            "vcpkg_available": Value::Null,
            "configure": if baseline_complete { "imported" } else { "not_verified" },
            "build": if baseline_complete { "imported" } else { "not_verified" },
            "ctest": if ctest_passed { "pass" } else { "fail" },
            "tests_passed": summary.map(|summary| summary.passed),
            "tests_failed": summary.map(|summary| summary.failed),
            "tests_total": summary.map(|summary| summary.total),
            "baseline_complete": baseline_complete,
        },
        "imported_artifact": {
            "path": import_args.artifact_path.display().to_string(),
            "kind": imported_kind,
            "source_gate": source_gate,
            "source_status": source_status,
            "source_report_ready": source_report_ready,
            "source_coverage": source_coverage,
            "sha256_unavailable": "std-only xtask import records artifact path and parsed counts; archive checksum can be added when a hashing dependency is introduced"
        },
        "local_probe_diagnosis": if report_ready { Value::Null } else { json!("legacy scheduler baseline import is not report-ready; import a non-empty passing CTest artifact from Windows x64 or macOS") },
        "steps": [{
            "label": "import-legacy-scheduler-baseline",
            "program": "legacy-scheduler import",
            "args": [import_args.artifact_path.display().to_string()],
            "success": ctest_passed,
            "duration_ms": started.elapsed().as_millis() as u64,
            "ctest_summary": summary.map(|summary| json!({
                "passed": summary.passed,
                "failed": summary.failed,
                "total": summary.total,
            })),
            "artifact_tail": tail_lines(artifact_text, 20)
        }],
        "blockers": blockers,
        "failures": failures,
        "remaining_before_report_ready": remaining_before_report_ready
    }))
}

fn legacy_scheduler_imported_ctest_summary(value: &Value) -> Option<CTestSummary> {
    let build_status = value.get("legacy_build_status")?;
    let passed = build_status.get("tests_passed").and_then(Value::as_u64)?;
    let failed = build_status.get("tests_failed").and_then(Value::as_u64)?;
    let total = build_status.get("tests_total").and_then(Value::as_u64)?;
    Some(CTestSummary {
        passed,
        failed,
        total,
    })
}

fn legacy_scheduler_import_host_supported(host_os: Option<&str>, host_arch: Option<&str>) -> bool {
    matches!(
        (
            host_os
                .map(normalize_legacy_scheduler_host_value)
                .as_deref(),
            host_arch
                .map(normalize_legacy_scheduler_host_value)
                .as_deref(),
        ),
        (Some("windows"), Some("x86_64"))
            | (Some("macos"), Some("x86_64"))
            | (Some("macos"), Some("aarch64"))
    )
}

fn legacy_scheduler_import_triplet(
    host_os: Option<&str>,
    host_arch: Option<&str>,
) -> Option<&'static str> {
    match (
        host_os
            .map(normalize_legacy_scheduler_host_value)
            .as_deref(),
        host_arch
            .map(normalize_legacy_scheduler_host_value)
            .as_deref(),
    ) {
        (Some("windows"), Some("x86_64")) => Some("x64-windows-release"),
        (Some("macos"), Some("x86_64")) => Some("x64-osx-release"),
        (Some("macos"), Some("aarch64")) => Some("arm64-osx-release"),
        _ => None,
    }
}

fn normalize_legacy_scheduler_host_value(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "win32" | "win64" | "windows-x64" | "x64-windows" => String::from("windows"),
        "darwin" | "osx" | "mac" | "macosx" => String::from("macos"),
        "amd64" | "x64" => String::from("x86_64"),
        "arm64" => String::from("aarch64"),
        other => other.to_string(),
    }
}

#[derive(Clone, Debug)]
struct LegacySchedulerGreenletInfo {
    python_executable: PathBuf,
    python_libdir: PathBuf,
    include_dir: PathBuf,
    python_module_dir: PathBuf,
    extension_path: PathBuf,
    version: String,
}

impl LegacySchedulerGreenletInfo {
    fn to_json(&self) -> Value {
        json!({
            "python_executable": self.python_executable.display().to_string(),
            "python_libdir": self.python_libdir.display().to_string(),
            "include_dir": self.include_dir.display().to_string(),
            "python_module_dir": self.python_module_dir.display().to_string(),
            "extension_path": self.extension_path.display().to_string(),
            "version": self.version,
        })
    }
}

fn legacy_scheduler_greenlet_info(python: &str) -> Result<LegacySchedulerGreenletInfo> {
    let python_executable = PathBuf::from(command_stdout(
        python,
        &["-c", "import sys; print(sys.executable)"],
    )?);
    let python_libdir = PathBuf::from(command_stdout(
        python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ],
    )?);
    let include_dir = PathBuf::from(command_stdout(
        python,
        &[
            "-c",
            "import greenlet, pathlib; print(pathlib.Path(greenlet.__file__).resolve().parent)",
        ],
    )?);
    let python_module_dir = PathBuf::from(command_stdout(
        python,
        &[
            "-c",
            "import greenlet, pathlib; print(pathlib.Path(greenlet.__file__).resolve().parent.parent)",
        ],
    )?);
    let extension_path = PathBuf::from(command_stdout(
        python,
        &[
            "-c",
            "import greenlet._greenlet as module; print(module.__file__)",
        ],
    )?);
    let version = command_stdout(
        python,
        &[
            "-c",
            "import greenlet; print(getattr(greenlet, '__version__', 'unknown'))",
        ],
    )?;

    Ok(LegacySchedulerGreenletInfo {
        python_executable,
        python_libdir,
        include_dir,
        python_module_dir,
        extension_path,
        version,
    })
}

fn legacy_scheduler_greenlet_config(info: &LegacySchedulerGreenletInfo) -> String {
    format!(
        r#"add_library(Greenlet INTERFACE IMPORTED)

set(Greenlet_INCLUDE_DIR "{}")
set(Greenlet_VERSION "{}")

set_target_properties(Greenlet PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${{Greenlet_INCLUDE_DIR}}"
    IMPORTED_LOCATION "{}"
    PYTHON_MODULE_DIR "{}"
)
"#,
        info.include_dir.display(),
        info.version,
        info.extension_path.display(),
        info.python_module_dir.display(),
)
}

fn legacy_scheduler_gtest_prefix(repo_root: &Path) -> Option<PathBuf> {
    if let Some(path) = env::var_os("CARBON_GTEST_CMAKE_PREFIX_PATH") {
        for candidate in env::split_paths(&path) {
            if candidate.join("share/gtest/GTestConfig.cmake").exists() {
                return Some(candidate);
            }
        }
    }

    [
        repo_root
            .join("carbonengine/resources/.cmake-build-linux-vcpkg-release/vcpkg_installed/x64-linux"),
        repo_root
            .join("carbonengine/resources/.cmake-build-linux-vcpkg-release-devfeatures/vcpkg_installed/x64-linux"),
        repo_root
            .join("carbonengine/resources/.cmake-build-linux-vcpkg-probe/vcpkg_installed/x64-linux"),
        repo_root
            .join("carbonengine/resources/vendor/github.com/microsoft/vcpkg/packages/gtest_x64-linux"),
        repo_root.join("carbonengine/scheduler/vendor/github.com/microsoft/vcpkg/installed/x64-linux"),
        PathBuf::from("/usr"),
        PathBuf::from("/usr/local"),
    ]
    .into_iter()
    .find(|candidate| candidate.join("share/gtest/GTestConfig.cmake").exists())
}

fn cmake_configure_args(source_dir: &Path, build_dir: &Path) -> Vec<String> {
    let mut args = Vec::new();
    if command_stdout("ninja", &["--version"]).is_ok() {
        args.push(String::from("-G"));
        args.push(String::from("Ninja"));
    }
    args.extend([
        String::from("-S"),
        source_dir.display().to_string(),
        String::from("-B"),
        build_dir.display().to_string(),
    ]);
    args
}

fn skipped_probe_step(label: &str) -> ProbeStepResult {
    ProbeStepResult {
        evidence: json!({
            "label": label,
            "program": Value::Null,
            "args": [],
            "current_dir": Value::Null,
            "status_code": Value::Null,
            "success": false,
            "skipped": true,
            "duration_ms": 0,
            "stdout_tail": [],
            "stderr_tail": []
        }),
        success: false,
        stdout: String::new(),
        stderr: String::new(),
    }
}

fn env_join_paths<'a, I>(paths: I) -> Result<String>
where
    I: IntoIterator<Item = &'a Path>,
{
    let paths = paths
        .into_iter()
        .filter(|path| !path.as_os_str().is_empty())
        .collect::<Vec<_>>();
    Ok(env::join_paths(paths)
        .context("joining environment search paths")?
        .to_string_lossy()
        .to_string())
}

fn run_probe_step(
    label: &str,
    program: &Path,
    args: &[String],
    current_dir: Option<&Path>,
    envs: &[(String, String)],
) -> ProbeStepResult {
    let started = Instant::now();
    let mut command = Command::new(program);
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    for (key, value) in envs {
        command.env(key, value);
    }

    match command.output() {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let success = output.status.success();
            ProbeStepResult {
                evidence: json!({
                    "label": label,
                    "program": program.display().to_string(),
                    "args": args,
                    "current_dir": current_dir.map(|path| path.display().to_string()),
                    "status_code": output.status.code(),
                    "success": success,
                    "duration_ms": started.elapsed().as_millis() as u64,
                    "stdout_tail": tail_lines(&stdout, 20),
                    "stderr_tail": tail_lines(&stderr, 20)
                }),
                success,
                stdout,
                stderr,
            }
        }
        Err(error) => ProbeStepResult {
            evidence: json!({
                "label": label,
                "program": program.display().to_string(),
                "args": args,
                "current_dir": current_dir.map(|path| path.display().to_string()),
                "success": false,
                "duration_ms": started.elapsed().as_millis() as u64,
                "error": error.to_string()
            }),
            success: false,
            stdout: String::new(),
            stderr: error.to_string(),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CTestSummary {
    passed: u64,
    failed: u64,
    total: u64,
}

fn readiness_blocker(
    code: &str,
    message: impl Into<String>,
    remediation: impl Into<String>,
) -> Value {
    json!({
        "code": code,
        "severity": "blocker",
        "message": message.into(),
        "remediation": remediation.into()
    })
}

fn blocker_codes(blockers: &[Value]) -> Vec<String> {
    blockers
        .iter()
        .filter_map(|blocker| blocker.get("code").and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

fn blocker_remediations(blockers: &[Value]) -> Vec<String> {
    let mut remediations = Vec::new();
    for blocker in blockers {
        if let Some(remediation) = blocker.get("remediation").and_then(Value::as_str) {
            if !remediations.iter().any(|existing| existing == remediation) {
                remediations.push(remediation.to_string());
            }
        }
    }
    remediations
}

fn legacy_scheduler_configure_blockers(output: &str) -> Vec<Value> {
    let mut blockers = Vec::new();
    if !cfg!(any(windows, target_os = "macos")) && output.contains("carbon-core:x64-linux") {
        blockers.push(readiness_blocker(
            "legacy_scheduler_unsupported_linux_carbon_core",
            "carbon-core vcpkg configure failed for x64-linux while probing the legacy scheduler",
            "import a non-empty passing supported-platform baseline with `cargo run -p xtask -- legacy-scheduler import <artifact.json|ctest.log> --host-os <windows|macos> --host-arch <x86_64|aarch64>`, or add an official Linux-supported carbon-core/vcpkg triplet",
        ));
        return blockers;
    }
    if output.contains("CMAKE_MAKE_PROGRAM is not set") {
        blockers.push(readiness_blocker(
            "legacy_scheduler_cmake_generator_missing",
            "CMake could not find the requested build program for the legacy scheduler",
            "install the requested CMake generator or rerun after allowing CMake to select an available generator",
        ));
    }
    if output.contains("CMAKE_C_COMPILER not set") || output.contains("CMAKE_CXX_COMPILER not set")
    {
        blockers.push(readiness_blocker(
            "legacy_scheduler_compiler_missing",
            "CMake did not resolve a C/C++ compiler for the legacy scheduler configure",
            "install a host C/C++ toolchain compatible with the selected legacy scheduler vcpkg triplet",
        ));
    }
    if blockers.is_empty() {
        blockers.push(readiness_blocker(
            "legacy_scheduler_cmake_configure_failed",
            "legacy scheduler CMake configure failed",
            "resolve the legacy scheduler CMake/vcpkg configure failure before recording the baseline",
        ));
    }
    blockers
}

fn legacy_scheduler_build_status(
    mode: LegacySchedulerMode,
    source_available: bool,
    toolchain_available: bool,
    vcpkg_available: bool,
    configure_success: Option<bool>,
    build_success: Option<bool>,
    ctest_success: Option<bool>,
    ctest_summary: Option<CTestSummary>,
) -> Value {
    json!({
        "mode": mode.as_str(),
        "source_available": source_available,
        "toolchain_available": toolchain_available,
        "vcpkg_available": vcpkg_available,
        "configure": probe_status(configure_success),
        "build": probe_status(build_success),
        "ctest": probe_status(ctest_success),
        "tests_passed": ctest_summary.map(|summary| summary.passed),
        "tests_failed": ctest_summary.map(|summary| summary.failed),
        "tests_total": ctest_summary.map(|summary| summary.total),
        "baseline_complete": ctest_success == Some(true)
            && ctest_summary.is_some_and(|summary| summary.total > 0 && summary.failed == 0),
    })
}

fn probe_status(success: Option<bool>) -> &'static str {
    match success {
        Some(true) => "pass",
        Some(false) => "fail",
        None => "not_run",
    }
}

fn report_readiness() -> Result<()> {
    let scheduler_fixture_status = readiness_line("scheduler-fixtures.json");
    let legacy_scheduler_status = readiness_line("legacy-scheduler.json");
    let rust_scheduler_python_status = readiness_line("rust-scheduler-python.json");
    let io_workload_status = readiness_line("io-workloads.json");
    let legacy_resources_status = readiness_line("legacy-resources.json");
    let rust_resources_status = readiness_line("rust-resources.json");
    let bench_status = readiness_line("bench-tier-local.json");
    println!("Report readiness");
    println!("  scheduler fixtures: {scheduler_fixture_status}");
    println!("  legacy scheduler: {legacy_scheduler_status}");
    println!("  rust scheduler Python/C API: {rust_scheduler_python_status}");
    println!("  IO workloads: {io_workload_status}");
    println!("  legacy resources: {legacy_resources_status}");
    println!("  rust resources: {rust_resources_status}");
    println!("  Tier 1 benchmarks: {bench_status}");
    Ok(())
}

fn rust_scheduler_python() -> Result<()> {
    let command = "cargo test -p carbon-scheduler-python --lib -- --test-threads=1; cargo build -p carbon-scheduler-python; PYTHONPATH=target/carbon/python python3 -c <flavor import smoke>; PYTHONPATH=target/carbon/python:<legacy tests> python3 -m unittest <full unchanged legacy scheduler Python suite>; compile/run Scheduler.h C++ smoke; compile/run real legacy capiTest/Tasklet.cpp, Channel.cpp, and Scheduler.cpp source slices one test per child process; run optional CARBON_CAPI_GTEST_IN_PROCESS=1 source-slice probe";
    let started = Instant::now();
    let cargo = env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let test_output = Command::new(&cargo)
        .args([
            "test",
            "-p",
            "carbon-scheduler-python",
            "--lib",
            "--",
            "--test-threads=1",
        ])
        .output()
        .context("running Rust scheduler Python bridge smoke tests")?;

    let mut build_output = None;
    let mut import_output = None;
    let mut legacy_subset_output = None;
    let mut capi_binary_smoke = None;
    let mut legacy_capi_tasklet_source_slice = None;
    let mut legacy_capi_channel_source_slice = None;
    let mut legacy_capi_scheduler_source_slice = None;
    let mut legacy_capi_tasklet_in_process_probe = None;
    let mut legacy_capi_channel_in_process_probe = None;
    let mut legacy_capi_scheduler_in_process_probe = None;
    let mut wheel_install_smoke = None;
    let mut package_dir = None;

    if test_output.status.success() {
        let output = Command::new(&cargo)
            .args(["build", "-p", "carbon-scheduler-python"])
            .output()
            .context("building Rust scheduler Python extension")?;
        if output.status.success() {
            let prepared_package_dir = prepare_scheduler_python_package("debug")?;
            let output = run_scheduler_python_import_smoke(&prepared_package_dir)
                .context("running Rust scheduler Python import smoke")?;
            if output.status.success() {
                let output = run_scheduler_python_legacy_subset(&prepared_package_dir)
                    .context("running unchanged legacy scheduler Python suite")?;
                let legacy_subset_success = output.status.success();
                legacy_subset_output = Some(output);
                if legacy_subset_success {
                    capi_binary_smoke = Some(
                        run_scheduler_capi_binary_smoke(&prepared_package_dir)
                            .context("running Scheduler.h C API binary smoke")?,
                    );
                    if capi_binary_smoke
                        .as_ref()
                        .is_some_and(SchedulerCapiBinarySmoke::success)
                    {
                        legacy_capi_tasklet_source_slice = Some(
                            run_scheduler_legacy_capi_tasklet_source_slice(&prepared_package_dir)
                                .context("running legacy capiTest Tasklet.cpp source slice")?,
                        );
                        if legacy_capi_tasklet_source_slice
                            .as_ref()
                            .is_some_and(SchedulerCapiBinarySmoke::success)
                        {
                            legacy_capi_channel_source_slice = Some(
                                run_scheduler_legacy_capi_channel_source_slice(
                                    &prepared_package_dir,
                                )
                                .context("running legacy capiTest Channel.cpp source slice")?,
                            );
                            if legacy_capi_channel_source_slice
                                .as_ref()
                                .is_some_and(SchedulerCapiBinarySmoke::success)
                            {
                                legacy_capi_scheduler_source_slice = Some(
                                    run_scheduler_legacy_capi_scheduler_source_slice(
                                        &prepared_package_dir,
                                    )
                                    .context(
                                        "running legacy capiTest Scheduler.cpp source slice",
                                    )?,
                                );
                                if legacy_capi_scheduler_source_slice
                                    .as_ref()
                                    .is_some_and(SchedulerCapiBinarySmoke::success)
                                {
                                    legacy_capi_tasklet_in_process_probe = Some(
                                        run_scheduler_legacy_capi_tasklet_in_process_probe(
                                            &prepared_package_dir,
                                        )
                                        .context(
                                            "running legacy capiTest Tasklet.cpp in-process probe",
                                        )?,
                                    );
                                    legacy_capi_channel_in_process_probe = Some(
                                        run_scheduler_legacy_capi_channel_in_process_probe(
                                            &prepared_package_dir,
                                        )
                                        .context(
                                            "running legacy capiTest Channel.cpp in-process probe",
                                        )?,
                                    );
                                    legacy_capi_scheduler_in_process_probe = Some(
                                        run_scheduler_legacy_capi_scheduler_in_process_probe(
                                            &prepared_package_dir,
                                        )
                                        .context(
                                            "running legacy capiTest Scheduler.cpp in-process probe",
                                        )?,
                                    );
                                    wheel_install_smoke =
                                        Some(run_scheduler_python_wheel_install_smoke().context(
                                            "running scheduler Python wheel install smoke",
                                        )?);
                                }
                            }
                        }
                    }
                }
            }
            package_dir = Some(prepared_package_dir);
            import_output = Some(output);
        }
        build_output = Some(output);
    }

    let duration_ms = started.elapsed().as_millis() as u64;
    let test_stdout = String::from_utf8_lossy(&test_output.stdout);
    let test_stderr = String::from_utf8_lossy(&test_output.stderr);
    let build_stdout = build_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stdout));
    let build_stderr = build_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stderr));
    let import_stdout = import_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stdout));
    let import_stderr = import_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stderr));
    let legacy_subset_stdout = legacy_subset_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stdout));
    let legacy_subset_stderr = legacy_subset_output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stderr));
    let legacy_unittest_summary = legacy_subset_output.as_ref().map(|output| {
        parse_python_unittest_summary(
            &String::from_utf8_lossy(&output.stdout),
            &String::from_utf8_lossy(&output.stderr),
        )
    });
    let status = if test_output.status.success()
        && build_output
            .as_ref()
            .is_some_and(|output| output.status.success())
        && import_output
            .as_ref()
            .is_some_and(|output| output.status.success())
        && legacy_subset_output
            .as_ref()
            .is_some_and(|output| output.status.success())
        && capi_binary_smoke
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success)
        && legacy_capi_tasklet_source_slice
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success)
        && legacy_capi_channel_source_slice
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success)
        && legacy_capi_scheduler_source_slice
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success)
        && wheel_install_smoke
            .as_ref()
            .is_some_and(SchedulerPythonWheelSmoke::success)
    {
        "pass"
    } else {
        "fail"
    };
    let unchanged_legacy_subset_count = rust_scheduler_unchanged_legacy_subset_count();
    let queuechannel_unchanged_subset = RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET
        .iter()
        .copied()
        .filter(|test| test.starts_with("test_queuechannel."))
        .collect::<Vec<_>>();
    let queuechannel_unchanged_subset_count = queuechannel_unchanged_subset.len();
    let unchanged_legacy_subset_summary = format!(
        "{unchanged_legacy_subset_count}-test unchanged legacy unittest suite passes against the Rust extension for C API exposure, direct tasklet.run, scheduler.run args/kwargs, targeted run order, scheduler run order, nested run order, schedule-yield order, multi-level schedule order, callback registration get/set/multiple-thread coverage, tasklet kill/invalid construction/weakref/cyclic cleanup/TaskletExit/throw/kill exception paths, switch and switch-trap paths, channel blocking/deadlock/accounting/preference/refcount/inter-thread/cleanup behavior, block_trap send/receive errors, closed/open channel basics, iterator/interface smoke, invalid channel construction, block-trap balance no-mutation checks, and {queuechannel_unchanged_subset_count} QueueChannel legacy tests covering buffered value/exception, blocked-receiver wakeup, nested ordering, block-trap receive, and main receive drain wrapper behavior"
    );
    let capi_in_process_probes_pass = legacy_capi_tasklet_in_process_probe
        .as_ref()
        .is_some_and(SchedulerCapiBinarySmoke::success)
        && legacy_capi_channel_in_process_probe
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success)
        && legacy_capi_scheduler_in_process_probe
            .as_ref()
            .is_some_and(SchedulerCapiBinarySmoke::success);
    let capi_in_process_remaining = if capi_in_process_probes_pass {
        "full legacy SchedulerCapiTest/capiTest binary harness compatibility beyond the passing in-process Tasklet.cpp, Channel.cpp, and Scheduler.cpp source-slice probe"
    } else {
        "full in-process legacy SchedulerCapiTest/capiTest binary compatibility beyond the passing per-test child-process source slices and current expanded Scheduler.h C++ smoke; the optional in-process source-slice probe is not yet green"
    };
    let mut evidence = json!({
        "schema": "carbon.evidence.gate.v1",
        "gate": "rust-scheduler-python",
        "component": "scheduler",
        "implementation": "rust_pyo3",
        "architecture_role": "legacy_python_c_api_compatibility_bridge_not_final_scheduler_core",
        "core_ownership_status": {
            "status": "partial",
            "target_state": "carbon-scheduler-core owns tasklet/channel/scheduler state and decisions; carbon-scheduler-python holds opaque Rust handles plus Python callables/exceptions needed to preserve the legacy _scheduler and scheduler._C_API surface",
            "already_rust_owned": [
                "semantic fixture runner uses Rust-owned scheduler/channel/tasklet identifiers",
                "scheduler core fixture slice has no Python objects",
                "owner-thread CoreScheduler API exposes Rust-owned CoreTaskletId/CoreChannelId handles for unbuffered channel rendezvous state, balance, blocked sender/receiver queues, preference clamping, close/open/clear, and block-trap no-mutation checks",
                "CoreScheduler now owns live bridge run-queue handles and scheduled-state authority for FIFO schedule/pop/remove/clear/count behavior per owner thread while carbon-scheduler-python keeps only a CoreTaskletId-to-PyObject registry for callable and Greenlet execution",
                "live PyO3 tasklet/channel objects now carry opaque CoreTaskletId/CoreChannelId handles, mirror unbuffered channel balance, preference, block-trap, and blocked sender/receiver queue transitions through CoreScheduler, consume CoreChannelOperationResult sender/receiver IDs, preferred peer-immediate handoff, and balance for covered unbuffered send/receive transfer decisions, use CoreScheduler snapshots for the sender-preferred pre-receive scheduling probe, and use CoreScheduler queue-front results for channel.queue introspection while preserving the legacy Python payload path",
                "live PyO3 tasklet objects now mirror alive/scheduled/paused/times_switched_to through CoreScheduler tasklet snapshots for setup, run, finish, block, continuation, clear, and kill/exception paths covered by bridge tests; covered bind/remove/insert/switch pause paths use explicit CoreScheduler pause/resume transitions and direct tasklet.run paused eligibility consults the core paused snapshot",
                "FFI crate owns ABI status/version and panic-containment bootstrap"
            ],
            "still_bridge_owned": [
                "Python tasklet object still stores callable/args/kwargs, Greenlet continuation state, blocked-channel PyObject links, pending exceptions, kill/continuation flags, and compatibility metrics; CoreScheduler snapshots are not yet authoritative for every lifecycle transition",
                "Python channel object still stores live Python payload/exception transfer behavior, pending message close-state details, and PyObject registries for legacy identity adaptation even though covered unbuffered transfer selection, immediate peer handoff, queue-front selection, and scheduler run-queue order now come from CoreScheduler results",
                "Python schedule/channel callbacks and refcount/GC/weakref cleanup remain bridge-local"
            ],
            "migration_blocker": "make CoreScheduler tasklet/channel snapshots authoritative for remaining lifecycle decisions, Python payload handoff tokens, and queue identity adapters while leaving PyO3 as a compatibility wrapper"
        },
            "status": status,
            "report_ready": false,
            "coverage": "initial_pyo3_cdylib_import_build_flavor_module_package_queuechannel_capi_constructor_property_counter_setup_run_teardown_resource_cleanup_core_run_queue_scheduled_authority_core_pause_resume_lifecycle_authority_full_unchanged_legacy_scheduler_python_suite_expanded_scheduler_h_cxx_binary_tasklet_lifecycle_run_control_channel_preference_invalid_argument_inside_tasklet_channel_send_smoke_real_legacy_tasklet_channel_scheduler_cpp_source_slices_installed_release_wheel_smoke_abi_class_symbol_contract",
        "command": command,
        "package_directory": package_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        "duration_ms": duration_ms,
        "test_stdout_tail": tail_lines(&test_stdout, 12),
        "test_stderr_tail": tail_lines(&test_stderr, 12),
        "build_stdout_tail": build_stdout
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "build_stderr_tail": build_stderr
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "import_stdout_tail": import_stdout
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "import_stderr_tail": import_stderr
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "legacy_subset_stdout_tail": legacy_subset_stdout
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "legacy_subset_stderr_tail": legacy_subset_stderr
            .as_deref()
            .map(|text| tail_lines(text, 12)),
        "legacy_unittest_summary": legacy_unittest_summary,
        "capi_binary_smoke": capi_binary_smoke
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_tasklet_source_slice": legacy_capi_tasklet_source_slice
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_channel_source_slice": legacy_capi_channel_source_slice
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_scheduler_source_slice": legacy_capi_scheduler_source_slice
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_tasklet_in_process_probe": legacy_capi_tasklet_in_process_probe
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_channel_in_process_probe": legacy_capi_channel_in_process_probe
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_scheduler_in_process_probe": legacy_capi_scheduler_in_process_probe
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "legacy_capi_in_process_probe_status": if capi_in_process_probes_pass { "pass" } else { "not_green" },
        "wheel_install_smoke": wheel_install_smoke
            .as_ref()
            .map(SchedulerPythonWheelSmoke::to_json),
        "unchanged_legacy_subset": RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET,
        "unchanged_legacy_subset_count": unchanged_legacy_subset_count,
        "queuechannel_unchanged_legacy_subset": queuechannel_unchanged_subset,
        "queuechannel_unchanged_legacy_subset_count": queuechannel_unchanged_subset_count,
        "covered_behaviors": [
            "PyO3 module population smoke for _scheduler",
            "Rust cdylib builds and imports as _scheduler through normal CPython import machinery",
            "Rust cdylib copies import as _scheduler_debug, _scheduler_trinitydev, and _scheduler_internal",
            "legacy BUILDFLAVOR-style aliasing into sys.modules['_scheduler'] works for release/debug/trinitydev/internal",
            "legacy scheduler package imports from target/carbon/python through PYTHONPATH",
            "legacy public symbol smoke for tasklet/channel/schedule_manager and basic scheduler functions",
            "scheduler._C_API is a named PyCapsule accepted by CPython PyCapsule_IsValid",
            "scheduler._C_API exposes a non-null initial ABI table pointer through PyCapsule_GetPointer",
            "Scheduler.h-style PyCapsule_Import(\"scheduler._C_API\", 0) returns the same stable ABI table pointer",
            "scheduler._C_API TaskletExit pointer-to-pointer resolves to scheduler.TaskletExit",
            "scheduler._C_API exposes callable constructor/current entries for PyTasklet_New, PyChannel_New, PyScheduler_GetCurrent, and PyScheduler_GetScheduler",
            "scheduler._C_API exposes callable tasklet type/property entries for check, block_trap, is_main, alive, times_switched_to, and context",
            "scheduler._C_API exposes callable channel type/property entries for check, queue, preference, and balance",
            "scheduler._C_API exposes callable scheduler counter entries for run count, active managers/channels, tasklet counts, and last timeout counters",
            "scheduler._C_API executes a simple PyTasklet_Setup plus PyScheduler_RunNTasklets callable workload and updates run count",
                "initial C API tasklet insert/kill safe paths match legacy C API smoke expectations",
                "initial C API channel send/receive/send_exception/send_throw safe waiting-receiver paths match legacy C API smoke expectations",
                "compiled Scheduler.h C API smoke validates PyChannel_GetPreference/PyChannel_SetPreference default, neutral/sender values, and legacy clamping",
                "compiled Scheduler.h C API smoke rejects invalid tasklet/channel/null arguments without succeeding",
                "compiled Scheduler.h C API smoke calls PyChannel_Send from inside a running tasklet, observes blocked sender balance/queue state, receives the payload on the main tasklet, and drains the sender continuation",
                "initial C API scheduler run_n_tasklets, timeout counter, and callback get/set smoke paths are callable",
            "Python-level channel send_exception/send_throw validation wrapper path matches the active legacy smoke expectations",
            "Python-level switch/raise/kill scheduler paths match the active legacy smoke expectations",
            "Python Greenlet continuations resume scheduler and channel yields in the active bridge tests",
            "Python schedule and channel callback invocation paths match the active legacy smoke expectations",
            "Python tasklet throw, kill, traceback payload, wrong-thread, and thread-exit cleanup smoke paths pass",
            "Python channel close/open, queue visibility, blocked handoff order, kill/raise cleanup, and inter-thread cleanup smoke paths pass",
            "Schedule manager wrapper lifecycle and thread-cache cleanup smoke paths pass",
            "Python-level scheduler.tasklet(callable)(args) plus scheduler.run_n_tasklets(1) executes a simple callable workload and updates run count",
            unchanged_legacy_subset_summary,
            "legacy Scheduler.h C++ binary smoke imports scheduler._C_API from the Rust extension and calls header-typed current tasklet, channel balance, channel preference get/set/clamping, tasklet lifecycle, tasklet insert/kill, run-count, RunNTasklets, invalid tasklet/channel/null argument paths, and PyChannel_Send from inside a running tasklet",
            "legacy capiTest/Tasklet.cpp source compiles against the Rust extension with a local GoogleTest-compatible shim and passes its tasklet constructor, setup/run, setup refcount, insert, check, block_trap, main-tasklet, alive, and kill C API tests",
            "legacy capiTest/Channel.cpp source compiles against the Rust extension with a local GoogleTest-compatible shim and passes its channel constructor/check, send/receive, killed-tasklet cleanup, send_exception, queue head, preference, balance, and send_throw C API tests",
            "legacy capiTest/Scheduler.cpp source compiles against the Rust extension with a local GoogleTest-compatible shim and passes its schedule/run-count/current/run-n/timeout, channel callback, schedule callback, fast callback, active manager/channel/tasklet counter, and last-timeout counter C API tests",
            "maturin wheel builds from crates/carbon-scheduler-python, installs into a fresh virtualenv, and imports installed _scheduler plus installed legacy scheduler package with QueueChannel and _C_API smoke coverage",
            "tasklet switch pending kill and current-tasklet TaskletExit smoke behavior is covered",
            "scheduler ABI version exposed through Python",
            "channel constructor shape accepts QueueChannel-style super().__init__(self)",
            "legacy scheduler/__init__.py re-exports the Rust _scheduler smoke module",
            "legacy QueueChannel wrapper instantiates as a subclass of Rust-backed channel",
            "legacy QueueChannel buffered send/receive path round trips queued values",
            "legacy QueueChannel queued exception path raises the queued exception type",
            "legacy QueueChannel direct send to a blocked receiver schedules the unblocked receiver tasklet",
            "all ten unchanged legacy test_queuechannel.py tests pass against the Rust extension, including buffered sends, queue length/balance, queued data, blocking receive wakeup, queued exception/throw, main-tasklet empty receive error, nested blocked receiver order, block-trap receive rejection, and main receive drain",
            "SchedulerTestCaseBase resource cleanup is guarded for active manager/channel counters, schedule-manager refcount lifecycle, and tasklet C API lifetime counters",
            "channel preference property accepts legacy -1/0/1 values",
            "nested tasklet global flag round trips",
            "legacy block_trap context manager toggles the stable current tasklet flag",
            "Python test binary embeds local Python library rpath for cargo test"
        ],
        "ignored_bridge_blockers": [],
        "remaining_before_report_ready": [
            "drain tasklet/channel/scheduler ownership from the PyO3 compatibility bridge into Rust-owned core IDs and handles while preserving the legacy _scheduler and scheduler._C_API surface",
            "broader wheel/install flavor alias and dependency-resolution matrix beyond the installed release-wheel smoke",
            capi_in_process_remaining,
            "broader GIL/refcount/panic-containment failure tests around the passing PyO3 unit smoke paths",
            "legacy carbonio/_socket/_ssl semantic trace comparison through the Rust scheduler capsule"
        ]
    });
    if capi_in_process_probes_pass {
        evidence
            .get_mut("covered_behaviors")
            .and_then(Value::as_array_mut)
            .expect("rust scheduler evidence covered_behaviors is an array")
            .push(json!("legacy capiTest/Tasklet.cpp, Channel.cpp, and Scheduler.cpp source-slice binaries pass the optional in-process probe with CARBON_CAPI_GTEST_IN_PROCESS=1, exercising repeated Py_InitializeFromConfig/Py_FinalizeEx fixture lifecycles in one process"));
    }
    let evidence_path = evidence_path("rust-scheduler-python.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "rust-scheduler-python: {status} (initial PyO3 cdylib build-flavor import/package/QueueChannel/C API constructor/property/counter/setup-run plus full unchanged legacy scheduler Python suite; final report not ready); evidence {}",
        evidence_path.display()
    );

    ensure_success(test_output, "Rust scheduler Python bridge smoke tests")?;
    if let Some(output) = build_output {
        ensure_success(output, "Rust scheduler Python extension build")?;
    }
    if let Some(output) = import_output {
        ensure_success(output, "Rust scheduler Python import smoke")?;
    }
    if let Some(output) = legacy_subset_output {
        ensure_success(output, "unchanged legacy scheduler Python suite")?;
    }
    if let Some(smoke) = capi_binary_smoke {
        ensure_success(
            smoke.compile_output,
            "Scheduler.h C API binary smoke compile",
        )?;
        if let Some(output) = smoke.run_output {
            ensure_success(output, "Scheduler.h C API binary smoke run")?;
        } else {
            bail!("Scheduler.h C API binary smoke did not run");
        }
    }
    if let Some(smoke) = legacy_capi_tasklet_source_slice {
        ensure_success(
            smoke.compile_output,
            "legacy capiTest Tasklet.cpp source slice compile",
        )?;
        if let Some(output) = smoke.run_output {
            ensure_success(output, "legacy capiTest Tasklet.cpp source slice run")?;
        } else {
            bail!("legacy capiTest Tasklet.cpp source slice did not run");
        }
    }
    if let Some(smoke) = legacy_capi_channel_source_slice {
        ensure_success(
            smoke.compile_output,
            "legacy capiTest Channel.cpp source slice compile",
        )?;
        if let Some(output) = smoke.run_output {
            ensure_success(output, "legacy capiTest Channel.cpp source slice run")?;
        } else {
            bail!("legacy capiTest Channel.cpp source slice did not run");
        }
    }
    if let Some(smoke) = legacy_capi_scheduler_source_slice {
        ensure_success(
            smoke.compile_output,
            "legacy capiTest Scheduler.cpp source slice compile",
        )?;
        if let Some(output) = smoke.run_output {
            ensure_success(output, "legacy capiTest Scheduler.cpp source slice run")?;
        } else {
            bail!("legacy capiTest Scheduler.cpp source slice did not run");
        }
    }
    if let Some(smoke) = wheel_install_smoke {
        ensure_success(smoke.build_output, "scheduler Python wheel build")?;
        ensure_success(smoke.venv_output, "scheduler Python wheel virtualenv")?;
        ensure_success(smoke.install_output, "scheduler Python wheel install")?;
        ensure_success(smoke.smoke_output, "scheduler Python installed wheel smoke")?;
    }
    if status != "pass" {
        bail!("rust scheduler Python bridge gate failed");
    }
    Ok(())
}

fn io_workloads() -> Result<()> {
    let started = Instant::now();
    let cargo = env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let xtask_build_profile = inferred_xtask_build_profile();
    let rust_build = rust_build_metadata();
    let target_cpu_native = rust_build
        .get("target_cpu_native")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let debug_assertions = rust_build
        .get("debug_assertions")
        .and_then(Value::as_bool)
        .unwrap_or(cfg!(debug_assertions));
    let scheduler_python_build_profile = if xtask_build_profile == "release-native" {
        "release-native"
    } else {
        "debug"
    };
    let mut build_args = vec!["build", "-p", "carbon-scheduler-python"];
    if scheduler_python_build_profile == "release-native" {
        build_args.extend(["--profile", "release-native"]);
    }
    let build_command = format!("{} {}", shell_quote(&cargo), build_args.join(" "));
    let command = format!(
        "validate fixtures/io semantic traces; {build_command}; python3 -c <loopback socket/ssl workload script>; c++ <io Scheduler.h semantic smoke>"
    );
    let build_output = Command::new(&cargo)
        .args(&build_args)
        .output()
        .context("building Rust scheduler Python extension for IO workloads")?;

    let mut package_dir = None;
    let mut workloads = Vec::<Value>::new();
    let mut failures = Vec::<Value>::new();
    let mut scheduler_capi_semantic_smoke = None;
    let io_process_sample_count = io_workload_process_sample_count();
    let semantic_trace_fixtures =
        match load_io_semantic_trace_fixture_evidence(Path::new("fixtures/io")) {
            Ok(evidence) => evidence,
            Err(error) => {
                failures.push(json!({
                    "kind": "semantic_trace_fixtures",
                    "implementation": "fixture_validator",
                    "error": error.to_string()
                }));
                json!({
                    "status": "fail",
                    "fixture_dir": "fixtures/io",
                    "error": error.to_string()
                })
            }
        };

    if build_output.status.success() {
        let prepared_package_dir =
            prepare_scheduler_python_package(scheduler_python_build_profile)?;
        let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
        let python_path = PathBuf::from(&python);
        let cert_dir = Path::new("carbonengine/io/tests/python/carboniotests/test/certdata");
        let scheduler_env = scheduler_python_runtime_env(&prepared_package_dir)?;
        let no_extra_env: Vec<(OsString, OsString)> = Vec::new();
        let script = io_workload_script();
        let runs = [
            ("socket", "python_stdlib_baseline", 80_u64, &no_extra_env),
            (
                "socket",
                "rust_scheduler_python_bridge",
                80_u64,
                &scheduler_env,
            ),
            ("ssl", "python_stdlib_baseline", 24_u64, &no_extra_env),
            (
                "ssl",
                "rust_scheduler_python_bridge",
                24_u64,
                &scheduler_env,
            ),
        ];

        for (kind, mode, requests, envs) in runs {
            match run_io_workload_process(
                &python_path,
                script,
                kind,
                mode,
                requests,
                cert_dir,
                envs,
                io_process_sample_count,
            ) {
                Ok(workload) => workloads.push(workload),
                Err(error) => failures.push(json!({
                    "kind": kind,
                    "implementation": mode,
                    "error": error.to_string()
                })),
            }
        }
        match run_io_scheduler_capi_semantic_smoke(&prepared_package_dir) {
            Ok(smoke) => {
                if !smoke.success() {
                    failures.push(json!({
                        "kind": "scheduler_capi_semantic_smoke",
                        "implementation": "rust_scheduler_python_bridge",
                        "error": "IO-facing Scheduler.h C API semantic smoke failed"
                    }));
                }
                scheduler_capi_semantic_smoke = Some(smoke);
            }
            Err(error) => failures.push(json!({
                "kind": "scheduler_capi_semantic_smoke",
                "implementation": "rust_scheduler_python_bridge",
                "error": error.to_string()
            })),
        }
        package_dir = Some(prepared_package_dir);
    } else {
        failures.push(json!({
            "kind": "build",
            "implementation": "rust_scheduler_python_bridge",
            "error": "cargo build -p carbon-scheduler-python failed"
        }));
    }

    let comparisons = io_workload_comparisons(
        &workloads,
        scheduler_python_build_profile,
        target_cpu_native,
        debug_assertions,
    );
    let legacy_carbonio_semantic_traces = legacy_carbonio_semantic_trace_blocker();
    let status = if failures.is_empty() && comparisons.len() == 2 {
        "pass"
    } else {
        "fail"
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let build_stdout = String::from_utf8_lossy(&build_output.stdout);
    let build_stderr = String::from_utf8_lossy(&build_output.stderr);
    let evidence = json!({
        "schema": "carbon.evidence.io_workloads.v1",
        "gate": "io-workloads",
        "component": "io",
        "status": status,
        "report_ready": false,
        "build_profile": xtask_build_profile,
        "target_cpu_native": target_cpu_native,
        "debug_assertions": debug_assertions,
        "scheduler_python_build_profile": scheduler_python_build_profile,
        "scheduler_python_build_command": build_command,
        "io_process_sample_count": io_process_sample_count,
        "coverage": "initial_loopback_socket_ssl_workloads_with_scheduler_bridge_resource_stats_io_semantic_trace_fixtures_and_io_scheduler_h_channel_semantic_smoke",
        "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
        "not_report_ready_reason": "Loopback TCP and TLS workloads now collect realistic request latency, throughput, CPU, and RSS for a Python baseline and the Rust scheduler Python bridge. fixtures/io now contains a bounded normalized semantic trace corpus for socket recv/send wakeups, SSL read/write wakeups, and SSL send_throw error propagation; xtask validates that fixture vocabulary/order and records it as fixture-only evidence. A compiled Scheduler.h consumer also proves the Rust scheduler capsule supports IO-facing PyChannel_GetBalance blocked-receiver wake decisions and PyChannel_SendThrow error propagation. This is not final Carbon IO parity: a normalized legacy carbonio/_socket/_ssl semantic trace comparison is tracked but blocked on a supported Windows/macOS legacy carbonio+legacy scheduler run or prebuilt legacy artifacts.",
        "command": command,
        "package_directory": package_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        "duration_ms": duration_ms,
        "host": {
            "os": env::consts::OS,
            "arch": env::consts::ARCH,
            "cpu_model": host_cpu_model().unwrap_or_else(|| String::from("unknown")),
            "logical_cpus": std::thread::available_parallelism().map(|value| value.get()).unwrap_or_default(),
            "ram_kb": host_mem_total_kb(),
            "rustc": command_stdout("rustc", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "cargo": command_stdout("cargo", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "rust_build": rust_build.clone(),
            "process_resource_measurement": if Path::new("/usr/bin/time").exists() { "external_time_v" } else { "wall_clock_only" },
        },
        "workloads": workloads,
        "comparisons": comparisons,
        "semantic_trace_fixtures": semantic_trace_fixtures,
        "legacy_carbonio_semantic_traces": legacy_carbonio_semantic_traces,
        "scheduler_capi_semantic_smoke": scheduler_capi_semantic_smoke
            .as_ref()
            .map(SchedulerCapiBinarySmoke::to_json),
        "covered_behaviors": [
            "loopback TCP accept/connect/send/receive request cycles",
            "loopback TLS handshake/read/write request cycles using the carbonengine/io certificate fixture corpus",
            "Rust scheduler Python bridge drives client request loops through scheduler.tasklet and scheduler.run_n_tasklets",
            "Rust scheduler QueueChannel transfers workload summaries back to the main tasklet",
            "repeated request latency p50/p95/p99, throughput, CPU percent, effective CPU burn, and peak RSS samples are recorded",
            "fixtures/io validates normalized semantic event-order fixtures for socket recv/send wake, SSL read/write wake, and SSL send_throw error wake without timing fields",
            "compiled Scheduler.h C++ consumer imports the Rust scheduler._C_API capsule",
            "PyChannel_GetBalance reports blocked receiver state and returns to zero after C API wake",
            "PyChannel_Send wakes a blocked receiver through the Rust scheduler capsule",
            "PyChannel_SendThrow propagates a RuntimeError through a blocked receiver tasklet"
        ],
        "remaining_before_report_ready": [
            "run the normalized legacy carbonio/_socket/_ssl semantic trace harness on a supported Windows/macOS legacy carbonio+legacy scheduler build or supplied prebuilt legacy artifacts",
            "replace fixture-only expected traces with captured legacy carbonio/_socket/_ssl traces and compare them against Rust scheduler capsule traces",
            "compare zero-mismatch semantic traces for libuv wakeups and blocked tasklets against the Rust scheduler capsule",
            "expand c_channel compatibility beyond the initial Scheduler.h channel wake/send_throw smoke",
            "prove SSL error propagation through the real carbonengine/io extension path",
            "add larger Tier 2 loopback/container workloads after Tier 1 parity is green"
        ],
        "build_stdout_tail": tail_lines(&build_stdout, 12),
        "build_stderr_tail": tail_lines(&build_stderr, 12),
        "failures": failures
    });
    let evidence_path = evidence_path("io-workloads.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "io-workloads: {status} (initial loopback TCP/TLS workload stats; final report not ready); evidence {}",
        evidence_path.display()
    );

    ensure_success(
        build_output,
        "Rust scheduler Python extension build for IO workloads",
    )?;
    if status == "pass" {
        Ok(())
    } else {
        bail!("io workloads failed; see {}", evidence_path.display())
    }
}

fn load_io_semantic_trace_fixture_evidence(fixture_dir: &Path) -> Result<Value> {
    if !fixture_dir.exists() {
        bail!(
            "IO semantic trace fixture directory {} is missing",
            fixture_dir.display()
        );
    }

    let mut fixture_paths = Vec::new();
    for entry in fs::read_dir(fixture_dir)
        .with_context(|| format!("reading IO fixture directory {}", fixture_dir.display()))?
    {
        let path = entry
            .with_context(|| format!("reading entry in {}", fixture_dir.display()))?
            .path();
        if path.extension().and_then(OsStr::to_str) == Some("json") {
            fixture_paths.push(path);
        }
    }
    fixture_paths.sort();
    if fixture_paths.is_empty() {
        bail!(
            "IO semantic trace fixture directory {} contains no JSON fixtures",
            fixture_dir.display()
        );
    }

    let mut fixtures = Vec::new();
    let mut total_event_count = 0_u64;
    let mut total_required_order_count = 0_u64;
    let mut kind_counts = BTreeMap::<String, u64>::new();

    for path in fixture_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading IO semantic trace fixture {}", path.display()))?;
        let value = serde_json::from_str::<Value>(&text)
            .with_context(|| format!("parsing IO semantic trace fixture {}", path.display()))?;
        let summary = validate_io_semantic_trace_fixture(&path, &value)?;
        total_event_count += summary
            .get("event_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        total_required_order_count += summary
            .get("required_order_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if let Some(kind) = summary.get("kind").and_then(Value::as_str) {
            *kind_counts.entry(kind.to_string()).or_default() += 1;
        }
        fixtures.push(summary);
    }

    Ok(json!({
        "schema": "carbon.evidence.io_semantic_trace_fixtures.v1",
        "status": "pass",
        "fixture_dir": fixture_dir.display().to_string(),
        "fixture_count": fixtures.len(),
        "total_event_count": total_event_count,
        "total_required_order_count": total_required_order_count,
        "kind_counts": kind_counts,
        "comparability": "fixture_schema_only_not_legacy_carbon_io",
        "parity_status": "not_legacy_comparable",
        "claim_scope": "bounded normalized expected-event fixtures only; no legacy Carbon IO parity or speedup claim",
        "timings_excluded": true,
        "fixtures": fixtures
    }))
}

fn validate_io_semantic_trace_fixture(path: &Path, fixture: &Value) -> Result<Value> {
    let schema = required_string_field(fixture, "schema", path)?;
    if schema != "carbon.io.semantic_trace_fixture.v1" {
        bail!(
            "IO semantic trace fixture {} has unsupported schema {}",
            path.display(),
            schema
        );
    }
    let fixture_name = required_string_field(fixture, "fixture", path)?;
    let kind = required_string_field(fixture, "kind", path)?;
    if !matches!(kind, "socket" | "ssl") {
        bail!(
            "IO semantic trace fixture {} has unsupported kind {}",
            path.display(),
            kind
        );
    }
    let source_refs = required_array_field(fixture, "source_refs", path)?;
    for (index, source) in source_refs.iter().enumerate() {
        if source
            .as_str()
            .is_none_or(|source| source.trim().is_empty())
        {
            bail!(
                "IO semantic trace fixture {} source_refs[{}] must be a non-empty string",
                path.display(),
                index
            );
        }
    }
    if fixture.get("timings_excluded").and_then(Value::as_bool) != Some(true) {
        bail!(
            "IO semantic trace fixture {} must set timings_excluded=true",
            path.display()
        );
    }

    let events = required_array_field(fixture, "expected_events", path)?;
    let required_order = required_array_field(fixture, "required_order", path)?;
    let mut event_names = Vec::<String>::new();
    for (index, event) in events.iter().enumerate() {
        let object = event.as_object().with_context(|| {
            format!(
                "IO semantic trace fixture {} expected_events[{}] must be an object",
                path.display(),
                index
            )
        })?;
        for timing_field in [
            "timestamp",
            "timestamp_us",
            "time_us",
            "duration_us",
            "elapsed_us",
        ] {
            if object.contains_key(timing_field) {
                bail!(
                    "IO semantic trace fixture {} expected_events[{}] contains timing field {}; semantic fixtures must be timing-free",
                    path.display(),
                    index,
                    timing_field
                );
            }
        }
        let event_name = event
            .get("event")
            .and_then(Value::as_str)
            .filter(|event| !event.trim().is_empty())
            .with_context(|| {
                format!(
                    "IO semantic trace fixture {} expected_events[{}].event must be a non-empty string",
                    path.display(),
                    index
                )
            })?;
        event_names.push(event_name.to_string());
    }

    let mut required_names = Vec::<String>::new();
    for (index, required) in required_order.iter().enumerate() {
        let required_name = required
            .as_str()
            .filter(|event| !event.trim().is_empty())
            .with_context(|| {
                format!(
                    "IO semantic trace fixture {} required_order[{}] must be a non-empty string",
                    path.display(),
                    index
                )
            })?;
        required_names.push(required_name.to_string());
    }

    let mut cursor = 0_usize;
    for required_name in &required_names {
        let Some(relative_index) = event_names[cursor..]
            .iter()
            .position(|event_name| event_name == required_name)
        else {
            bail!(
                "IO semantic trace fixture {} required event {} was not found in order after event index {}",
                path.display(),
                required_name,
                cursor
            );
        };
        cursor += relative_index + 1;
    }

    Ok(json!({
        "status": "pass",
        "path": path.display().to_string(),
        "fixture": fixture_name,
        "kind": kind,
        "feature_area": fixture.get("feature_area").cloned().unwrap_or(Value::Null),
        "source_refs": fixture.get("source_refs").cloned().unwrap_or_else(|| json!([])),
        "covered_behaviors": fixture.get("covered_behaviors").cloned().unwrap_or_else(|| json!([])),
        "event_count": event_names.len(),
        "required_order_count": required_names.len(),
        "first_event": event_names.first().cloned().unwrap_or_default(),
        "last_event": event_names.last().cloned().unwrap_or_default(),
        "event_names": event_names,
        "required_order": required_names,
        "parity_status": "fixture_validated_not_legacy_compared",
        "comparability": "fixture_schema_only_not_legacy_carbon_io",
        "claim_scope": fixture.get("claim_scope").cloned().unwrap_or_else(|| json!("expected event order only; not legacy Carbon IO parity")),
        "timings_excluded": true
    }))
}

fn required_string_field<'a>(value: &'a Value, field: &str, path: &Path) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|field_value| !field_value.trim().is_empty())
        .with_context(|| {
            format!(
                "IO semantic trace fixture {} missing non-empty string field {}",
                path.display(),
                field
            )
        })
}

fn required_array_field<'a>(value: &'a Value, field: &str, path: &Path) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .filter(|field_value| !field_value.is_empty())
        .with_context(|| {
            format!(
                "IO semantic trace fixture {} missing non-empty array field {}",
                path.display(),
                field
            )
        })
}

fn scheduler_python_runtime_env(package_dir: &Path) -> Result<Vec<(OsString, OsString)>> {
    let root = env::current_dir().context("resolving current directory")?;
    let package_dir = root.join(package_dir);
    let mut python_paths = vec![package_dir];
    if let Some(existing) = env::var_os("PYTHONPATH") {
        python_paths.extend(env::split_paths(&existing));
    }
    let python_path = env::join_paths(python_paths).context("joining IO workload PYTHONPATH")?;
    let mut envs = vec![(OsString::from("PYTHONPATH"), python_path)];

    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
    if let Ok(python_libdir) = command_stdout(
        &python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ],
    ) {
        if !python_libdir.is_empty() {
            let ld_library_path = match env::var("LD_LIBRARY_PATH") {
                Ok(existing) if !existing.is_empty() => format!("{python_libdir}:{existing}"),
                _ => python_libdir,
            };
            envs.push((OsString::from("LD_LIBRARY_PATH"), ld_library_path.into()));
        }
    }

    Ok(envs)
}

fn io_workload_process_sample_count() -> u64 {
    env::var("CARBON_IO_WORKLOAD_SAMPLES")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(50))
        .unwrap_or(5)
}

fn run_io_workload_process(
    python: &Path,
    script: &str,
    kind: &str,
    implementation: &str,
    requests: u64,
    cert_dir: &Path,
    envs: &[(OsString, OsString)],
    process_sample_count: u64,
) -> Result<Value> {
    let mut first_workload: Option<Value> = None;
    let mut process_metrics = Vec::new();
    let mut run_samples = Vec::new();
    let mut latency_samples_us = Vec::new();
    let mut duration_samples_us = Vec::new();
    let mut throughput_request_samples = Vec::new();
    let mut throughput_byte_samples = Vec::new();
    let runs = process_sample_count.max(1);

    for sample_index in 0..runs {
        let measured = run_single_io_workload_process(
            python,
            script,
            kind,
            implementation,
            requests,
            cert_dir,
            envs,
        )?;
        let stdout = String::from_utf8_lossy(&measured.output.stdout);
        let stderr = String::from_utf8_lossy(&measured.output.stderr);
        if !measured.output.status.success() {
            bail!(
                "{kind} IO workload for {implementation} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                measured.output.status.code(),
                stdout,
                stderr
            );
        }

        let workload: Value = serde_json::from_str(stdout.trim()).with_context(|| {
            format!("parsing {kind} IO workload JSON for {implementation}: {stdout}")
        })?;
        if first_workload.is_none() {
            first_workload = Some(workload.clone());
        }

        let sample_latency = workload
            .get("latency_samples_us")
            .and_then(Value::as_array)
            .map(|values| values.iter().filter_map(Value::as_u64).collect::<Vec<_>>())
            .unwrap_or_default();
        latency_samples_us.extend(sample_latency.iter().copied());

        let duration_us = workload
            .get("duration_us")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let throughput_requests_per_sec = workload
            .get("throughput_requests_per_sec")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let throughput_bytes_per_sec = workload
            .get("throughput_bytes_per_sec")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        duration_samples_us.push(duration_us);
        throughput_request_samples.push(throughput_requests_per_sec);
        throughput_byte_samples.push(throughput_bytes_per_sec);

        run_samples.push(json!({
            "sample_index": sample_index,
            "duration_us": duration_us,
            "latency_us": workload.get("latency_us").cloned().unwrap_or_else(|| json!({"count": 0})),
            "latency_sample_count": sample_latency.len(),
            "throughput_requests_per_sec": throughput_requests_per_sec,
            "throughput_bytes_per_sec": throughput_bytes_per_sec,
            "scheduler": workload.get("scheduler").cloned().unwrap_or(Value::Null),
            "process_metrics": process_metrics_sample_json(std::slice::from_ref(&measured.metrics)),
            "stderr_tail": tail_lines(&stderr, 12),
        }));
        process_metrics.push(measured.metrics);
    }

    let mut workload = first_workload.unwrap_or_else(|| {
        json!({
            "component": "io",
            "kind": kind,
            "workload": format!("{kind}_loopback_{implementation}"),
            "implementation": implementation,
            "requests": requests,
            "payload_bytes": 0,
            "bytes_transferred": 0,
            "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
            "parity_status": "not_legacy_comparable",
            "scheduler": null
        })
    });
    let requests_per_run = requests;
    let request_sample_count = latency_samples_us.len() as u64;
    let total_duration_us = duration_samples_us.iter().sum::<u64>();
    let mean_duration_us = mean_u64_rounded(&duration_samples_us);
    let total_bytes_transferred = workload
        .get("bytes_transferred")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .saturating_mul(runs);
    let bytes_transferred_per_run = workload
        .get("bytes_transferred")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let aggregate_throughput_requests_per_sec = if total_duration_us > 0 {
        ((request_sample_count as u128 * 1_000_000) / total_duration_us as u128) as u64
    } else {
        0
    };
    let aggregate_throughput_bytes_per_sec = if total_duration_us > 0 {
        ((total_bytes_transferred as u128 * 1_000_000) / total_duration_us as u128) as u64
    } else {
        0
    };

    if let Some(object) = workload.as_object_mut() {
        object.insert(String::from("requests_per_run"), json!(requests_per_run));
        object.insert(String::from("requests"), json!(request_sample_count));
        object.insert(
            String::from("request_sample_count"),
            json!(request_sample_count),
        );
        object.insert(
            String::from("bytes_transferred_per_run"),
            json!(bytes_transferred_per_run),
        );
        object.insert(
            String::from("bytes_transferred"),
            json!(total_bytes_transferred),
        );
        object.insert(
            String::from("process_sample_count"),
            json!(process_metrics.len()),
        );
        object.insert(String::from("run_sample_count"), json!(run_samples.len()));
        object.insert(String::from("duration_us"), json!(mean_duration_us));
        object.insert(
            String::from("duration_ms"),
            json!(duration_ms_from_us(mean_duration_us)),
        );
        object.insert(
            String::from("duration_us_stats"),
            sample_stats_us(&duration_samples_us),
        );
        object.insert(
            String::from("latency_us"),
            sample_stats_us(&latency_samples_us),
        );
        object.insert(
            String::from("latency_samples_us"),
            json!(latency_samples_us),
        );
        object.insert(
            String::from("throughput_requests_per_sec"),
            json!(aggregate_throughput_requests_per_sec),
        );
        object.insert(
            String::from("throughput_bytes_per_sec"),
            json!(aggregate_throughput_bytes_per_sec),
        );
        object.insert(
            String::from("throughput_requests_per_sec_stats"),
            sample_stats_u64(&throughput_request_samples),
        );
        object.insert(
            String::from("throughput_bytes_per_sec_stats"),
            sample_stats_u64(&throughput_byte_samples),
        );
        object.insert(
            String::from("process_samples"),
            process_metrics_sample_json(&process_metrics),
        );
        object.insert(
            String::from("process_stats"),
            process_metrics_summary(&process_metrics),
        );
        object.insert(
            String::from("process_duration_us"),
            json!(mean_u64_rounded(
                &process_metrics
                    .iter()
                    .map(|metrics| metrics.wall_time_us)
                    .collect::<Vec<_>>()
            )),
        );
        object.insert(String::from("run_samples"), Value::Array(run_samples));
        object.insert(
            String::from("command"),
            json!(format!(
                "{} -c <loopback socket/ssl workload script> {} {} {} {}",
                shell_quote_os(python.as_os_str()),
                shell_quote(implementation),
                shell_quote(kind),
                requests,
                shell_quote_os(cert_dir.as_os_str())
            )),
        );
    }
    Ok(workload)
}

fn run_single_io_workload_process(
    python: &Path,
    script: &str,
    kind: &str,
    implementation: &str,
    requests: u64,
    cert_dir: &Path,
    envs: &[(OsString, OsString)],
) -> Result<MeasuredProcessOutput> {
    let args = vec![
        OsString::from("-c"),
        OsString::from(script),
        OsString::from(implementation),
        OsString::from(kind),
        OsString::from(requests.to_string()),
        cert_dir.as_os_str().to_os_string(),
    ];
    run_timed_process_with_env(python, &args, None, envs)
        .with_context(|| format!("running {kind} IO workload for {implementation}"))
}

fn io_workload_comparisons(
    workloads: &[Value],
    rust_build_profile: &str,
    target_cpu_native: bool,
    debug_assertions: bool,
) -> Vec<Value> {
    ["socket", "ssl"]
        .into_iter()
        .filter_map(|kind| {
            let baseline = workloads.iter().find(|workload| {
                workload.get("kind").and_then(Value::as_str) == Some(kind)
                    && workload.get("implementation").and_then(Value::as_str)
                        == Some("python_stdlib_baseline")
            })?;
            let scheduler = workloads.iter().find(|workload| {
                workload.get("kind").and_then(Value::as_str) == Some(kind)
                    && workload.get("implementation").and_then(Value::as_str)
                        == Some("rust_scheduler_python_bridge")
            })?;
            Some(io_workload_comparison(
                kind,
                baseline,
                scheduler,
                rust_build_profile,
                target_cpu_native,
                debug_assertions,
            ))
        })
        .collect()
}

fn legacy_carbonio_semantic_trace_blocker() -> Value {
    json!({
        "legacy_carbonio_trace_status": "blocked",
        "legacy_carbonio_trace_comparability": "not_comparable_legacy_carbonio_unavailable_on_this_host",
        "legacy_carbonio_trace_scope": "normalized semantic events for carbonio/_socket/_ssl module patching, socketpair creation, recv block, send wake, dispatch wake, recv result, block_trap send error, close wakeup, and SSL handshake/read/write; timings are intentionally excluded",
        "legacy_carbonio_trace_runs": [],
        "legacy_carbonio_trace_hashes": {
            "legacy_carbonio_legacy_scheduler": null,
            "legacy_carbonio_rust_scheduler_capsule": null
        },
        "legacy_carbonio_trace_comparison": {
            "parity_status": "blocked",
            "mismatch_count": null,
            "mismatches": [],
            "required_result": "zero normalized semantic mismatches before legacy Carbon IO parity can be claimed"
        },
        "legacy_carbonio_build": {
            "host_os": env::consts::OS,
            "status": "blocked",
            "blocked_reason": "carbonengine/io and the legacy carbon-scheduler vcpkg port are not configured as supported Linux legacy build targets in this workspace; a Windows/macOS legacy carbonio+legacy scheduler run or supplied prebuilt legacy artifacts are required for this comparison",
            "evidence_refs": [
                "carbonengine/io/CMakeLists.txt",
                "carbonengine/io/CMakePresets.json",
                "carbonengine/io/vcpkg.json",
                "carbonengine/vcpkg-registry/ports/carbon-scheduler/vcpkg.json",
                "carbonengine/io/tests/python/carboniotests/__main__.py",
                "carbonengine/io/tests/python/carboniotests/test/test_socket.py",
                "carbonengine/io/tests/python/carboniotests/test/test_ssl.py"
            ]
        },
        "planned_trace_cases": [
            "socketpair recv blocks then send wakes receiver through carbonio dispatch",
            "blocking send under block_trap raises the legacy visible error",
            "incremental socket reads preserve event order and payload bytes",
            "close wakes pending recv with the legacy visible close/error event",
            "SSL handshake/read/write wake paths preserve normalized success/error events"
        ]
    })
}

fn io_workload_comparison(
    kind: &str,
    baseline: &Value,
    scheduler: &Value,
    rust_build_profile: &str,
    target_cpu_native: bool,
    debug_assertions: bool,
) -> Value {
    let requests = baseline
        .get("requests")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let requests_per_run = baseline
        .get("requests_per_run")
        .and_then(Value::as_u64)
        .unwrap_or(requests);
    let request_sample_count = baseline
        .get("request_sample_count")
        .and_then(Value::as_u64)
        .unwrap_or(requests);
    let legacy_process_sample_count = baseline
        .get("process_sample_count")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            baseline
                .get("process_stats")
                .and_then(|stats| stats.get("sample_count"))
                .and_then(Value::as_u64)
                .unwrap_or(1)
        });
    let rust_process_sample_count = scheduler
        .get("process_sample_count")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            scheduler
                .get("process_stats")
                .and_then(|stats| stats.get("sample_count"))
                .and_then(Value::as_u64)
                .unwrap_or(1)
        });
    let baseline_duration_us = baseline
        .get("duration_us")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let scheduler_duration_us = scheduler
        .get("duration_us")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let baseline_cpu_ms = number_at(
        baseline,
        &["process_stats", "cpu_burn_effective_ms", "mean"],
    );
    let scheduler_cpu_ms = number_at(
        scheduler,
        &["process_stats", "cpu_burn_effective_ms", "mean"],
    );
    let scale_units = 100_000_f64;
    let scale_basis =
        "linear estimate from mean local process samples; request loop includes socket setup and this is not a production claim";

    json!({
        "component": "io",
        "kind": kind,
        "workload": format!("{kind}_loopback_request_cycles"),
        "legacy_implementation": "python_stdlib_baseline",
        "rust_implementation": "rust_scheduler_python_bridge",
        "rust_build_profile": rust_build_profile,
        "target_cpu_native": target_cpu_native,
        "debug_assertions": debug_assertions,
        "legacy_command_template": baseline.get("command").cloned().unwrap_or(Value::Null),
        "rust_command_template": scheduler.get("command").cloned().unwrap_or(Value::Null),
        "sample_count": request_sample_count,
        "process_sample_count": legacy_process_sample_count.min(rust_process_sample_count),
        "legacy_process_sample_count": legacy_process_sample_count,
        "rust_process_sample_count": rust_process_sample_count,
        "requests": request_sample_count,
        "requests_per_run": requests_per_run,
        "payload_bytes": baseline.get("payload_bytes").cloned().unwrap_or(Value::Null),
        "bytes_transferred": baseline.get("bytes_transferred").cloned().unwrap_or(Value::Null),
        "bytes_transferred_per_run": baseline.get("bytes_transferred_per_run").cloned().unwrap_or(Value::Null),
        "legacy_duration_us": baseline_duration_us,
        "rust_duration_us": scheduler_duration_us,
        "legacy_sample_stats_us": baseline.get("latency_us").cloned().unwrap_or_else(|| json!({"count": 0})),
        "rust_sample_stats_us": scheduler.get("latency_us").cloned().unwrap_or_else(|| json!({"count": 0})),
        "legacy_process_stats": baseline.get("process_stats").cloned().unwrap_or_else(|| json!({})),
        "rust_process_stats": scheduler.get("process_stats").cloned().unwrap_or_else(|| json!({})),
        "legacy_throughput_requests_per_sec": baseline.get("throughput_requests_per_sec").cloned().unwrap_or(Value::Null),
        "rust_throughput_requests_per_sec": scheduler.get("throughput_requests_per_sec").cloned().unwrap_or(Value::Null),
        "legacy_throughput_bytes_per_sec": baseline.get("throughput_bytes_per_sec").cloned().unwrap_or(Value::Null),
        "rust_throughput_bytes_per_sec": scheduler.get("throughput_bytes_per_sec").cloned().unwrap_or(Value::Null),
        "speedup": speedup_ratio(baseline_duration_us, scheduler_duration_us),
        "parity_status": "not_legacy_comparable",
        "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
        "not_comparable_reason": "Baseline is Python stdlib loopback and target is the Rust scheduler Python bridge; the legacy Carbon carbonio/_socket/_ssl extension is not sampled in this comparison.",
        "claim_scope": "realistic local IO workload resource stats only; not a legacy Carbon IO speedup claim",
        "resource_comparison": {
            "wall_time_ratio_baseline_over_scheduler_bridge": speedup_ratio(baseline_duration_us, scheduler_duration_us),
            "cpu_burn_effective_ratio_baseline_over_scheduler_bridge": optional_ratio_f64(baseline_cpu_ms, scheduler_cpu_ms),
            "peak_rss_ratio_scheduler_bridge_over_baseline_p95": optional_ratio(
                number_at(scheduler, &["process_stats", "max_rss_kb", "p95"]).map(|value| value as u64),
                number_at(baseline, &["process_stats", "max_rss_kb", "p95"]).map(|value| value as u64),
            ),
            "linear_scale_estimate_100k_units": {
                "basis": scale_basis,
                "units": 100_000,
                "legacy_wall_seconds": scale_duration_seconds(baseline_duration_us, requests_per_run, scale_units),
                "rust_wall_seconds": scale_duration_seconds(scheduler_duration_us, requests_per_run, scale_units),
                "legacy_cpu_burn_seconds": scale_optional_ms_seconds(baseline_cpu_ms, requests_per_run, scale_units),
                "rust_cpu_burn_seconds": scale_optional_ms_seconds(scheduler_cpu_ms, requests_per_run, scale_units)
            }
        }
    })
}

fn scale_duration_seconds(duration_us: u64, units: u64, scaled_units: f64) -> f64 {
    if units == 0 {
        return 0.0;
    }
    (duration_us as f64 / 1_000_000.0) / units as f64 * scaled_units
}

fn scale_optional_ms_seconds(value_ms: Option<f64>, units: u64, scaled_units: f64) -> Value {
    let Some(value_ms) = value_ms else {
        return Value::Null;
    };
    if units == 0 {
        return Value::Null;
    }
    json!((value_ms / 1000.0) / units as f64 * scaled_units)
}

fn io_workload_script() -> &'static str {
    r#"
import json
import os
import socket
import ssl
import sys
import threading
import time

implementation = sys.argv[1]
kind = sys.argv[2]
requests = int(sys.argv[3])
cert_dir = sys.argv[4]
host = "127.0.0.1"
payload = (b"carbon-io-workload-" * 16)[:256]

def percentile(sorted_values, percentile_value):
    if not sorted_values:
        return 0
    rank = (percentile_value * len(sorted_values) + 99) // 100
    index = max(0, min(len(sorted_values) - 1, rank - 1))
    return sorted_values[index]

def stats(values):
    sorted_values = sorted(values)
    return {
        "count": len(sorted_values),
        "min": sorted_values[0] if sorted_values else 0,
        "mean": (sum(sorted_values) / len(sorted_values)) if sorted_values else 0,
        "p50": percentile(sorted_values, 50),
        "p95": percentile(sorted_values, 95),
        "p99": percentile(sorted_values, 99),
        "max": sorted_values[-1] if sorted_values else 0,
    }

def read_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("socket closed before full payload was received")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)

def make_server_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.set_ciphers("@SECLEVEL=1:ALL")
    except ssl.SSLError:
        pass
    context.load_cert_chain(os.path.join(cert_dir, "keycert.pem"))
    return context

def make_client_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        context.set_ciphers("@SECLEVEL=1:ALL")
    except ssl.SSLError:
        pass
    return context

def run_server(listener, request_count, use_ssl, errors):
    server_context = make_server_context() if use_ssl else None
    listener.settimeout(10.0)
    try:
        with listener:
            for _ in range(request_count):
                conn, _addr = listener.accept()
                conn.settimeout(10.0)
                if server_context is not None:
                    conn = server_context.wrap_socket(conn, server_side=True)
                with conn:
                    data = read_exact(conn, len(payload))
                    conn.sendall(data)
    except BaseException as exc:
        errors.append(repr(exc))

def client_loop(port, request_count, use_ssl):
    client_context = make_client_context() if use_ssl else None
    latencies = []
    for _ in range(request_count):
        started = time.perf_counter_ns()
        raw = socket.create_connection((host, port), timeout=10.0)
        if client_context is not None:
            conn = client_context.wrap_socket(raw, server_hostname="localhost")
        else:
            conn = raw
        with conn:
            conn.sendall(payload)
            data = read_exact(conn, len(payload))
        if data != payload:
            raise RuntimeError("loopback payload mismatch")
        latencies.append((time.perf_counter_ns() - started) // 1000)
    return latencies

def run_workload():
    use_ssl = kind == "ssl"
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, 0))
    listener.listen()
    port = listener.getsockname()[1]
    errors = []
    server = threading.Thread(target=run_server, args=(listener, requests, use_ssl, errors), daemon=True)
    server.start()
    started = time.perf_counter_ns()
    scheduler_info = None
    if implementation == "rust_scheduler_python_bridge":
        import scheduler
        result_channel = scheduler.QueueChannel()
        def tasklet_client():
            result_channel.send(client_loop(port, requests, use_ssl))
        task = scheduler.tasklet(tasklet_client)()
        scheduler.run_n_tasklets(1)
        latencies = result_channel.receive()
        scheduler_info = {
            "run_count": scheduler.getruncount(),
            "tasklet_alive": task.alive,
            "tasklet_scheduled": task.scheduled,
            "tasklet_times_switched_to": task.times_switched_to,
            "result_channel_balance": result_channel.balance,
            "result_channel_length": len(result_channel),
        }
    elif implementation == "python_stdlib_baseline":
        latencies = client_loop(port, requests, use_ssl)
    else:
        raise RuntimeError(f"unknown implementation: {implementation}")
    duration_us = (time.perf_counter_ns() - started) // 1000
    server.join(timeout=10.0)
    if server.is_alive():
        raise RuntimeError("server thread did not finish")
    if errors:
        raise RuntimeError("server failed: " + "; ".join(errors))
    throughput_requests_per_sec = int(requests * 1000000 / duration_us) if duration_us else 0
    throughput_bytes_per_sec = int(requests * len(payload) * 2 * 1000000 / duration_us) if duration_us else 0
    result = {
        "component": "io",
        "kind": kind,
        "workload": f"{kind}_loopback_{implementation}",
        "implementation": implementation,
        "requests": requests,
        "payload_bytes": len(payload),
        "bytes_transferred": requests * len(payload) * 2,
        "duration_us": duration_us,
        "duration_ms": (duration_us + 999) // 1000,
        "latency_us": stats(latencies),
        "latency_samples_us": latencies,
        "throughput_requests_per_sec": throughput_requests_per_sec,
        "throughput_bytes_per_sec": throughput_bytes_per_sec,
        "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
        "parity_status": "not_legacy_comparable",
        "scheduler": scheduler_info,
    }
    print(json.dumps(result, sort_keys=True))

run_workload()
"#
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalabilityTier {
    Quick,
    Full,
}

impl ScalabilityTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "quick" => Ok(Self::Quick),
            "full" => Ok(Self::Full),
            other => bail!("unsupported bench-scalability tier: {other}"),
        }
    }
}

#[derive(Clone, Debug)]
struct ScalabilityConfig {
    tier: ScalabilityTier,
    families: BTreeSet<String>,
    samples: u64,
    output: PathBuf,
}

fn bench_scalability(args: Vec<String>) -> Result<()> {
    if args
        .iter()
        .any(|arg| arg.as_str() == "--help" || arg.as_str() == "-h")
    {
        println!(
            "usage: xtask bench-scalability [--tier quick|full] [--families scheduler,io,data] [--samples N] [--output path]"
        );
        return Ok(());
    }
    let started = Instant::now();
    let config = parse_bench_scalability_args(args)?;
    let rust_build = rust_build_metadata();
    let build_profile = inferred_xtask_build_profile();
    let target_cpu_native = rust_build
        .get("target_cpu_native")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let debug_assertions = rust_build
        .get("debug_assertions")
        .and_then(Value::as_bool)
        .unwrap_or(cfg!(debug_assertions));
    let current_exe = env::current_exe().context("resolving current xtask executable")?;
    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
    let python_path = PathBuf::from(&python);
    let cert_dir = Path::new("carbonengine/io/tests/python/carboniotests/test/certdata");

    let mut rows = Vec::<Value>::new();
    let mut failures = Vec::<Value>::new();

    if config.families.contains("scheduler") {
        for spec in scalability_scheduler_specs(config.tier) {
            match run_scalability_xtask_worker_samples(&current_exe, &spec, config.samples) {
                Ok(row) => rows.push(row),
                Err(error) => failures.push(json!({
                    "family": "scheduler",
                    "workload": spec.workload,
                    "error": error.to_string()
                })),
            }
        }
    }

    if config.families.contains("io") {
        for spec in scalability_io_specs(config.tier) {
            match run_scalability_io_samples(&python_path, cert_dir, &spec, config.samples) {
                Ok(row) => rows.push(row),
                Err(error) => failures.push(json!({
                    "family": "io",
                    "workload": spec.workload_name(),
                    "error": error.to_string()
                })),
            }
        }
    }

    if config.families.contains("data") {
        for spec in scalability_data_specs(config.tier) {
            match run_scalability_xtask_worker_samples(&current_exe, &spec, config.samples) {
                Ok(row) => rows.push(row),
                Err(error) => failures.push(json!({
                    "family": "data",
                    "workload": spec.workload,
                    "error": error.to_string()
                })),
            }
        }
    }

    let status = if failures.is_empty() { "pass" } else { "fail" };
    let evidence = json!({
        "schema": "carbon.evidence.scalability_matrix.v1",
        "gate": "bench-scalability",
        "component": "performance",
        "status": status,
        "report_ready": false,
        "tier": config.tier.as_str(),
        "families": config.families.iter().cloned().collect::<Vec<_>>(),
        "samples_per_row": config.samples,
        "build_profile": build_profile,
        "target_cpu_native": target_cpu_native,
        "debug_assertions": debug_assertions,
        "coverage": "local_scalability_pressure_matrix_for_scheduler_io_and_data_workloads_with_tail_latency_throughput_stability_cpu_and_rss",
        "comparability": "local_resource_pressure_evidence_not_a_legacy_speedup_claim",
        "claim_scope": "Local pressure matrix for scalability and resource-shape discussion. Rows are not Rust-vs-legacy speedup claims unless a future row explicitly records a comparable parity-checked baseline.",
        "command": format!(
            "cargo run -p xtask -- bench-scalability --tier {} --families {} --samples {}",
            config.tier.as_str(),
            config.families.iter().cloned().collect::<Vec<_>>().join(","),
            config.samples
        ),
        "recommended_blog_command": "RUSTFLAGS=\"-C target-cpu=native\" cargo run --release -p xtask -- bench-scalability --tier quick",
        "duration_ms": started.elapsed().as_millis() as u64,
        "host": {
            "os": env::consts::OS,
            "arch": env::consts::ARCH,
            "cpu_model": host_cpu_model().unwrap_or_else(|| String::from("unknown")),
            "logical_cpus": std::thread::available_parallelism().map(|value| value.get()).unwrap_or_default(),
            "ram_kb": host_mem_total_kb(),
            "rustc": command_stdout("rustc", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "cargo": command_stdout("cargo", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "rust_build": rust_build,
            "process_resource_measurement": if Path::new("/usr/bin/time").exists() { "external_time_v" } else { "wall_clock_only" },
        },
        "metrics_recorded": [
            "throughput operations/sec, events/sec, requests/sec, network bytes/sec, data bytes/sec, and rows/sec where applicable",
            "latency p50, p95, p99, p99.9, max, and p99/p50 tail ratio",
            "throughput stability across process-sample windows with p5/p50/p95 and coefficient of variation",
            "process CPU burn, CPU percent, peak RSS, and CPU seconds per GiB/op where enough data exists"
        ],
        "arrow_scope": {
            "status": "planned_phase_2",
            "reason": "The current matrix first establishes non-Arrow scheduler, IO, and data baselines. Arrow should be added only for a concrete row/record-batch pipeline so conversion cost, RSS, and rows/sec can be measured fairly."
        },
        "summary": scalability_summary(&rows),
        "rows": rows,
        "failures": failures,
        "covered_behaviors": [
            "scheduler runnable tasklet pressure using generated CoreScheduler semantic scenarios",
            "scheduler blocked receiver wake pressure using generated channel-pair scenarios",
            "loopback TCP and TLS payload/concurrency pressure with request latency and network MB/sec",
            "Rust data pipeline pressure for checksum/compression and catalog export/parse rows",
            "process resource metrics for every row when /usr/bin/time -v is available"
        ],
        "remaining_before_report_ready": [
            "add comparable legacy scheduler process pressure rows before claiming scheduler speedups",
            "add captured legacy Carbon IO rows before claiming Carbon IO speedups",
            "add Arrow rows only after a concrete record-batch data path is selected",
            "decide CI thresholds separately from this blog-oriented local evidence snapshot"
        ]
    });
    write_json(&config.output, &evidence)?;
    println!(
        "bench-scalability: {status} ({} rows, tier {}, samples {}); evidence {}",
        evidence
            .get("rows")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default(),
        config.tier.as_str(),
        config.samples,
        config.output.display()
    );

    if status == "pass" {
        Ok(())
    } else {
        bail!("bench-scalability failed; see {}", config.output.display())
    }
}

fn parse_bench_scalability_args(args: Vec<String>) -> Result<ScalabilityConfig> {
    let mut tier = ScalabilityTier::Quick;
    let mut families = ["scheduler", "io", "data"]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<_>>();
    let mut samples = None;
    let mut output = evidence_path("scalability-matrix.json");
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--tier" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--tier requires quick or full");
                };
                tier = ScalabilityTier::parse(value)?;
            }
            "--families" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--families requires a comma-separated list");
                };
                families = parse_scalability_families(value)?;
            }
            "--samples" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--samples requires a positive integer");
                };
                let parsed = value
                    .parse::<u64>()
                    .with_context(|| format!("parsing --samples value {value}"))?;
                if parsed == 0 {
                    bail!("--samples must be greater than zero");
                }
                samples = Some(parsed.min(50));
            }
            "--output" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output requires a path");
                };
                output = PathBuf::from(value);
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask bench-scalability [--tier quick|full] [--families scheduler,io,data] [--samples N] [--output path]"
                );
                return Ok(ScalabilityConfig {
                    tier,
                    families,
                    samples: samples.unwrap_or_else(|| default_scalability_samples(tier)),
                    output,
                });
            }
            other => bail!("unknown bench-scalability option: {other}"),
        }
        index += 1;
    }

    Ok(ScalabilityConfig {
        tier,
        families,
        samples: samples.unwrap_or_else(|| default_scalability_samples(tier)),
        output,
    })
}

fn default_scalability_samples(tier: ScalabilityTier) -> u64 {
    match tier {
        ScalabilityTier::Quick => 3,
        ScalabilityTier::Full => 7,
    }
}

fn parse_scalability_families(value: &str) -> Result<BTreeSet<String>> {
    let mut families = BTreeSet::new();
    for family in value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if !matches!(family, "scheduler" | "io" | "data") {
            bail!("unsupported bench-scalability family: {family}");
        }
        families.insert(family.to_string());
    }
    if families.is_empty() {
        bail!("--families must include at least one family");
    }
    Ok(families)
}

#[derive(Clone, Debug)]
struct ScalabilityWorkerSpec {
    workload: String,
    args: Vec<OsString>,
}

#[derive(Clone, Debug)]
struct ScalabilityIoSpec {
    kind: &'static str,
    payload_bytes: u64,
    concurrency: u64,
    requests_per_connection: u64,
}

impl ScalabilityIoSpec {
    fn workload_name(&self) -> String {
        format!(
            "{}_loopback_payload_{}_concurrency_{}",
            self.kind, self.payload_bytes, self.concurrency
        )
    }
}

fn scalability_scheduler_specs(tier: ScalabilityTier) -> Vec<ScalabilityWorkerSpec> {
    let cases = match tier {
        ScalabilityTier::Quick => vec![
            ("runnable-tasklets", 128_u64, 40_u64),
            ("runnable-tasklets", 1024, 10),
            ("channel-pairs", 32, 40),
            ("channel-pairs", 256, 8),
        ],
        ScalabilityTier::Full => vec![
            ("runnable-tasklets", 128_u64, 100_u64),
            ("runnable-tasklets", 1024, 40),
            ("runnable-tasklets", 8192, 6),
            ("channel-pairs", 32, 100),
            ("channel-pairs", 256, 30),
            ("channel-pairs", 2048, 4),
        ],
    };

    cases
        .into_iter()
        .map(|(kind, count, iterations)| ScalabilityWorkerSpec {
            workload: format!("scheduler_{kind}_{count}"),
            args: vec![
                OsString::from("bench-scalability-worker"),
                OsString::from("scheduler"),
                OsString::from(kind),
                OsString::from(count.to_string()),
                OsString::from(iterations.to_string()),
            ],
        })
        .collect()
}

fn scalability_data_specs(tier: ScalabilityTier) -> Vec<ScalabilityWorkerSpec> {
    let cases = match tier {
        ScalabilityTier::Quick => vec![
            ("md5-gzip", 1_048_576_u64, 12_u64),
            ("md5-gzip", 16_777_216, 3),
            ("catalog-roundtrip", 1_000, 10),
            ("catalog-roundtrip", 10_000, 2),
        ],
        ScalabilityTier::Full => vec![
            ("md5-gzip", 1_048_576_u64, 30_u64),
            ("md5-gzip", 16_777_216, 8),
            ("md5-gzip", 67_108_864, 3),
            ("catalog-roundtrip", 1_000, 30),
            ("catalog-roundtrip", 10_000, 8),
            ("catalog-roundtrip", 50_000, 2),
        ],
    };

    cases
        .into_iter()
        .map(
            |(kind, size_or_records, iterations)| ScalabilityWorkerSpec {
                workload: format!("data_{kind}_{size_or_records}"),
                args: vec![
                    OsString::from("bench-scalability-worker"),
                    OsString::from("data"),
                    OsString::from(kind),
                    OsString::from(size_or_records.to_string()),
                    OsString::from(iterations.to_string()),
                ],
            },
        )
        .collect()
}

fn scalability_io_specs(tier: ScalabilityTier) -> Vec<ScalabilityIoSpec> {
    match tier {
        ScalabilityTier::Quick => vec![
            ScalabilityIoSpec {
                kind: "socket",
                payload_bytes: 256,
                concurrency: 1,
                requests_per_connection: 80,
            },
            ScalabilityIoSpec {
                kind: "socket",
                payload_bytes: 16_384,
                concurrency: 8,
                requests_per_connection: 30,
            },
            ScalabilityIoSpec {
                kind: "ssl",
                payload_bytes: 256,
                concurrency: 1,
                requests_per_connection: 24,
            },
            ScalabilityIoSpec {
                kind: "ssl",
                payload_bytes: 4096,
                concurrency: 4,
                requests_per_connection: 12,
            },
        ],
        ScalabilityTier::Full => vec![
            ScalabilityIoSpec {
                kind: "socket",
                payload_bytes: 256,
                concurrency: 1,
                requests_per_connection: 200,
            },
            ScalabilityIoSpec {
                kind: "socket",
                payload_bytes: 4096,
                concurrency: 4,
                requests_per_connection: 120,
            },
            ScalabilityIoSpec {
                kind: "socket",
                payload_bytes: 65_536,
                concurrency: 16,
                requests_per_connection: 40,
            },
            ScalabilityIoSpec {
                kind: "ssl",
                payload_bytes: 256,
                concurrency: 1,
                requests_per_connection: 80,
            },
            ScalabilityIoSpec {
                kind: "ssl",
                payload_bytes: 4096,
                concurrency: 4,
                requests_per_connection: 40,
            },
            ScalabilityIoSpec {
                kind: "ssl",
                payload_bytes: 16_384,
                concurrency: 8,
                requests_per_connection: 20,
            },
        ],
    }
}

fn run_scalability_xtask_worker_samples(
    current_exe: &Path,
    spec: &ScalabilityWorkerSpec,
    samples: u64,
) -> Result<Value> {
    let mut run_values = Vec::new();
    let mut process_metrics = Vec::new();
    let mut run_samples = Vec::new();

    for sample_index in 0..samples {
        let measured = run_timed_process(current_exe, &spec.args, None)
            .with_context(|| format!("running scalability worker {}", spec.workload))?;
        let stdout = String::from_utf8_lossy(&measured.output.stdout);
        let stderr = String::from_utf8_lossy(&measured.output.stderr);
        if !measured.output.status.success() {
            bail!(
                "scalability worker {} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                spec.workload,
                measured.output.status.code(),
                stdout,
                stderr
            );
        }
        let row: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing scalability worker JSON: {stdout}"))?;
        run_samples.push(scalability_run_sample(
            sample_index,
            &row,
            &measured.metrics,
            &stderr,
        ));
        process_metrics.push(measured.metrics);
        run_values.push(row);
    }

    aggregate_scalability_run_values(run_values, process_metrics, run_samples)
        .with_context(|| format!("aggregating scalability worker {}", spec.workload))
}

fn run_scalability_io_samples(
    python: &Path,
    cert_dir: &Path,
    spec: &ScalabilityIoSpec,
    samples: u64,
) -> Result<Value> {
    let script = scalability_io_workload_script();
    let args = vec![
        OsString::from("-c"),
        OsString::from(script),
        OsString::from(spec.kind),
        OsString::from(spec.payload_bytes.to_string()),
        OsString::from(spec.concurrency.to_string()),
        OsString::from(spec.requests_per_connection.to_string()),
        cert_dir.as_os_str().to_os_string(),
    ];
    let mut run_values = Vec::new();
    let mut process_metrics = Vec::new();
    let mut run_samples = Vec::new();

    for sample_index in 0..samples {
        let measured = run_timed_process(python, &args, None)
            .with_context(|| format!("running scalability IO workload {}", spec.workload_name()))?;
        let stdout = String::from_utf8_lossy(&measured.output.stdout);
        let stderr = String::from_utf8_lossy(&measured.output.stderr);
        if !measured.output.status.success() {
            bail!(
                "scalability IO workload {} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                spec.workload_name(),
                measured.output.status.code(),
                stdout,
                stderr
            );
        }
        let row: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing scalability IO JSON: {stdout}"))?;
        run_samples.push(scalability_run_sample(
            sample_index,
            &row,
            &measured.metrics,
            &stderr,
        ));
        process_metrics.push(measured.metrics);
        run_values.push(row);
    }

    aggregate_scalability_run_values(run_values, process_metrics, run_samples).with_context(|| {
        format!(
            "aggregating scalability IO workload {}",
            spec.workload_name()
        )
    })
}

fn scalability_run_sample(
    sample_index: u64,
    row: &Value,
    metrics: &ProcessMetrics,
    stderr: &str,
) -> Value {
    json!({
        "sample_index": sample_index,
        "duration_us": row.get("duration_us").cloned().unwrap_or(Value::Null),
        "latency_us": row.get("latency_us_extended").cloned()
            .or_else(|| row.get("latency_us").cloned())
            .unwrap_or_else(|| json!({"count": 0})),
        "primary_throughput_metric": row.get("primary_throughput_metric").cloned().unwrap_or(Value::Null),
        "primary_throughput_per_sec": row
            .get("primary_throughput_metric")
            .and_then(Value::as_str)
            .and_then(|metric| row.get(metric))
            .cloned()
            .unwrap_or(Value::Null),
        "process_metrics": process_metrics_sample_json(std::slice::from_ref(metrics)),
        "stderr_tail": tail_lines(stderr, 12)
    })
}

fn aggregate_scalability_run_values(
    run_values: Vec<Value>,
    process_metrics: Vec<ProcessMetrics>,
    run_samples: Vec<Value>,
) -> Result<Value> {
    let Some(mut row) = run_values.first().cloned() else {
        bail!("no scalability samples were recorded");
    };
    let duration_samples = run_values
        .iter()
        .filter_map(|value| value.get("duration_us").and_then(Value::as_u64))
        .collect::<Vec<_>>();
    let latency_samples = run_values
        .iter()
        .flat_map(|value| {
            value
                .get("latency_samples_us")
                .and_then(Value::as_array)
                .map(|samples| samples.iter().filter_map(Value::as_u64).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let throughput_samples = collect_throughput_samples(&run_values);
    let primary_metric = row
        .get("primary_throughput_metric")
        .and_then(Value::as_str)
        .map(str::to_string);
    let total_operations = sum_u64_key(&run_values, "operation_count");
    let total_events = sum_u64_key(&run_values, "event_count");
    let total_requests = sum_u64_key(&run_values, "request_count");
    let total_network_bytes = sum_u64_key(&run_values, "network_bytes_transferred");
    let total_data_bytes = sum_u64_key(&run_values, "data_bytes_processed");
    let total_rows = sum_u64_key(&run_values, "row_count_processed");
    let total_duration_us = duration_samples.iter().sum::<u64>();

    if let Some(object) = row.as_object_mut() {
        object.insert(
            String::from("process_sample_count"),
            json!(process_metrics.len()),
        );
        object.insert(String::from("run_sample_count"), json!(run_values.len()));
        object.insert(
            String::from("duration_us"),
            json!(mean_u64_rounded(&duration_samples)),
        );
        object.insert(
            String::from("duration_us_stats"),
            sample_stats_us_extended(&duration_samples),
        );
        object.insert(
            String::from("latency_us_extended"),
            sample_stats_us_extended(&latency_samples),
        );
        object.insert(
            String::from("latency_samples_us"),
            json!(bounded_u64_samples(&latency_samples, 20_000)),
        );
        object.insert(
            String::from("latency_sample_count"),
            json!(latency_samples.len()),
        );
        object.insert(
            String::from("process_samples"),
            process_metrics_sample_json(&process_metrics),
        );
        object.insert(
            String::from("process_stats"),
            process_metrics_summary(&process_metrics),
        );
        object.insert(String::from("run_samples"), Value::Array(run_samples));
        object.insert(
            String::from("throughput_samples"),
            throughput_samples_json(&throughput_samples),
        );
        for (metric, samples) in &throughput_samples {
            object.insert(
                format!("{metric}_stats"),
                sample_stats_f64_extended(samples),
            );
        }
        if let Some(primary_metric) = primary_metric.as_deref() {
            object.insert(
                String::from("stability"),
                throughput_stability_summary(
                    throughput_samples
                        .get(primary_metric)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                ),
            );
        }
        object.insert(
            String::from("aggregate_totals"),
            json!({
                "duration_us": total_duration_us,
                "operation_count": total_operations,
                "event_count": total_events,
                "request_count": total_requests,
                "network_bytes_transferred": total_network_bytes,
                "data_bytes_processed": total_data_bytes,
                "row_count_processed": total_rows
            }),
        );
        object.insert(
            String::from("resource_efficiency"),
            scalability_resource_efficiency(
                &process_metrics,
                total_operations,
                total_network_bytes,
                total_data_bytes,
            ),
        );
    }

    Ok(row)
}

fn collect_throughput_samples(run_values: &[Value]) -> BTreeMap<String, Vec<f64>> {
    let mut samples = BTreeMap::<String, Vec<f64>>::new();
    for value in run_values {
        let Some(object) = value.as_object() else {
            continue;
        };
        for (key, value) in object {
            if key.starts_with("throughput_") && key.ends_with("_per_sec") {
                if let Some(number) = json_number(value) {
                    samples.entry(key.clone()).or_default().push(number);
                }
            }
        }
    }
    samples
}

fn throughput_samples_json(samples: &BTreeMap<String, Vec<f64>>) -> Value {
    Value::Object(
        samples
            .iter()
            .map(|(metric, values)| (metric.clone(), json!(values)))
            .collect(),
    )
}

fn sum_u64_key(values: &[Value], key: &str) -> u64 {
    values
        .iter()
        .filter_map(|value| value.get(key).and_then(Value::as_u64))
        .sum()
}

fn bounded_u64_samples(samples: &[u64], limit: usize) -> Vec<u64> {
    if samples.len() <= limit {
        samples.to_vec()
    } else {
        samples.iter().take(limit).copied().collect()
    }
}

fn scalability_resource_efficiency(
    process_metrics: &[ProcessMetrics],
    operations: u64,
    network_bytes: u64,
    data_bytes: u64,
) -> Value {
    let cpu_ms = sum_effective_cpu_burn_ms(process_metrics);
    json!({
        "cpu_burn_effective_ms_total": cpu_ms,
        "cpu_seconds_per_gib_network": cpu_seconds_per_gib(cpu_ms, network_bytes),
        "cpu_seconds_per_gib_data": cpu_seconds_per_gib(cpu_ms, data_bytes),
        "cpu_ms_per_million_operations": cpu_ms_per_million_operations(cpu_ms, operations)
    })
}

fn cpu_seconds_per_gib(cpu_ms: Option<f64>, bytes: u64) -> Value {
    let Some(cpu_ms) = cpu_ms else {
        return Value::Null;
    };
    if bytes == 0 {
        return Value::Null;
    }
    json!((cpu_ms / 1000.0) / (bytes as f64 / 1_073_741_824.0))
}

fn cpu_ms_per_million_operations(cpu_ms: Option<f64>, operations: u64) -> Value {
    let Some(cpu_ms) = cpu_ms else {
        return Value::Null;
    };
    if operations == 0 {
        return Value::Null;
    }
    json!(cpu_ms / (operations as f64 / 1_000_000.0))
}

fn throughput_stability_summary(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "window_count": 0,
            "basis": "process_sample_windows"
        });
    }
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    let stddev = variance.sqrt();
    let coefficient_of_variation = if mean == 0.0 {
        None
    } else {
        Some(stddev / mean)
    };
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let p5 = percentile_nearest_rank_f64(&sorted, 5);
    let p95 = percentile_nearest_rank_f64(&sorted, 95);
    json!({
        "basis": "process_sample_windows",
        "window_count": samples.len(),
        "mean": mean,
        "stddev": stddev,
        "coefficient_of_variation": coefficient_of_variation,
        "p5": p5,
        "p50": percentile_nearest_rank_f64(&sorted, 50),
        "p95": p95,
        "worst_window_drop_from_mean": if mean > 0.0 { Some((mean - p5) / mean) } else { None },
        "p95_over_p5": if p5 > 0.0 { Some(p95 / p5) } else { None }
    })
}

fn sample_stats_us_extended(samples: &[u64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0
        });
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().sum::<u64>();
    let p50 = percentile_nearest_rank(&sorted, 50);
    let p99 = percentile_nearest_rank(&sorted, 99);
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "mean": sum as f64 / sorted.len() as f64,
        "p50": p50,
        "p95": percentile_nearest_rank(&sorted, 95),
        "p99": p99,
        "p99_9": percentile_nearest_rank_per_mille(&sorted, 999),
        "max": sorted[sorted.len() - 1],
        "tail_ratio_p99_over_p50": if p50 > 0 { Some(p99 as f64 / p50 as f64) } else { None }
    })
}

fn sample_stats_f64_extended(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0
        });
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let sum = sorted.iter().sum::<f64>();
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "mean": sum / sorted.len() as f64,
        "p5": percentile_nearest_rank_f64(&sorted, 5),
        "p50": percentile_nearest_rank_f64(&sorted, 50),
        "p95": percentile_nearest_rank_f64(&sorted, 95),
        "p99": percentile_nearest_rank_f64(&sorted, 99),
        "max": sorted[sorted.len() - 1]
    })
}

fn percentile_nearest_rank_per_mille(sorted_samples: &[u64], per_mille: usize) -> u64 {
    let rank = (per_mille * sorted_samples.len()).div_ceil(1000);
    let index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    sorted_samples[index]
}

fn scalability_summary(rows: &[Value]) -> Value {
    let mut family_counts = BTreeMap::<String, u64>::new();
    let mut peak_network_bytes_per_sec = 0.0_f64;
    let mut peak_data_bytes_per_sec = 0.0_f64;
    let mut peak_operations_per_sec = 0.0_f64;
    let mut worst_latency_p99_us = 0.0_f64;
    let mut worst_latency_p999_us = 0.0_f64;
    let mut highest_peak_rss_kb = 0.0_f64;
    let mut highest_cpu_percent = 0.0_f64;
    let mut stable_rows = 0_u64;

    for row in rows {
        if let Some(family) = row.get("family").and_then(Value::as_str) {
            *family_counts.entry(family.to_string()).or_default() += 1;
        }
        peak_network_bytes_per_sec = peak_network_bytes_per_sec
            .max(number_at(row, &["throughput_network_bytes_per_sec"]).unwrap_or_default());
        peak_data_bytes_per_sec = peak_data_bytes_per_sec
            .max(number_at(row, &["throughput_data_bytes_per_sec"]).unwrap_or_default());
        peak_operations_per_sec = peak_operations_per_sec
            .max(number_at(row, &["throughput_operations_per_sec"]).unwrap_or_default());
        worst_latency_p99_us = worst_latency_p99_us
            .max(number_at(row, &["latency_us_extended", "p99"]).unwrap_or_default());
        worst_latency_p999_us = worst_latency_p999_us
            .max(number_at(row, &["latency_us_extended", "p99_9"]).unwrap_or_default());
        highest_peak_rss_kb = highest_peak_rss_kb
            .max(number_at(row, &["process_stats", "max_rss_kb", "p95"]).unwrap_or_default());
        highest_cpu_percent = highest_cpu_percent
            .max(number_at(row, &["process_stats", "cpu_percent", "p95"]).unwrap_or_default());
        if number_at(row, &["stability", "coefficient_of_variation"])
            .is_some_and(|value| value <= 0.10)
        {
            stable_rows += 1;
        }
    }

    json!({
        "row_count": rows.len(),
        "family_counts": family_counts,
        "peak_network_bytes_per_sec": peak_network_bytes_per_sec,
        "peak_data_bytes_per_sec": peak_data_bytes_per_sec,
        "peak_operations_per_sec": peak_operations_per_sec,
        "worst_latency_p99_us": worst_latency_p99_us,
        "worst_latency_p99_9_us": worst_latency_p999_us,
        "highest_peak_rss_kb_p95": highest_peak_rss_kb,
        "highest_cpu_percent_p95": highest_cpu_percent,
        "stable_rows_cv_le_10_percent": stable_rows
    })
}

fn bench_scalability_worker(args: Vec<String>) -> Result<()> {
    let Some(family) = args.first().map(String::as_str) else {
        bail!("bench-scalability-worker requires a family");
    };
    let row = match family {
        "scheduler" => bench_scalability_scheduler_worker(&args[1..])?,
        "data" => bench_scalability_data_worker(&args[1..])?,
        other => bail!("unsupported bench-scalability-worker family: {other}"),
    };
    println!("{}", serde_json::to_string(&row)?);
    Ok(())
}

fn bench_scalability_scheduler_worker(args: &[String]) -> Result<Value> {
    if args.len() != 3 {
        bail!("scheduler scalability worker requires <runnable-tasklets|channel-pairs> <count> <iterations>");
    }
    let kind = args[0].as_str();
    let count = parse_positive_u64(&args[1], "scheduler pressure count")?;
    let iterations = parse_positive_u64(&args[2], "scheduler pressure iterations")?;
    let scenario = match kind {
        "runnable-tasklets" => generated_runnable_tasklets_scenario(count)?,
        "channel-pairs" => generated_channel_pairs_scenario(count)?,
        other => bail!("unsupported scheduler scalability kind: {other}"),
    };
    let mut latency_samples = Vec::with_capacity(iterations as usize);
    let mut event_count = 0_u64;
    let operation_count = match kind {
        "runnable-tasklets" => count.saturating_mul(iterations),
        "channel-pairs" => count.saturating_mul(2).saturating_mul(iterations),
        _ => iterations,
    };
    let started = Instant::now();
    for _ in 0..iterations {
        let sample_started = Instant::now();
        let trace = run_scenario(&scenario)
            .map_err(|error| anyhow!("scheduler scalability scenario failed: {error}"))?;
        latency_samples.push(sample_started.elapsed().as_micros() as u64);
        event_count = event_count.saturating_add(trace.events.len() as u64);
        black_box(trace.events.len());
        black_box(&trace.final_state);
    }
    let duration_us = started.elapsed().as_micros() as u64;
    let pressure = if kind == "runnable-tasklets" {
        json!({
            "axis": "tasklet_count",
            "tasklet_count": count,
            "iterations_per_process": iterations
        })
    } else {
        json!({
            "axis": "channel_pair_count",
            "channel_pair_count": count,
            "tasklet_count": count.saturating_mul(2),
            "iterations_per_process": iterations
        })
    };

    Ok(json!({
        "family": "scheduler",
        "component": "scheduler",
        "workload": format!("scheduler_{kind}_{count}"),
        "implementation": "rust_scheduler_core_generated_scenario",
        "pressure": pressure,
        "duration_us": duration_us,
        "duration_ms": duration_ms_from_us(duration_us),
        "operation_count": operation_count,
        "event_count": event_count,
        "latency_samples_us": latency_samples,
        "latency_us_extended": sample_stats_us_extended(&latency_samples),
        "throughput_operations_per_sec": rate_per_second_us(operation_count, duration_us),
        "throughput_events_per_sec": rate_per_second_us(event_count, duration_us),
        "primary_throughput_metric": "throughput_operations_per_sec",
        "primary_latency_metric": "latency_us_extended",
        "parity_status": "partial_pass",
        "parity_gate": "scheduler-fixtures.json",
        "comparability": "rust_only_generated_pressure_not_legacy_comparable",
        "claim_eligibility": "resource_evidence_only_no_speedup_claim",
        "claim_scope": "Rust scheduler-core generated pressure row; no matched legacy scheduler process baseline."
    }))
}

fn generated_runnable_tasklets_scenario(count: u64) -> Result<Scenario> {
    let count = usize::try_from(count).context("scheduler tasklet count exceeds usize")?;
    let tasklets = (0..count)
        .map(|index| TaskletSpec {
            id: format!("t{index}"),
            initially_scheduled: true,
            initially_bound: true,
            body: vec![Operation::Append {
                target: String::from("completed"),
                value: json!(index),
            }],
        })
        .collect();
    Ok(Scenario {
        nested_tasklets: true,
        channel_callbacks: false,
        channels: Vec::new(),
        tasklets,
        entrypoint: Entrypoint::RunScheduler,
    })
}

fn generated_channel_pairs_scenario(pair_count: u64) -> Result<Scenario> {
    let pair_count = usize::try_from(pair_count).context("channel pair count exceeds usize")?;
    let mut channels = Vec::with_capacity(pair_count);
    let mut tasklets = Vec::with_capacity(pair_count.saturating_mul(2));

    for index in 0..pair_count {
        let channel = format!("c{index}");
        channels.push(ChannelSpec {
            id: channel.clone(),
            preference: 0,
        });
        tasklets.push(TaskletSpec {
            id: format!("receiver{index}"),
            initially_scheduled: true,
            initially_bound: true,
            body: vec![
                Operation::Receive {
                    channel: channel.clone(),
                    bind: Some(String::from("received")),
                },
                Operation::Append {
                    target: String::from("completed"),
                    value: json!(format!("receiver{index}")),
                },
            ],
        });
        tasklets.push(TaskletSpec {
            id: format!("sender{index}"),
            initially_scheduled: true,
            initially_bound: true,
            body: vec![
                Operation::Send {
                    channel,
                    value: json!(index),
                },
                Operation::Append {
                    target: String::from("completed"),
                    value: json!(format!("sender{index}")),
                },
            ],
        });
    }

    Ok(Scenario {
        nested_tasklets: true,
        channel_callbacks: false,
        channels,
        tasklets,
        entrypoint: Entrypoint::RunScheduler,
    })
}

fn bench_scalability_data_worker(args: &[String]) -> Result<Value> {
    if args.len() != 3 {
        bail!("data scalability worker requires <md5-gzip|catalog-roundtrip> <size-or-records> <iterations>");
    }
    let kind = args[0].as_str();
    let size_or_records = parse_positive_u64(&args[1], "data pressure size")?;
    let iterations = parse_positive_u64(&args[2], "data pressure iterations")?;
    match kind {
        "md5-gzip" => bench_scalability_md5_gzip_worker(size_or_records, iterations),
        "catalog-roundtrip" => bench_scalability_catalog_worker(size_or_records, iterations),
        other => bail!("unsupported data scalability kind: {other}"),
    }
}

fn bench_scalability_md5_gzip_worker(data_bytes: u64, iterations: u64) -> Result<Value> {
    let bytes = deterministic_bytes(data_bytes)?;
    let mut latency_samples = Vec::with_capacity(iterations as usize);
    let mut compressed_bytes = 0_u64;
    let mut checksum = String::new();
    let started = Instant::now();
    for _ in 0..iterations {
        let sample_started = Instant::now();
        checksum = md5_hex(&bytes);
        let compressed = gzip_compress(&bytes).context("compressing scalability payload")?;
        compressed_bytes = compressed_bytes.saturating_add(compressed.len() as u64);
        latency_samples.push(sample_started.elapsed().as_micros() as u64);
        black_box(&checksum);
        black_box(compressed_bytes);
    }
    let duration_us = started.elapsed().as_micros() as u64;
    let data_bytes_processed = data_bytes.saturating_mul(iterations).saturating_mul(2);

    Ok(json!({
        "family": "data",
        "component": "resources",
        "workload": format!("data_md5_gzip_{data_bytes}"),
        "implementation": "rust_resources_core_md5_gzip",
        "pressure": {
            "axis": "payload_bytes",
            "payload_bytes": data_bytes,
            "iterations_per_process": iterations
        },
        "duration_us": duration_us,
        "duration_ms": duration_ms_from_us(duration_us),
        "operation_count": iterations.saturating_mul(2),
        "data_bytes_processed": data_bytes_processed,
        "compressed_bytes_produced": compressed_bytes,
        "latency_samples_us": latency_samples,
        "latency_us_extended": sample_stats_us_extended(&latency_samples),
        "throughput_operations_per_sec": rate_per_second_us(iterations.saturating_mul(2), duration_us),
        "throughput_data_bytes_per_sec": rate_per_second_us(data_bytes_processed, duration_us),
        "primary_throughput_metric": "throughput_data_bytes_per_sec",
        "primary_latency_metric": "latency_us_extended",
        "checksum_sample": checksum,
        "parity_status": "partial_pass",
        "parity_gate": "rust-resources.json",
        "comparability": "rust_only_data_pressure_not_legacy_comparable",
        "claim_eligibility": "resource_evidence_only_no_speedup_claim",
        "claim_scope": "Rust checksum/compression pressure row; no matched legacy process baseline."
    }))
}

fn bench_scalability_catalog_worker(record_count: u64, iterations: u64) -> Result<Value> {
    let catalog = generated_resource_catalog(record_count)?;
    let mut latency_samples = Vec::with_capacity(iterations as usize);
    let mut data_bytes_processed = 0_u64;
    let mut row_count_processed = 0_u64;
    let started = Instant::now();
    for _ in 0..iterations {
        let sample_started = Instant::now();
        let document = export_legacy_csv_resource_group(&catalog);
        let parsed = parse_legacy_csv_resource_group(&document)
            .map_err(|error| anyhow!("parsing generated resource catalog failed: {error}"))?;
        row_count_processed = row_count_processed
            .saturating_add(parsed.resources.len() as u64)
            .saturating_add(record_count);
        data_bytes_processed =
            data_bytes_processed.saturating_add((document.len() as u64).saturating_mul(2));
        latency_samples.push(sample_started.elapsed().as_micros() as u64);
        black_box(parsed.resources.len());
        black_box(document.len());
    }
    let duration_us = started.elapsed().as_micros() as u64;

    Ok(json!({
        "family": "data",
        "component": "resources",
        "workload": format!("data_catalog_roundtrip_{record_count}"),
        "implementation": "rust_resources_core_catalog_csv_export_parse",
        "pressure": {
            "axis": "record_count",
            "record_count": record_count,
            "iterations_per_process": iterations
        },
        "duration_us": duration_us,
        "duration_ms": duration_ms_from_us(duration_us),
        "operation_count": iterations.saturating_mul(2),
        "row_count_processed": row_count_processed,
        "data_bytes_processed": data_bytes_processed,
        "latency_samples_us": latency_samples,
        "latency_us_extended": sample_stats_us_extended(&latency_samples),
        "throughput_operations_per_sec": rate_per_second_us(iterations.saturating_mul(2), duration_us),
        "throughput_rows_per_sec": rate_per_second_us(row_count_processed, duration_us),
        "throughput_data_bytes_per_sec": rate_per_second_us(data_bytes_processed, duration_us),
        "primary_throughput_metric": "throughput_rows_per_sec",
        "primary_latency_metric": "latency_us_extended",
        "parity_status": "partial_pass",
        "parity_gate": "rust-resources.json",
        "comparability": "rust_only_data_pressure_not_legacy_comparable",
        "claim_eligibility": "resource_evidence_only_no_speedup_claim",
        "claim_scope": "Rust catalog export/parse pressure row; no matched legacy process baseline."
    }))
}

fn deterministic_bytes(size: u64) -> Result<Vec<u8>> {
    let size = usize::try_from(size).context("deterministic payload size exceeds usize")?;
    let mut bytes = Vec::with_capacity(size);
    for index in 0..size {
        bytes.push(((index.wrapping_mul(31).wrapping_add(17)) & 0xff) as u8);
    }
    Ok(bytes)
}

fn generated_resource_catalog(record_count: u64) -> Result<ResourceCatalog> {
    let record_count = usize::try_from(record_count).context("record count exceeds usize")?;
    let mut resources = Vec::with_capacity(record_count);
    for index in 0..record_count {
        resources.push(ResourceRecord {
            path: format!("generated/asset_{index:08}.bin"),
            location: format!("chunks/{:02}/asset_{index:08}.bin", index % 97),
            size_bytes: 1024 + (index as u64 % 8192),
            compressed_size_bytes: Some(512 + (index as u64 % 4096)),
            checksum: Some(format!("{:032x}", index as u128 * 1_000_003 + 17)),
            binary_operation: None,
            prefix: None,
        });
    }
    Ok(ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes: Some(
            resources
                .iter()
                .filter_map(|record| record.compressed_size_bytes)
                .sum(),
        ),
        total_uncompressed_size_bytes: resources.iter().map(|record| record.size_bytes).sum(),
        resources,
    })
}

fn parse_positive_u64(value: &str, label: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("parsing {label}: {value}"))?;
    if parsed == 0 {
        bail!("{label} must be greater than zero");
    }
    Ok(parsed)
}

fn scalability_io_workload_script() -> &'static str {
    r#"
import json
import os
import socket
import ssl
import sys
import threading
import time

kind = sys.argv[1]
payload_bytes = int(sys.argv[2])
concurrency = int(sys.argv[3])
requests_per_connection = int(sys.argv[4])
cert_dir = sys.argv[5]
host = "127.0.0.1"
payload = bytes((i * 31 + 17) & 0xff for i in range(payload_bytes))

def percentile(sorted_values, percentile_value):
    if not sorted_values:
        return 0
    rank = (percentile_value * len(sorted_values) + 99) // 100
    index = max(0, min(len(sorted_values) - 1, rank - 1))
    return sorted_values[index]

def percentile_per_mille(sorted_values, per_mille):
    if not sorted_values:
        return 0
    rank = (per_mille * len(sorted_values) + 999) // 1000
    index = max(0, min(len(sorted_values) - 1, rank - 1))
    return sorted_values[index]

def stats(values):
    sorted_values = sorted(values)
    p50 = percentile(sorted_values, 50)
    p99 = percentile(sorted_values, 99)
    return {
        "count": len(sorted_values),
        "min": sorted_values[0] if sorted_values else 0,
        "mean": (sum(sorted_values) / len(sorted_values)) if sorted_values else 0,
        "p50": p50,
        "p95": percentile(sorted_values, 95),
        "p99": p99,
        "p99_9": percentile_per_mille(sorted_values, 999),
        "max": sorted_values[-1] if sorted_values else 0,
        "tail_ratio_p99_over_p50": (p99 / p50) if p50 else None,
    }

def read_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("socket closed before full payload was received")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)

def make_server_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.set_ciphers("@SECLEVEL=1:ALL")
    except ssl.SSLError:
        pass
    context.load_cert_chain(os.path.join(cert_dir, "keycert.pem"))
    return context

def make_client_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        context.set_ciphers("@SECLEVEL=1:ALL")
    except ssl.SSLError:
        pass
    return context

def handle_server_connection(conn, use_ssl, server_context, errors):
    try:
        conn.settimeout(15.0)
        if use_ssl:
            conn = server_context.wrap_socket(conn, server_side=True)
        with conn:
            for _ in range(requests_per_connection):
                data = read_exact(conn, len(payload))
                if data != payload:
                    raise RuntimeError("server payload mismatch")
                conn.sendall(data)
    except BaseException as exc:
        errors.append(repr(exc))

def run_server(listener, use_ssl, errors):
    server_context = make_server_context() if use_ssl else None
    workers = []
    listener.settimeout(15.0)
    try:
        with listener:
            for _ in range(concurrency):
                conn, _addr = listener.accept()
                worker = threading.Thread(
                    target=handle_server_connection,
                    args=(conn, use_ssl, server_context, errors),
                )
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join(timeout=20.0)
                if worker.is_alive():
                    errors.append("server worker did not finish")
    except BaseException as exc:
        errors.append(repr(exc))

def client_connection(port, use_ssl, latencies, errors):
    client_context = make_client_context() if use_ssl else None
    try:
        raw = socket.create_connection((host, port), timeout=15.0)
        if use_ssl:
            conn = client_context.wrap_socket(raw, server_hostname="localhost")
        else:
            conn = raw
        with conn:
            for _ in range(requests_per_connection):
                started = time.perf_counter_ns()
                conn.sendall(payload)
                data = read_exact(conn, len(payload))
                if data != payload:
                    raise RuntimeError("client payload mismatch")
                latencies.append((time.perf_counter_ns() - started) // 1000)
    except BaseException as exc:
        errors.append(repr(exc))

def run_workload():
    use_ssl = kind == "ssl"
    if kind not in ("socket", "ssl"):
        raise RuntimeError(f"unsupported kind: {kind}")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, 0))
    listener.listen(concurrency)
    port = listener.getsockname()[1]
    errors = []
    latencies = []
    server = threading.Thread(target=run_server, args=(listener, use_ssl, errors))
    server.start()
    clients = []
    started = time.perf_counter_ns()
    for _ in range(concurrency):
        client = threading.Thread(target=client_connection, args=(port, use_ssl, latencies, errors))
        client.start()
        clients.append(client)
    for client in clients:
        client.join(timeout=20.0)
        if client.is_alive():
            errors.append("client worker did not finish")
    server.join(timeout=20.0)
    if server.is_alive():
        errors.append("server did not finish")
    if errors:
        raise RuntimeError("; ".join(errors))
    duration_us = (time.perf_counter_ns() - started) // 1000
    request_count = concurrency * requests_per_connection
    network_bytes = request_count * len(payload) * 2
    result = {
        "family": "io",
        "component": "io",
        "workload": f"{kind}_loopback_payload_{payload_bytes}_concurrency_{concurrency}",
        "implementation": "python_stdlib_loopback",
        "kind": kind,
        "pressure": {
            "axis": "payload_bytes_x_concurrency",
            "payload_bytes": payload_bytes,
            "concurrency": concurrency,
            "requests_per_connection": requests_per_connection,
        },
        "duration_us": duration_us,
        "duration_ms": (duration_us + 999) // 1000,
        "request_count": request_count,
        "operation_count": request_count,
        "network_bytes_transferred": network_bytes,
        "latency_samples_us": latencies,
        "latency_us_extended": stats(latencies),
        "throughput_requests_per_sec": int(request_count * 1000000 / duration_us) if duration_us else 0,
        "throughput_network_bytes_per_sec": int(network_bytes * 1000000 / duration_us) if duration_us else 0,
        "primary_throughput_metric": "throughput_network_bytes_per_sec",
        "primary_latency_metric": "latency_us_extended",
        "parity_status": "not_legacy_comparable",
        "comparability": "python_stdlib_loopback_capacity_not_legacy_carbon_io",
        "claim_eligibility": "resource_evidence_only_no_speedup_claim",
        "claim_scope": "Local loopback socket/TLS pressure row. This is network capacity evidence, not a legacy Carbon IO speedup claim.",
    }
    print(json.dumps(result, sort_keys=True))

run_workload()
"#
}

fn bench_scheduler_core(args: Vec<String>) -> Result<()> {
    let fixture_path = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures/scheduler/run_order.json"));
    let iterations = args
        .get(1)
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("parsing scheduler benchmark iterations: {value}"))
        })
        .transpose()?
        .unwrap_or(5_000);
    if iterations == 0 {
        bail!("bench-scheduler-core iterations must be greater than zero");
    }

    let fixture = carbon_scheduler_trace::load_fixture(&fixture_path)?;
    let mut samples_us = Vec::with_capacity(iterations as usize);
    let mut events = 0_u64;
    let started = Instant::now();
    for _ in 0..iterations {
        let sample_started = Instant::now();
        let trace = run_scenario(&fixture.scenario)
            .map_err(|error| anyhow!("scheduler benchmark scenario failed: {error}"))?;
        samples_us.push(sample_started.elapsed().as_micros() as u64);
        events += trace.events.len() as u64;
        black_box(trace.events.len());
    }
    let duration_us = started.elapsed().as_micros() as u64;
    let rust_build = rust_build_metadata();

    println!(
        "{}",
        serde_json::to_string(&json!({
            "component": "scheduler",
            "workload": "run_order_fixture_rust_core_process",
            "fixture": fixture_path.display().to_string(),
            "fixture_name": fixture.name,
            "implementation": "rust_xtask_process",
            "iterations": iterations,
            "duration_us": duration_us,
            "duration_ms": duration_ms_from_us(duration_us),
            "sample_stats_us": sample_stats_us(&samples_us),
            "events": events,
            "throughput_events_per_sec": rate_per_second_us(events, duration_us),
            "build_profile": inferred_xtask_build_profile(),
            "target_cpu_native": rust_build
                .get("target_cpu_native")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            "debug_assertions": cfg!(debug_assertions),
            "parity_status": "partial_pass",
            "parity_gate": "scheduler-fixtures.json",
            "comparability": "rust_scheduler_process_not_legacy_comparable",
            "claim": "scheduler_resource_efficiency_evidence_only_no_speedup_claim"
        }))?
    );
    Ok(())
}

fn prepare_scheduler_python_package(build_profile: &str) -> Result<PathBuf> {
    let package_dir = Path::new("target/carbon/python");
    fs::remove_dir_all(package_dir).ok();
    fs::create_dir_all(package_dir.join("scheduler"))
        .with_context(|| format!("creating {}", package_dir.join("scheduler").display()))?;

    let shared_library = scheduler_python_shared_library_path(build_profile);
    for module_name in required_scheduler_extension_module_names() {
        fs::copy(
            &shared_library,
            package_dir.join(format!("{module_name}.so")),
        )
        .with_context(|| {
            format!(
                "copying {} to {}",
                shared_library.display(),
                package_dir.join(format!("{module_name}.so")).display()
            )
        })?;
    }
    fs::copy(
        "carbonengine/scheduler/python/scheduler/__init__.py",
        package_dir.join("scheduler/__init__.py"),
    )
    .with_context(|| {
        format!(
            "copying legacy scheduler package into {}",
            package_dir.display()
        )
    })?;

    Ok(package_dir.to_path_buf())
}

fn scheduler_python_shared_library_path(build_profile: &str) -> PathBuf {
    Path::new("target").join(build_profile).join(format!(
        "{}carbon_scheduler_python{}",
        env::consts::DLL_PREFIX,
        env::consts::DLL_SUFFIX
    ))
}

fn required_scheduler_extension_module_names() -> [&'static str; 4] {
    [
        "_scheduler",
        "_scheduler_debug",
        "_scheduler_trinitydev",
        "_scheduler_internal",
    ]
}

struct SchedulerCapiBinarySmoke {
    source_path: PathBuf,
    binary_path: PathBuf,
    compile_command: String,
    run_command: String,
    compile_output: Output,
    run_output: Option<Output>,
}

impl SchedulerCapiBinarySmoke {
    fn success(&self) -> bool {
        self.compile_output.status.success()
            && self
                .run_output
                .as_ref()
                .is_some_and(|output| output.status.success())
    }

    fn to_json(&self) -> Value {
        let compile_stdout = String::from_utf8_lossy(&self.compile_output.stdout);
        let compile_stderr = String::from_utf8_lossy(&self.compile_output.stderr);
        let run_stdout = self
            .run_output
            .as_ref()
            .map(|output| String::from_utf8_lossy(&output.stdout));
        let run_stderr = self
            .run_output
            .as_ref()
            .map(|output| String::from_utf8_lossy(&output.stderr));
        json!({
            "source_path": self.source_path.display().to_string(),
            "binary_path": self.binary_path.display().to_string(),
            "compile_command": self.compile_command,
            "run_command": self.run_command,
            "compile_status": self.compile_output.status.code(),
            "run_status": self.run_output.as_ref().and_then(|output| output.status.code()),
            "status": if self.success() { "pass" } else { "fail" },
            "compile_stdout_tail": tail_lines(&compile_stdout, 12),
            "compile_stderr_tail": tail_lines(&compile_stderr, 12),
            "run_stdout_tail": run_stdout.as_deref().map(|text| tail_lines(text, 12)),
            "run_stderr_tail": run_stderr.as_deref().map(|text| tail_lines(text, 12))
        })
    }
}

struct SchedulerPythonWheelSmoke {
    wheel_dir: PathBuf,
    wheel_path: Option<PathBuf>,
    venv_dir: PathBuf,
    venv_python: PathBuf,
    build_command: String,
    venv_command: String,
    install_command: String,
    smoke_command: String,
    build_output: Output,
    venv_output: Output,
    install_output: Output,
    smoke_output: Output,
}

impl SchedulerPythonWheelSmoke {
    fn success(&self) -> bool {
        self.build_output.status.success()
            && self.venv_output.status.success()
            && self.install_output.status.success()
            && self.smoke_output.status.success()
    }

    fn to_json(&self) -> Value {
        let build_stdout = String::from_utf8_lossy(&self.build_output.stdout);
        let build_stderr = String::from_utf8_lossy(&self.build_output.stderr);
        let venv_stdout = String::from_utf8_lossy(&self.venv_output.stdout);
        let venv_stderr = String::from_utf8_lossy(&self.venv_output.stderr);
        let install_stdout = String::from_utf8_lossy(&self.install_output.stdout);
        let install_stderr = String::from_utf8_lossy(&self.install_output.stderr);
        let smoke_stdout = String::from_utf8_lossy(&self.smoke_output.stdout);
        let smoke_stderr = String::from_utf8_lossy(&self.smoke_output.stderr);
        json!({
            "status": if self.success() { "pass" } else { "fail" },
            "wheel_dir": self.wheel_dir.display().to_string(),
            "wheel_path": self.wheel_path.as_ref().map(|path| path.display().to_string()),
            "venv_dir": self.venv_dir.display().to_string(),
            "venv_python": self.venv_python.display().to_string(),
            "build_command": self.build_command,
            "venv_command": self.venv_command,
            "install_command": self.install_command,
            "smoke_command": self.smoke_command,
            "build_status": self.build_output.status.code(),
            "venv_status": self.venv_output.status.code(),
            "install_status": self.install_output.status.code(),
            "smoke_status": self.smoke_output.status.code(),
            "build_stdout_tail": tail_lines(&build_stdout, 12),
            "build_stderr_tail": tail_lines(&build_stderr, 12),
            "venv_stdout_tail": tail_lines(&venv_stdout, 12),
            "venv_stderr_tail": tail_lines(&venv_stderr, 12),
            "install_stdout_tail": tail_lines(&install_stdout, 12),
            "install_stderr_tail": tail_lines(&install_stderr, 12),
            "smoke_stdout_tail": tail_lines(&smoke_stdout, 12),
            "smoke_stderr_tail": tail_lines(&smoke_stderr, 12)
        })
    }
}

fn run_scheduler_capi_binary_smoke(package_dir: &Path) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_capi_binary_smoke_from_source(
        package_dir,
        "target/carbon/scheduler-capi-smoke",
        "scheduler_capi_smoke.cpp",
        "scheduler_capi_smoke",
        scheduler_capi_binary_smoke_source(),
        "Scheduler.h C API binary smoke",
    )
}

fn run_io_scheduler_capi_semantic_smoke(package_dir: &Path) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_capi_binary_smoke_from_source(
        package_dir,
        "target/carbon/io-capi-smoke",
        "io_scheduler_capi_semantic_smoke.cpp",
        "io_scheduler_capi_semantic_smoke",
        io_scheduler_capi_semantic_smoke_source(),
        "IO-facing Scheduler.h C API semantic smoke",
    )
}

fn run_scheduler_legacy_capi_tasklet_source_slice(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "tasklet",
        "Tasklet.cpp",
        "legacy capiTest Tasklet.cpp source slice",
        false,
    )
}

fn run_scheduler_legacy_capi_tasklet_in_process_probe(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "tasklet-in-process",
        "Tasklet.cpp",
        "legacy capiTest Tasklet.cpp in-process source-slice probe",
        true,
    )
}

fn run_scheduler_legacy_capi_channel_source_slice(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "channel",
        "Channel.cpp",
        "legacy capiTest Channel.cpp source slice",
        false,
    )
}

fn run_scheduler_legacy_capi_channel_in_process_probe(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "channel-in-process",
        "Channel.cpp",
        "legacy capiTest Channel.cpp in-process source-slice probe",
        true,
    )
}

fn run_scheduler_legacy_capi_scheduler_source_slice(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "scheduler",
        "Scheduler.cpp",
        "legacy capiTest Scheduler.cpp source slice",
        false,
    )
}

fn run_scheduler_legacy_capi_scheduler_in_process_probe(
    package_dir: &Path,
) -> Result<SchedulerCapiBinarySmoke> {
    run_scheduler_legacy_capi_source_slice(
        package_dir,
        "scheduler-in-process",
        "Scheduler.cpp",
        "legacy capiTest Scheduler.cpp in-process source-slice probe",
        true,
    )
}

fn run_scheduler_python_wheel_install_smoke() -> Result<SchedulerPythonWheelSmoke> {
    let root = env::current_dir().context("resolving current directory")?;
    let wheel_dir = root.join("target/carbon/scheduler-python-wheel");
    let venv_dir = root.join("target/carbon/scheduler-python-wheel-venv");
    fs::remove_dir_all(&wheel_dir).ok();
    fs::remove_dir_all(&venv_dir).ok();
    fs::create_dir_all(&wheel_dir).with_context(|| format!("creating {}", wheel_dir.display()))?;

    let python = env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python3"));
    let python_path = PathBuf::from(&python);

    let build_args = vec![
        OsString::from("-m"),
        OsString::from("maturin"),
        OsString::from("build"),
        OsString::from("--manifest-path"),
        OsString::from("crates/carbon-scheduler-python/Cargo.toml"),
        OsString::from("--out"),
        wheel_dir.clone().into_os_string(),
        OsString::from("--compatibility"),
        OsString::from("linux"),
        OsString::from("--auditwheel"),
        OsString::from("skip"),
    ];
    let build_command = command_line(&python_path, &build_args);
    let build_output = Command::new(&python)
        .args(&build_args)
        .output()
        .context("building scheduler Python wheel with maturin")?;

    let wheel_path = if build_output.status.success() {
        newest_wheel_in_dir(&wheel_dir)?
    } else {
        None
    };

    let venv_args = vec![
        OsString::from("-m"),
        OsString::from("venv"),
        venv_dir.clone().into_os_string(),
    ];
    let venv_command = command_line(&python_path, &venv_args);
    let venv_output = if build_output.status.success() {
        Command::new(&python)
            .args(&venv_args)
            .output()
            .context("creating scheduler Python wheel smoke virtualenv")?
    } else {
        skipped_output("wheel build failed; virtualenv creation skipped")?
    };

    let venv_python = venv_python_path(&venv_dir);
    let install_args = wheel_path
        .as_ref()
        .map(|wheel_path| {
            vec![
                OsString::from("-m"),
                OsString::from("pip"),
                OsString::from("install"),
                OsString::from("--no-deps"),
                wheel_path.clone().into_os_string(),
            ]
        })
        .unwrap_or_else(|| {
            vec![
                OsString::from("-c"),
                OsString::from("raise SystemExit('wheel build failed')"),
            ]
        });
    let install_command = command_line(&venv_python, &install_args);
    let install_output = if venv_output.status.success() && wheel_path.is_some() {
        Command::new(&venv_python)
            .args(&install_args)
            .env_remove("PYTHONPATH")
            .output()
            .context("installing scheduler Python wheel into virtualenv")?
    } else {
        skipped_output("virtualenv creation or wheel build failed; install skipped")?
    };

    let smoke_args = vec![
        OsString::from("-c"),
        OsString::from(scheduler_python_installed_wheel_smoke_script()),
    ];
    let smoke_command = command_line(&venv_python, &smoke_args);
    let smoke_output = if install_output.status.success() {
        Command::new(&venv_python)
            .args(&smoke_args)
            .env_remove("PYTHONPATH")
            .env("BUILDFLAVOR", "release")
            .output()
            .context("running scheduler Python installed wheel smoke")?
    } else {
        skipped_output("wheel install failed; installed wheel smoke skipped")?
    };

    Ok(SchedulerPythonWheelSmoke {
        wheel_dir,
        wheel_path,
        venv_dir,
        venv_python,
        build_command,
        venv_command,
        install_command,
        smoke_command,
        build_output,
        venv_output,
        install_output,
        smoke_output,
    })
}

fn newest_wheel_in_dir(dir: &Path) -> Result<Option<PathBuf>> {
    let mut wheels = fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(OsStr::to_str) == Some("whl"))
        .collect::<Vec<_>>();
    wheels.sort();
    Ok(wheels.pop())
}

fn venv_python_path(venv_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        venv_dir.join("Scripts/python.exe")
    } else {
        venv_dir.join("bin/python")
    }
}

fn skipped_output(message: &str) -> Result<Output> {
    let shell = if cfg!(windows) { "cmd" } else { "sh" };
    let args = if cfg!(windows) {
        vec!["/C", "echo", message]
    } else {
        vec!["-c", "printf '%s\\n' \"$1\"", "sh", message]
    };
    Command::new(shell)
        .args(args)
        .output()
        .with_context(|| format!("creating skipped command output: {message}"))
}

fn scheduler_python_installed_wheel_smoke_script() -> &'static str {
    r#"
import ctypes
import sys

assert not any(path.endswith("target/carbon/python") for path in sys.path), sys.path

import _scheduler
import scheduler

assert _scheduler.bridge_status() == "pyo3_smoke"
assert scheduler.bridge_status() == "pyo3_smoke"
assert scheduler.__file__ and "site-packages" in scheduler.__file__, scheduler.__file__
assert hasattr(scheduler, "QueueChannel")
assert hasattr(scheduler, "_C_API")

py_capsule_is_valid = ctypes.pythonapi.PyCapsule_IsValid
py_capsule_is_valid.argtypes = [ctypes.py_object, ctypes.c_char_p]
py_capsule_is_valid.restype = ctypes.c_int
assert py_capsule_is_valid(scheduler._C_API, b"scheduler._C_API") == 1

queue = scheduler.QueueChannel()
assert isinstance(queue, scheduler.channel)
assert queue.preference == 1
queue.send("installed-wheel")
assert len(queue) == 1
assert queue.balance == 1
assert queue.receive() == "installed-wheel"
queue.send_exception(ValueError, "queued")
try:
    queue.receive()
except ValueError:
    pass
else:
    raise AssertionError("QueueChannel did not raise queued ValueError")

assert scheduler.getcurrent().block_trap is False
with scheduler.block_trap():
    assert scheduler.getcurrent().block_trap is True
assert scheduler.getcurrent().block_trap is False

print("scheduler installed wheel smoke ok", scheduler.__file__)
"#
}

fn run_scheduler_legacy_capi_source_slice(
    package_dir: &Path,
    slice_name: &str,
    legacy_source_file: &str,
    context_name: &str,
    in_process: bool,
) -> Result<SchedulerCapiBinarySmoke> {
    let root = env::current_dir().context("resolving current directory")?;
    let package_dir = root.join(package_dir);
    let smoke_dir = root.join(format!("target/carbon/scheduler-capi-legacy-{slice_name}"));
    fs::create_dir_all(smoke_dir.join("gtest"))
        .with_context(|| format!("creating {}", smoke_dir.join("gtest").display()))?;
    fs::write(
        smoke_dir.join("gtest/gtest.h"),
        legacy_capi_gtest_shim_source(),
    )
    .with_context(|| format!("writing {}", smoke_dir.join("gtest/gtest.h").display()))?;
    fs::write(
        smoke_dir.join("StringConversions.h"),
        legacy_capi_string_conversions_shim_source(),
    )
    .with_context(|| {
        format!(
            "writing {}",
            smoke_dir.join("StringConversions.h").display()
        )
    })?;

    let interpreter_source =
        root.join("carbonengine/scheduler/tests/capiTest/InterpreterWithSchedulerModule.cpp");
    let legacy_source = root
        .join("carbonengine/scheduler/tests/capiTest")
        .join(legacy_source_file);
    let source_path = smoke_dir.join(format!("legacy_capi_{slice_name}_source_slice.cpp"));
    let binary_path = smoke_dir.join(format!(
        "legacy_capi_{slice_name}_source_slice{}",
        env::consts::EXE_SUFFIX
    ));
    fs::write(
        &source_path,
        legacy_capi_source_slice_source(&interpreter_source, &legacy_source),
    )
    .with_context(|| format!("writing {}", source_path.display()))?;

    let cxx = env::var_os("CXX").unwrap_or_else(|| OsString::from("c++"));
    let cxx_path = PathBuf::from(&cxx);
    let mut compile_args = vec![
        OsString::from("-std=c++17"),
        OsString::from("-Wall"),
        OsString::from("-Wextra"),
        OsString::from("-Icarbonengine/scheduler/include"),
        OsString::from(format!("-I{}", smoke_dir.display())),
    ];
    compile_args.extend(python_config_flags(&["--includes"])?);
    compile_args.push(source_path.clone().into_os_string());
    compile_args.push(OsString::from("-o"));
    compile_args.push(binary_path.clone().into_os_string());
    compile_args.extend(
        python_config_flags(&["--ldflags", "--embed"])
            .or_else(|_| python_config_flags(&["--ldflags"]))?,
    );

    let compile_command = command_line(&cxx_path, &compile_args);
    let compile_output = Command::new(&cxx)
        .args(&compile_args)
        .output()
        .with_context(|| format!("compiling {context_name}"))?;

    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
    let stdlib_path = command_stdout(
        &python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_paths().get('stdlib') or '')",
        ],
    )
    .unwrap_or_default();
    let greenlet_module_path = command_stdout(
        &python,
        &[
            "-c",
            "import greenlet, pathlib; print(pathlib.Path(greenlet.__file__).resolve().parent.parent)",
        ],
    )
    .unwrap_or_else(|_| package_dir.display().to_string());
    let greenlet_cextension_path = command_stdout(
        &python,
        &[
            "-c",
            "import greenlet, pathlib; print(pathlib.Path(greenlet.__file__).resolve().parent)",
        ],
    )
    .unwrap_or_else(|_| package_dir.display().to_string());
    let python_libdir = command_stdout(
        &python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ],
    )
    .ok();

    let python_path = env::join_paths([package_dir.as_path()])
        .with_context(|| format!("joining {context_name} PYTHONPATH"))?;
    let run_args: Vec<OsString> = Vec::new();
    let run_command = if in_process {
        format!(
            "CARBON_CAPI_GTEST_IN_PROCESS=1 {}",
            command_line(&binary_path, &run_args)
        )
    } else {
        command_line(&binary_path, &run_args)
    };
    let run_output = if compile_output.status.success() {
        let mut command = Command::new(&binary_path);
        command
            .env("PYTHONPATH", python_path)
            .env("BUILDFLAVOR", "debug")
            .env("SCHEDULER_CEXTENSION_MODULE_PATH", &package_dir)
            .env("SCHEDULER_PACKAGE_PATH", &package_dir)
            .env("STDLIB_PATH", stdlib_path)
            .env("GREENLET_CEXTENSION_MODULE_PATH", greenlet_cextension_path)
            .env("GREENLET_MODULE_PATH", greenlet_module_path);
        if in_process {
            command.env("CARBON_CAPI_GTEST_IN_PROCESS", "1");
        }
        if let Some(python_libdir) = python_libdir.filter(|value| !value.is_empty()) {
            let ld_library_path = match env::var("LD_LIBRARY_PATH") {
                Ok(existing) if !existing.is_empty() => format!("{python_libdir}:{existing}"),
                _ => python_libdir,
            };
            command.env("LD_LIBRARY_PATH", ld_library_path);
        }
        Some(
            command
                .output()
                .with_context(|| format!("running {context_name}"))?,
        )
    } else {
        None
    };

    Ok(SchedulerCapiBinarySmoke {
        source_path,
        binary_path,
        compile_command,
        run_command,
        compile_output,
        run_output,
    })
}

fn run_scheduler_capi_binary_smoke_from_source(
    package_dir: &Path,
    smoke_dir: &str,
    source_file: &str,
    binary_stem: &str,
    source: &str,
    context_name: &str,
) -> Result<SchedulerCapiBinarySmoke> {
    let root = env::current_dir().context("resolving current directory")?;
    let package_dir = root.join(package_dir);
    let smoke_dir = root.join(smoke_dir);
    fs::create_dir_all(&smoke_dir).with_context(|| format!("creating {}", smoke_dir.display()))?;
    let source_path = smoke_dir.join(source_file);
    let binary_path = smoke_dir.join(format!("{binary_stem}{}", env::consts::EXE_SUFFIX));
    fs::write(&source_path, source)
        .with_context(|| format!("writing {}", source_path.display()))?;

    let cxx = env::var_os("CXX").unwrap_or_else(|| OsString::from("c++"));
    let cxx_path = PathBuf::from(&cxx);
    let mut compile_args = vec![
        OsString::from("-std=c++17"),
        OsString::from("-Wall"),
        OsString::from("-Wextra"),
        OsString::from("-Icarbonengine/scheduler/include"),
    ];
    compile_args.extend(python_config_flags(&["--includes"])?);
    compile_args.push(source_path.clone().into_os_string());
    compile_args.push(OsString::from("-o"));
    compile_args.push(binary_path.clone().into_os_string());
    compile_args.extend(
        python_config_flags(&["--ldflags", "--embed"])
            .or_else(|_| python_config_flags(&["--ldflags"]))?,
    );

    let compile_command = command_line(&cxx_path, &compile_args);
    let compile_output = Command::new(&cxx)
        .args(&compile_args)
        .output()
        .with_context(|| format!("compiling {context_name}"))?;

    let python_path = env::join_paths([package_dir.as_path()])
        .context("joining Scheduler.h C API binary smoke PYTHONPATH")?;
    let run_args: Vec<OsString> = Vec::new();
    let run_command = command_line(&binary_path, &run_args);
    let run_output = if compile_output.status.success() {
        Some(
            Command::new(&binary_path)
                .env("PYTHONPATH", python_path)
                .env("BUILDFLAVOR", "release")
                .output()
                .with_context(|| format!("running {context_name}"))?,
        )
    } else {
        None
    };

    Ok(SchedulerCapiBinarySmoke {
        source_path,
        binary_path,
        compile_command,
        run_command,
        compile_output,
        run_output,
    })
}

fn legacy_capi_source_slice_source(interpreter_source: &Path, legacy_source: &Path) -> String {
    format!(
        r#"#include <Python.h>
#include <strings.h>
#include <cstdlib>

static inline void carbon_source_slice_fixture_decref(PyObject*)
{{
}}

#ifdef Py_DecRef
#undef Py_DecRef
#endif
#define Py_DecRef(object) carbon_source_slice_fixture_decref(reinterpret_cast<PyObject*>(object))
#include "{}"
#undef Py_DecRef

#include "{}"

int main(int argc, char** argv)
{{
    (void)argc;
    (void)argv;
    return ::testing::RunAllTests(argv[0]);
}}
"#,
        cpp_include_path_literal(interpreter_source),
        cpp_include_path_literal(legacy_source)
    )
}

fn cpp_include_path_literal(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn legacy_capi_string_conversions_shim_source() -> &'static str {
    r#"#pragma once

#include <cstring>
#include <string>

inline std::wstring UTF8ToWide(const char* utf8String)
{
    if (utf8String == nullptr)
    {
        return std::wstring();
    }
    std::wstring converted;
    converted.reserve(std::strlen(utf8String));
    for (const unsigned char* cursor = reinterpret_cast<const unsigned char*>(utf8String); *cursor != 0; ++cursor)
    {
        converted.push_back(static_cast<wchar_t>(*cursor));
    }
    return converted;
}

inline std::wstring UTF8ToWide(const std::string& utf8String)
{
    return UTF8ToWide(utf8String.c_str());
}

inline std::string WideToUTF8(const wchar_t* wideString)
{
    if (wideString == nullptr)
    {
        return std::string();
    }
    std::string converted;
    for (const wchar_t* cursor = wideString; *cursor != 0; ++cursor)
    {
        converted.push_back(static_cast<char>(*cursor));
    }
    return converted;
}

inline std::string WideToUTF8(const std::wstring& wideString)
{
    return WideToUTF8(wideString.c_str());
}
"#
}

fn legacy_capi_gtest_shim_source() -> &'static str {
    r#"#pragma once

#include <Python.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

namespace testing
{
class Test
{
public:
    virtual ~Test() = default;
    virtual void SetUp() {}
    virtual void TearDown() {}
};

class TestFailure : public std::runtime_error
{
public:
    explicit TestFailure(std::string message)
        : std::runtime_error(std::move(message))
    {
    }
};

struct TestCase
{
    const char* suite;
    const char* name;
    void (*run)();
};

inline std::vector<TestCase>& Registry()
{
    static std::vector<TestCase> tests;
    return tests;
}

class Registrar
{
public:
    Registrar(const char* suite, const char* name, void (*run)())
    {
        Registry().push_back(TestCase{suite, name, run});
    }
};

inline std::string FailureMessage(const char* file, int line, const char* assertion, const char* expression)
{
    return std::string(file) + ":" + std::to_string(line) + ": " + assertion + " failed: " + expression;
}

template <typename Left, typename Right>
void ExpectEq(const Left& left, const Right& right, const char* expression, const char* file, int line)
{
    if (!(left == right))
    {
        throw TestFailure(FailureMessage(file, line, "EXPECT_EQ", expression));
    }
}

template <typename Left, typename Right>
void ExpectNe(const Left& left, const Right& right, const char* expression, const char* file, int line)
{
    if (!(left != right))
    {
        throw TestFailure(FailureMessage(file, line, "EXPECT_NE", expression));
    }
}

inline void ExpectTrue(bool value, const char* expression, const char* file, int line)
{
    if (!value)
    {
        throw TestFailure(FailureMessage(file, line, "EXPECT_TRUE", expression));
    }
}

inline void ExpectFalse(bool value, const char* expression, const char* file, int line)
{
    if (value)
    {
        throw TestFailure(FailureMessage(file, line, "EXPECT_FALSE", expression));
    }
}

template <typename Fixture>
void RunFixture(void (Fixture::*body)())
{
    Fixture fixture;
    bool setupDone = false;
    try
    {
        fixture.SetUp();
        setupDone = true;
        (fixture.*body)();
        fixture.TearDown();
    }
    catch (...)
    {
        if (setupDone)
        {
            try
            {
                fixture.TearDown();
            }
            catch (...)
            {
            }
        }
        throw;
    }
}

inline std::string FullName(const TestCase& test)
{
    return std::string(test.suite) + "." + test.name;
}

inline int RunOneTest(const TestCase& test)
{
    try
    {
        test.run();
        std::cout << "[  PASSED  ] " << FullName(test) << "\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "[  FAILED  ] " << FullName(test) << ": " << error.what() << "\n";
        if (PyErr_Occurred())
        {
            PyErr_Print();
        }
        return 1;
    }
    catch (...)
    {
        std::cerr << "[  FAILED  ] " << FullName(test) << ": unknown exception\n";
        if (PyErr_Occurred())
        {
            PyErr_Print();
        }
        return 1;
    }
}

inline int RunSelectedTest(const char* selectedName)
{
    for (const TestCase& test : Registry())
    {
        if (FullName(test) == selectedName)
        {
            return RunOneTest(test);
        }
    }
    std::cerr << "[  FAILED  ] unknown selected test: " << selectedName << "\n";
    return 1;
}

inline int RunTestInChild(const char* executablePath, const TestCase& test)
{
    const std::string fullName = FullName(test);
    pid_t pid = fork();
    if (pid < 0)
    {
        std::cerr << "[  FAILED  ] " << fullName << ": fork failed\n";
        return 1;
    }
    if (pid == 0)
    {
        setenv("CARBON_CAPI_GTEST_FILTER", fullName.c_str(), 1);
        execl(executablePath, executablePath, static_cast<char*>(nullptr));
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        std::cerr << "[  FAILED  ] " << fullName << ": waitpid failed\n";
        return 1;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
    {
        return 0;
    }
    std::cerr << "[  FAILED  ] " << fullName << ": child exited with status " << status << "\n";
    return 1;
}

inline int RunAllTests(const char* executablePath)
{
    if (const char* selectedName = std::getenv("CARBON_CAPI_GTEST_FILTER"))
    {
        if (*selectedName != 0)
        {
            return RunSelectedTest(selectedName);
        }
    }

    const char* inProcess = std::getenv("CARBON_CAPI_GTEST_IN_PROCESS");
    int failed = 0;
    int total = 0;
    for (const TestCase& test : Registry())
    {
        ++total;
        if (inProcess != nullptr && *inProcess != 0)
        {
            failed += RunOneTest(test);
        }
        else
        {
            failed += RunTestInChild(executablePath, test);
        }
    }
    std::cout << "[==========] " << total << " tests ran.\n";
    if (failed == 0)
    {
        std::cout << "[  PASSED  ] " << total << " tests.\n";
    }
    else
    {
        std::cerr << "[  FAILED  ] " << failed << " tests.\n";
    }
    return failed == 0 ? 0 : 1;
}
}

#define TEST_F(Fixture, Name) \
class Fixture##_##Name##_Test : public Fixture \
{ \
public: \
    void TestBody(); \
    static void Run() { ::testing::RunFixture<Fixture##_##Name##_Test>(&Fixture##_##Name##_Test::TestBody); } \
}; \
static ::testing::Registrar Fixture##_##Name##_registrar(#Fixture, #Name, &Fixture##_##Name##_Test::Run); \
void Fixture##_##Name##_Test::TestBody()

#define EXPECT_EQ(left, right) ::testing::ExpectEq((left), (right), #left " == " #right, __FILE__, __LINE__)
#define ASSERT_EQ(left, right) EXPECT_EQ(left, right)
#define EXPECT_NE(left, right) ::testing::ExpectNe((left), (right), #left " != " #right, __FILE__, __LINE__)
#define ASSERT_NE(left, right) EXPECT_NE(left, right)
#define EXPECT_TRUE(expression) ::testing::ExpectTrue(static_cast<bool>(expression), #expression, __FILE__, __LINE__)
#define ASSERT_TRUE(expression) EXPECT_TRUE(expression)
#define EXPECT_FALSE(expression) ::testing::ExpectFalse(static_cast<bool>(expression), #expression, __FILE__, __LINE__)
#define ASSERT_FALSE(expression) EXPECT_FALSE(expression)
"#
}

fn scheduler_capi_binary_smoke_source() -> &'static str {
    r#"#include <Python.h>
		#include "Scheduler.h"

		#include <cstdio>
		#include <cstring>

	static SchedulerCAPI* g_scheduler_api = nullptr;

	static PyObject* c_api_tasklet_send(PyObject*, PyObject* args)
	{
	    PyObject* channel = nullptr;
	    PyObject* value = nullptr;
	    if (!PyArg_ParseTuple(args, "OO", &channel, &value))
	    {
	        return nullptr;
	    }
	    if (g_scheduler_api == nullptr)
	    {
	        PyErr_SetString(PyExc_RuntimeError, "SchedulerCAPI is not initialized");
	        return nullptr;
	    }
	    if (g_scheduler_api->PyChannel_Send(reinterpret_cast<PyChannelObject*>(channel), value) != 0)
	    {
	        return nullptr;
	    }
	    Py_RETURN_NONE;
	}

	static PyMethodDef c_api_tasklet_send_def = {
	    "c_api_tasklet_send",
	    c_api_tasklet_send,
	    METH_VARARGS,
	    "Call PyChannel_Send through Scheduler.h from a running tasklet."
	};

static int fail(const char* message)
{
    std::fprintf(stderr, "%s\n", message);
    if (PyErr_Occurred())
    {
        PyErr_Print();
    }
	    return 1;
	}

	static PyObject* main_attr(const char* name)
	{
	    PyObject* main = PyImport_AddModule("__main__");
	    if (main == nullptr)
	    {
	        return nullptr;
	    }
	    return PyObject_GetAttrString(main, name);
	}

	static int expect_none_result(PyObject* result, const char* message)
	{
	    if (result == nullptr)
	    {
	        return fail(message);
	    }
	    if (result != Py_None)
	    {
	        Py_DECREF(result);
	        return fail("Scheduler C API returned a non-None result");
	    }
	    Py_DECREF(result);
	    return 0;
	}

		static int expect_list_long(const char* list_name, long expected)
		{
		    PyObject* list = main_attr(list_name);
	    if (list == nullptr || !PyList_Check(list))
	    {
	        Py_XDECREF(list);
	        return fail("expected Python list is missing");
	    }
	    PyObject* item = PyList_GetItem(list, 0);
	    if (item == nullptr || !PyLong_Check(item))
	    {
	        Py_DECREF(list);
	        return fail("expected Python list item is missing");
	    }
	    long actual = PyLong_AsLong(item);
	    Py_DECREF(list);
	    if (actual != expected)
	    {
	        return fail("Python list value did not match expected value");
		    }
		    return 0;
		}

		static int expect_queue_tasklet(SchedulerCAPI* api, PyChannelObject* channel, PyTaskletObject* tasklet)
		{
		    PyObject* queued = api->PyChannel_GetQueue(channel);
		    if (queued == nullptr)
		    {
		        return fail("PyChannel_GetQueue returned null");
		    }
		    int same_object = queued == reinterpret_cast<PyObject*>(tasklet);
		    Py_DECREF(queued);
		    if (!same_object)
		    {
		        return fail("PyChannel_GetQueue did not return the expected blocked tasklet");
		    }
		    return 0;
		}

		static int expect_unicode_value(PyObject* value, const char* expected)
		{
		    if (value == nullptr)
		    {
		        return fail("expected unicode value but C API returned null");
		    }
		    const char* actual = PyUnicode_AsUTF8(value);
		    if (actual == nullptr)
		    {
		        Py_DECREF(value);
		        return fail("C API value was not UTF-8 unicode");
		    }
		    if (std::strcmp(actual, expected) != 0)
		    {
		        Py_DECREF(value);
		        return fail("C API unicode value did not match expected payload");
		    }
		    Py_DECREF(value);
		    return 0;
		}

		static int install_c_api_tasklet_helpers()
		{
		    PyObject* main = PyImport_AddModule("__main__");
		    if (main == nullptr)
		    {
		        return fail("could not import __main__ for C API helpers");
		    }
		    PyObject* helper = PyCFunction_NewEx(&c_api_tasklet_send_def, nullptr, nullptr);
		    if (helper == nullptr)
		    {
		        return fail("could not create c_api_tasklet_send helper");
		    }
		    int status = PyObject_SetAttrString(main, "c_api_tasklet_send", helper);
		    Py_DECREF(helper);
		    if (status != 0)
		    {
		        return fail("could not install c_api_tasklet_send helper");
		    }
		    return 0;
		}

		static int expect_int_error(int result, const char* message)
		{
		    if (result != -1)
		    {
		        return fail(message);
		    }
		    if (!PyErr_Occurred())
		    {
		        return fail("C API error path did not set a Python exception");
		    }
		    PyErr_Clear();
		    return 0;
		}

		static int expect_null_error(PyObject* result, const char* message)
		{
		    if (result != nullptr)
		    {
		        Py_DECREF(result);
		        return fail(message);
		    }
		    if (!PyErr_Occurred())
		    {
		        return fail("C API null error path did not set a Python exception");
		    }
		    PyErr_Clear();
		    return 0;
		}

int main()
{
    Py_Initialize();
    if (PyRun_SimpleString("import scheduler") != 0)
    {
        return fail("failed to import scheduler");
    }

    SchedulerCAPI* api = SchedulerAPI();
    if (api == nullptr)
    {
        return fail("SchedulerAPI returned null");
    }
    g_scheduler_api = api;
    if (sizeof(SchedulerCAPI) != 40 * sizeof(void*))
    {
        return fail("SchedulerCAPI size does not match 40 pointer slots");
    }
    if (api->PyTasklet_New == nullptr || api->PyTasklet_Setup == nullptr ||
        api->PyTasklet_Insert == nullptr || api->PyTasklet_GetBlockTrap == nullptr ||
        api->PyTasklet_SetBlockTrap == nullptr || api->PyTasklet_Alive == nullptr ||
        api->PyTasklet_Kill == nullptr || api->PyScheduler_GetRunCount == nullptr ||
        api->PyScheduler_GetCurrent == nullptr || api->PyScheduler_RunNTasklets == nullptr ||
        api->PyTasklet_Check == nullptr || api->PyTasklet_IsMain == nullptr ||
        api->PyChannel_New == nullptr || api->PyChannel_Send == nullptr ||
        api->PyChannel_Receive == nullptr || api->PyChannel_GetQueue == nullptr ||
        api->PyChannel_GetPreference == nullptr || api->PyChannel_SetPreference == nullptr ||
        api->PyChannel_Check == nullptr || api->PyChannel_GetBalance == nullptr ||
        api->PyChannel_SendThrow == nullptr || api->PyTaskletType == nullptr ||
        api->PyChannelType == nullptr || api->TaskletExit == nullptr ||
        *api->TaskletExit == nullptr)
    {
        return fail("required SchedulerCAPI slots are null");
    }

    PyObject* current = api->PyScheduler_GetCurrent();
    if (current == nullptr)
    {
        return fail("PyScheduler_GetCurrent returned null");
    }
    if (!api->PyTasklet_Check(current))
    {
        Py_DECREF(current);
        return fail("current object is not a tasklet");
    }
    if (!api->PyTasklet_IsMain(reinterpret_cast<PyTaskletObject*>(current)))
    {
        Py_DECREF(current);
        return fail("current tasklet is not main");
    }

    PyChannelObject* channel = api->PyChannel_New(api->PyChannelType);
    if (channel == nullptr)
    {
        Py_DECREF(current);
        return fail("PyChannel_New returned null");
    }
    if (!api->PyChannel_Check(reinterpret_cast<PyObject*>(channel)))
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("new object is not a channel");
    }
    if (api->PyChannel_GetBalance(channel) != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("new channel balance is not zero");
    }
    if (api->PyChannel_GetPreference(channel) != -1)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("new channel preference is not legacy receiver preference");
    }
    api->PyChannel_SetPreference(channel, 0);
    if (api->PyChannel_GetPreference(channel) != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("PyChannel_SetPreference did not accept neutral preference");
    }
    api->PyChannel_SetPreference(channel, 1);
    if (api->PyChannel_GetPreference(channel) != 1)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("PyChannel_SetPreference did not accept sender preference");
    }
    api->PyChannel_SetPreference(channel, -2);
    if (api->PyChannel_GetPreference(channel) != -1)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("PyChannel_SetPreference did not clamp low preference");
    }
    api->PyChannel_SetPreference(channel, 2);
    if (api->PyChannel_GetPreference(channel) != 1)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("PyChannel_SetPreference did not clamp high preference");
    }
    api->PyChannel_SetPreference(channel, -1);

    if (api->PyTasklet_Check(reinterpret_cast<PyObject*>(channel)) != 0 ||
        api->PyChannel_Check(current) != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return fail("C API type checks accepted the wrong object type");
    }
    if (expect_int_error(api->PyChannel_Send(nullptr, Py_None), "PyChannel_Send accepted a null channel") != 0 ||
        expect_int_error(api->PyChannel_Send(channel, nullptr), "PyChannel_Send accepted a null value") != 0 ||
        expect_null_error(api->PyChannel_Receive(reinterpret_cast<PyChannelObject*>(current)), "PyChannel_Receive accepted a tasklet object") != 0 ||
        expect_int_error(api->PyChannel_SendThrow(channel, nullptr, nullptr, nullptr), "PyChannel_SendThrow accepted a null exception") != 0 ||
        expect_int_error(api->PyTasklet_Setup(reinterpret_cast<PyTaskletObject*>(channel), nullptr, nullptr), "PyTasklet_Setup accepted a channel object") != 0 ||
        expect_int_error(api->PyTasklet_Insert(reinterpret_cast<PyTaskletObject*>(channel)), "PyTasklet_Insert accepted a channel object") != 0 ||
        expect_int_error(api->PyTasklet_Kill(reinterpret_cast<PyTaskletObject*>(channel)), "PyTasklet_Kill accepted a channel object") != 0 ||
        expect_null_error(api->PyScheduler_RunNTasklets(0), "PyScheduler_RunNTasklets accepted zero") != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(channel));
        Py_DECREF(current);
        return 1;
    }

    if (api->PyScheduler_GetRunCount() != 1)
    {
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return fail("initial scheduler run count is not one");
		    }

		    if (install_c_api_tasklet_helpers() != 0)
		    {
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return 1;
		    }
		    if (PyRun_SimpleString(
		            "c_api_sender_trace = []\n"
		            "def c_api_sender(ch, payload):\n"
		            "    c_api_sender_trace.append(('before', ch.balance))\n"
		            "    c_api_tasklet_send(ch, payload)\n"
		            "    c_api_sender_trace.append(('after', ch.balance))\n") != 0)
		    {
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return fail("failed to define C API sender tasklet");
		    }
		    PyObject* sender_callable = main_attr("c_api_sender");
		    PyObject* sender_tasklet_args = PyTuple_Pack(1, sender_callable);
		    PyTaskletObject* sender_tasklet = api->PyTasklet_New(api->PyTaskletType, sender_tasklet_args);
		    PyObject* sender_payload = PyUnicode_FromString("sender-payload");
		    PyObject* sender_setup_args = PyTuple_Pack(2, reinterpret_cast<PyObject*>(channel), sender_payload);
		    if (sender_callable == nullptr || sender_tasklet_args == nullptr || sender_tasklet == nullptr ||
		        sender_payload == nullptr || sender_setup_args == nullptr ||
		        api->PyTasklet_Setup(sender_tasklet, sender_setup_args, nullptr) != 0)
		    {
		        Py_XDECREF(sender_setup_args);
		        Py_XDECREF(sender_payload);
		        Py_XDECREF(reinterpret_cast<PyObject*>(sender_tasklet));
		        Py_XDECREF(sender_tasklet_args);
		        Py_XDECREF(sender_callable);
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return fail("failed to set up C API sender tasklet");
		    }
		    Py_DECREF(sender_setup_args);
		    Py_DECREF(sender_payload);
		    if (expect_none_result(api->PyScheduler_RunNTasklets(1), "C API sender tasklet did not block") != 0)
		    {
		        Py_DECREF(reinterpret_cast<PyObject*>(sender_tasklet));
		        Py_DECREF(sender_tasklet_args);
		        Py_DECREF(sender_callable);
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return 1;
		    }
		    if (api->PyChannel_GetBalance(channel) != 1 || expect_queue_tasklet(api, channel, sender_tasklet) != 0)
		    {
		        Py_DECREF(reinterpret_cast<PyObject*>(sender_tasklet));
		        Py_DECREF(sender_tasklet_args);
		        Py_DECREF(sender_callable);
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return fail("C API sender did not block on channel send");
		    }
		    if (expect_unicode_value(api->PyChannel_Receive(channel), "sender-payload") != 0 ||
		        api->PyChannel_GetBalance(channel) != 0 ||
		        expect_none_result(api->PyScheduler_RunNTasklets(1), "C API sender continuation did not drain") != 0 ||
		        api->PyScheduler_GetRunCount() != 1 ||
		        api->PyTasklet_Alive(sender_tasklet) != 0 ||
		        PyRun_SimpleString("assert c_api_sender_trace == [('before', 0), ('after', 0)], c_api_sender_trace\n") != 0)
		    {
		        Py_DECREF(reinterpret_cast<PyObject*>(sender_tasklet));
		        Py_DECREF(sender_tasklet_args);
		        Py_DECREF(sender_callable);
		        Py_DECREF(reinterpret_cast<PyObject*>(channel));
		        Py_DECREF(current);
		        return fail("C API sender receive/drain trace failed");
		    }
		    Py_DECREF(reinterpret_cast<PyObject*>(sender_tasklet));
		    Py_DECREF(sender_tasklet_args);
		    Py_DECREF(sender_callable);

		    if (PyRun_SimpleString(
	            "import scheduler\n"
	            "c_api_value = [0]\n"
	            "def c_api_foo(x):\n"
	            "    c_api_value[0] = x\n") != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("failed to define tasklet setup callable");
	    }
	    PyObject* foo = main_attr("c_api_foo");
	    if (foo == nullptr || !PyCallable_Check(foo))
	    {
	        Py_XDECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("tasklet setup callable is missing");
	    }
	    PyObject* tasklet_args = PyTuple_Pack(1, foo);
	    PyTaskletObject* tasklet = api->PyTasklet_New(api->PyTaskletType, tasklet_args);
	    if (tasklet == nullptr || !api->PyTasklet_Check(reinterpret_cast<PyObject*>(tasklet)))
	    {
	        Py_XDECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_New did not return a tasklet");
	    }
	    if (api->PyTasklet_IsMain(tasklet) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("new tasklet main state is wrong");
	    }
	    if (api->PyTasklet_GetBlockTrap(tasklet) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("new tasklet block_trap default is wrong");
	    }
	    api->PyTasklet_SetBlockTrap(tasklet, 1);
	    if (api->PyTasklet_GetBlockTrap(tasklet) != 1)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_SetBlockTrap did not update block_trap");
	    }
	    api->PyTasklet_SetBlockTrap(tasklet, 0);
	    PyObject* callable_value = PyLong_FromLong(101);
	    PyObject* callable_args = PyTuple_Pack(1, callable_value);
	    Py_DECREF(callable_value);
	    if (api->PyTasklet_Setup(tasklet, callable_args, nullptr) != 0)
	    {
	        Py_DECREF(callable_args);
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_Setup failed");
	    }
	    Py_DECREF(callable_args);
	    if (api->PyScheduler_GetRunCount() != 2 || api->PyTasklet_Alive(tasklet) != 1)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("run count or alive state after PyTasklet_Setup is wrong");
	    }
	    if (expect_none_result(api->PyScheduler_RunNTasklets(1), "PyScheduler_RunNTasklets failed") != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return 1;
	    }
	    if (api->PyScheduler_GetRunCount() != 1 || api->PyTasklet_Alive(tasklet) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("tasklet did not complete after RunNTasklets");
	    }
	    if (expect_list_long("c_api_value", 101) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	        Py_DECREF(tasklet_args);
	        Py_DECREF(foo);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return 1;
	    }
	    Py_DECREF(reinterpret_cast<PyObject*>(tasklet));
	    Py_DECREF(tasklet_args);
	    Py_DECREF(foo);

	    if (PyRun_SimpleString(
	            "insert_value = [0]\n"
	            "def c_api_pause():\n"
	            "    scheduler.schedule_remove()\n"
	            "    insert_value[0] = 1\n"
	            "paused_tasklet = scheduler.tasklet(c_api_pause)()\n"
	            "scheduler.run()\n") != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("failed to create paused tasklet");
	    }
	    PyObject* paused = main_attr("paused_tasklet");
	    if (paused == nullptr || !api->PyTasklet_Check(paused))
	    {
	        Py_XDECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("paused tasklet is missing");
	    }
	    if (api->PyTasklet_Insert(reinterpret_cast<PyTaskletObject*>(paused)) != 0)
	    {
	        Py_DECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_Insert failed for paused tasklet");
	    }
	    if (api->PyScheduler_GetRunCount() != 2)
	    {
	        Py_DECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("run count after PyTasklet_Insert is wrong");
	    }
	    if (expect_none_result(api->PyScheduler_RunNTasklets(1), "RunNTasklets failed after insert") != 0)
	    {
	        Py_DECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return 1;
	    }
	    if (expect_list_long("insert_value", 1) != 0 || api->PyScheduler_GetRunCount() != 1 ||
	        api->PyTasklet_Alive(reinterpret_cast<PyTaskletObject*>(paused)) != 0)
	    {
	        Py_DECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("inserted tasklet did not resume and complete");
	    }
	    if (api->PyTasklet_Insert(reinterpret_cast<PyTaskletObject*>(paused)) != -1)
	    {
	        Py_DECREF(paused);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_Insert unexpectedly accepted a dead tasklet");
	    }
	    PyErr_Clear();
	    Py_DECREF(paused);

	    if (PyRun_SimpleString(
	            "kill_value = [0]\n"
	            "def c_api_kill_target():\n"
	            "    kill_value[0] = 1\n") != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("failed to define kill target");
	    }
	    PyObject* kill_callable = main_attr("c_api_kill_target");
	    PyObject* kill_tasklet_args = PyTuple_Pack(1, kill_callable);
	    PyTaskletObject* kill_tasklet = api->PyTasklet_New(api->PyTaskletType, kill_tasklet_args);
	    PyObject* empty_args = PyTuple_New(0);
	    if (kill_callable == nullptr || kill_tasklet == nullptr || empty_args == nullptr ||
	        api->PyTasklet_Setup(kill_tasklet, empty_args, nullptr) != 0)
	    {
	        Py_XDECREF(empty_args);
	        Py_XDECREF(reinterpret_cast<PyObject*>(kill_tasklet));
	        Py_XDECREF(kill_tasklet_args);
	        Py_XDECREF(kill_callable);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("failed to setup kill tasklet");
	    }
	    Py_DECREF(empty_args);
	    if (api->PyScheduler_GetRunCount() != 2 || api->PyTasklet_Kill(kill_tasklet) != 0 ||
	        api->PyScheduler_GetRunCount() != 1 || api->PyTasklet_Alive(kill_tasklet) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(kill_tasklet));
	        Py_DECREF(kill_tasklet_args);
	        Py_DECREF(kill_callable);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("PyTasklet_Kill did not remove scheduled tasklet");
	    }
	    if (expect_list_long("kill_value", 0) != 0)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(kill_tasklet));
	        Py_DECREF(kill_tasklet_args);
	        Py_DECREF(kill_callable);
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return 1;
	    }
	    Py_DECREF(reinterpret_cast<PyObject*>(kill_tasklet));
	    Py_DECREF(kill_tasklet_args);
	    Py_DECREF(kill_callable);

	    if (api->PyScheduler_GetRunCount() != 1)
	    {
	        Py_DECREF(reinterpret_cast<PyObject*>(channel));
	        Py_DECREF(current);
	        return fail("final scheduler run count is not one");
	    }

	    Py_DECREF(reinterpret_cast<PyObject*>(channel));
	    Py_DECREF(current);
		    std::printf("Scheduler.h C API binary smoke passed tasklet lifecycle, scheduler run-control, channel preference, invalid argument, and inside-tasklet channel send checks\n");
    Py_Finalize();
    return 0;
}
	"#
}

fn io_scheduler_capi_semantic_smoke_source() -> &'static str {
    r#"#include <Python.h>
#include "Scheduler.h"

#include <cstdio>

static int fail(const char* message)
{
    std::fprintf(stderr, "%s\n", message);
    if (PyErr_Occurred())
    {
        PyErr_Print();
    }
    return 1;
}

static PyObject* main_attr(const char* name)
{
    PyObject* main = PyImport_AddModule("__main__");
    if (main == nullptr)
    {
        return nullptr;
    }
    return PyObject_GetAttrString(main, name);
}

static int expect_none_result(PyObject* result, const char* message)
{
    if (result == nullptr)
    {
        return fail(message);
    }
    if (result != Py_None)
    {
        Py_DECREF(result);
        return fail("Scheduler C API returned a non-None result");
    }
    Py_DECREF(result);
    return 0;
}

static int expect_balance(SchedulerCAPI* api, PyChannelObject* channel, int expected, const char* message)
{
    int actual = api->PyChannel_GetBalance(channel);
    if (actual != expected)
    {
        std::fprintf(stderr, "%s: expected %d, got %d\n", message, expected, actual);
        return fail("PyChannel_GetBalance mismatch");
    }
    return 0;
}

static int expect_queue_tasklet(SchedulerCAPI* api, PyChannelObject* channel, PyTaskletObject* tasklet)
{
    PyObject* queued = api->PyChannel_GetQueue(channel);
    if (queued == nullptr)
    {
        return fail("PyChannel_GetQueue returned null for blocked receiver");
    }
    int same_object = queued == reinterpret_cast<PyObject*>(tasklet);
    Py_DECREF(queued);
    if (!same_object)
    {
        return fail("PyChannel_GetQueue did not return the blocked receiver tasklet");
    }
    return 0;
}

static int run_one_tasklet(SchedulerCAPI* api, const char* message)
{
    return expect_none_result(api->PyScheduler_RunNTasklets(1), message);
}

static int drain_scheduler(SchedulerCAPI* api)
{
    for (int attempt = 0; attempt < 8 && api->PyScheduler_GetRunCount() > 1; ++attempt)
    {
        if (run_one_tasklet(api, "PyScheduler_RunNTasklets failed while draining") != 0)
        {
            return 1;
        }
    }
    if (api->PyScheduler_GetRunCount() != 1)
    {
        return fail("scheduler did not drain back to the main tasklet");
    }
    return 0;
}

static PyTaskletObject* setup_receiver(SchedulerCAPI* api, PyChannelObject* channel, const char* label)
{
    PyObject* receiver_callable = main_attr("io_capi_receiver");
    if (receiver_callable == nullptr || !PyCallable_Check(receiver_callable))
    {
        Py_XDECREF(receiver_callable);
        fail("io_capi_receiver callable is missing");
        return nullptr;
    }

    PyObject* tasklet_constructor_args = PyTuple_Pack(1, receiver_callable);
    PyTaskletObject* tasklet = api->PyTasklet_New(api->PyTaskletType, tasklet_constructor_args);
    PyObject* label_object = PyUnicode_FromString(label);
    PyObject* setup_args = PyTuple_Pack(2, reinterpret_cast<PyObject*>(channel), label_object);
    int setup_status = -1;
    if (tasklet != nullptr && setup_args != nullptr)
    {
        setup_status = api->PyTasklet_Setup(tasklet, setup_args, nullptr);
    }

    Py_XDECREF(setup_args);
    Py_XDECREF(label_object);
    Py_XDECREF(reinterpret_cast<PyObject*>(tasklet_constructor_args));
    Py_DECREF(receiver_callable);

    if (tasklet == nullptr || setup_status != 0)
    {
        Py_XDECREF(reinterpret_cast<PyObject*>(tasklet));
        fail("failed to set up receiver tasklet");
        return nullptr;
    }
    return tasklet;
}

static int assert_python_trace(const char* script, const char* message)
{
    if (PyRun_SimpleString(script) != 0)
    {
        return fail(message);
    }
    return 0;
}

int main()
{
    Py_Initialize();
    if (PyRun_SimpleString(
            "import scheduler\n"
            "io_trace = []\n"
            "def io_capi_receiver(ch, label):\n"
            "    io_trace.append((label, 'before', ch.balance))\n"
            "    try:\n"
            "        value = ch.receive()\n"
            "        io_trace.append((label, 'value', value, ch.balance))\n"
            "    except Exception as exc:\n"
            "        io_trace.append((label, 'error', type(exc).__name__, str(exc), ch.balance))\n") != 0)
    {
        return fail("failed to define IO C API receiver tasklet");
    }

    SchedulerCAPI* api = SchedulerAPI();
    if (api == nullptr)
    {
        return fail("SchedulerAPI returned null");
    }
    if (api->PyTasklet_New == nullptr || api->PyTasklet_Setup == nullptr ||
        api->PyChannel_New == nullptr || api->PyChannel_Send == nullptr ||
        api->PyChannel_SendThrow == nullptr || api->PyChannel_GetQueue == nullptr ||
        api->PyChannel_GetBalance == nullptr || api->PyChannel_SetPreference == nullptr ||
        api->PyScheduler_GetRunCount == nullptr || api->PyScheduler_RunNTasklets == nullptr ||
        api->PyTasklet_Check == nullptr || api->PyChannel_Check == nullptr ||
        api->PyTaskletType == nullptr || api->PyChannelType == nullptr)
    {
        return fail("required IO-facing SchedulerCAPI slots are null");
    }

    PyChannelObject* value_channel = api->PyChannel_New(api->PyChannelType);
    if (value_channel == nullptr || !api->PyChannel_Check(reinterpret_cast<PyObject*>(value_channel)))
    {
        Py_XDECREF(reinterpret_cast<PyObject*>(value_channel));
        return fail("PyChannel_New did not return a channel for value wake smoke");
    }
    api->PyChannel_SetPreference(value_channel, -1);
    PyTaskletObject* value_receiver = setup_receiver(api, value_channel, "value");
    if (value_receiver == nullptr || !api->PyTasklet_Check(reinterpret_cast<PyObject*>(value_receiver)))
    {
        Py_XDECREF(reinterpret_cast<PyObject*>(value_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(value_channel));
        return fail("value receiver tasklet setup failed");
    }
    if (run_one_tasklet(api, "value receiver did not block on channel receive") != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(value_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(value_channel));
        return 1;
    }
    if (expect_balance(api, value_channel, -1, "blocked value receiver balance") != 0 ||
        expect_queue_tasklet(api, value_channel, value_receiver) != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(value_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(value_channel));
        return 1;
    }

    PyObject* payload = PyUnicode_FromString("io-ready");
    int send_status = api->PyChannel_Send(value_channel, payload);
    Py_DECREF(payload);
    if (send_status != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(value_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(value_channel));
        return fail("PyChannel_Send failed to wake blocked receiver");
    }
    if (drain_scheduler(api) != 0 ||
        expect_balance(api, value_channel, 0, "value wake completion balance") != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(value_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(value_channel));
        return 1;
    }
    Py_DECREF(reinterpret_cast<PyObject*>(value_receiver));
    Py_DECREF(reinterpret_cast<PyObject*>(value_channel));

    PyChannelObject* throw_channel = api->PyChannel_New(api->PyChannelType);
    if (throw_channel == nullptr || !api->PyChannel_Check(reinterpret_cast<PyObject*>(throw_channel)))
    {
        Py_XDECREF(reinterpret_cast<PyObject*>(throw_channel));
        return fail("PyChannel_New did not return a channel for send_throw smoke");
    }
    api->PyChannel_SetPreference(throw_channel, -1);
    PyTaskletObject* throw_receiver = setup_receiver(api, throw_channel, "throw");
    if (throw_receiver == nullptr || !api->PyTasklet_Check(reinterpret_cast<PyObject*>(throw_receiver)))
    {
        Py_XDECREF(reinterpret_cast<PyObject*>(throw_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));
        return fail("throw receiver tasklet setup failed");
    }
    if (run_one_tasklet(api, "throw receiver did not block on channel receive") != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(throw_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));
        return 1;
    }
    if (expect_balance(api, throw_channel, -1, "blocked throw receiver balance") != 0 ||
        expect_queue_tasklet(api, throw_channel, throw_receiver) != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(throw_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));
        return 1;
    }

    PyObject* error_value = PyUnicode_FromString("ssl-wakeup");
    int throw_status = api->PyChannel_SendThrow(
        throw_channel,
        reinterpret_cast<PyObject*>(PyExc_RuntimeError),
        error_value,
        nullptr);
    Py_DECREF(error_value);
    if (throw_status != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(throw_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));
        return fail("PyChannel_SendThrow failed to wake blocked receiver");
    }
    if (drain_scheduler(api) != 0 ||
        expect_balance(api, throw_channel, 0, "send_throw completion balance") != 0)
    {
        Py_DECREF(reinterpret_cast<PyObject*>(throw_receiver));
        Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));
        return 1;
    }
    Py_DECREF(reinterpret_cast<PyObject*>(throw_receiver));
    Py_DECREF(reinterpret_cast<PyObject*>(throw_channel));

    if (assert_python_trace(
            "assert io_trace == [\n"
            "    ('value', 'before', 0),\n"
            "    ('value', 'value', 'io-ready', 0),\n"
            "    ('throw', 'before', 0),\n"
            "    ('throw', 'error', 'RuntimeError', 'ssl-wakeup', 0),\n"
            "], io_trace\n",
            "IO C API semantic trace did not match expected wake/send_throw behavior") != 0)
    {
        return 1;
    }

    std::printf("IO Scheduler.h C API semantic smoke passed channel balance wake and send_throw checks\n");
    Py_Finalize();
    return 0;
}
"#
}

fn python_config_flags(args: &[&str]) -> Result<Vec<OsString>> {
    let python_config =
        env::var_os("PYTHON_CONFIG").unwrap_or_else(|| OsString::from("python3-config"));
    let output = Command::new(&python_config)
        .args(args)
        .output()
        .with_context(|| {
            format!(
                "running {} {}",
                shell_quote_os(Path::new(&python_config).as_os_str()),
                args.join(" ")
            )
        })?;
    if !output.status.success() {
        bail!(
            "{} {} failed",
            shell_quote_os(Path::new(&python_config).as_os_str()),
            args.join(" ")
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(OsString::from)
        .collect())
}

fn run_scheduler_python_legacy_subset(package_dir: &Path) -> Result<Output> {
    let root = env::current_dir().context("resolving current directory")?;
    let package_dir = root.join(package_dir);
    let tests_dir = root.join("carbonengine/scheduler/tests/python/scheduler/tests");
    let python_path = env::join_paths([package_dir.as_path(), tests_dir.as_path()])
        .context("joining scheduler legacy subset PYTHONPATH")?;
    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
    let python_libdir = command_stdout(
        &python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ],
    )
    .ok();

    let mut command = Command::new(&python);
    command
        .args(["-m", "unittest"])
        .args(RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET)
        .env("PYTHONPATH", python_path)
        .env("BUILDFLAVOR", "release");
    if let Some(python_libdir) = python_libdir.filter(|value| !value.is_empty()) {
        let ld_library_path = match env::var("LD_LIBRARY_PATH") {
            Ok(existing) if !existing.is_empty() => format!("{python_libdir}:{existing}"),
            _ => python_libdir,
        };
        command.env("LD_LIBRARY_PATH", ld_library_path);
    }

    command.output().context("running legacy scheduler subset")
}

fn run_scheduler_python_import_smoke(package_dir: &Path) -> Result<Output> {
    let package_dir = env::current_dir()
        .context("resolving current directory")?
        .join(package_dir);
    let python = env::var("PYTHON").unwrap_or_else(|_| String::from("python3"));
    let python_libdir = command_stdout(
        &python,
        &[
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ],
    )
    .ok();
    let script = r#"
import importlib
import sys
import ctypes

class SchedulerCAPI(ctypes.Structure):
    _fields_ = [
        ("PyTasklet_New", ctypes.c_void_p),
        ("PyTasklet_Setup", ctypes.c_void_p),
        ("PyTasklet_Insert", ctypes.c_void_p),
        ("PyTasklet_GetBlockTrap", ctypes.c_void_p),
        ("PyTasklet_SetBlockTrap", ctypes.c_void_p),
        ("PyTasklet_IsMain", ctypes.c_void_p),
        ("PyTasklet_Check", ctypes.c_void_p),
        ("PyTasklet_Alive", ctypes.c_void_p),
        ("PyTasklet_Kill", ctypes.c_void_p),
        ("PyChannel_New", ctypes.c_void_p),
        ("PyChannel_Send", ctypes.c_void_p),
        ("PyChannel_Receive", ctypes.c_void_p),
        ("PyChannel_SendException", ctypes.c_void_p),
        ("PyChannel_GetQueue", ctypes.c_void_p),
        ("PyChannel_GetPreference", ctypes.c_void_p),
        ("PyChannel_SetPreference", ctypes.c_void_p),
        ("PyChannel_GetBalance", ctypes.c_void_p),
        ("PyChannel_Check", ctypes.c_void_p),
        ("PyChannel_SendThrow", ctypes.c_void_p),
        ("PyScheduler_GetScheduler", ctypes.c_void_p),
        ("PyScheduler_Schedule", ctypes.c_void_p),
        ("PyScheduler_GetRunCount", ctypes.c_void_p),
        ("PyScheduler_GetCurrent", ctypes.c_void_p),
        ("PyScheduler_RunWithTimeout", ctypes.c_void_p),
        ("PyScheduler_RunNTasklets", ctypes.c_void_p),
        ("PyScheduler_SetChannelCallback", ctypes.c_void_p),
        ("PyScheduler_GetChannelCallback", ctypes.c_void_p),
        ("PyScheduler_SetScheduleCallback", ctypes.c_void_p),
        ("PyScheduler_SetScheduleFastCallback", ctypes.c_void_p),
        ("PyScheduler_GetNumberOfActiveScheduleManagers", ctypes.c_void_p),
        ("PyScheduler_GetNumberOfActiveChannels", ctypes.c_void_p),
        ("PyScheduler_GetAllTimeTaskletCount", ctypes.c_void_p),
        ("PyScheduler_GetActiveTaskletCount", ctypes.c_void_p),
        ("PyScheduler_GetTaskletsCompletedLastRunWithTimeout", ctypes.c_void_p),
        ("PyScheduler_GetTaskletsSwitchedLastRunWithTimeout", ctypes.c_void_p),
        ("PyTaskletType", ctypes.c_void_p),
        ("PyChannelType", ctypes.c_void_p),
        ("TaskletExit", ctypes.c_void_p),
        ("PyTasklet_GetTimesSwitchedTo", ctypes.c_void_p),
        ("PyTasklet_GetContext", ctypes.c_void_p),
    ]

GET_INT = ctypes.CFUNCTYPE(ctypes.c_int)
GET_INT_OBJECT = ctypes.PYFUNCTYPE(ctypes.c_int, ctypes.py_object)
SET_OBJECT_INT = ctypes.PYFUNCTYPE(None, ctypes.py_object, ctypes.c_int)
GET_LONG_OBJECT = ctypes.PYFUNCTYPE(ctypes.c_long, ctypes.py_object)
GET_CSTR_OBJECT = ctypes.PYFUNCTYPE(ctypes.c_char_p, ctypes.py_object)
GET_OBJECT_PTR_OBJECT = ctypes.PYFUNCTYPE(ctypes.c_void_p, ctypes.py_object)
GET_OBJECT_PTR = ctypes.PYFUNCTYPE(ctypes.c_void_p)
NEW_TASKLET = ctypes.PYFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p, ctypes.py_object)
NEW_CHANNEL = ctypes.PYFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)
SETUP_TASKLET = ctypes.PYFUNCTYPE(ctypes.c_int, ctypes.py_object, ctypes.py_object, ctypes.c_void_p)
RUN_N_TASKLETS = ctypes.PYFUNCTYPE(ctypes.c_void_p, ctypes.c_int)

def assert_int_capi(function_pointer, expected, label):
    assert function_pointer, f"{label} function pointer is null"
    actual = GET_INT(function_pointer)()
    assert actual == expected, f"{label} returned {actual}, expected {expected}"

def assert_int_object_capi(function_pointer, target, expected, label):
    assert function_pointer, f"{label} function pointer is null"
    actual = GET_INT_OBJECT(function_pointer)(target)
    assert actual == expected, f"{label} returned {actual}, expected {expected}"

def object_from_pointer(pointer, label):
    assert pointer, f"{label} returned a null pointer"
    return ctypes.cast(pointer, ctypes.py_object).value

FLAVORS = [
    ("release", "_scheduler"),
    ("debug", "_scheduler_debug"),
    ("trinitydev", "_scheduler_trinitydev"),
    ("internal", "_scheduler_internal"),
]

for flavor, module_name in FLAVORS:
    for cached_name in ("scheduler", "_scheduler", module_name):
        sys.modules.pop(cached_name, None)
    extension = importlib.import_module(module_name)
    sys.modules["_scheduler"] = extension
    scheduler = importlib.import_module("scheduler")

    assert scheduler.bridge_status() == "pyo3_smoke"
    assert scheduler.abi_version() == 1
    assert hasattr(scheduler, "_C_API")
    py_capsule_is_valid = ctypes.pythonapi.PyCapsule_IsValid
    py_capsule_is_valid.argtypes = [ctypes.py_object, ctypes.c_char_p]
    py_capsule_is_valid.restype = ctypes.c_int
    py_capsule_get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    py_capsule_get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    py_capsule_get_pointer.restype = ctypes.c_void_p
    py_capsule_import = ctypes.pythonapi.PyCapsule_Import
    py_capsule_import.argtypes = [ctypes.c_char_p, ctypes.c_int]
    py_capsule_import.restype = ctypes.c_void_p
    py_decref = ctypes.pythonapi.Py_DecRef
    py_decref.argtypes = [ctypes.c_void_p]
    py_decref.restype = None
    assert py_capsule_is_valid(scheduler._C_API, b"scheduler._C_API") == 1
    api_pointer = py_capsule_get_pointer(scheduler._C_API, b"scheduler._C_API")
    assert api_pointer
    imported_api_pointer = py_capsule_import(b"scheduler._C_API", 0)
    assert imported_api_pointer == api_pointer
    api = ctypes.cast(api_pointer, ctypes.POINTER(SchedulerCAPI)).contents
    current = scheduler.getcurrent()
    assert api.PyScheduler_GetCurrent, "PyScheduler_GetCurrent function pointer is null"
    capi_current = object_from_pointer(
        GET_OBJECT_PTR(api.PyScheduler_GetCurrent)(),
        "PyScheduler_GetCurrent",
    )
    assert capi_current is current
    assert api.PyScheduler_GetScheduler, "PyScheduler_GetScheduler function pointer is null"
    capi_scheduler = object_from_pointer(
        GET_OBJECT_PTR(api.PyScheduler_GetScheduler)(),
        "PyScheduler_GetScheduler",
    )
    assert isinstance(capi_scheduler, scheduler.schedule_manager)
    assert capi_scheduler is scheduler.get_schedule_manager()
    assert api.PyTasklet_New, "PyTasklet_New function pointer is null"
    def capi_callable():
        return None
    capi_tasklet = object_from_pointer(
        NEW_TASKLET(api.PyTasklet_New)(api.PyTaskletType, (capi_callable,)),
        "PyTasklet_New",
    )
    assert isinstance(capi_tasklet, scheduler.tasklet)
    assert capi_tasklet.context == b"rust-pyo3-smoke:callable-bound".decode()
    assert api.PyChannel_New, "PyChannel_New function pointer is null"
    capi_channel = object_from_pointer(
        NEW_CHANNEL(api.PyChannel_New)(api.PyChannelType),
        "PyChannel_New(type)",
    )
    assert isinstance(capi_channel, scheduler.channel)
    capi_default_channel = object_from_pointer(
        NEW_CHANNEL(api.PyChannel_New)(None),
        "PyChannel_New(null)",
    )
    assert isinstance(capi_default_channel, scheduler.channel)
    assert_int_object_capi(api.PyTasklet_Check, current, 1, "PyTasklet_Check(current)")
    assert_int_object_capi(api.PyTasklet_Check, capi_tasklet, 1, "PyTasklet_Check(new)")
    assert_int_object_capi(api.PyTasklet_Check, scheduler, 0, "PyTasklet_Check(module)")
    assert_int_object_capi(api.PyTasklet_GetBlockTrap, current, 0, "PyTasklet_GetBlockTrap")
    assert api.PyTasklet_SetBlockTrap, "PyTasklet_SetBlockTrap function pointer is null"
    SET_OBJECT_INT(api.PyTasklet_SetBlockTrap)(current, 1)
    assert current.block_trap is True
    assert_int_object_capi(api.PyTasklet_GetBlockTrap, current, 1, "PyTasklet_GetBlockTrap")
    SET_OBJECT_INT(api.PyTasklet_SetBlockTrap)(current, 0)
    assert current.block_trap is False
    assert_int_object_capi(api.PyTasklet_IsMain, current, 1, "PyTasklet_IsMain")
    assert_int_object_capi(api.PyTasklet_Alive, current, 1, "PyTasklet_Alive")
    assert api.PyTasklet_GetTimesSwitchedTo, "PyTasklet_GetTimesSwitchedTo function pointer is null"
    assert GET_LONG_OBJECT(api.PyTasklet_GetTimesSwitchedTo)(current) == current.times_switched_to
    assert api.PyTasklet_GetContext, "PyTasklet_GetContext function pointer is null"
    assert GET_CSTR_OBJECT(api.PyTasklet_GetContext)(current) == current.context.encode()
    base_channel = capi_channel
    assert_int_object_capi(api.PyChannel_Check, base_channel, 1, "PyChannel_Check(channel)")
    assert_int_object_capi(api.PyChannel_Check, scheduler, 0, "PyChannel_Check(module)")
    assert_int_object_capi(api.PyChannel_GetPreference, base_channel, -1, "PyChannel_GetPreference")
    assert api.PyChannel_SetPreference, "PyChannel_SetPreference function pointer is null"
    SET_OBJECT_INT(api.PyChannel_SetPreference)(base_channel, 0)
    assert base_channel.preference == 0
    SET_OBJECT_INT(api.PyChannel_SetPreference)(base_channel, 2)
    assert base_channel.preference == 1
    SET_OBJECT_INT(api.PyChannel_SetPreference)(base_channel, -2)
    assert base_channel.preference == -1
    assert_int_object_capi(api.PyChannel_GetBalance, base_channel, 0, "PyChannel_GetBalance")
    assert api.PyChannel_GetQueue, "PyChannel_GetQueue function pointer is null"
    queue_pointer = GET_OBJECT_PTR_OBJECT(api.PyChannel_GetQueue)(base_channel)
    assert queue_pointer
    assert ctypes.cast(queue_pointer, ctypes.py_object).value is None
    py_decref(queue_pointer)
    assert_int_capi(api.PyScheduler_GetRunCount, scheduler.getruncount(), "PyScheduler_GetRunCount")
    assert_int_capi(
        api.PyScheduler_GetNumberOfActiveScheduleManagers,
        scheduler.get_number_of_active_schedule_managers(),
        "PyScheduler_GetNumberOfActiveScheduleManagers",
    )
    assert_int_capi(
        api.PyScheduler_GetNumberOfActiveChannels,
        scheduler.get_number_of_active_channels(),
        "PyScheduler_GetNumberOfActiveChannels",
    )
    assert_int_capi(
        api.PyScheduler_GetAllTimeTaskletCount,
        scheduler.get_all_time_tasklet_count(),
        "PyScheduler_GetAllTimeTaskletCount",
    )
    assert_int_capi(
        api.PyScheduler_GetActiveTaskletCount,
        scheduler.get_active_tasklet_count(),
        "PyScheduler_GetActiveTaskletCount",
    )
    assert_int_capi(
        api.PyScheduler_GetTaskletsCompletedLastRunWithTimeout,
        0,
        "PyScheduler_GetTaskletsCompletedLastRunWithTimeout",
    )
    assert_int_capi(
        api.PyScheduler_GetTaskletsSwitchedLastRunWithTimeout,
        0,
        "PyScheduler_GetTaskletsSwitchedLastRunWithTimeout",
    )
    assert api.PyTasklet_Setup, "PyTasklet_Setup function pointer is null"
    assert api.PyScheduler_RunNTasklets, "PyScheduler_RunNTasklets function pointer is null"
    run_values = []
    def capi_record(value):
        run_values.append(value)
    runnable_tasklet = object_from_pointer(
        NEW_TASKLET(api.PyTasklet_New)(api.PyTaskletType, (capi_record,)),
        "PyTasklet_New(runnable)",
    )
    assert scheduler.getruncount() == 1
    setup_result = SETUP_TASKLET(api.PyTasklet_Setup)(runnable_tasklet, (41,), None)
    assert setup_result == 0, f"PyTasklet_Setup returned {setup_result}"
    assert scheduler.getruncount() == 2
    assert runnable_tasklet.scheduled is True
    assert runnable_tasklet.alive is True
    run_result_pointer = RUN_N_TASKLETS(api.PyScheduler_RunNTasklets)(1)
    assert run_result_pointer, "PyScheduler_RunNTasklets returned null"
    assert ctypes.cast(run_result_pointer, ctypes.py_object).value is None
    py_decref(run_result_pointer)
    assert run_values == [41]
    assert scheduler.getruncount() == 1
    assert runnable_tasklet.scheduled is False
    assert runnable_tasklet.alive is False
    assert runnable_tasklet.times_switched_to == 1
    python_run_values = []
    scheduler.tasklet(lambda value: python_run_values.append(value))(42)
    assert scheduler.getruncount() == 2
    scheduler.run_n_tasklets(1)
    assert python_run_values == [42]
    assert scheduler.getruncount() == 1
    assert api.PyTaskletType
    assert api.PyChannelType
    assert api.TaskletExit
    assert ctypes.cast(api.TaskletExit, ctypes.POINTER(ctypes.c_void_p))[0] == id(scheduler.TaskletExit)
    assert scheduler.channel.__module__ == "_scheduler"
    queue = scheduler.QueueChannel()
    assert isinstance(queue, scheduler.channel)
    assert queue.preference == 1
    queue.send("import-smoke")
    assert queue.balance == 1
    assert len(queue) == 1
    assert queue.receive() == "import-smoke"
    queue.send_exception(ValueError, "queued")
    try:
        queue.receive()
    except ValueError:
        pass
    else:
        raise AssertionError("QueueChannel did not raise queued ValueError")
    assert scheduler.getcurrent().block_trap is False
    with scheduler.block_trap():
        assert scheduler.getcurrent().block_trap is True
    assert scheduler.getcurrent().block_trap is False
    print(f"scheduler import smoke ok: {flavor} -> {module_name}")
"#;

    let mut command = Command::new(&python);
    command.arg("-c").arg(script).env("PYTHONPATH", package_dir);
    if let Some(python_libdir) = python_libdir.filter(|value| !value.is_empty()) {
        let ld_library_path = match env::var("LD_LIBRARY_PATH") {
            Ok(existing) if !existing.is_empty() => format!("{python_libdir}:{existing}"),
            _ => python_libdir,
        };
        command.env("LD_LIBRARY_PATH", ld_library_path);
    }

    command.output().context("running Python import smoke")
}

fn legacy_resources() -> Result<()> {
    let command = "ctest --test-dir .cmake-build-linux-vcpkg-probe --output-on-failure";
    let started = Instant::now();
    let output = Command::new("ctest")
        .args([
            "--test-dir",
            ".cmake-build-linux-vcpkg-probe",
            "--output-on-failure",
        ])
        .current_dir("carbonengine/resources")
        .output()
        .context("running legacy resources CTest gate")?;
    let duration_ms = started.elapsed().as_millis() as u64;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let (tests_passed, tests_failed) = parse_ctest_summary(&stdout);
    let status = if output.status.success() {
        "pass"
    } else {
        "fail"
    };
    let evidence = json!({
        "schema": "carbon.evidence.gate.v1",
        "gate": "legacy-resources",
        "component": "resources",
        "implementation": "legacy_cpp",
        "status": status,
        "report_ready": output.status.success(),
        "coverage": "full_legacy_resources_ctest",
        "command": command,
        "working_directory": "carbonengine/resources",
        "duration_ms": duration_ms,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "stdout_tail": tail_lines(&stdout, 12),
        "stderr_tail": tail_lines(&stderr, 12),
    });
    let evidence_path = evidence_path("legacy-resources.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "legacy-resources: {status} ({} passed, {} failed); evidence {}",
        tests_passed.unwrap_or_default(),
        tests_failed.unwrap_or_default(),
        evidence_path.display()
    );

    ensure_success(output, "legacy resources CTest")
}

fn rust_resources() -> Result<()> {
    let command = "cargo test -p carbon-resources-core";
    let started = Instant::now();
    let cargo = env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let output = Command::new(cargo)
        .args(["test", "-p", "carbon-resources-core"])
        .output()
        .context("running Rust resources core tests")?;
    let cli_parity_cases = if output.status.success() {
        rust_resource_cli_parity_cases()
            .context("running Rust resources CLI artifact parity cases")?
    } else {
        Vec::new()
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let status = if output.status.success() {
        "pass"
    } else {
        "fail"
    };
    let evidence = json!({
        "schema": "carbon.evidence.gate.v1",
        "gate": "rust-resources",
        "component": "resources",
        "implementation": "rust",
        "status": status,
        "report_ready": false,
        "coverage": "initial_resource_tools_catalog_indicies_corpus_filter_path_matrix_filter_rule_property_matrix_platform_create_group_goldens_create_group_create_from_filter_merge_diff_remove_malformed_imports_bundle_stream_splitting_bundle_bytes_patch_manifest_payload_generation_remote_bundle_copy_only_local_apply_malformed_patch_apply_rejections_binary_patch_corruption_variants_apply_input_failure_cleanup_and_cli_artifact_slice",
        "command": format!("{command}; xtask rust resource CLI artifact parity cases"),
        "duration_ms": duration_ms,
        "stdout_tail": tail_lines(&stdout, 12),
        "stderr_tail": tail_lines(&stderr, 12),
        "cli_parity_case_count": cli_parity_cases.len(),
        "cli_parity_cases": cli_parity_cases,
        "covered_behaviors": [
            "md5 string checksum",
            "md5 file checksum",
            "md5 stream checksum over legacy file chunks",
            "legacy FNV path checksum",
            "gzip legacy fixture decompression",
            "gzip round trip",
            "gzip stream chunk compression/decompression round trip",
            "legacy FileDataStreamIn chunked read behavior",
            "legacy FileDataStreamOut byte output behavior",
            "legacy CompressedFileDataStreamOut gzip round trip behavior",
            "rolling Adler checksum formula",
            "legacy ResourceTools FindMatchingChunks string cases",
            "legacy ResourceTools FindMatchingChunk file offset cases",
            "legacy ResourceTools CountMatchingChunks patch fixture offsets",
            "legacy ChunkIndex generation and persisted index lookup",
            "legacy ChunkIndex checksum-filtered generation and lookup",
            "legacy ResourceTools bundle stream many-files-to-many-uncompressed-chunks reconstruction",
            "legacy ResourceTools bundle stream many-files-to-single-compressed-chunk reconstruction",
            "legacy YAML ResourceGroup import",
            "legacy CSV ResourceGroup import",
            "legacy CSV prefix path import",
            "byte-for-byte legacy YAML ResourceGroup export",
            "byte-for-byte skip-compression YAML ResourceGroup export",
            "byte-for-byte Linux/macOS/Windows create-group YAML fixture export with platform BinaryOperation values",
            "byte-for-byte Linux/macOS/Windows create-group skip-compression YAML fixture export with platform BinaryOperation values",
            "byte-for-byte empty YAML ResourceGroup export",
            "byte-for-byte legacy CSV ResourceGroup export",
            "byte-for-byte Linux/macOS/Windows create-group CSV and prefixed CSV fixture export with platform BinaryOperation values",
            "byte-for-byte legacy Indicies ResourceGroup v0 CSV to v0.1 YAML export",
            "byte-for-byte legacy Indicies binary-operation v0 CSV to v0.1 YAML export",
            "byte-for-byte legacy Indicies YAML ResourceGroup corpus roundtrip",
            "legacy filter prefix map parsing",
            "legacy filter wildcard matching",
            "legacy filter generated wildcard and ellipsis path matrix",
            "legacy filter generated include/exclude section-property matrix",
            "legacy filter include/exclude rules",
            "legacy filter local respath rule quirks",
            "create ResourceGroup from directory",
            "create ResourceGroup skip-compression output",
            "create ResourceGroup legacy CSV sorted output",
            "create ResourceGroup legacy CSV prefix output",
            "merge ResourceGroup YAML additive output",
            "merge ResourceGroup YAML identical output",
            "merge ResourceGroup legacy CSV additive output",
            "merge ResourceGroup legacy CSV intersect output",
            "diff ResourceGroup legacy CSV additions output",
            "diff ResourceGroup legacy CSV changes output",
            "diff ResourceGroup legacy CSV subtractions output",
            "remove ResourceGroup YAML output",
            "remove ResourceGroup missing path handling",
            "future-minor YAML ResourceGroup version clamp",
            "future-major YAML ResourceGroup version rejection",
            "missing required YAML ResourceGroup parameter rejection",
            "invalid YAML parse result mapping",
            "empty legacy CSV ResourceGroup import",
            "nonsense legacy CSV rejection",
            "invalid legacy CSV size field rejection",
            "out-of-range legacy CSV binary operation rejection",
            "legacy BundleGroup manifest import",
            "byte-for-byte BundleGroup create-bundle local CDN manifest export",
            "byte-for-byte BundleGroup create-bundle remote CDN manifest export",
            "byte-for-byte BundleGroup unpack fixture manifest export",
            "legacy local BundleGroup create manifest and chunk byte output",
            "legacy remote CDN BundleGroup create manifest and compressed payload byte output",
            "legacy local BundleGroup unpack resource byte output",
            "legacy local BundleGroup 42-chunk boundary unpack with 41 full chunks and one tail chunk",
            "legacy compressed remote CDN BundleGroup unpack resource byte output",
            "legacy remote CDN BundleGroup local mirror cache hit, bad-cache replacement, and checksum failure behavior",
            "legacy PatchGroup manifest import",
            "byte-for-byte PatchGroup create-patch manifest export",
            "byte-for-byte PatchGroup chunked and old manifest export",
            "legacy PatchGroup local CDN patch payload byte checks",
            "legacy PatchGroup chunked local CDN patch payload byte checks",
            "legacy PatchGroup normal local patch payload generation byte parity",
            "legacy PatchGroup chunked local patch payload generation byte parity",
            "legacy PatchGroup old-layout local patch payload generation byte parity",
            "legacy PatchGroup copy-only local patch creation and apply semantics without binary patch payloads",
            "legacy binary patch corruption rejection for short payloads, invalid headers, truncated streams, and target-length mismatches",
            "legacy PatchGroup malformed local apply manifest rejection for zero apply chunk size, target offset overflow, source range overflow, and overlapping copy ranges",
            "legacy PatchGroup local CDN patch apply byte checks",
            "legacy PatchGroup chunked local CDN patch apply byte checks",
            "legacy PatchGroup old-layout local CDN patch apply byte checks",
            "legacy filter index mapping import",
            "CreateFromFilter ResourceGroup output",
            "CreateFromFilter mapping-driven ResourceGroup output",
            "Rust resources legacy-style CLI no-command and invalid-operation exit-code parity",
            "Rust resources legacy-style top-level help header and operation-list shape for no command, invalid command, and --help",
            "Rust resources legacy-style operation-specific help shape for all resource commands",
            "Rust resources legacy-style CLI invalid-argument and runtime-failure exit-code parity",
            "Rust resource CLI create-group YAML artifact parity",
            "Rust resource CLI create-group CSV artifact parity",
            "Rust resource CLI create-group prefixed CSV artifact parity",
            "Rust resource CLI create-group skip-compression YAML artifact parity",
            "Rust resource CLI create-group-from-filter YAML artifact parity",
            "Rust resource CLI create-group export-resources local-relative artifact parity",
            "Rust resource CLI create-group-from-filter export-resources local-relative artifact parity",
            "Rust resource CLI merge-group YAML and CSV artifact parity",
            "Rust resource CLI diff-group additions/changes/subtractions artifact parity",
            "Rust resource CLI remove-resources artifact parity and ignore-missing success path",
            "Rust resource CLI create-bundle local manifest and local-CDN payload artifact parity",
            "Rust resource CLI create-bundle remote-CDN compressed payload artifact parity",
            "Rust resource CLI create-and-unpack local bundle roundtrip payload artifact parity",
            "Rust resource CLI create-bundle zero chunk size failure leaves no output trees",
            "Rust resource CLI create-bundle missing resource source failure leaves no output trees",
            "Rust resource CLI unpack-bundle local resource payload and manifest artifact parity",
            "Rust resource CLI unpack-bundle chunk boundary payload/checksum evidence",
            "Rust resource CLI unpack-bundle missing-chunk failure leaves no output tree",
            "Rust resource CLI unpack-bundle remote-requested-local-chunks failure leaves no output tree",
            "Rust resource CLI unpack-bundle local-requested-remote-compressed-chunks failure leaves no output tree",
            "Rust resource CLI unpack-bundle remote-CDN compressed resource payload and manifest artifact parity",
            "Rust resource CLI unpack-bundle remote-CDN mirror download then cache-hit stats",
            "Rust resource CLI create-patch local manifest and local-CDN payload artifact parity",
            "Rust resource CLI create-patch zero chunk size failure leaves no output tree",
            "Rust resource CLI create-patch missing previous resource source failure leaves no output tree",
            "Rust resource CLI create-patch missing next resource source failure leaves no output tree",
            "Rust resource CLI create-patch no-change empty manifest and payload artifact parity",
            "Rust resource CLI create-patch chunked local manifest and local-CDN payload artifact parity",
            "Rust resource CLI create/apply copy-only patch records without binary patch payloads",
            "Rust resource CLI apply-patch local resource payload artifact parity",
            "Rust resource CLI apply-patch chunked local resource payload artifact parity",
            "Rust resource CLI apply-patch old-layout local resource payload artifact parity",
            "Rust resource CLI apply-patch missing previous resources failure leaves no output tree",
            "Rust resource CLI apply-patch missing next resources failure leaves no output tree",
            "Rust resource CLI apply-patch missing patch payload failure leaves no output tree",
            "Rust resource CLI apply-patch corrupt patch payload failure leaves no output tree"
        ],
        "remaining_before_report_ready": [
            "broader bundle corpus across destination types and CLI failure behavior beyond zero-chunk, missing-source, missing-chunk, remote-requested-local, and local-requested-remote failure cleanup",
            "broader patch temp-file cleanup modes and larger patch corpus beyond the normal/no-change/chunked/old-layout/copy-only/missing-previous/missing-next/missing-payload/corrupt-payload/malformed-manifest/binary-corruption apply-patch and zero-chunk/missing-source local create-patch cases",
            "broader filter-file corpus and fuzz coverage beyond the generated wildcard and rule-property matrices",
            "network-backed remote/catalog behavior beyond local mirror/cache bundle retrieval and CLI remote-CDN cache evidence",
            "broader detailed CLI output/error text compatibility, broader apply-patch/unpack-bundle corpus, and broader CLI failure behavior"
        ]
    });
    let evidence_path = evidence_path("rust-resources.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "rust-resources: {status} (initial resource tools/catalog/Indicies-corpus/filter/create-group/create-from-filter/merge/diff/remove/malformed-imports/bundle/patch-manifest-payload-generation-remote-bundle-local-apply-and-cli-artifact slice); evidence {}",
        evidence_path.display()
    );

    ensure_success(output, "Rust resources core tests")
}

fn rust_resource_cli_parity_cases() -> Result<Vec<Value>> {
    let runner = env::current_exe().context("resolving current xtask executable")?;
    let repo_root = env::current_dir().context("resolving repository root")?;
    let output_root = Path::new("target/carbon/evidence/rust-resources-cli");
    fs::remove_dir_all(output_root).ok();
    fs::create_dir_all(output_root)
        .with_context(|| format!("creating {}", output_root.display()))?;

    let create_input =
        Path::new("carbonengine/resources/tests/testData/CreateResourceFiles/ResourceFiles");
    let create_expected_yaml = Path::new(
        "carbonengine/resources/tests/testData/CreateResourceFiles/ResourceGroupLinux.yaml",
    );
    let create_expected_skip = Path::new(
        "carbonengine/resources/tests/testData/CreateResourceFiles/ResourceGroupSkipCompressionLinux.yaml",
    );
    let create_expected_csv = Path::new(
        "carbonengine/resources/tests/testData/CreateResourceFiles/ResourceGroupLinux.csv",
    );
    let create_expected_prefixed_csv = Path::new(
        "carbonengine/resources/tests/testData/CreateResourceFiles/ResourceGroupLinuxPrefixed.csv",
    );

    let mut cases = Vec::new();
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunWithoutArguments",
        &[],
        4,
        "legacy_cli_exit_code_no_command_specified",
    )?);
    cases.push(run_rust_resource_legacy_cli_help_shape_case(
        &runner,
        "ResourcesCliTest.RunWithoutArgumentsHelpShape",
        &[],
        4,
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunWithNonesenseArguments",
        &[OsString::from("Nonesense")],
        3,
        "legacy_cli_exit_code_invalid_operation",
    )?);
    cases.push(run_rust_resource_legacy_cli_help_shape_case(
        &runner,
        "ResourcesCliTest.RunWithNonesenseArgumentsHelpShape",
        &[OsString::from("Nonesense")],
        3,
    )?);
    cases.push(run_rust_resource_legacy_cli_help_shape_case(
        &runner,
        "ResourcesCliTest.RunHelpShape",
        &[OsString::from("--help")],
        3,
    )?);
    for (legacy_test, operation) in [
        ("ResourcesCliTest.RunCreateGroupHelpShape", "create-group"),
        (
            "ResourcesCliTest.RunCreateGroupFromFilterHelpShape",
            "create-group-from-filter",
        ),
        ("ResourcesCliTest.RunCreatePatchHelpShape", "create-patch"),
        ("ResourcesCliTest.RunCreateBundleHelpShape", "create-bundle"),
        ("ResourcesCliTest.RunMergeGroupHelpShape", "merge-group"),
        ("ResourcesCliTest.RunDiffGroupHelpShape", "diff-group"),
        (
            "ResourcesCliTest.RunRemoveResourcesHelpShape",
            "remove-resources",
        ),
        ("ResourcesCliTest.RunApplyPatchHelpShape", "apply-patch"),
        ("ResourcesCliTest.RunUnpackBundleHelpShape", "unpack-bundle"),
    ] {
        cases.push(run_rust_resource_legacy_cli_operation_help_shape_case(
            &runner,
            legacy_test,
            operation,
        )?);
    }
    for (legacy_test, operation) in [
        (
            "ResourcesCliTest.RunCreateGroupWithNoArgumentsHelpShape",
            "create-group",
        ),
        (
            "ResourcesCliTest.RunCreateGroupFromFilterWithNoArgumentsHelpShape",
            "create-group-from-filter",
        ),
        (
            "ResourcesCliTest.RunCreatePatchWithNoArgumentsHelpShape",
            "create-patch",
        ),
        (
            "ResourcesCliTest.RunCreateBundleWithNoArgumentsHelpShape",
            "create-bundle",
        ),
        (
            "ResourcesCliTest.RunMergeGroupWithNoArgumentsHelpShape",
            "merge-group",
        ),
        (
            "ResourcesCliTest.RunDiffGroupWithNoArgumentsHelpShape",
            "diff-group",
        ),
        (
            "ResourcesCliTest.RunRemoveResourcesWithNoArgumentsHelpShape",
            "remove-resources",
        ),
        (
            "ResourcesCliTest.RunApplyPatchWithNoArgumentsHelpShape",
            "apply-patch",
        ),
        (
            "ResourcesCliTest.RunUnpackBundleWithNoArgumentsHelpShape",
            "unpack-bundle",
        ),
    ] {
        cases.push(run_rust_resource_legacy_cli_operation_usage_shape_case(
            &runner,
            legacy_test,
            operation,
        )?);
    }
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunCreateGroupWithNoArguments",
        &[OsString::from("create-group")],
        2,
        "legacy_cli_exit_code_invalid_operation_arguments",
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunCreatePatchWithNoArguments",
        &[OsString::from("create-patch")],
        2,
        "legacy_cli_exit_code_invalid_operation_arguments",
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunCreateBundleWithNoArguments",
        &[OsString::from("create-bundle")],
        2,
        "legacy_cli_exit_code_invalid_operation_arguments",
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunApplyPatchWithNoArguments",
        &[OsString::from("apply-patch")],
        2,
        "legacy_cli_exit_code_invalid_operation_arguments",
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RunUnpackBundleWithNoArguments",
        &[OsString::from("unpack-bundle")],
        2,
        "legacy_cli_exit_code_invalid_operation_arguments",
    )?);
    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.CreateOperationWithInvalidInput",
        &[
            OsString::from("create-group"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            OsString::from("INVALID_PATH"),
        ],
        1,
        "legacy_cli_exit_code_valid_operation_runtime_failure",
    )?);
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromDirectory",
        &[
            OsString::from("rust-create-group"),
            create_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_root.join("create-group.yaml").into_os_string(),
        ],
        &output_root.join("create-group.yaml"),
        create_expected_yaml,
    )?);
    let create_group_export_work_dir = output_root.join("create-group-export-work");
    fs::create_dir_all(&create_group_export_work_dir)
        .with_context(|| format!("creating {}", create_group_export_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromDirectoryExportResources",
        &create_group_export_work_dir,
        &[
            OsString::from("rust-create-group"),
            repo_root
                .join("carbonengine/resources/tests/testData/CreateResourceFiles/ResourceFiles")
                .into_os_string(),
            OsString::from("--output-file"),
            OsString::from("GroupOut/ResourceGroup.yaml"),
            OsString::from("--export-resources"),
            OsString::from("--export-resources-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--export-resources-destination-path"),
            OsString::from("ExportedResources"),
        ],
        &create_group_export_work_dir.join("GroupOut/ResourceGroup.yaml"),
        create_expected_yaml,
        &create_group_export_work_dir.join("ExportedResources"),
        create_input,
    )?);
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromDirectoryWithSkipCompression",
        &[
            OsString::from("rust-create-group"),
            OsString::from("--skip-compression"),
            create_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_root
                .join("create-group-skip-compression.yaml")
                .into_os_string(),
        ],
        &output_root.join("create-group-skip-compression.yaml"),
        create_expected_skip,
    )?);
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromDirectoryOldDocumentFormat",
        &[
            OsString::from("rust-create-group"),
            create_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_root.join("create-group.csv").into_os_string(),
            OsString::from("--document-version"),
            OsString::from("0.0.0"),
        ],
        &output_root.join("create-group.csv"),
        create_expected_csv,
    )?);
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromDirectoryOldDocumentFormatWithPrefix",
        &[
            OsString::from("rust-create-group"),
            create_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_root
                .join("create-group-prefixed.csv")
                .into_os_string(),
            OsString::from("--document-version"),
            OsString::from("0.0.0"),
            OsString::from("--resource-prefix"),
            OsString::from("test"),
        ],
        &output_root.join("create-group-prefixed.csv"),
        create_expected_prefixed_csv,
    )?);

    let filter_output_dir = output_root.join("create-group-from-filter");
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromFilter",
        &[
            OsString::from("rust-create-group-from-filter"),
            OsString::from("--filter-index-mapping-file"),
            OsString::from(
                "carbonengine/resources/tests/testData/FilterFiles/resFilterIndexMapping.yaml",
            ),
            OsString::from("--filter-file-basepath"),
            OsString::from("carbonengine/resources/tests/testData/FilterFiles"),
            OsString::from("--prefix-map-basepath"),
            create_input.as_os_str().to_os_string(),
            OsString::from("--output-directory"),
            filter_output_dir.as_os_str().to_os_string(),
        ],
        &filter_output_dir.join("ResourceGroup.yaml"),
        create_expected_yaml,
    )?);
    let filter_export_work_dir = output_root.join("create-group-from-filter-export-work");
    fs::create_dir_all(&filter_export_work_dir)
        .with_context(|| format!("creating {}", filter_export_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.CreateResourceGroupFromFilterExportResources",
        &filter_export_work_dir,
        &[
            OsString::from("rust-create-group-from-filter"),
            OsString::from("--filter-index-mapping-file"),
            repo_root
                .join(
                    "carbonengine/resources/tests/testData/FilterFiles/resFilterIndexMapping.yaml",
                )
                .into_os_string(),
            OsString::from("--filter-file-basepath"),
            repo_root
                .join("carbonengine/resources/tests/testData/FilterFiles")
                .into_os_string(),
            OsString::from("--prefix-map-basepath"),
            repo_root
                .join("carbonengine/resources/tests/testData/CreateResourceFiles/ResourceFiles")
                .into_os_string(),
            OsString::from("--output-directory"),
            OsString::from("GroupOut"),
            OsString::from("--export-resources"),
            OsString::from("--export-resources-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--export-resources-destination-path"),
            OsString::from("ExportedResources"),
            OsString::from("--number-of-threads"),
            OsString::from("0"),
        ],
        &filter_export_work_dir.join("GroupOut/ResourceGroup.yaml"),
        create_expected_yaml,
        &filter_export_work_dir.join("ExportedResources"),
        create_input,
    )?);

    let create_bundle_work_dir = output_root.join("create-bundle-work");
    fs::create_dir_all(&create_bundle_work_dir)
        .with_context(|| format!("creating {}", create_bundle_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.CreateBundle",
        &create_bundle_work_dir,
        &[
            OsString::from("rust-create-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt")
                .into_os_string(),
            OsString::from("--resource-source-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/Res")
                .into_os_string(),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroup.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOut"),
            OsString::from("--chunk-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("1000"),
        ],
        &create_bundle_work_dir.join("BundleOut/BundleResourceGroup.yaml"),
        Path::new("carbonengine/resources/tests/testData/CreateBundle/BundleResourceGroup.yaml"),
        &create_bundle_work_dir.join("CreateBundleOut"),
        Path::new("carbonengine/resources/tests/testData/CreateBundle/CreateBundleOut"),
    )?);
    let create_and_unpack_bundle_work_dir = output_root.join("create-and-unpack-bundle-work");
    fs::create_dir_all(&create_and_unpack_bundle_work_dir)
        .with_context(|| format!("creating {}", create_and_unpack_bundle_work_dir.display()))?;
    cases.push(run_rust_resource_cli_create_and_unpack_bundle_case(
        &runner,
        "ResourcesLibraryTest.CreateAndUnpackBundle",
        &create_and_unpack_bundle_work_dir,
        &repo_root.join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt"),
        &repo_root.join("carbonengine/resources/tests/testData/Bundle/Res"),
    )?);

    let create_bundle_remote_work_dir = output_root.join("create-bundle-remote-work");
    fs::create_dir_all(&create_bundle_remote_work_dir)
        .with_context(|| format!("creating {}", create_bundle_remote_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.CreateBundleRemoteCDN",
        &create_bundle_remote_work_dir,
        &[
            OsString::from("rust-create-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt")
                .into_os_string(),
            OsString::from("--resource-source-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/Res")
                .into_os_string(),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroupRemoteCDN.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOutRemoteCDN"),
            OsString::from("--chunk-destination-type"),
            OsString::from("REMOTE_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("1000"),
        ],
        &create_bundle_remote_work_dir.join("BundleOut/BundleResourceGroupRemoteCDN.yaml"),
        Path::new(
            "carbonengine/resources/tests/testData/CreateBundle/BundleResourceGroupRemoteCDN.yaml",
        ),
        &create_bundle_remote_work_dir.join("CreateBundleOutRemoteCDN"),
        Path::new("carbonengine/resources/tests/testData/CreateBundle/CreateBundleOutRemoteCDN"),
    )?);
    let create_bundle_zero_chunk_work_dir = output_root.join("create-bundle-zero-chunk-work");
    fs::create_dir_all(&create_bundle_zero_chunk_work_dir)
        .with_context(|| format!("creating {}", create_bundle_zero_chunk_work_dir.display()))?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.CreateBundleWithZeroChunkSizeFails",
        &create_bundle_zero_chunk_work_dir,
        &[
            OsString::from("rust-create-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt")
                .into_os_string(),
            OsString::from("--resource-source-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/Res")
                .into_os_string(),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroup.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOut"),
            OsString::from("--chunk-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("0"),
        ],
        &[
            create_bundle_zero_chunk_work_dir.join("BundleOut"),
            create_bundle_zero_chunk_work_dir.join("CreateBundleOut"),
        ],
    )?);
    let create_bundle_missing_source_work_dir =
        output_root.join("create-bundle-missing-source-work");
    fs::create_dir_all(&create_bundle_missing_source_work_dir).with_context(|| {
        format!(
            "creating {}",
            create_bundle_missing_source_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.CreateBundleMissingResourceSourceFails",
        &create_bundle_missing_source_work_dir,
        &[
            OsString::from("rust-create-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt")
                .into_os_string(),
            OsString::from("--resource-source-path"),
            OsString::from("MissingResources"),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroup.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOut"),
            OsString::from("--chunk-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("1000"),
        ],
        &[
            create_bundle_missing_source_work_dir.join("BundleOut"),
            create_bundle_missing_source_work_dir.join("CreateBundleOut"),
        ],
    )?);

    let unpack_bundle_work_dir = output_root.join("unpack-bundle-work");
    fs::create_dir_all(&unpack_bundle_work_dir)
        .with_context(|| format!("creating {}", unpack_bundle_work_dir.display()))?;
    cases.push(run_rust_resource_cli_roundtrip_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.UnpackBundle",
        &unpack_bundle_work_dir,
        &[
            OsString::from("rust-unpack-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/BundleResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--chunk-source-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/LocalRemoteChunks")
                .into_os_string(),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--output-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        &unpack_bundle_work_dir.join("UnpackBundleOut/ResourceGroup.yaml"),
        &unpack_bundle_work_dir.join("UnpackBundleOut"),
        Path::new("carbonengine/resources/tests/testData/Bundle/Res"),
    )?);
    let unpack_bundle_boundary_work_dir = output_root.join("unpack-bundle-boundary-work");
    fs::create_dir_all(&unpack_bundle_boundary_work_dir)
        .with_context(|| format!("creating {}", unpack_bundle_boundary_work_dir.display()))?;
    cases.push(run_rust_resource_cli_bundle_boundary_case(
        &runner,
        "ResourcesLibraryTest.UnpackBundleChunkBoundary",
        &unpack_bundle_boundary_work_dir,
        repo_root.join("carbonengine/resources/tests/testData/Bundle/BundleResourceGroup.yaml"),
        repo_root.join("carbonengine/resources/tests/testData/Bundle/LocalRemoteChunks"),
        repo_root.join("carbonengine/resources/tests/testData/Bundle/Res"),
    )?);
    let unpack_bundle_missing_chunk_work_dir = output_root.join("unpack-bundle-missing-chunk-work");
    let unpack_bundle_empty_chunk_dir = unpack_bundle_missing_chunk_work_dir.join("EmptyChunks");
    fs::create_dir_all(&unpack_bundle_empty_chunk_dir)
        .with_context(|| format!("creating {}", unpack_bundle_empty_chunk_dir.display()))?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.UnpackBundleMissingChunkFails",
        &unpack_bundle_missing_chunk_work_dir,
        &[
            OsString::from("rust-unpack-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/BundleResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--chunk-source-base-path"),
            OsString::from("EmptyChunks"),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--output-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        &[unpack_bundle_missing_chunk_work_dir.join("UnpackBundleOut")],
    )?);
    let unpack_bundle_remote_requested_local_work_dir =
        output_root.join("unpack-bundle-remote-requested-local-work");
    fs::create_dir_all(&unpack_bundle_remote_requested_local_work_dir).with_context(|| {
        format!(
            "creating {}",
            unpack_bundle_remote_requested_local_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.UnpackBundleExpectingRemoteCdnButPassedLocalCdn",
        &unpack_bundle_remote_requested_local_work_dir,
        &[
            OsString::from("rust-unpack-bundle"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/BundleResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--chunk-source-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Bundle/LocalRemoteChunks")
                .into_os_string(),
            OsString::from("--chunk-source-type"),
            OsString::from("REMOTE_CDN"),
            OsString::from("--remote-cache-base-path"),
            OsString::from("RemoteCache"),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--output-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        &[unpack_bundle_remote_requested_local_work_dir.join("UnpackBundleOut")],
    )?);
    let unpack_remote_bundle_as_local_work_dir =
        output_root.join("unpack-remote-bundle-as-local-work");
    fs::create_dir_all(&unpack_remote_bundle_as_local_work_dir).with_context(|| {
        format!(
            "creating {}",
            unpack_remote_bundle_as_local_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.UnpackRemoteBundleAsLocal",
        &unpack_remote_bundle_as_local_work_dir,
        &[
            OsString::from("rust-unpack-bundle"),
            repo_root
                .join(
                    "carbonengine/resources/tests/testData/CreateBundle/BundleResourceGroupRemoteCDN.yaml",
                )
                .into_os_string(),
            OsString::from("--chunk-source-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/CreateBundle/CreateBundleOutRemoteCDN")
                .into_os_string(),
            OsString::from("--chunk-source-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--output-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        &[unpack_remote_bundle_as_local_work_dir.join("UnpackBundleOut")],
    )?);
    let unpack_bundle_remote_work_dir = output_root.join("unpack-bundle-remote-work");
    fs::create_dir_all(&unpack_bundle_remote_work_dir)
        .with_context(|| format!("creating {}", unpack_bundle_remote_work_dir.display()))?;
    cases.push(run_rust_resource_cli_remote_cdn_unpack_cache_case(
        &runner,
        "ResourcesLibraryTest.UnpackBundleRemoteCDN",
        &unpack_bundle_remote_work_dir,
        repo_root.join(
            "carbonengine/resources/tests/testData/CreateBundle/BundleResourceGroupRemoteCDN.yaml",
        ),
        repo_root
            .join("carbonengine/resources/tests/testData/CreateBundle/CreateBundleOutRemoteCDN"),
        Path::new("carbonengine/resources/tests/testData/Bundle/Res"),
    )?);

    let create_patch_work_dir = output_root.join("create-patch-work");
    fs::create_dir_all(&create_patch_work_dir)
        .with_context(|| format!("creating {}", create_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesCliTest.CreatePatch",
        &create_patch_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt")
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_next.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--resource-source-base-path-next"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        &create_patch_work_dir.join("PatchOut/PatchResourceGroup.yaml"),
        Path::new("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml"),
        &create_patch_work_dir.join("PatchOut/Patches"),
        Path::new("carbonengine/resources/tests/testData/Patch/LocalCDNPatches"),
    )?);
    let create_patch_zero_chunk_work_dir = output_root.join("create-patch-zero-chunk-work");
    fs::create_dir_all(&create_patch_zero_chunk_work_dir)
        .with_context(|| format!("creating {}", create_patch_zero_chunk_work_dir.display()))?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.CreatePatchWithZeroChunkSizeFails",
        &create_patch_zero_chunk_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join(
                    "carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt",
                )
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_next.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--resource-source-base-path-next"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("0"),
        ],
        &[create_patch_zero_chunk_work_dir.join("PatchOut")],
    )?);
    let create_patch_missing_previous_work_dir =
        output_root.join("create-patch-missing-previous-work");
    fs::create_dir_all(&create_patch_missing_previous_work_dir).with_context(|| {
        format!(
            "creating {}",
            create_patch_missing_previous_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.CreatePatchMissingPreviousResourcesFails",
        &create_patch_missing_previous_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join(
                    "carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt",
                )
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_next.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            OsString::from("MissingPreviousResources"),
            OsString::from("--resource-source-base-path-next"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        &[create_patch_missing_previous_work_dir.join("PatchOut")],
    )?);
    let create_patch_missing_next_work_dir = output_root.join("create-patch-missing-next-work");
    fs::create_dir_all(&create_patch_missing_next_work_dir)
        .with_context(|| format!("creating {}", create_patch_missing_next_work_dir.display()))?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.CreatePatchMissingNextResourcesFails",
        &create_patch_missing_next_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join(
                    "carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt",
                )
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_next.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--resource-source-base-path-next"),
            OsString::from("MissingNextResources"),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        &[create_patch_missing_next_work_dir.join("PatchOut")],
    )?);

    let create_no_change_patch_work_dir = output_root.join("create-patch-no-change-work");
    fs::create_dir_all(&create_no_change_patch_work_dir)
        .with_context(|| format!("creating {}", create_no_change_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_no_change_patch_case(
        &runner,
        "ResourcesLibraryTest.CreatePatchWhereBuildsHaveNoChanges",
        &create_no_change_patch_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt")
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--resource-source-base-path-next"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        &create_no_change_patch_work_dir.join("PatchOut/PatchResourceGroup.yaml"),
        &create_no_change_patch_work_dir.join("PatchOut/Patches"),
    )?);

    let create_chunked_patch_work_dir = output_root.join("create-patch-chunked-work");
    fs::create_dir_all(&create_chunked_patch_work_dir)
        .with_context(|| format!("creating {}", create_chunked_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_manifest_and_directory_case(
        &runner,
        "ResourcesLibraryTest.CreatePatchWithChunking",
        &create_chunked_patch_work_dir,
        &[
            OsString::from("rust-create-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/resFileIndexShort_build_previous.txt")
                .into_os_string(),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/resFileIndexShort_build_next.txt")
                .into_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--resource-source-base-path-next"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/NextBuildResources")
                .into_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--resource-group-relative-path"),
            OsString::from("ResourceGroup_previousBuild_latestBuild.yaml"),
            OsString::from("--patch-file-relative-path-prefix"),
            OsString::from("Patches/Patch1"),
            OsString::from("--chunk-size"),
            OsString::from("500"),
        ],
        &create_chunked_patch_work_dir.join("PatchOut/PatchResourceGroup.yaml"),
        Path::new(
            "carbonengine/resources/tests/testData/PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml",
        ),
        &create_chunked_patch_work_dir.join("PatchOut/Patches"),
        Path::new("carbonengine/resources/tests/testData/PatchWithInputChunk/LocalCDNPatches"),
    )?);

    let copy_only_patch_work_dir = output_root.join("create-apply-copy-only-patch-work");
    fs::create_dir_all(&copy_only_patch_work_dir)
        .with_context(|| format!("creating {}", copy_only_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_copy_only_patch_case(
        &runner,
        "ResourcesLibraryTest.CreateAndApplyPatchCopyOnlyRecords",
        &copy_only_patch_work_dir,
    )?);

    let apply_patch_work_dir = output_root.join("apply-patch-work");
    fs::create_dir_all(&apply_patch_work_dir)
        .with_context(|| format!("creating {}", apply_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_directory_case(
        &runner,
        "ResourcesCliTest.ApplyPatch",
        &apply_patch_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/LocalCDNPatches")
                .into_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &apply_patch_work_dir.join("ApplyPatchOut"),
        Path::new("carbonengine/resources/tests/testData/Patch/NextBuildResources"),
    )?);
    let apply_chunked_patch_work_dir = output_root.join("apply-patch-chunked-work");
    fs::create_dir_all(&apply_chunked_patch_work_dir)
        .with_context(|| format!("creating {}", apply_chunked_patch_work_dir.display()))?;
    cases.push(run_rust_resource_cli_directory_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchWithChunking",
        &apply_chunked_patch_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/LocalCDNPatches")
                .into_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/PatchWithInputChunk/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &apply_chunked_patch_work_dir.join("ApplyPatchOut"),
        Path::new("carbonengine/resources/tests/testData/PatchWithInputChunk/NextBuildResources"),
    )?);

    let apply_old_patch_work_dir = output_root.join("apply-patch-old-work");
    fs::create_dir_all(&apply_old_patch_work_dir)
        .with_context(|| format!("creating {}", apply_old_patch_work_dir.display()))?;
    let apply_old_patch_expected_dir = apply_old_patch_work_dir.join("ExpectedOldPatchOut");
    prepare_legacy_old_patch_apply_expected_directory(&repo_root, &apply_old_patch_expected_dir)?;
    cases.push(run_rust_resource_cli_directory_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchOldLayoutFixture",
        &apply_old_patch_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/Old/PatchResourceGroup_previousBuild_latestBuild.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/Old/LocalCDNPatches")
                .into_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &apply_old_patch_work_dir.join("ApplyPatchOut"),
        &apply_old_patch_expected_dir,
    )?);

    let apply_patch_missing_previous_work_dir =
        output_root.join("apply-patch-missing-previous-work");
    fs::create_dir_all(&apply_patch_missing_previous_work_dir).with_context(|| {
        format!(
            "creating {}",
            apply_patch_missing_previous_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchMissingPreviousResourcesFails",
        &apply_patch_missing_previous_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/LocalCDNPatches")
                .into_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            OsString::from("MissingPreviousResources"),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &[apply_patch_missing_previous_work_dir.join("ApplyPatchOut")],
    )?);

    let apply_patch_missing_next_work_dir = output_root.join("apply-patch-missing-next-work");
    fs::create_dir_all(&apply_patch_missing_next_work_dir)
        .with_context(|| format!("creating {}", apply_patch_missing_next_work_dir.display()))?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchMissingNextResourcesFails",
        &apply_patch_missing_next_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/LocalCDNPatches")
                .into_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            OsString::from("MissingNextResources"),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &[apply_patch_missing_next_work_dir.join("ApplyPatchOut")],
    )?);

    let apply_patch_missing_payload_work_dir = output_root.join("apply-patch-missing-payload-work");
    fs::create_dir_all(&apply_patch_missing_payload_work_dir).with_context(|| {
        format!(
            "creating {}",
            apply_patch_missing_payload_work_dir.display()
        )
    })?;
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchMissingPatchPayloadFails",
        &apply_patch_missing_payload_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            OsString::from("MissingLocalCDNPatches"),
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &[apply_patch_missing_payload_work_dir.join("ApplyPatchOut")],
    )?);

    let apply_patch_corrupt_payload_work_dir = output_root.join("apply-patch-corrupt-payload-work");
    fs::create_dir_all(&apply_patch_corrupt_payload_work_dir).with_context(|| {
        format!(
            "creating {}",
            apply_patch_corrupt_payload_work_dir.display()
        )
    })?;
    let corrupt_patch_payload_dir = prepare_corrupt_legacy_patch_payload_directory(
        &repo_root,
        &apply_patch_corrupt_payload_work_dir,
    )?;
    let corrupt_patch_payload_dir_name = corrupt_patch_payload_dir
        .file_name()
        .ok_or_else(|| anyhow!("corrupt patch payload directory has no final component"))?
        .to_os_string();
    cases.push(run_rust_resource_cli_failure_no_output_case(
        &runner,
        "ResourcesLibraryTest.ApplyPatchCorruptPatchPayloadFails",
        &apply_patch_corrupt_payload_work_dir,
        &[
            OsString::from("rust-apply-patch"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml")
                .into_os_string(),
            OsString::from("--patch-binaries-base-path"),
            corrupt_patch_payload_dir_name,
            OsString::from("--resources-to-patch-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources")
                .into_os_string(),
            OsString::from("--next-resources-base-path"),
            repo_root
                .join("carbonengine/resources/tests/testData/Patch/NextBuildResources")
                .into_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        &[apply_patch_corrupt_payload_work_dir.join("ApplyPatchOut")],
    )?);

    let merge_yaml_base = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/BaseResourceGroup.yaml",
    );
    let merge_yaml_input = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/MergeResourceGroup.yaml",
    );
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.MergeGroup",
        &[
            OsString::from("rust-merge-group"),
            merge_yaml_base.as_os_str().to_os_string(),
            merge_yaml_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            output_root.join("merge-group.yaml").into_os_string(),
        ],
        &output_root.join("merge-group.yaml"),
        Path::new(
            "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/ExpectedMergedResourceGroup.yaml",
        ),
    )?);

    let merge_csv_base = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/CSVAdditive/BaseResourceGroup.txt",
    );
    let merge_csv_input = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/CSVAdditive/MergeResourceGroup.txt",
    );
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesLibraryTest.MergeResourceGroupCSVAdditive",
        &[
            OsString::from("rust-merge-group"),
            merge_csv_base.as_os_str().to_os_string(),
            merge_csv_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            output_root.join("merge-group.txt").into_os_string(),
            OsString::from("--document-version"),
            OsString::from("0.0.0"),
        ],
        &output_root.join("merge-group.txt"),
        Path::new(
            "carbonengine/resources/tests/testData/MergeGroups/CSVAdditive/ExpectedMergedResourceGroup.txt",
        ),
    )?);

    let diff_base = Path::new("carbonengine/resources/tests/testData/DiffGroups/resFileIndex.txt");
    for (legacy_test, target, expected, output_name) in [
        (
            "ResourcesCliTest.DiffResourceGroupsWithTwoAdditions",
            "carbonengine/resources/tests/testData/DiffGroups/resFileIndexWithAdditions.txt",
            "carbonengine/resources/tests/testData/DiffGroups/ExpectedDiffWithAdditions.txt",
            "diff-additions.txt",
        ),
        (
            "ResourcesCliTest.DiffResourceGroupsWithTwoChanges",
            "carbonengine/resources/tests/testData/DiffGroups/resFileIndexWithChanges.txt",
            "carbonengine/resources/tests/testData/DiffGroups/ExpectedDiffWithChanges.txt",
            "diff-changes.txt",
        ),
        (
            "ResourcesCliTest.DiffResourceGroupsWithTwoSubtractions",
            "carbonengine/resources/tests/testData/DiffGroups/resFileIndexWithSubtractions.txt",
            "carbonengine/resources/tests/testData/DiffGroups/ExpectedDiffWithSubtractions.txt",
            "diff-subtractions.txt",
        ),
    ] {
        cases.push(run_rust_resource_cli_artifact_case(
            &runner,
            legacy_test,
            &[
                OsString::from("rust-diff-group"),
                diff_base.as_os_str().to_os_string(),
                OsString::from(target),
                OsString::from("--diff-output-path"),
                output_root.join(output_name).into_os_string(),
            ],
            &output_root.join(output_name),
            Path::new(expected),
        )?);
    }

    let remove_base =
        Path::new("carbonengine/resources/tests/testData/RemoveResource/BaseResourceGroup.yaml");
    let remove_list =
        Path::new("carbonengine/resources/tests/testData/RemoveResource/ResourcesToRemoveList.txt");
    let remove_expected = Path::new(
        "carbonengine/resources/tests/testData/RemoveResource/ResourceGroupAfterRemove.yaml",
    );
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.RemoveResources",
        &[
            OsString::from("rust-remove-resources"),
            remove_base.as_os_str().to_os_string(),
            remove_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            output_root.join("remove-resources.yaml").into_os_string(),
        ],
        &output_root.join("remove-resources.yaml"),
        remove_expected,
    )?);

    let remove_unknown_list = Path::new(
        "carbonengine/resources/tests/testData/RemoveResource/ResourcesToRemoveListWithUnknownResource.txt",
    );
    cases.push(run_rust_resource_cli_artifact_case(
        &runner,
        "ResourcesCliTest.RemoveResourcesWithUnknownResourceIgnoreOnResourceNotFound",
        &[
            OsString::from("rust-remove-resources"),
            remove_base.as_os_str().to_os_string(),
            remove_unknown_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            output_root
                .join("remove-resources-ignore-missing.yaml")
                .into_os_string(),
            OsString::from("--ignore-missing-resources"),
        ],
        &output_root.join("remove-resources-ignore-missing.yaml"),
        remove_base,
    )?);

    cases.push(run_rust_resource_legacy_cli_exit_code_case(
        &runner,
        "ResourcesCliTest.RemoveResourcesWithUnknownResourceWithInvalidPathToResourcesFile",
        &[
            OsString::from("remove-resources"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            remove_base.as_os_str().to_os_string(),
            OsString::from("carbonengine/resources/tests/testData/INVALID_PATH"),
            OsString::from("--output-resource-group-path"),
            output_root
                .join("remove-resources-invalid-list.yaml")
                .into_os_string(),
        ],
        1,
        "legacy_cli_exit_code_valid_operation_runtime_failure",
    )?);
    cases.push(run_rust_resource_cli_failure_case(
        &runner,
        "ResourcesCliTest.RemoveResourcesWithUnknownResource",
        &[
            OsString::from("rust-remove-resources"),
            remove_base.as_os_str().to_os_string(),
            remove_unknown_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            output_root
                .join("remove-resources-unknown-fail.yaml")
                .into_os_string(),
        ],
    )?);

    Ok(cases)
}

fn run_rust_resource_cli_artifact_case(
    runner: &Path,
    legacy_test: &str,
    args: &[OsString],
    actual_path: &Path,
    expected_path: &Path,
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let actual =
        fs::read(actual_path).with_context(|| format!("reading {}", actual_path.display()))?;
    let expected =
        fs::read(expected_path).with_context(|| format!("reading {}", expected_path.display()))?;
    if actual != expected {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {} but it did not match {}",
            actual_path.display(),
            expected_path.display()
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_and_output_bytes_match_legacy_golden",
        "command": command_line(runner, args),
        "actual_path": actual_path.display().to_string(),
        "expected_path": expected_path.display().to_string(),
        "bytes": actual.len(),
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_cli_manifest_and_directory_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    args: &[OsString],
    actual_manifest_path: &Path,
    expected_manifest_path: &Path,
    actual_directory: &Path,
    expected_directory: &Path,
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let actual_manifest = fs::read(actual_manifest_path)
        .with_context(|| format!("reading {}", actual_manifest_path.display()))?;
    let expected_manifest = fs::read(expected_manifest_path)
        .with_context(|| format!("reading {}", expected_manifest_path.display()))?;
    if actual_manifest != expected_manifest {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {} but it did not match {}",
            actual_manifest_path.display(),
            expected_manifest_path.display()
        );
    }

    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(expected_directory, actual_directory)?;

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_manifest_bytes_and_directory_subset_match_legacy_golden",
        "command": command_line(runner, args),
        "working_directory": working_directory.display().to_string(),
        "actual_manifest_path": actual_manifest_path.display().to_string(),
        "expected_manifest_path": expected_manifest_path.display().to_string(),
        "manifest_bytes": actual_manifest.len(),
        "actual_directory": actual_directory.display().to_string(),
        "expected_directory": expected_directory.display().to_string(),
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_cli_roundtrip_manifest_and_directory_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    args: &[OsString],
    actual_manifest_path: &Path,
    actual_directory: &Path,
    expected_directory: &Path,
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let actual_manifest_text = fs::read_to_string(actual_manifest_path)
        .with_context(|| format!("reading {}", actual_manifest_path.display()))?;
    let actual_catalog = parse_legacy_yaml_resource_group(&actual_manifest_text)
        .map_err(|error| anyhow!("parsing {} failed: {error}", actual_manifest_path.display()))?;
    let stable_manifest = export_legacy_yaml_resource_group(&actual_catalog);
    if stable_manifest.as_bytes() != actual_manifest_text.as_bytes() {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {} but it did not round-trip to stable legacy YAML bytes",
            actual_manifest_path.display()
        );
    }

    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(expected_directory, actual_directory)?;

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_manifest_roundtrip_and_directory_subset_match_legacy_golden",
        "command": command_line(runner, args),
        "working_directory": working_directory.display().to_string(),
        "actual_manifest_path": actual_manifest_path.display().to_string(),
        "manifest_bytes": actual_manifest_text.len(),
        "manifest_resources": actual_catalog.resources.len(),
        "actual_directory": actual_directory.display().to_string(),
        "expected_directory": expected_directory.display().to_string(),
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4),
        "compatibility_note": "legacy ResourcesCliTest.UnpackBundle asserts ResourceGroup.yaml exists and resource payloads match; this Rust process case additionally validates stable legacy YAML round-trip bytes for the emitted manifest"
    }))
}

fn run_rust_resource_cli_create_and_unpack_bundle_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    resource_index: &Path,
    resource_source: &Path,
) -> Result<Value> {
    let create_args = vec![
        OsString::from("rust-create-bundle"),
        resource_index.as_os_str().to_os_string(),
        OsString::from("--resource-source-path"),
        resource_source.as_os_str().to_os_string(),
        OsString::from("--bundle-resourcegroup-relative-path"),
        OsString::from("BundleResourceGroup.yaml"),
        OsString::from("--bundle-resourcegroup-destination-path"),
        OsString::from("resPath"),
        OsString::from("--bundle-resourcegroup-destination-type"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--chunk-destination-path"),
        OsString::from("CreateAndUnpackBundleOut"),
        OsString::from("--chunk-destination-type"),
        OsString::from("LOCAL_CDN"),
        OsString::from("--chunk-size"),
        OsString::from("1000"),
    ];
    let create_output = Command::new(runner)
        .args(&create_args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity create half {legacy_test}"))?;
    let create_stdout = String::from_utf8_lossy(&create_output.stdout).to_string();
    let create_stderr = String::from_utf8_lossy(&create_output.stderr).to_string();
    ensure_success(create_output, legacy_test)?;

    let unpack_args = vec![
        OsString::from("rust-unpack-bundle"),
        OsString::from("resPath/BundleResourceGroup.yaml"),
        OsString::from("--chunk-source-base-path"),
        OsString::from("CreateAndUnpackBundleOut"),
        OsString::from("--resource-destination-type"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--output-base-path"),
        OsString::from("CreateAndUnpackBundleOut2"),
    ];
    let unpack_output = Command::new(runner)
        .args(&unpack_args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity unpack half {legacy_test}"))?;
    let unpack_stdout = String::from_utf8_lossy(&unpack_output.stdout).to_string();
    let unpack_stderr = String::from_utf8_lossy(&unpack_output.stderr).to_string();
    ensure_success(unpack_output, legacy_test)?;

    let unpacked_manifest_path =
        working_directory.join("CreateAndUnpackBundleOut2/ResourceGroup.yaml");
    let unpacked_manifest_text = fs::read_to_string(&unpacked_manifest_path)
        .with_context(|| format!("reading {}", unpacked_manifest_path.display()))?;
    let unpacked_catalog =
        parse_legacy_yaml_resource_group(&unpacked_manifest_text).map_err(|error| {
            anyhow!(
                "parsing {} failed: {error}",
                unpacked_manifest_path.display()
            )
        })?;
    let stable_manifest = export_legacy_yaml_resource_group(&unpacked_catalog);
    if stable_manifest.as_bytes() != unpacked_manifest_text.as_bytes() {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {} but it did not round-trip to stable legacy YAML bytes",
            unpacked_manifest_path.display()
        );
    }

    let output_directory = working_directory.join("CreateAndUnpackBundleOut2");
    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(resource_source, &output_directory)?;
    let expected_file_count = count_regular_files(resource_source)? + 1;
    let actual_file_count = count_regular_files(&output_directory)?;
    if actual_file_count != expected_file_count {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {actual_file_count} files under {} but expected {expected_file_count}",
            output_directory.display()
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "create_bundle_then_unpack_manifest_roundtrip_and_payload_bytes_match_legacy_fixture",
        "create_command": command_line(runner, &create_args),
        "unpack_command": command_line(runner, &unpack_args),
        "working_directory": working_directory.display().to_string(),
        "created_manifest_path": working_directory.join("resPath/BundleResourceGroup.yaml").display().to_string(),
        "created_chunk_directory": working_directory.join("CreateAndUnpackBundleOut").display().to_string(),
        "unpacked_manifest_path": unpacked_manifest_path.display().to_string(),
        "unpacked_directory": output_directory.display().to_string(),
        "expected_directory": resource_source.display().to_string(),
        "manifest_bytes": unpacked_manifest_text.len(),
        "manifest_resources": unpacked_catalog.resources.len(),
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "actual_file_count": actual_file_count,
        "expected_file_count": expected_file_count,
        "create_stdout_tail": tail_lines(&create_stdout, 4),
        "create_stderr_tail": tail_lines(&create_stderr, 4),
        "unpack_stdout_tail": tail_lines(&unpack_stdout, 4),
        "unpack_stderr_tail": tail_lines(&unpack_stderr, 4),
        "compatibility_note": "legacy ResourcesLibraryTest.CreateAndUnpackBundle creates a local bundle then unpacks it and checks resource payload bytes; this Rust process case validates the same local roundtrip and stable legacy YAML output"
    }))
}

fn run_rust_resource_cli_bundle_boundary_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    bundle_manifest_path: PathBuf,
    chunk_source_directory: PathBuf,
    expected_directory: PathBuf,
) -> Result<Value> {
    let args = [
        OsString::from("rust-unpack-bundle"),
        bundle_manifest_path.as_os_str().to_os_string(),
        OsString::from("--chunk-source-base-path"),
        chunk_source_directory.as_os_str().to_os_string(),
        OsString::from("--resource-destination-type"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--output-base-path"),
        OsString::from("UnpackBundleOut"),
    ];
    let output = Command::new(runner)
        .args(args.iter())
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let actual_directory = working_directory.join("UnpackBundleOut");
    let actual_manifest_path = actual_directory.join("ResourceGroup.yaml");
    let actual_manifest_text = fs::read_to_string(&actual_manifest_path)
        .with_context(|| format!("reading {}", actual_manifest_path.display()))?;
    let actual_catalog = parse_legacy_yaml_resource_group(&actual_manifest_text)
        .map_err(|error| anyhow!("parsing {} failed: {error}", actual_manifest_path.display()))?;
    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(&expected_directory, &actual_directory)?;

    let bundle_text = fs::read_to_string(&bundle_manifest_path)
        .with_context(|| format!("reading {}", bundle_manifest_path.display()))?;
    let bundle_catalog = parse_legacy_yaml_bundle_resource_group(&bundle_text)
        .map_err(|error| anyhow!("parsing {} failed: {error}", bundle_manifest_path.display()))?;
    if bundle_catalog.chunk_size != 1000 {
        bail!(
            "Rust resources CLI parity case {legacy_test} expected ChunkSize=1000, got {}",
            bundle_catalog.chunk_size
        );
    }
    if bundle_catalog.resources.len() != 42 {
        bail!(
            "Rust resources CLI parity case {legacy_test} expected 42 chunk records, got {}",
            bundle_catalog.resources.len()
        );
    }

    let mut one_kib_chunks = 0_usize;
    let mut tail_chunks = 0_usize;
    let mut chunk_payload_bytes = 0_u64;
    for (index, chunk) in bundle_catalog.resources.iter().enumerate() {
        let expected_size = if index == 41 { 961 } else { 1000 };
        if chunk.size_bytes != expected_size {
            bail!(
                "Rust resources CLI parity case {legacy_test} expected chunk {index} size {expected_size}, got {}",
                chunk.size_bytes
            );
        }
        if chunk.resource_type != "BinaryChunk" {
            bail!(
                "Rust resources CLI parity case {legacy_test} expected BinaryChunk at chunk {index}, got {}",
                chunk.resource_type
            );
        }
        let chunk_path = chunk_source_directory.join(chunk.location.replace('\\', "/"));
        let chunk_data = fs::read(&chunk_path)
            .with_context(|| format!("reading chunk payload {}", chunk_path.display()))?;
        if chunk_data.len() as u64 != chunk.size_bytes || md5_hex(&chunk_data) != chunk.checksum {
            bail!(
                "Rust resources CLI parity case {legacy_test} chunk payload {} does not match manifest size/checksum",
                chunk_path.display()
            );
        }
        chunk_payload_bytes += chunk_data.len() as u64;
        if chunk.size_bytes == 1000 {
            one_kib_chunks += 1;
        } else {
            tail_chunks += 1;
        }
    }
    if one_kib_chunks != 41 || tail_chunks != 1 || chunk_payload_bytes != 41_961 {
        bail!(
            "Rust resources CLI parity case {legacy_test} unexpected boundary summary: one_kib={one_kib_chunks}, tail={tail_chunks}, bytes={chunk_payload_bytes}"
        );
    }

    let spanning_resources = actual_catalog
        .resources
        .iter()
        .filter(|resource| resource.size_bytes > bundle_catalog.chunk_size)
        .count();
    if spanning_resources < 2 {
        bail!(
            "Rust resources CLI parity case {legacy_test} expected at least two unpacked resources to span chunk boundaries, got {spanning_resources}"
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_bundle_chunk_boundary_manifest_payloads_and_unpacked_bytes_match_legacy_fixture",
        "command": command_line(runner, &args),
        "working_directory": working_directory.display().to_string(),
        "bundle_manifest_path": bundle_manifest_path.display().to_string(),
        "actual_manifest_path": actual_manifest_path.display().to_string(),
        "actual_directory": actual_directory.display().to_string(),
        "expected_directory": expected_directory.display().to_string(),
        "chunk_source_directory": chunk_source_directory.display().to_string(),
        "chunk_size": bundle_catalog.chunk_size,
        "chunk_records": bundle_catalog.resources.len(),
        "one_kib_chunks": one_kib_chunks,
        "tail_chunks": tail_chunks,
        "tail_chunk_bytes": 961,
        "chunk_payload_bytes": chunk_payload_bytes,
        "spanning_resources": spanning_resources,
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_cli_remote_cdn_unpack_cache_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    bundle_manifest_path: PathBuf,
    remote_mirror_directory: PathBuf,
    expected_directory: &Path,
) -> Result<Value> {
    let cache_directory = working_directory.join("RemoteCache");
    let first_output_directory = working_directory.join("UnpackBundleOutFirst");
    let second_output_directory = working_directory.join("UnpackBundleOutSecond");
    let first_stats_path = working_directory.join("remote-first-stats.json");
    let second_stats_path = working_directory.join("remote-second-stats.json");

    let first_args = [
        OsString::from("rust-unpack-bundle"),
        bundle_manifest_path.as_os_str().to_os_string(),
        OsString::from("--chunk-source-type"),
        OsString::from("REMOTE_CDN"),
        OsString::from("--chunk-source-base-path"),
        remote_mirror_directory.as_os_str().to_os_string(),
        OsString::from("--remote-cache-base-path"),
        OsString::from("RemoteCache"),
        OsString::from("--stats-output"),
        OsString::from("remote-first-stats.json"),
        OsString::from("--resource-destination-type"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--output-base-path"),
        OsString::from("UnpackBundleOutFirst"),
    ];
    let first_output = Command::new(runner)
        .args(first_args.iter())
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running first Rust resources CLI parity case {legacy_test}"))?;
    let first_stdout = String::from_utf8_lossy(&first_output.stdout).to_string();
    let first_stderr = String::from_utf8_lossy(&first_output.stderr).to_string();
    ensure_success(first_output, legacy_test)?;

    let second_args = [
        OsString::from("rust-unpack-bundle"),
        bundle_manifest_path.as_os_str().to_os_string(),
        OsString::from("--chunk-source-type"),
        OsString::from("REMOTE_CDN"),
        OsString::from("--chunk-source-base-path"),
        remote_mirror_directory.as_os_str().to_os_string(),
        OsString::from("--remote-cache-base-path"),
        OsString::from("RemoteCache"),
        OsString::from("--stats-output"),
        OsString::from("remote-second-stats.json"),
        OsString::from("--resource-destination-type"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--output-base-path"),
        OsString::from("UnpackBundleOutSecond"),
    ];
    let second_output = Command::new(runner)
        .args(second_args.iter())
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running second Rust resources CLI parity case {legacy_test}"))?;
    let second_stdout = String::from_utf8_lossy(&second_output.stdout).to_string();
    let second_stderr = String::from_utf8_lossy(&second_output.stderr).to_string();
    ensure_success(second_output, legacy_test)?;

    let first_manifest_path = first_output_directory.join("ResourceGroup.yaml");
    let second_manifest_path = second_output_directory.join("ResourceGroup.yaml");
    let first_manifest_text = fs::read_to_string(&first_manifest_path)
        .with_context(|| format!("reading {}", first_manifest_path.display()))?;
    let second_manifest_text = fs::read_to_string(&second_manifest_path)
        .with_context(|| format!("reading {}", second_manifest_path.display()))?;
    let first_catalog = parse_legacy_yaml_resource_group(&first_manifest_text)
        .map_err(|error| anyhow!("parsing {} failed: {error}", first_manifest_path.display()))?;
    let second_catalog = parse_legacy_yaml_resource_group(&second_manifest_text)
        .map_err(|error| anyhow!("parsing {} failed: {error}", second_manifest_path.display()))?;
    if export_legacy_yaml_resource_group(&first_catalog).as_bytes()
        != first_manifest_text.as_bytes()
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} first manifest did not round-trip to stable legacy YAML bytes"
        );
    }
    if export_legacy_yaml_resource_group(&second_catalog).as_bytes()
        != second_manifest_text.as_bytes()
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} second manifest did not round-trip to stable legacy YAML bytes"
        );
    }
    if first_manifest_text != second_manifest_text {
        bail!("Rust resources CLI parity case {legacy_test} first and second manifests differ");
    }

    let (first_directory_files, first_directory_bytes) =
        assert_directory_subset_bytes(expected_directory, &first_output_directory)?;
    let (second_directory_files, second_directory_bytes) =
        assert_directory_subset_bytes(expected_directory, &second_output_directory)?;
    let first_actual_files = count_regular_files(&first_output_directory)?;
    let second_actual_files = count_regular_files(&second_output_directory)?;
    if first_actual_files != first_directory_files + 1 {
        bail!(
            "Rust resources CLI parity case {legacy_test} first run wrote {first_actual_files} files, expected {} resources plus ResourceGroup.yaml",
            first_directory_files
        );
    }
    if second_actual_files != second_directory_files + 1 {
        bail!(
            "Rust resources CLI parity case {legacy_test} second run wrote {second_actual_files} files, expected {} resources plus ResourceGroup.yaml",
            second_directory_files
        );
    }

    let first_stats_text = fs::read_to_string(&first_stats_path)
        .with_context(|| format!("reading {}", first_stats_path.display()))?;
    let second_stats_text = fs::read_to_string(&second_stats_path)
        .with_context(|| format!("reading {}", second_stats_path.display()))?;
    let first_stats: Value = serde_json::from_str(&first_stats_text)
        .with_context(|| format!("parsing {}", first_stats_path.display()))?;
    let second_stats: Value = serde_json::from_str(&second_stats_text)
        .with_context(|| format!("parsing {}", second_stats_path.display()))?;

    let stat_u64 = |stats: &Value, key: &str| -> Result<u64> {
        stats
            .get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("missing numeric remote CDN stat {key}"))
    };
    if first_stats.get("chunk_source_type").and_then(Value::as_str) != Some("REMOTE_CDN")
        || second_stats
            .get("chunk_source_type")
            .and_then(Value::as_str)
            != Some("REMOTE_CDN")
    {
        bail!("Rust resources CLI parity case {legacy_test} did not emit REMOTE_CDN stats");
    }
    if stat_u64(&first_stats, "downloads")? != 2
        || stat_u64(&first_stats, "cache_hits")? != 0
        || stat_u64(&first_stats, "replaced_bad_cache_entries")? != 0
        || stat_u64(&first_stats, "bytes_copied_to_cache")? == 0
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} first remote CDN stats are not a clean mirror download: {first_stats}"
        );
    }
    if stat_u64(&second_stats, "downloads")? != 0
        || stat_u64(&second_stats, "cache_hits")? != 2
        || stat_u64(&second_stats, "replaced_bad_cache_entries")? != 0
        || stat_u64(&second_stats, "bytes_copied_to_cache")? != 0
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} second remote CDN stats are not a cache-hit run: {second_stats}"
        );
    }
    let cache_files = count_regular_files(&cache_directory)?;
    if cache_files != 2 {
        bail!(
            "Rust resources CLI parity case {legacy_test} expected two cached remote CDN payloads, got {cache_files}"
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_remote_cdn_mirror_download_then_cache_hit_stats_and_unpacked_bytes_match_legacy_fixture",
        "first_command": command_line(runner, &first_args),
        "second_command": command_line(runner, &second_args),
        "working_directory": working_directory.display().to_string(),
        "bundle_manifest_path": bundle_manifest_path.display().to_string(),
        "remote_mirror_directory": remote_mirror_directory.display().to_string(),
        "remote_cache_directory": cache_directory.display().to_string(),
        "first_stats_path": first_stats_path.display().to_string(),
        "second_stats_path": second_stats_path.display().to_string(),
        "first_stats": first_stats,
        "second_stats": second_stats,
        "cache_files": cache_files,
        "first_actual_manifest_path": first_manifest_path.display().to_string(),
        "second_actual_manifest_path": second_manifest_path.display().to_string(),
        "first_actual_directory": first_output_directory.display().to_string(),
        "second_actual_directory": second_output_directory.display().to_string(),
        "expected_directory": expected_directory.display().to_string(),
        "manifest_resources": first_catalog.resources.len(),
        "directory_files": first_directory_files,
        "directory_bytes": first_directory_bytes,
        "second_directory_files": second_directory_files,
        "second_directory_bytes": second_directory_bytes,
        "first_stdout_tail": tail_lines(&first_stdout, 4),
        "first_stderr_tail": tail_lines(&first_stderr, 4),
        "second_stdout_tail": tail_lines(&second_stdout, 4),
        "second_stderr_tail": tail_lines(&second_stderr, 4),
        "compatibility_note": "local mirror exercises legacy REMOTE_CDN payload naming, compression, checksum validation, cache fill, and cache-hit behavior without requiring external network access"
    }))
}

fn run_rust_resource_cli_directory_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    args: &[OsString],
    actual_directory: &Path,
    expected_directory: &Path,
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(expected_directory, actual_directory)?;
    let actual_files = count_regular_files(actual_directory)?;
    if actual_files != directory_files {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {actual_files} files, but expected exactly {directory_files}"
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_and_directory_exact_file_set_matches_legacy_golden",
        "command": command_line(runner, args),
        "working_directory": working_directory.display().to_string(),
        "actual_directory": actual_directory.display().to_string(),
        "expected_directory": expected_directory.display().to_string(),
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn prepare_legacy_old_patch_apply_expected_directory(
    repo_root: &Path,
    expected_directory: &Path,
) -> Result<()> {
    if expected_directory.exists() {
        fs::remove_dir_all(expected_directory)
            .with_context(|| format!("removing {}", expected_directory.display()))?;
    }
    let patch_fixture_root = repo_root.join("carbonengine/resources/tests/testData/Patch");
    let previous_root = patch_fixture_root.join("PreviousBuildResources");
    let next_root = patch_fixture_root.join("NextBuildResources");
    copy_directory_recursive(&previous_root, expected_directory)?;

    for relative_path in [
        "introMovie.txt",
        "introMoviePrefixed.txt",
        "testResource2.txt",
    ] {
        let source_path = next_root.join(relative_path);
        let destination_path = expected_directory.join(relative_path);
        if let Some(parent) = destination_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating directory {}", parent.display()))?;
        }
        fs::copy(&source_path, &destination_path).with_context(|| {
            format!(
                "copying old-layout expected resource {} to {}",
                source_path.display(),
                destination_path.display()
            )
        })?;
    }

    Ok(())
}

fn prepare_corrupt_legacy_patch_payload_directory(
    repo_root: &Path,
    working_directory: &Path,
) -> Result<PathBuf> {
    let patch_fixture_root = repo_root.join("carbonengine/resources/tests/testData/Patch");
    let source_directory = patch_fixture_root.join("LocalCDNPatches");
    let corrupt_directory = working_directory.join("CorruptLocalCDNPatches");
    if corrupt_directory.exists() {
        fs::remove_dir_all(&corrupt_directory)
            .with_context(|| format!("removing {}", corrupt_directory.display()))?;
    }
    copy_directory_recursive(&source_directory, &corrupt_directory)?;

    let patch_manifest_path = patch_fixture_root.join("PatchResourceGroup.yaml");
    let patch_manifest = fs::read_to_string(&patch_manifest_path)
        .with_context(|| format!("reading {}", patch_manifest_path.display()))?;
    let catalog = parse_legacy_yaml_patch_resource_group(&patch_manifest)
        .map_err(|error| anyhow!("parsing patch manifest failed: {error}"))?;
    let corrupt_location = catalog
        .resources
        .iter()
        .find(|resource| !resource.location.is_empty())
        .map(|resource| resource.location.replace('\\', "/"))
        .ok_or_else(|| anyhow!("patch manifest has no payload locations to corrupt"))?;
    let corrupt_payload_path = corrupt_directory.join(corrupt_location);
    fs::write(&corrupt_payload_path, b"corrupt patch payload").with_context(|| {
        format!(
            "writing corrupt patch payload {}",
            corrupt_payload_path.display()
        )
    })?;

    Ok(corrupt_directory)
}

fn run_rust_resource_cli_no_change_patch_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    args: &[OsString],
    actual_manifest_path: &Path,
    actual_patch_directory: &Path,
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    ensure_success(output, legacy_test)?;

    let actual_manifest = fs::read_to_string(actual_manifest_path)
        .with_context(|| format!("reading {}", actual_manifest_path.display()))?;
    let catalog = parse_legacy_yaml_patch_resource_group(&actual_manifest)
        .map_err(|error| anyhow!("parsing no-change patch manifest failed: {error}"))?;
    if !catalog.resources.is_empty() {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {} patch records for a no-change build",
            catalog.resources.len()
        );
    }
    if catalog.removed_resource_relative_paths.is_some() {
        bail!("Rust resources CLI parity case {legacy_test} wrote removed resources for a no-change build");
    }
    if catalog.total_compressed_size_bytes != Some(0) || catalog.total_uncompressed_size_bytes != 0
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote non-zero patch totals: compressed={:?}, uncompressed={}",
            catalog.total_compressed_size_bytes,
            catalog.total_uncompressed_size_bytes
        );
    }

    let payload_path =
        actual_patch_directory.join(catalog.resource_group_resource.location.replace('\\', "/"));
    let payload =
        fs::read(&payload_path).with_context(|| format!("reading {}", payload_path.display()))?;
    let expected_empty_group = ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes: Some(0),
        total_uncompressed_size_bytes: 0,
        resources: Vec::new(),
    };
    let expected_payload = export_legacy_yaml_resource_group(&expected_empty_group).into_bytes();
    if payload != expected_payload {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote unexpected empty diff ResourceGroup payload {}",
            payload_path.display()
        );
    }
    if catalog.resource_group_resource.size_bytes != payload.len() as u64
        || catalog.resource_group_resource.checksum != md5_hex(&payload)
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote inconsistent ResourceGroup payload metadata"
        );
    }
    let payload_files = count_regular_files(actual_patch_directory)?;
    if payload_files != 1 {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {payload_files} patch payload files for a no-change build"
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_and_empty_patch_manifest_matches_no_change_legacy_behavior",
        "command": command_line(runner, args),
        "working_directory": working_directory.display().to_string(),
        "actual_manifest_path": actual_manifest_path.display().to_string(),
        "actual_patch_directory": actual_patch_directory.display().to_string(),
        "manifest_bytes": actual_manifest.len(),
        "payload_files": payload_files,
        "resource_group_payload_bytes": payload.len(),
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_cli_copy_only_patch_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
) -> Result<Value> {
    let previous_dir = working_directory.join("PreviousBuildResources");
    let next_dir = working_directory.join("NextBuildResources");
    fs::create_dir_all(&previous_dir)
        .with_context(|| format!("creating {}", previous_dir.display()))?;
    fs::create_dir_all(&next_dir).with_context(|| format!("creating {}", next_dir.display()))?;

    let previous = b"AAAABBBBCCCCDDDD".to_vec();
    let latest = b"BBBBAAAADDDDCCCC".to_vec();
    fs::write(previous_dir.join("copy.dat"), &previous)
        .with_context(|| format!("writing {}", previous_dir.join("copy.dat").display()))?;
    fs::write(next_dir.join("copy.dat"), &latest)
        .with_context(|| format!("writing {}", next_dir.join("copy.dat").display()))?;

    fs::write(
        working_directory.join("previous.yaml"),
        export_legacy_yaml_resource_group(&single_resource_catalog("copy.dat", &previous)),
    )
    .with_context(|| {
        format!(
            "writing {}",
            working_directory.join("previous.yaml").display()
        )
    })?;
    fs::write(
        working_directory.join("next.yaml"),
        export_legacy_yaml_resource_group(&single_resource_catalog("copy.dat", &latest)),
    )
    .with_context(|| format!("writing {}", working_directory.join("next.yaml").display()))?;

    let create_args = [
        OsString::from("rust-create-patch"),
        OsString::from("previous.yaml"),
        OsString::from("next.yaml"),
        OsString::from("--resource-source-type-previous"),
        OsString::from("LOCAL_RELATIVE"),
        OsString::from("--resource-source-base-path-previous"),
        OsString::from("PreviousBuildResources"),
        OsString::from("--resource-source-base-path-next"),
        OsString::from("NextBuildResources"),
        OsString::from("--patch-resourcegroup-destination-path"),
        OsString::from("PatchOut"),
        OsString::from("--patch-destination-base-path"),
        OsString::from("PatchOut/Patches"),
        OsString::from("--patch-destination-type"),
        OsString::from("LOCAL_CDN"),
        OsString::from("--chunk-size"),
        OsString::from("4"),
    ];
    let create_output = Command::new(runner)
        .args(create_args.iter())
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test} create"))?;
    let create_stdout = String::from_utf8_lossy(&create_output.stdout).to_string();
    let create_stderr = String::from_utf8_lossy(&create_output.stderr).to_string();
    ensure_success(create_output, legacy_test)?;

    let manifest_path = working_directory.join("PatchOut/PatchResourceGroup.yaml");
    let manifest = fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let catalog = parse_legacy_yaml_patch_resource_group(&manifest)
        .map_err(|error| anyhow!("parsing copy-only patch manifest failed: {error}"))?;
    if catalog.resources.is_empty() {
        bail!("Rust resources CLI parity case {legacy_test} wrote no copy-only patch records");
    }
    let generated_records = catalog
        .resources
        .iter()
        .filter(|record| !record.location.is_empty())
        .count();
    if generated_records != 0 {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {generated_records} generated patch payload records"
        );
    }
    if !catalog
        .resources
        .iter()
        .all(|record| record.compressed_size_bytes.is_none() && record.checksum == md5_hex(&[]))
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote inconsistent copy-only record metadata"
        );
    }
    if catalog.total_compressed_size_bytes != Some(0)
        || catalog.total_uncompressed_size_bytes != latest.len() as u64
    {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote unexpected copy-only totals: compressed={:?}, uncompressed={}",
            catalog.total_compressed_size_bytes,
            catalog.total_uncompressed_size_bytes
        );
    }

    let payload_dir = working_directory.join("PatchOut/Patches");
    let payload_files = count_regular_files(&payload_dir)?;
    if payload_files != 1 {
        bail!(
            "Rust resources CLI parity case {legacy_test} wrote {payload_files} payload files for a copy-only patch"
        );
    }

    let apply_args = [
        OsString::from("rust-apply-patch"),
        OsString::from("PatchOut/PatchResourceGroup.yaml"),
        OsString::from("--patch-binaries-base-path"),
        OsString::from("PatchOut/Patches"),
        OsString::from("--resources-to-patch-base-path"),
        OsString::from("PreviousBuildResources"),
        OsString::from("--next-resources-base-path"),
        OsString::from("NextBuildResources"),
        OsString::from("--output-base-path"),
        OsString::from("ApplyPatchOut"),
    ];
    let apply_output = Command::new(runner)
        .args(apply_args.iter())
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI parity case {legacy_test} apply"))?;
    let apply_stdout = String::from_utf8_lossy(&apply_output.stdout).to_string();
    let apply_stderr = String::from_utf8_lossy(&apply_output.stderr).to_string();
    ensure_success(apply_output, legacy_test)?;
    let (directory_files, directory_bytes) =
        assert_directory_subset_bytes(&next_dir, &working_directory.join("ApplyPatchOut"))?;

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "exit_success_copy_only_patch_records_have_no_binary_payloads_and_apply_rebuilds_latest",
        "create_command": command_line(runner, &create_args),
        "apply_command": command_line(runner, &apply_args),
        "working_directory": working_directory.display().to_string(),
        "actual_manifest_path": manifest_path.display().to_string(),
        "actual_patch_directory": payload_dir.display().to_string(),
        "copy_only_records": catalog.resources.len(),
        "generated_patch_records": generated_records,
        "payload_files": payload_files,
        "directory_files": directory_files,
        "directory_bytes": directory_bytes,
        "create_stdout_tail": tail_lines(&create_stdout, 4),
        "create_stderr_tail": tail_lines(&create_stderr, 4),
        "apply_stdout_tail": tail_lines(&apply_stdout, 4),
        "apply_stderr_tail": tail_lines(&apply_stderr, 4)
    }))
}

fn single_resource_catalog(path: &str, data: &[u8]) -> ResourceCatalog {
    ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes: None,
        total_uncompressed_size_bytes: data.len() as u64,
        resources: vec![ResourceRecord {
            path: String::from(path),
            location: String::from(path),
            size_bytes: data.len() as u64,
            compressed_size_bytes: None,
            checksum: Some(md5_hex(data)),
            binary_operation: None,
            prefix: None,
        }],
    }
}

fn count_regular_files(path: &Path) -> Result<u64> {
    let mut files = 0_u64;
    if !path.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry.with_context(|| format!("reading entry in {}", path.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("reading file type for {}", entry.path().display()))?;
        if file_type.is_dir() {
            files += count_regular_files(&entry.path())?;
        } else if file_type.is_file() {
            files += 1;
        }
    }
    Ok(files)
}

fn assert_directory_subset_bytes(
    expected_directory: &Path,
    actual_directory: &Path,
) -> Result<(u64, u64)> {
    fn visit(
        expected_root: &Path,
        expected_path: &Path,
        actual_root: &Path,
        files: &mut u64,
        bytes: &mut u64,
    ) -> Result<()> {
        for entry in fs::read_dir(expected_path)
            .with_context(|| format!("reading expected directory {}", expected_path.display()))?
        {
            let entry = entry.with_context(|| {
                format!(
                    "reading expected directory entry {}",
                    expected_path.display()
                )
            })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("reading file type for {}", path.display()))?;
            if file_type.is_dir() {
                visit(expected_root, &path, actual_root, files, bytes)?;
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let relative_path = path
                .strip_prefix(expected_root)
                .with_context(|| format!("stripping {}", expected_root.display()))?;
            let actual_path = actual_root.join(relative_path);
            let actual_path =
                resolve_existing_path_case_insensitive(&actual_path).unwrap_or(actual_path);
            let expected_data =
                fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            let actual_data = fs::read(&actual_path)
                .with_context(|| format!("reading {}", actual_path.display()))?;
            if actual_data != expected_data {
                bail!(
                    "directory subset mismatch: {} does not match {}",
                    actual_path.display(),
                    path.display()
                );
            }
            *files += 1;
            *bytes += expected_data.len() as u64;
        }
        Ok(())
    }

    let mut files = 0_u64;
    let mut bytes = 0_u64;
    visit(
        expected_directory,
        expected_directory,
        actual_directory,
        &mut files,
        &mut bytes,
    )?;
    Ok((files, bytes))
}

fn resolve_existing_path_case_insensitive(path: &Path) -> Option<PathBuf> {
    if path.exists() {
        return Some(path.to_path_buf());
    }

    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => resolved.push(component.as_os_str()),
            Component::Normal(name) => {
                let direct = resolved.join(name);
                if direct.exists() {
                    resolved = direct;
                    continue;
                }

                let search_dir = if resolved.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    resolved.as_path()
                };
                let mut matched = None;
                for entry in fs::read_dir(search_dir).ok()? {
                    let entry = entry.ok()?;
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .eq_ignore_ascii_case(&name.to_string_lossy())
                    {
                        matched = Some(entry.path());
                        break;
                    }
                }
                resolved = matched?;
            }
        }
    }

    resolved.exists().then_some(resolved)
}

fn run_rust_resource_cli_failure_case(
    runner: &Path,
    legacy_test: &str,
    args: &[OsString],
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .output()
        .with_context(|| format!("running Rust resources CLI failure parity case {legacy_test}"))?;
    let status_code = output.status.code();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        bail!("Rust resources CLI parity case {legacy_test} unexpectedly succeeded");
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "nonzero_failure_for_legacy_error_path",
        "command": command_line(runner, args),
        "status_code": status_code,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4),
        "compatibility_note": "legacy resources-cli returns operation-specific numeric error codes; xtask wrapper currently proves failure semantics only, not exact legacy exit code parity"
    }))
}

fn run_rust_resource_cli_failure_no_output_case(
    runner: &Path,
    legacy_test: &str,
    working_directory: &Path,
    args: &[OsString],
    forbidden_output_paths: &[PathBuf],
) -> Result<Value> {
    let output = Command::new(runner)
        .args(args)
        .current_dir(working_directory)
        .output()
        .with_context(|| format!("running Rust resources CLI failure parity case {legacy_test}"))?;
    let status_code = output.status.code();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        bail!("Rust resources CLI parity case {legacy_test} unexpectedly succeeded");
    }
    for forbidden_output_path in forbidden_output_paths {
        if forbidden_output_path.exists() {
            bail!(
                "Rust resources CLI parity case {legacy_test} failed but still created {}",
                forbidden_output_path.display()
            );
        }
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "nonzero_failure_and_no_output_tree_for_legacy_error_path",
        "command": command_line(runner, args),
        "working_directory": working_directory.display().to_string(),
        "status_code": status_code,
        "forbidden_output_paths": forbidden_output_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4),
        "compatibility_note": "resource CLI failures must fail before writing output trees; exact legacy numeric exit-code parity remains future work"
    }))
}

fn run_rust_resource_legacy_cli_exit_code_case(
    runner: &Path,
    legacy_test: &str,
    legacy_args: &[OsString],
    expected_status_code: i32,
    assertion: &str,
) -> Result<Value> {
    let mut args = vec![OsString::from("rust-resources-cli")];
    args.extend_from_slice(legacy_args);
    let output = Command::new(runner).args(&args).output().with_context(|| {
        format!("running Rust resources legacy CLI exit-code parity case {legacy_test}")
    })?;
    let status_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if status_code != expected_status_code {
        bail!(
            "Rust resources legacy CLI parity case {legacy_test} returned {status_code}, expected {expected_status_code}"
        );
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": assertion,
        "command": command_line(runner, &args),
        "status_code": status_code,
        "expected_legacy_status_code": expected_status_code,
        "stdout_tail": tail_lines(&stdout, 4),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_legacy_cli_help_shape_case(
    runner: &Path,
    legacy_test: &str,
    legacy_args: &[OsString],
    expected_status_code: i32,
) -> Result<Value> {
    let mut args = vec![OsString::from("rust-resources-cli")];
    args.extend_from_slice(legacy_args);
    let output = Command::new(runner).args(&args).output().with_context(|| {
        format!("running Rust resources legacy CLI help-shape parity case {legacy_test}")
    })?;
    let status_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if status_code != expected_status_code {
        bail!(
            "Rust resources legacy CLI help-shape parity case {legacy_test} returned {status_code}, expected {expected_status_code}"
        );
    }

    let required_markers = [
        "====================",
        "resources-cli",
        "Name:resources",
        "Version: 4.3.1",
        "Operations:",
        "create-group",
        "create-group-from-filter",
        "create-patch",
        "create-bundle",
        "merge-group",
        "diff-group",
        "remove-resources",
        "apply-patch",
        "unpack-bundle",
    ];
    for marker in required_markers {
        if !stdout.contains(marker) {
            bail!(
                "Rust resources legacy CLI help-shape parity case {legacy_test} missing stdout marker {marker:?}; stdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "legacy_style_top_level_help_shape_and_operation_list",
        "command": command_line(runner, &args),
        "status_code": status_code,
        "expected_legacy_status_code": expected_status_code,
        "required_stdout_markers": required_markers,
        "stdout_tail": tail_lines(&stdout, 16),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_legacy_cli_operation_help_shape_case(
    runner: &Path,
    legacy_test: &str,
    operation: &str,
) -> Result<Value> {
    let args = vec![
        OsString::from("rust-resources-cli"),
        OsString::from(operation),
        OsString::from("--help"),
    ];
    let output = Command::new(runner).args(&args).output().with_context(|| {
        format!("running Rust resources legacy CLI operation help-shape parity case {legacy_test}")
    })?;
    let status_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if status_code != 0 {
        bail!(
            "Rust resources legacy CLI operation help-shape parity case {legacy_test} returned {status_code}, expected 0"
        );
    }
    if !stderr.trim().is_empty() {
        bail!(
            "Rust resources legacy CLI operation help-shape parity case {legacy_test} wrote stderr:\n{stderr}"
        );
    }

    let Some(required_markers) = legacy_resources_operation_help_markers(operation) else {
        bail!("missing operation help markers for {operation}");
    };
    for marker in required_markers {
        if !stdout.contains(marker) {
            bail!(
                "Rust resources legacy CLI operation help-shape parity case {legacy_test} missing stdout marker {marker:?}; stdout:\n{stdout}"
            );
        }
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "legacy_style_operation_help_shape",
        "operation": operation,
        "command": command_line(runner, &args),
        "status_code": status_code,
        "expected_legacy_status_code": 0,
        "required_stdout_markers": required_markers,
        "stdout_tail": tail_lines(&stdout, 20),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn run_rust_resource_legacy_cli_operation_usage_shape_case(
    runner: &Path,
    legacy_test: &str,
    operation: &str,
) -> Result<Value> {
    let args = vec![
        OsString::from("rust-resources-cli"),
        OsString::from(operation),
    ];
    let output = Command::new(runner).args(&args).output().with_context(|| {
        format!("running Rust resources legacy CLI operation usage-shape parity case {legacy_test}")
    })?;
    let status_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if status_code != 2 {
        bail!(
            "Rust resources legacy CLI operation usage-shape parity case {legacy_test} returned {status_code}, expected 2"
        );
    }
    if !stderr.trim().is_empty() {
        bail!(
            "Rust resources legacy CLI operation usage-shape parity case {legacy_test} wrote stderr:\n{stderr}"
        );
    }

    let Some(required_markers) = legacy_resources_operation_help_markers(operation) else {
        bail!("missing operation usage markers for {operation}");
    };
    for marker in required_markers {
        if !stdout.contains(marker) {
            bail!(
                "Rust resources legacy CLI operation usage-shape parity case {legacy_test} missing stdout marker {marker:?}; stdout:\n{stdout}"
            );
        }
    }

    Ok(json!({
        "legacy_test": legacy_test,
        "status": "pass",
        "assertion": "legacy_style_operation_usage_shape_for_invalid_arguments",
        "operation": operation,
        "command": command_line(runner, &args),
        "status_code": status_code,
        "expected_legacy_status_code": 2,
        "required_stdout_markers": required_markers,
        "stdout_tail": tail_lines(&stdout, 20),
        "stderr_tail": tail_lines(&stderr, 4)
    }))
}

fn rust_resources_legacy_cli(args: Vec<String>) -> i32 {
    let Some((operation, operation_args)) = args.split_first() else {
        print_legacy_resources_cli_usage();
        return 4;
    };

    if operation == "--help" || operation == "-h" {
        print_legacy_resources_cli_usage();
        return 3;
    }

    if operation_args
        .iter()
        .any(|arg| arg == "--help" || arg == "-h")
    {
        if let Some(help) = legacy_resources_operation_help(operation) {
            println!("{help}");
            return 0;
        }
    }

    let translated_args = translate_legacy_resources_cli_args(operation, operation_args);
    if translated_args.is_none() {
        print_legacy_resources_cli_usage();
        return 3;
    }
    let translated_args = translated_args.unwrap_or_default();
    if translated_args.is_empty() {
        if let Some(help) = legacy_resources_operation_help(operation) {
            println!("{help}");
        } else {
            eprintln!("resources operation {operation} requires arguments");
        }
        return 2;
    }

    let result = match operation.as_str() {
        "create-group" => rust_create_group(translated_args),
        "create-group-from-filter" => rust_create_group_from_filter(translated_args),
        "create-bundle" => rust_create_bundle(translated_args),
        "unpack-bundle" => rust_unpack_bundle(translated_args),
        "create-patch" => rust_create_patch(translated_args),
        "apply-patch" => rust_apply_patch(translated_args),
        "merge-group" => rust_merge_group(translated_args),
        "diff-group" => rust_diff_group(translated_args),
        "remove-resources" => rust_remove_resources(translated_args),
        _ => unreachable!("validated operation {operation}"),
    };

    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("Error: {error:#}");
            legacy_resources_cli_error_code(&error)
        }
    }
}

fn translate_legacy_resources_cli_args(operation: &str, args: &[String]) -> Option<Vec<String>> {
    match operation {
        "create-group"
        | "create-group-from-filter"
        | "create-bundle"
        | "create-patch"
        | "apply-patch"
        | "merge-group"
        | "diff-group"
        | "remove-resources" => Some(args.to_vec()),
        "unpack-bundle" => Some(
            args.iter()
                .map(|arg| {
                    if arg == "--resource-destination-base-path" {
                        String::from("--output-base-path")
                    } else {
                        arg.clone()
                    }
                })
                .collect(),
        ),
        _ => None,
    }
}

fn legacy_resources_cli_error_code(error: &anyhow::Error) -> i32 {
    let message = format!("{error:#}");
    let invalid_argument_markers = [
        " requires ",
        " requires an ",
        " requires one ",
        " requires base ",
        " requires resource ",
        " requires --",
        "unknown rust-",
        "unsupported rust-",
        "currently supports only",
        "supports LOCAL_CDN",
        "accepts exactly",
        "unexpected rust-",
        "parsing --",
    ];
    if invalid_argument_markers
        .iter()
        .any(|marker| message.contains(marker))
    {
        2
    } else {
        1
    }
}

fn print_legacy_resources_cli_usage() {
    println!(
        "====================\nresources-cli\nName:resources\nVersion: 4.3.1 [EXTENDED FEATURE DEVELOPMENT BUILD]\n====================\n\nOperations:\n\tcreate-group\t\tCreate a Resource Group from a given directory.\n\tcreate-group-from-filter\t\tCreate filtered Resource Group(s).\n\tcreate-patch\t\tCreates a patch binaries and a Patch Resource Group from two supplied ResourceGroups and two resource source directories, one for previous build and one for next.\n\tcreate-bundle\t\tCreates a bundle from supplied ResourceGroup. Bundle takes the form of individual binary chunks.\n\tmerge-group\t\tMerge two Resource Groups together\n\tdiff-group\t\tOutputs a list of additions and subtractions between the two provided ResourceGroups.\n\tremove-resources\t\tRemove resources from a ResourceGroup identified by supplied text file containing a list of RelativePaths to remove.\n\tapply-patch\t\tApplies a patch supplied via a Patch Resource Group to a directory [Only available in extended feature development build]\n\tunpack-bundle\t\tExtracts a bundle to original files given a Bundle Resource Group and a source for chunks [Only available in extended feature development build]"
    );
}

fn legacy_resources_operation_help(operation: &str) -> Option<&'static str> {
    match operation {
        "create-group" => Some(
            r#"Usage: create-group [options] input-directory

Create a Resource Group from a given directory.

Positional arguments:
input-directory                         Base directory to create resource group from.

Optional arguments:
-h --help                               shows help message and exits
-v --version                            prints version information and exits
--verbosity-level                       Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--output-file                           Filename for created resource group. [default: "ResourceGroup.yaml"]
--document-version                      Document version for created resource group. [default: "0.1.0"]
--resource-prefix                       Optional resource path prefix, such as "res" or "app" [default: ""]
--skip-compression                      Set skip compression calculations on resources. [default: false]
--export-resources                      Export resources after processing. see --export-resources-destination-type and --export-resources-destination-path [default: false]
--export-resources-destination-type     Represents the type of repository where exported resources will be saved. Requires --export-resources [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--export-resources-destination-path     Represents the base path where the exported resources will be saved. Requires --export-resources [default: "ExportedResources"]"#,
        ),
        "create-group-from-filter" => Some(
            r#"Usage: create-group-from-filter [options]

Create filtered Resource Group(s).

Optional arguments:
-h --help                                        shows help message and exits
-v --version                                     prints version information and exits
--verbosity-level                                Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--filter-index-mapping-file                      Path to filter index mapping file for resource filtering. See carbon-resources documentation for file specification. [Accepts multiple] [required]
--filter-file-basepath                           Base path to filter files. [Accepts multiple] [required]
--output-resource-file-basepath                  Base path for output resource files. [Accepts multiple] [default: ""]
--document-version                               Document version for created resource group. [default: "0.1.0"]
--resource-prefix                                Optional resource path prefix, such as "res" or "app" [default: ""]
--skip-compression                               Set skip compression calculations on resources. [default: false]
--export-resources                               Export resources after processing. see --export-resources-destination-type and --export-resources-destination-path [default: false]
--export-resources-destination-type              Type of repository where exported resources will be saved. Requires --export-resources [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--export-resources-destination-path              Base path where the exported resources will be saved. Requires --export-resources [default: "ExportedResources"]
--prefix-map-basepath                            Base directory for prefix mappings defined in filter files. [default: ""]
--skip-non-existent-input-directories            Skips input directories specified that don't exist rather than error. [default: false]
--stream-chunk-size                              Chunks stream size in bytes for streaming data. [default: "20971520"]
--remote-url-to-attempt-to-get-compression-info  If supplied, url is checked to get compression information. [default: ""]
--skip-binary-operation-calculation              Set skip to skip binary operation for resources [default: false]
--number-of-threads                              Nnumber of threads to use for async processes. [default: "28"]
--network-retry-count                            Number of retries to attempt when encountering a failed download. [default: "3"]
--network-retry-backoff-multiplier               Multiplier in seconds to wait for when retrying, value will multiply on each retry to backoff. [default: "1"]"#,
        ),
        "create-patch" => Some(
            r#"Usage: create-patch [options] previous-resourcegroup-path next-resourcegroup-path

Creates a patch binaries and a Patch Resource Group from two supplied ResourceGroups and two resource source directories, one for previous build and one for next.

Positional arguments:
previous-resourcegroup-path             Filename to previous resourceGroup.
next-resourcegroup-path                 Filename to next resourceGroup.

Optional arguments:
-h --help                               shows help message and exits
-v --version                            prints version information and exits
--verbosity-level                       Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--resource-source-base-path-previous    Represents the base path to source resources for previous. [Accepts multiple] [required]
--resource-source-base-path-next        Represents the base path to source resources for next. [Accepts multiple] [required]
--resource-source-type-next             Represents the type of repository to source resources for next. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--patch-destination-type                Represents the type of repository where binary patches will be saved. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--resourcegroup-relative-path           Relative path for output resourceGroup which will contain the diff between the supplied previous ResourceGroup and next ResourceGroup. [default: "ResourceGroup.yaml"]
--patchResourcegroup-relative-path      Relative path for output PatchResourceGroup which will contain all the patches produced. [default: "PatchResourceGroup.yaml"]
--resource-source-type-previous         Represents the type of repository to source resources for previous. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--patch-destination-base-path           Represents the base path where binary patches will be saved. [default: "PatchOut/Patches/"]
--patch-resourcegroup-destination-type  Represents the type of repository where the patch ResourceGroup will be saved. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--patch-resourcegroup-destination-path  Represents the base path where the patch ResourceGroup will be saved. [default: "PatchOut/"]
--patch-prefix                          Relative path prefix for produced patch binaries. Default is 'Patches/Patch' which will produce patches such as Patches/Patch.1 ... [default: "Patches/Patch"]
--chunk-size                            Files are processed in chunks, maxInputFileChunkSize indicate the size of this chunk. Files smaller than chunk will be processed in one pass. [default: "50000000"]
--network-retry-count                   Number of retries to attempt when encountering a failed download. [default: "3"]
--download-retry                        Multiplier in seconds to wait for when retrying, value will multiply on each retry to backoff. [default: "1"]
--index-folder                          The folder in which to place indexes generated for patch files. [default: "/tmp/carbonResources/chunkIndexes"]
--skip-compression                      Set skip compression calculations on patches. [default: false]"#,
        ),
        "create-bundle" => Some(
            r#"Usage: create-bundle [options] resourcegroup-path

Creates a bundle from supplied ResourceGroup. Bundle takes the form of individual binary chunks.

Positional arguments:
resourcegroup-path                       Path to ResourceGroup to bundle.

Optional arguments:
-h --help                                shows help message and exits
-v --version                             prints version information and exits
--verbosity-level                        Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--resource-source-path                   Represents the base path where the resources will be sourced. [Accepts multiple] [required]
--resource-source-type                   Represents the type of repository where resources will be sourced. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--resourcegroup-relative-path            Relative path to save a ResourceGroup the Bundle was based off [default: "ResourceGroup.yaml"]
--bundle-resourcegroup-relative-path     Relative path to save a Bundle ResourceGroup [default: "BundleResourceGroup.yaml"]
--chunk-destination-type                 Represents the type of repository where chunks will be saved. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--chunk-destination-path                 Represents the base path where the chunks will be saved. [default: "BundleOut/Chunks/"]
--bundle-resourcegroup-destination-type  Represents the type of repository where the bundle ResourceGroup will be saved. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--bundle-resourcegroup-destination-path  Represents the base path where the bundle ResourceGroup will be saved. [default: "BundleOut/"]
--chunk-size                             Represents the target size of the produced chunks in bytes. Note that produced chunks may not exactly match this value. [default: "50000000"]
--stream-chunk-size                      Chunks stream size in bytes for streaming data. [default: "10000000"]
--network-retry-count                    Number of retries to attempt when encountering a failed download. [default: "3"]
--download-retry-seconds                 Multiplier in seconds to wait for when retrying, value will multiply on each retry to backoff. [default: "1"]"#,
        ),
        "merge-group" => Some(
            r#"Usage: merge-group [options] base-resource-group-path merge-resource-group-path

Merge two Resource Groups together

Positional arguments:
base-resource-group-path            The path to the Resource Group to act as a base for the merge.
merge-resource-group-path           The path to the Resource Group to act as a target for the merge.

Optional arguments:
-h --help                           shows help message and exits
-v --version                        prints version information and exits
--verbosity-level                   Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--document-version                  Document version for created resource group. [default: "0.1.0"]
--merge-output-resource-group-path  The path in which to place the merged Resource Group. [default: "ResourceGroup.yaml"]"#,
        ),
        "diff-group" => Some(
            r#"Usage: diff-group [options] base-resource-group-path diff-resource-group-path

Outputs a list of additions and subtractions between the two provided ResourceGroups.

Positional arguments:
base-resource-group-path  The path to the Resource Group to act as a base for the diff.
diff-resource-group-path  The path to the Resource Group to act as a target for the diff.

Optional arguments:
-h --help                 shows help message and exits
-v --version              prints version information and exits
--verbosity-level         Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--diff-output-path        The path in which to place diff output. [default: "Diff.txt"]"#,
        ),
        "remove-resources" => Some(
            r#"Usage: remove-resources [options] resource-group-path resource-list-path

Remove resources from a ResourceGroup identified by supplied text file containing a list of RelativePaths to remove.

Positional arguments:
resource-group-path           The path to the Resource Group to remove resources from.
resource-list-path            Path to text file containing list of RelativePaths of resources to remove, separated by newlines.

Optional arguments:
-h --help                     shows help message and exits
-v --version                  prints version information and exits
--verbosity-level             Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--document-version            Document version for created resource group. [default: "0.1.0"]
--output-resource-group-path  Filename for created resource group. [default: "ResourceGroup.yaml"]
--ignore-missing-resources    Set to ignore 'resource not found' errors caused by supplying a list with Resources not present in ResourceGroup. [default: false]"#,
        ),
        "apply-patch" => Some(
            r#"Usage: apply-patch [options] patch-resource-group-path

Applies a patch supplied via a Patch Resource Group to a directory [Only available in extended feature development build]

Positional arguments:
patch-resource-group-path         The path to the PatchResourceGroup.yaml file.

Optional arguments:
-h --help                         shows help message and exits
-v --version                      prints version information and exits
--verbosity-level                 Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--patch-binaries-base-path        The paths to the folders containing the patch binaries. [Accepts multiple] [required]
--resources-to-patch-base-path    The paths to the folders containing resources to patch. [Accepts multiple] [required]
--next-resources-base-path        The path to resources after the patch. This is used to get fully added files which are not included in the generated patch files. [Accepts multiple] [required]
--patch-binaries-source-type      The type of repository the patch binaries are sourced from. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--resources-to-patch-source-type  The type of repository the resources to patch are sourced from. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--next-resources-source-type      The type of repository the resources after the patch are sourced from. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]
--output-base-path                The path in which to place the patched version of the files. [default: "ApplyPatchOut"]
--output-destination-type         The type of repository in which to place the patched version of the files. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_RELATIVE"]"#,
        ),
        "unpack-bundle" => Some(
            r#"Usage: unpack-bundle [options] bundle-resource-group-path

Extracts a bundle to original files given a Bundle Resource Group and a source for chunks [Only available in extended feature development build]

Positional arguments:
bundle-resource-group-path        The path to the BundleResourceGroup.yaml file

Optional arguments:
-h --help                         shows help message and exits
-v --version                      prints version information and exits
--verbosity-level                 Set verbosity to level [Choices: 1 - n to register for updates from n nested processes, -1 for all, 0 for none.] [default: "0"]
--chunk-source-base-path          The path to the directory containing the bundled files. [Accepts multiple] [required]
--chunk-source-type               The type of repository from which to retrieve the bundle files. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]
--resource-destination-base-path  The path to the directory in which to place the unbundled files. [default: "UnpackBundleOut"]
--resource-destination-type       The type of repository in which to place the bundle files. [Choices: LOCAL_RELATIVE, LOCAL_CDN, REMOTE_CDN] [default: "LOCAL_CDN"]"#,
        ),
        _ => None,
    }
}

fn legacy_resources_operation_help_markers(operation: &str) -> Option<&'static [&'static str]> {
    match operation {
        "create-group" => Some(&[
            "Usage: create-group [options] input-directory",
            "Create a Resource Group from a given directory.",
            "Positional arguments:",
            "Optional arguments:",
            "--output-file",
            "--export-resources-destination-type",
        ]),
        "create-group-from-filter" => Some(&[
            "Usage: create-group-from-filter [options]",
            "Create filtered Resource Group(s).",
            "Optional arguments:",
            "--filter-index-mapping-file",
            "--prefix-map-basepath",
            "--number-of-threads",
        ]),
        "create-patch" => Some(&[
            "Usage: create-patch [options] previous-resourcegroup-path next-resourcegroup-path",
            "Creates a patch binaries",
            "previous-resourcegroup-path",
            "--resource-source-base-path-previous",
            "--patchResourcegroup-relative-path",
            "--skip-compression",
        ]),
        "create-bundle" => Some(&[
            "Usage: create-bundle [options] resourcegroup-path",
            "Creates a bundle from supplied ResourceGroup.",
            "--resource-source-path",
            "--chunk-destination-type",
            "--bundle-resourcegroup-destination-path",
            "--stream-chunk-size",
        ]),
        "merge-group" => Some(&[
            "Usage: merge-group [options] base-resource-group-path merge-resource-group-path",
            "Merge two Resource Groups together",
            "--document-version",
            "--merge-output-resource-group-path",
        ]),
        "diff-group" => Some(&[
            "Usage: diff-group [options] base-resource-group-path diff-resource-group-path",
            "Outputs a list of additions and subtractions",
            "--diff-output-path",
        ]),
        "remove-resources" => Some(&[
            "Usage: remove-resources [options] resource-group-path resource-list-path",
            "Remove resources from a ResourceGroup",
            "--output-resource-group-path",
            "--ignore-missing-resources",
        ]),
        "apply-patch" => Some(&[
            "Usage: apply-patch [options] patch-resource-group-path",
            "Applies a patch supplied via a Patch Resource Group",
            "--patch-binaries-base-path",
            "--resources-to-patch-base-path",
            "--output-destination-type",
        ]),
        "unpack-bundle" => Some(&[
            "Usage: unpack-bundle [options] bundle-resource-group-path",
            "Extracts a bundle to original files",
            "--chunk-source-base-path",
            "--resource-destination-base-path",
            "--resource-destination-type",
        ]),
        _ => None,
    }
}

fn rust_create_group(args: Vec<String>) -> Result<()> {
    let mut input_directory: Option<PathBuf> = None;
    let mut output_file = PathBuf::from("ResourceGroup.yaml");
    let mut document_version = String::from("0.1.0");
    let mut resource_prefix: Option<String> = None;
    let mut calculate_compressions = true;
    let mut export_resources = false;
    let mut export_resources_destination_type = String::from("LOCAL_RELATIVE");
    let mut export_resources_destination_path: Option<PathBuf> = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--output-file" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output-file requires a value");
                };
                output_file = PathBuf::from(value);
            }
            "--document-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--document-version requires a value");
                };
                document_version = value.clone();
            }
            "--resource-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-prefix requires a value");
                };
                resource_prefix = Some(value.clone());
            }
            "--skip-compression" => {
                calculate_compressions = false;
            }
            "--export-resources" => {
                export_resources = true;
            }
            "--export-resources-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--export-resources-destination-type requires a value");
                };
                export_resources_destination_type = value.clone();
            }
            "--export-resources-destination-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--export-resources-destination-path requires a value");
                };
                export_resources_destination_path = Some(PathBuf::from(value));
            }
            "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("--verbosity-level requires a value");
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-create-group [options] <input-directory>\n\noptions:\n  --output-file <path>\n  --document-version <0.1.0|0.0.0>\n  --resource-prefix <prefix>\n  --skip-compression\n  --export-resources\n  --export-resources-destination-type LOCAL_RELATIVE\n  --export-resources-destination-path <path>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-create-group option: {value}"),
            value => {
                if input_directory.is_some() {
                    bail!("rust-create-group accepts exactly one input directory");
                }
                input_directory = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    let Some(input_directory) = input_directory else {
        bail!("rust-create-group requires an input directory");
    };

    let catalog = create_legacy_resource_group_from_directory(
        &input_directory,
        resource_prefix.as_deref(),
        calculate_compressions,
    )
    .map_err(|error| anyhow!("creating Rust resource group failed: {error}"))?;

    let output = match document_version.as_str() {
        "0.0.0" => carbon_resources_core::export_legacy_csv_resource_group(&catalog),
        "0.1.0" => export_legacy_yaml_resource_group(&catalog),
        other => bail!("unsupported rust-create-group document version: {other}"),
    };

    if let Some(parent) = output_file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    fs::write(&output_file, output)
        .with_context(|| format!("writing Rust create-group output {}", output_file.display()))?;

    if export_resources {
        if export_resources_destination_type != "LOCAL_RELATIVE" {
            bail!(
                "rust-create-group currently supports only LOCAL_RELATIVE export resources output, got {}",
                export_resources_destination_type
            );
        }
        let Some(destination_path) = export_resources_destination_path else {
            bail!("rust-create-group requires --export-resources-destination-path when --export-resources is set");
        };
        export_legacy_local_relative_resources(&catalog, &input_directory, &destination_path)
            .map_err(|error| anyhow!("exporting Rust create-group resources failed: {error}"))?;
    }
    Ok(())
}

fn rust_create_group_from_filter(args: Vec<String>) -> Result<()> {
    let mut filter_index_mapping_file: Option<PathBuf> = None;
    let mut filter_file_basepath: Option<PathBuf> = None;
    let mut prefix_map_basepath: Option<PathBuf> = None;
    let mut output_directory = PathBuf::from(".");
    let mut calculate_compressions = true;
    let mut export_resources = false;
    let mut export_resources_destination_type = String::from("LOCAL_RELATIVE");
    let mut export_resources_destination_path: Option<PathBuf> = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--filter-index-mapping-file" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--filter-index-mapping-file requires a value");
                };
                filter_index_mapping_file = Some(PathBuf::from(value));
            }
            "--filter-file-basepath" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--filter-file-basepath requires a value");
                };
                filter_file_basepath = Some(PathBuf::from(value));
            }
            "--prefix-map-basepath" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--prefix-map-basepath requires a value");
                };
                prefix_map_basepath = Some(PathBuf::from(value));
            }
            "--output-directory" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output-directory requires a value");
                };
                output_directory = PathBuf::from(value);
            }
            "--skip-compression" => {
                calculate_compressions = false;
            }
            "--export-resources" => {
                export_resources = true;
            }
            "--export-resources-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--export-resources-destination-type requires a value");
                };
                export_resources_destination_type = value.clone();
            }
            "--export-resources-destination-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--export-resources-destination-path requires a value");
                };
                export_resources_destination_path = Some(PathBuf::from(value));
            }
            "--number-of-threads" | "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("{} requires a value", args[index - 1]);
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-create-group-from-filter [options]\n\noptions:\n  --filter-index-mapping-file <path>\n  --filter-file-basepath <path>\n  --prefix-map-basepath <path>\n  --output-directory <path>\n  --skip-compression\n  --export-resources\n  --export-resources-destination-type LOCAL_RELATIVE\n  --export-resources-destination-path <path>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => {
                bail!("unknown rust-create-group-from-filter option: {value}")
            }
            value => bail!("unexpected rust-create-group-from-filter argument: {value}"),
        }
        index += 1;
    }

    let Some(filter_index_mapping_file) = filter_index_mapping_file else {
        bail!("rust-create-group-from-filter requires --filter-index-mapping-file");
    };
    let Some(filter_file_basepath) = filter_file_basepath else {
        bail!("rust-create-group-from-filter requires --filter-file-basepath");
    };
    let Some(prefix_map_basepath) = prefix_map_basepath else {
        bail!("rust-create-group-from-filter requires --prefix-map-basepath");
    };

    let mapping_text = fs::read_to_string(&filter_index_mapping_file)
        .with_context(|| format!("reading {}", filter_index_mapping_file.display()))?;
    let mappings = parse_legacy_filter_index_mapping_yaml(&mapping_text)
        .map_err(|error| anyhow!("parsing filter index mapping failed: {error}"))?;
    let groups = create_legacy_resource_groups_from_filter_mapping(
        &prefix_map_basepath,
        &filter_file_basepath,
        &mappings,
        calculate_compressions,
    )
    .map_err(|error| anyhow!("creating filtered Rust resource groups failed: {error}"))?;

    if export_resources && export_resources_destination_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-create-group-from-filter currently supports only LOCAL_RELATIVE export resources output, got {}",
            export_resources_destination_type
        );
    }
    let export_resources_destination_path = if export_resources {
        Some(export_resources_destination_path.ok_or_else(|| {
            anyhow!(
                "rust-create-group-from-filter requires --export-resources-destination-path when --export-resources is set"
            )
        })?)
    } else {
        None
    };

    for (relative_output_path, catalog) in groups {
        let output_file = output_directory.join(relative_output_path);
        if let Some(parent) = output_file
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory {}", parent.display()))?;
        }
        fs::write(&output_file, export_legacy_yaml_resource_group(&catalog)).with_context(
            || {
                format!(
                    "writing Rust create-group-from-filter output {}",
                    output_file.display()
                )
            },
        )?;
        if let Some(destination_path) = &export_resources_destination_path {
            export_legacy_local_relative_resources(
                &catalog,
                &prefix_map_basepath,
                destination_path,
            )
            .map_err(|error| {
                anyhow!("exporting Rust create-group-from-filter resources failed: {error}")
            })?;
        }
    }
    Ok(())
}

fn rust_create_bundle(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut resource_source_path: Option<PathBuf> = None;
    let mut bundle_manifest_relative_path: Option<PathBuf> = None;
    let mut bundle_manifest_destination_path = PathBuf::from(".");
    let mut bundle_manifest_destination_type = String::from("LOCAL_RELATIVE");
    let mut chunk_destination_path: Option<PathBuf> = None;
    let mut chunk_destination_type = String::from("LOCAL_CDN");
    let mut chunk_size = 0_u64;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--resource-source-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-source-path requires a value");
                };
                resource_source_path = Some(PathBuf::from(value));
            }
            "--bundle-resourcegroup-relative-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--bundle-resourcegroup-relative-path requires a value");
                };
                bundle_manifest_relative_path = Some(PathBuf::from(value));
            }
            "--bundle-resourcegroup-destination-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--bundle-resourcegroup-destination-path requires a value");
                };
                bundle_manifest_destination_path = PathBuf::from(value);
            }
            "--bundle-resourcegroup-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--bundle-resourcegroup-destination-type requires a value");
                };
                bundle_manifest_destination_type = value.clone();
            }
            "--chunk-destination-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-destination-path requires a value");
                };
                chunk_destination_path = Some(PathBuf::from(value));
            }
            "--chunk-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-destination-type requires a value");
                };
                chunk_destination_type = value.clone();
            }
            "--chunk-size" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-size requires a value");
                };
                chunk_size = value
                    .parse::<u64>()
                    .with_context(|| format!("parsing --chunk-size value {value}"))?;
            }
            "--number-of-threads" | "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("{} requires a value", args[index - 1]);
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-create-bundle [options] <resource-group-path>\n\noptions:\n  --resource-source-path <path>\n  --bundle-resourcegroup-relative-path <path>\n  --bundle-resourcegroup-destination-path <path>\n  --bundle-resourcegroup-destination-type LOCAL_RELATIVE\n  --chunk-destination-path <path>\n  --chunk-destination-type LOCAL_CDN|REMOTE_CDN\n  --chunk-size <bytes>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-create-bundle option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 1 {
        bail!("rust-create-bundle requires one resource group path");
    }
    if bundle_manifest_destination_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-create-bundle currently supports only LOCAL_RELATIVE bundle manifest output, got {}",
            bundle_manifest_destination_type
        );
    }
    if !matches!(chunk_destination_type.as_str(), "LOCAL_CDN" | "REMOTE_CDN") {
        bail!(
            "rust-create-bundle supports LOCAL_CDN or REMOTE_CDN chunk output, got {}",
            chunk_destination_type
        );
    }
    if chunk_size == 0 {
        bail!("rust-create-bundle requires --chunk-size greater than zero");
    }

    let Some(resource_source_path) = resource_source_path else {
        bail!("rust-create-bundle requires --resource-source-path");
    };
    let Some(bundle_manifest_relative_path) = bundle_manifest_relative_path else {
        bail!("rust-create-bundle requires --bundle-resourcegroup-relative-path");
    };
    let Some(chunk_destination_path) = chunk_destination_path else {
        bail!("rust-create-bundle requires --chunk-destination-path");
    };

    let catalog = load_legacy_resource_catalog(&positional[0])?;
    let chunk_destination_relative_path =
        chunk_destination_path.to_string_lossy().replace('\\', "/");
    let bundle = match chunk_destination_type.as_str() {
        "LOCAL_CDN" => create_legacy_local_bundle_from_resource_group(
            &catalog,
            &resource_source_path,
            "ResourceGroup.yaml",
            &chunk_destination_relative_path,
            chunk_size,
            10_000_000,
        ),
        "REMOTE_CDN" => create_legacy_remote_cdn_bundle_from_resource_group(
            &catalog,
            &resource_source_path,
            "ResourceGroup.yaml",
            &chunk_destination_relative_path,
            chunk_size,
            10_000_000,
        ),
        other => unreachable!("validated chunk destination type {other}"),
    }
    .map_err(|error| anyhow!("creating Rust bundle failed: {error}"))?;

    let manifest_path = bundle_manifest_destination_path.join(&bundle_manifest_relative_path);
    if let Some(parent) = manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating bundle manifest dir {}", parent.display()))?;
    }
    fs::write(
        &manifest_path,
        export_legacy_yaml_bundle_resource_group(&bundle.catalog),
    )
    .with_context(|| format!("writing Rust bundle manifest {}", manifest_path.display()))?;

    write_legacy_bundle_payload(&bundle.resource_group_resource, &chunk_destination_path)?;
    for chunk in &bundle.chunks {
        write_legacy_bundle_payload(chunk, &chunk_destination_path)?;
    }
    Ok(())
}

fn write_legacy_bundle_payload(
    resource: &LegacyBundleDataResource,
    output_base: &Path,
) -> Result<()> {
    let output_path = output_base.join(resource.record.location.replace('\\', "/"));
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating bundle payload dir {}", parent.display()))?;
    }
    fs::write(&output_path, &resource.data)
        .with_context(|| format!("writing Rust bundle payload {}", output_path.display()))
}

fn rust_unpack_bundle(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut chunk_source_base_path: Option<PathBuf> = None;
    let mut remote_cache_base_path: Option<PathBuf> = None;
    let mut stats_output_path: Option<PathBuf> = None;
    let mut chunk_source_type = String::from("LOCAL_CDN");
    let mut output_base_path = PathBuf::from("UnpackBundleOut");
    let mut resource_destination_type = String::from("LOCAL_RELATIVE");
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--chunk-source-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-source-base-path requires a value");
                };
                chunk_source_base_path = Some(PathBuf::from(value));
            }
            "--remote-cache-base-path" | "--chunk-cache-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("{} requires a value", args[index - 1]);
                };
                remote_cache_base_path = Some(PathBuf::from(value));
            }
            "--stats-output" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--stats-output requires a value");
                };
                stats_output_path = Some(PathBuf::from(value));
            }
            "--chunk-source-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-source-type requires a value");
                };
                chunk_source_type = value.clone();
            }
            "--resource-destination-base-path" | "--output-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("{} requires a value", args[index - 1]);
                };
                output_base_path = PathBuf::from(value);
            }
            "--resource-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-destination-type requires a value");
                };
                resource_destination_type = value.clone();
            }
            "--number-of-threads" | "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("{} requires a value", args[index - 1]);
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-unpack-bundle [options] <bundle-resource-group-path>\n\noptions:\n  --chunk-source-base-path <path>\n  --chunk-source-type LOCAL_CDN|REMOTE_CDN\n  --remote-cache-base-path <path>\n  --stats-output <path>\n  --resource-destination-base-path <path>\n  --resource-destination-type LOCAL_RELATIVE"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-unpack-bundle option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 1 {
        bail!("rust-unpack-bundle requires one bundle resource group path");
    }
    if !matches!(chunk_source_type.as_str(), "LOCAL_CDN" | "REMOTE_CDN") {
        bail!(
            "rust-unpack-bundle supports LOCAL_CDN or REMOTE_CDN chunk input, got {}",
            chunk_source_type
        );
    }
    if resource_destination_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-unpack-bundle currently supports only LOCAL_RELATIVE resource output, got {}",
            resource_destination_type
        );
    }
    let Some(chunk_source_base_path) = chunk_source_base_path else {
        bail!("rust-unpack-bundle requires --chunk-source-base-path");
    };
    if chunk_source_type == "LOCAL_CDN" && remote_cache_base_path.is_some() {
        bail!("rust-unpack-bundle accepts --remote-cache-base-path only with REMOTE_CDN input");
    }
    if chunk_source_type == "REMOTE_CDN" && remote_cache_base_path.is_none() {
        bail!("rust-unpack-bundle requires --remote-cache-base-path with REMOTE_CDN input");
    }

    let bundle_text = fs::read_to_string(&positional[0])
        .with_context(|| format!("reading {}", positional[0].display()))?;
    let catalog = parse_legacy_yaml_bundle_resource_group(&bundle_text)
        .map_err(|error| anyhow!("parsing Rust bundle manifest failed: {error}"))?;
    let (unpacked, cache_stats) = match chunk_source_type.as_str() {
        "LOCAL_CDN" => {
            let unpacked =
                unpack_legacy_local_bundle_from_cdn(&catalog, &chunk_source_base_path)
                    .map_err(|error| anyhow!("unpacking Rust local bundle failed: {error}"))?;
            (
                unpacked,
                Some(json!({
                    "chunk_source_type": "LOCAL_CDN",
                    "cache_hits": 0,
                    "downloads": 0,
                    "replaced_bad_cache_entries": 0,
                    "bytes_copied_to_cache": 0
                })),
            )
        }
        "REMOTE_CDN" => {
            let remote_cache_base_path = remote_cache_base_path
                .as_ref()
                .expect("REMOTE_CDN cache path validated above");
            let remote = unpack_legacy_remote_bundle_from_local_mirror(
                &catalog,
                &chunk_source_base_path,
                remote_cache_base_path,
            )
            .map_err(|error| anyhow!("unpacking Rust remote CDN bundle failed: {error}"))?;
            (
                remote.unpacked,
                Some(json!({
                    "chunk_source_type": "REMOTE_CDN",
                    "cache_hits": remote.cache_stats.cache_hits,
                    "downloads": remote.cache_stats.downloads,
                    "replaced_bad_cache_entries": remote.cache_stats.replaced_bad_cache_entries,
                    "bytes_copied_to_cache": remote.cache_stats.bytes_copied_to_cache
                })),
            )
        }
        other => unreachable!("validated chunk source type {other}"),
    };
    if let Some(stats_output_path) = stats_output_path {
        let Some(cache_stats) = cache_stats else {
            unreachable!("cache stats are always populated");
        };
        write_json(&stats_output_path, &cache_stats)?;
    }

    write_legacy_relative_payload(
        &output_base_path,
        Path::new("ResourceGroup.yaml"),
        &unpacked.resource_group_resource.data,
        "bundle resource group",
    )?;
    for resource in &unpacked.resources {
        write_legacy_relative_payload(
            &output_base_path,
            Path::new(&resource.path),
            &resource.data,
            "unpacked bundle resource",
        )?;
    }
    Ok(())
}

fn rust_create_patch(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut resource_source_type_previous = String::from("LOCAL_RELATIVE");
    let mut previous_resource_base_path: Option<PathBuf> = None;
    let mut next_resource_base_path: Option<PathBuf> = None;
    let mut patch_manifest_destination_path = PathBuf::from(".");
    let mut patch_destination_base_path: Option<PathBuf> = None;
    let mut patch_destination_type = String::from("LOCAL_CDN");
    let mut resource_group_relative_path = String::from("ResourceGroup.yaml");
    let mut patch_file_relative_path_prefix = String::from("Patches/Patch");
    let mut chunk_size = 0_u64;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--resource-source-type-previous" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-source-type-previous requires a value");
                };
                resource_source_type_previous = value.clone();
            }
            "--resource-source-base-path-previous" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-source-base-path-previous requires a value");
                };
                previous_resource_base_path = Some(PathBuf::from(value));
            }
            "--resource-source-base-path-next" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-source-base-path-next requires a value");
                };
                next_resource_base_path = Some(PathBuf::from(value));
            }
            "--patch-resourcegroup-destination-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-resourcegroup-destination-path requires a value");
                };
                patch_manifest_destination_path = PathBuf::from(value);
            }
            "--patch-destination-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-destination-base-path requires a value");
                };
                patch_destination_base_path = Some(PathBuf::from(value));
            }
            "--patch-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-destination-type requires a value");
                };
                patch_destination_type = value.clone();
            }
            "--resource-group-relative-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resource-group-relative-path requires a value");
                };
                resource_group_relative_path = value.clone();
            }
            "--patch-file-relative-path-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-file-relative-path-prefix requires a value");
                };
                patch_file_relative_path_prefix = value.clone();
            }
            "--chunk-size" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--chunk-size requires a value");
                };
                chunk_size = value
                    .parse::<u64>()
                    .with_context(|| format!("parsing --chunk-size value {value}"))?;
            }
            "--number-of-threads" | "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("{} requires a value", args[index - 1]);
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-create-patch [options] <previous-resource-group-path> <next-resource-group-path>\n\noptions:\n  --resource-source-type-previous LOCAL_RELATIVE\n  --resource-source-base-path-previous <path>\n  --resource-source-base-path-next <path>\n  --patch-resourcegroup-destination-path <path>\n  --patch-destination-base-path <path>\n  --patch-destination-type LOCAL_CDN\n  --resource-group-relative-path <path>\n  --patch-file-relative-path-prefix <prefix>\n  --chunk-size <bytes>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-create-patch option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 2 {
        bail!("rust-create-patch requires previous and next resource group paths");
    }
    if resource_source_type_previous != "LOCAL_RELATIVE" {
        bail!(
            "rust-create-patch currently supports only LOCAL_RELATIVE previous resources, got {}",
            resource_source_type_previous
        );
    }
    if patch_destination_type != "LOCAL_CDN" {
        bail!(
            "rust-create-patch currently supports only LOCAL_CDN patch output, got {}",
            patch_destination_type
        );
    }
    if chunk_size == 0 {
        bail!("rust-create-patch requires --chunk-size greater than zero");
    }
    let Some(previous_resource_base_path) = previous_resource_base_path else {
        bail!("rust-create-patch requires --resource-source-base-path-previous");
    };
    let Some(next_resource_base_path) = next_resource_base_path else {
        bail!("rust-create-patch requires --resource-source-base-path-next");
    };
    let Some(patch_destination_base_path) = patch_destination_base_path else {
        bail!("rust-create-patch requires --patch-destination-base-path");
    };

    let previous_catalog = load_legacy_resource_catalog(&positional[0])?;
    let next_catalog = load_legacy_resource_catalog(&positional[1])?;
    let patch = create_legacy_local_patch_from_resource_groups_with_options(
        &previous_catalog,
        &next_catalog,
        &previous_resource_base_path,
        &next_resource_base_path,
        chunk_size,
        &resource_group_relative_path,
        &patch_file_relative_path_prefix,
    )
    .map_err(|error| anyhow!("creating Rust patch failed: {error}"))?;

    let manifest_path = patch_manifest_destination_path.join("PatchResourceGroup.yaml");
    if let Some(parent) = manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating patch manifest dir {}", parent.display()))?;
    }
    fs::write(
        &manifest_path,
        export_legacy_yaml_patch_resource_group(&patch.catalog),
    )
    .with_context(|| format!("writing Rust patch manifest {}", manifest_path.display()))?;

    write_legacy_patch_payload(&patch.resource_group_resource, &patch_destination_base_path)?;
    for resource in &patch.resources {
        write_legacy_patch_payload(resource, &patch_destination_base_path)?;
    }
    Ok(())
}

fn write_legacy_patch_payload(
    resource: &LegacyPatchDataResource,
    output_base: &Path,
) -> Result<()> {
    let output_path = output_base.join(resource.location.replace('\\', "/"));
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating patch payload dir {}", parent.display()))?;
    }
    fs::write(&output_path, &resource.data)
        .with_context(|| format!("writing Rust patch payload {}", output_path.display()))
}

fn rust_apply_patch(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut patch_binaries_base_path: Option<PathBuf> = None;
    let mut patch_binaries_source_type = String::from("LOCAL_CDN");
    let mut resources_to_patch_base_path: Option<PathBuf> = None;
    let mut resources_to_patch_source_type = String::from("LOCAL_RELATIVE");
    let mut next_resources_base_path: Option<PathBuf> = None;
    let mut next_resources_source_type = String::from("LOCAL_RELATIVE");
    let mut output_base_path = PathBuf::from("ApplyPatchOut");
    let mut output_destination_type = String::from("LOCAL_RELATIVE");
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--patch-binaries-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-binaries-base-path requires a value");
                };
                patch_binaries_base_path = Some(PathBuf::from(value));
            }
            "--patch-binaries-source-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--patch-binaries-source-type requires a value");
                };
                patch_binaries_source_type = value.clone();
            }
            "--resources-to-patch-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resources-to-patch-base-path requires a value");
                };
                resources_to_patch_base_path = Some(PathBuf::from(value));
            }
            "--resources-to-patch-source-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--resources-to-patch-source-type requires a value");
                };
                resources_to_patch_source_type = value.clone();
            }
            "--next-resources-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--next-resources-base-path requires a value");
                };
                next_resources_base_path = Some(PathBuf::from(value));
            }
            "--next-resources-source-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--next-resources-source-type requires a value");
                };
                next_resources_source_type = value.clone();
            }
            "--output-base-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output-base-path requires a value");
                };
                output_base_path = PathBuf::from(value);
            }
            "--output-destination-type" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output-destination-type requires a value");
                };
                output_destination_type = value.clone();
            }
            "--number-of-threads" | "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("{} requires a value", args[index - 1]);
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-apply-patch [options] <patch-resource-group-path>\n\noptions:\n  --patch-binaries-base-path <path>\n  --patch-binaries-source-type LOCAL_CDN\n  --resources-to-patch-base-path <path>\n  --resources-to-patch-source-type LOCAL_RELATIVE\n  --next-resources-base-path <path>\n  --next-resources-source-type LOCAL_RELATIVE\n  --output-base-path <path>\n  --output-destination-type LOCAL_RELATIVE"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-apply-patch option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 1 {
        bail!("rust-apply-patch requires one patch resource group path");
    }
    if patch_binaries_source_type != "LOCAL_CDN" {
        bail!(
            "rust-apply-patch currently supports only LOCAL_CDN patch binary input, got {}",
            patch_binaries_source_type
        );
    }
    if resources_to_patch_source_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-apply-patch currently supports only LOCAL_RELATIVE previous resource input, got {}",
            resources_to_patch_source_type
        );
    }
    if next_resources_source_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-apply-patch currently supports only LOCAL_RELATIVE next resource input, got {}",
            next_resources_source_type
        );
    }
    if output_destination_type != "LOCAL_RELATIVE" {
        bail!(
            "rust-apply-patch currently supports only LOCAL_RELATIVE resource output, got {}",
            output_destination_type
        );
    }
    let Some(patch_binaries_base_path) = patch_binaries_base_path else {
        bail!("rust-apply-patch requires --patch-binaries-base-path");
    };
    let Some(resources_to_patch_base_path) = resources_to_patch_base_path else {
        bail!("rust-apply-patch requires --resources-to-patch-base-path");
    };
    let Some(next_resources_base_path) = next_resources_base_path else {
        bail!("rust-apply-patch requires --next-resources-base-path");
    };

    let patch_text = fs::read_to_string(&positional[0])
        .with_context(|| format!("reading {}", positional[0].display()))?;
    let catalog = parse_legacy_yaml_patch_resource_group(&patch_text)
        .map_err(|error| anyhow!("parsing Rust patch manifest failed: {error}"))?;
    let applied = apply_legacy_local_patch_from_directories(
        &catalog,
        &resources_to_patch_base_path,
        &next_resources_base_path,
        &patch_binaries_base_path,
    )
    .map_err(|error| anyhow!("applying Rust patch failed: {error}"))?;

    copy_directory_recursive(&resources_to_patch_base_path, &output_base_path)?;
    if let Some(removed_paths) = &catalog.removed_resource_relative_paths {
        for removed_path in removed_paths {
            remove_legacy_relative_output_path(&output_base_path, Path::new(removed_path))?;
        }
    }
    for resource in &applied.resources {
        write_legacy_relative_payload(
            &output_base_path,
            Path::new(&resource.path),
            &resource.data,
            "applied patch resource",
        )?;
    }
    Ok(())
}

fn copy_directory_recursive(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("creating destination directory {}", destination.display()))?;
    for entry in fs::read_dir(source).with_context(|| format!("reading {}", source.display()))? {
        let entry = entry.with_context(|| format!("reading entry in {}", source.display()))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .with_context(|| format!("reading file type for {}", source_path.display()))?;
        if file_type.is_dir() {
            copy_directory_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating directory {}", parent.display()))?;
            }
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copying {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        }
    }
    Ok(())
}

fn remove_legacy_relative_output_path(output_base: &Path, relative_path: &Path) -> Result<()> {
    let output_path = output_base.join(relative_path);
    let Some(output_path) = resolve_existing_path_case_insensitive(&output_path) else {
        return Ok(());
    };
    if output_path.is_dir() {
        fs::remove_dir_all(&output_path)
            .with_context(|| format!("removing directory {}", output_path.display()))?;
    } else {
        fs::remove_file(&output_path)
            .with_context(|| format!("removing file {}", output_path.display()))?;
    }
    Ok(())
}

fn write_legacy_relative_payload(
    output_base: &Path,
    relative_path: &Path,
    data: &[u8],
    label: &str,
) -> Result<()> {
    let requested_output_path = output_base.join(relative_path);
    let output_path = resolve_existing_path_case_insensitive(&requested_output_path)
        .unwrap_or(requested_output_path);
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating {label} dir {}", parent.display()))?;
    }
    fs::write(&output_path, data)
        .with_context(|| format!("writing {label} {}", output_path.display()))
}

fn rust_merge_group(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut output_file = PathBuf::from("ResourceGroup.yaml");
    let mut document_version = String::from("0.1.0");
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--merge-output-resource-group-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--merge-output-resource-group-path requires a value");
                };
                output_file = PathBuf::from(value);
            }
            "--document-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--document-version requires a value");
                };
                document_version = value.clone();
            }
            "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("--verbosity-level requires a value");
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-merge-group [options] <base-resource-group-path> <merge-resource-group-path>\n\noptions:\n  --merge-output-resource-group-path <path>\n  --document-version <0.1.0|0.0.0>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-merge-group option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 2 {
        bail!("rust-merge-group requires base and merge resource group paths");
    }

    let base = load_legacy_resource_catalog(&positional[0])?;
    let merge = load_legacy_resource_catalog(&positional[1])?;
    let merged = merge_legacy_resource_catalogs(&base, &merge);
    let output = match document_version.as_str() {
        "0.0.0" => export_legacy_csv_resource_group(&merged),
        "0.1.0" => export_legacy_yaml_resource_group(&merged),
        other => bail!("unsupported rust-merge-group document version: {other}"),
    };

    if let Some(parent) = output_file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    fs::write(&output_file, output)
        .with_context(|| format!("writing Rust merge output {}", output_file.display()))
}

fn rust_diff_group(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut output_file = PathBuf::from("Diff.txt");
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--diff-output-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--diff-output-path requires a value");
                };
                output_file = PathBuf::from(value);
            }
            "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("--verbosity-level requires a value");
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-diff-group [options] <base-resource-group-path> <diff-resource-group-path>\n\noptions:\n  --diff-output-path <path>"
                );
                return Ok(());
            }
            value if value.starts_with('-') => bail!("unknown rust-diff-group option: {value}"),
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 2 {
        bail!("rust-diff-group requires base and diff resource group paths");
    }

    let base = load_legacy_resource_catalog(&positional[0])?;
    let target = load_legacy_resource_catalog(&positional[1])?;
    let diff = diff_legacy_resource_catalogs(&base, &target);
    let output = export_legacy_diff(&diff);

    if let Some(parent) = output_file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    fs::write(&output_file, output)
        .with_context(|| format!("writing Rust diff output {}", output_file.display()))
}

fn rust_remove_resources(args: Vec<String>) -> Result<()> {
    let mut positional = Vec::<PathBuf>::new();
    let mut output_file = PathBuf::from("ResourceGroup.yaml");
    let mut document_version = String::from("0.1.0");
    let mut ignore_missing_resources = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--output-resource-group-path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--output-resource-group-path requires a value");
                };
                output_file = PathBuf::from(value);
            }
            "--document-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    bail!("--document-version requires a value");
                };
                document_version = value.clone();
            }
            "--ignore-missing-resources" => {
                ignore_missing_resources = true;
            }
            "--verbosity-level" => {
                index += 1;
                if args.get(index).is_none() {
                    bail!("--verbosity-level requires a value");
                }
            }
            "--help" | "-h" => {
                println!(
                    "usage: xtask rust-remove-resources [options] <resource-group-path> <resource-list-path>\n\noptions:\n  --output-resource-group-path <path>\n  --document-version <0.1.0|0.0.0>\n  --ignore-missing-resources"
                );
                return Ok(());
            }
            value if value.starts_with('-') => {
                bail!("unknown rust-remove-resources option: {value}")
            }
            value => positional.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positional.len() != 2 {
        bail!("rust-remove-resources requires resource group and resource list paths");
    }

    let catalog = load_legacy_resource_catalog(&positional[0])?;
    let list = fs::read_to_string(&positional[1])
        .with_context(|| format!("reading {}", positional[1].display()))?;
    let paths_to_remove = list.lines().map(str::to_string).collect::<Vec<_>>();
    let removed = remove_legacy_resources(&catalog, &paths_to_remove, !ignore_missing_resources)
        .map_err(|error| anyhow!("removing resources failed: {error}"))?;
    let output = match document_version.as_str() {
        "0.0.0" => export_legacy_csv_resource_group(&removed),
        "0.1.0" => export_legacy_yaml_resource_group(&removed),
        other => bail!("unsupported rust-remove-resources document version: {other}"),
    };

    if let Some(parent) = output_file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    fs::write(&output_file, output).with_context(|| {
        format!(
            "writing Rust remove-resources output {}",
            output_file.display()
        )
    })
}

fn load_legacy_resource_catalog(path: &Path) -> Result<ResourceCatalog> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    if text.trim_start().starts_with("Version:") {
        parse_legacy_yaml_resource_group(&text).map_err(|error| {
            anyhow!(
                "parsing YAML resource group {} failed: {error}",
                path.display()
            )
        })
    } else {
        parse_legacy_csv_resource_group(&text).map_err(|error| {
            anyhow!(
                "parsing CSV resource group {} failed: {error}",
                path.display()
            )
        })
    }
}

#[derive(Clone, Debug)]
struct ProcessMetrics {
    wall_time_us: u64,
    user_cpu_ms: Option<u64>,
    system_cpu_ms: Option<u64>,
    cpu_percent: Option<f64>,
    max_rss_kb: Option<u64>,
    measurement_source: &'static str,
}

impl ProcessMetrics {
    fn cpu_burn_ms(&self) -> Option<u64> {
        Some(self.user_cpu_ms? + self.system_cpu_ms?)
    }

    fn effective_cpu_burn_ms(&self) -> Option<f64> {
        match self.cpu_burn_ms() {
            Some(value) if value > 0 => Some(value as f64),
            _ => Some((self.wall_time_us as f64 / 1000.0) * (self.cpu_percent? / 100.0)),
        }
    }

    fn effective_cpu_burn_source(&self) -> &'static str {
        if self.cpu_burn_ms().unwrap_or_default() > 0 {
            "user_plus_system_time"
        } else if self.cpu_percent.is_some() {
            "wall_time_times_cpu_percent"
        } else {
            "unavailable"
        }
    }
}

struct MeasuredProcessOutput {
    output: Output,
    metrics: ProcessMetrics,
}

fn run_timed_process(
    program: &Path,
    args: &[OsString],
    current_dir: Option<&Path>,
) -> Result<MeasuredProcessOutput> {
    run_timed_process_with_env(program, args, current_dir, &[])
}

fn run_timed_process_with_env(
    program: &Path,
    args: &[OsString],
    current_dir: Option<&Path>,
    envs: &[(OsString, OsString)],
) -> Result<MeasuredProcessOutput> {
    let time_program = Path::new("/usr/bin/time");
    let use_time = time_program.exists();
    let started = Instant::now();
    let mut command = if use_time {
        let mut command = Command::new(time_program);
        command.arg("-v").arg(program);
        command
    } else {
        Command::new(program)
    };
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command
        .output()
        .with_context(|| format!("running timed process {}", program.display()))?;
    let wall_time_us = started.elapsed().as_micros() as u64;
    let mut metrics = if use_time {
        parse_time_v_metrics(&String::from_utf8_lossy(&output.stderr))
    } else {
        ProcessMetrics {
            wall_time_us,
            user_cpu_ms: None,
            system_cpu_ms: None,
            cpu_percent: None,
            max_rss_kb: None,
            measurement_source: "wall_clock_only",
        }
    };
    metrics.wall_time_us = wall_time_us;

    Ok(MeasuredProcessOutput { output, metrics })
}

fn parse_time_v_metrics(stderr: &str) -> ProcessMetrics {
    let mut metrics = ProcessMetrics {
        wall_time_us: 0,
        user_cpu_ms: None,
        system_cpu_ms: None,
        cpu_percent: None,
        max_rss_kb: None,
        measurement_source: "external_time_v",
    };

    for line in stderr.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("User time (seconds):") {
            metrics.user_cpu_ms = parse_seconds_to_ms(value);
        } else if let Some(value) = line.strip_prefix("System time (seconds):") {
            metrics.system_cpu_ms = parse_seconds_to_ms(value);
        } else if let Some(value) = line.strip_prefix("Percent of CPU this job got:") {
            metrics.cpu_percent = value.trim().trim_end_matches('%').parse::<f64>().ok();
        } else if let Some(value) = line.strip_prefix("Maximum resident set size (kbytes):") {
            metrics.max_rss_kb = value.trim().parse::<u64>().ok();
        }
    }

    metrics
}

fn parse_seconds_to_ms(value: &str) -> Option<u64> {
    Some((value.trim().parse::<f64>().ok()? * 1000.0).round() as u64)
}

fn process_metrics_sample_json(samples: &[ProcessMetrics]) -> Value {
    Value::Array(
        samples
            .iter()
            .map(|sample| {
                json!({
                    "wall_time_us": sample.wall_time_us,
                    "user_cpu_ms": sample.user_cpu_ms,
                    "system_cpu_ms": sample.system_cpu_ms,
                    "cpu_burn_ms": sample.cpu_burn_ms(),
                    "cpu_burn_effective_ms": sample.effective_cpu_burn_ms(),
                    "cpu_burn_effective_source": sample.effective_cpu_burn_source(),
                    "cpu_percent": sample.cpu_percent,
                    "max_rss_kb": sample.max_rss_kb,
                    "measurement_source": sample.measurement_source
                })
            })
            .collect(),
    )
}

fn process_metrics_summary(samples: &[ProcessMetrics]) -> Value {
    let wall_time_us = samples
        .iter()
        .map(|sample| sample.wall_time_us)
        .collect::<Vec<_>>();
    let user_cpu_ms = samples
        .iter()
        .filter_map(|sample| sample.user_cpu_ms)
        .collect::<Vec<_>>();
    let system_cpu_ms = samples
        .iter()
        .filter_map(|sample| sample.system_cpu_ms)
        .collect::<Vec<_>>();
    let cpu_burn_ms = samples
        .iter()
        .filter_map(ProcessMetrics::cpu_burn_ms)
        .collect::<Vec<_>>();
    let cpu_burn_effective_ms = samples
        .iter()
        .filter_map(ProcessMetrics::effective_cpu_burn_ms)
        .collect::<Vec<_>>();
    let cpu_percent = samples
        .iter()
        .filter_map(|sample| sample.cpu_percent)
        .collect::<Vec<_>>();
    let max_rss_kb = samples
        .iter()
        .filter_map(|sample| sample.max_rss_kb)
        .collect::<Vec<_>>();
    let direct_cpu_burn_samples = samples
        .iter()
        .filter(|sample| sample.effective_cpu_burn_source() == "user_plus_system_time")
        .count();
    let estimated_cpu_burn_samples = samples
        .iter()
        .filter(|sample| sample.effective_cpu_burn_source() == "wall_time_times_cpu_percent")
        .count();
    let unavailable_cpu_burn_samples = samples
        .iter()
        .filter(|sample| sample.effective_cpu_burn_source() == "unavailable")
        .count();
    let cpu_burn_effective_quality =
        if samples.is_empty() || unavailable_cpu_burn_samples == samples.len() {
            "missing"
        } else if unavailable_cpu_burn_samples > 0 {
            "partial"
        } else if estimated_cpu_burn_samples == 0 {
            "direct_user_system_time"
        } else if direct_cpu_burn_samples == 0 {
            "estimated_from_wall_time_times_cpu_percent"
        } else {
            "mixed_direct_and_estimated"
        };
    let source = if samples
        .iter()
        .any(|sample| sample.measurement_source == "external_time_v")
    {
        "external_time_v"
    } else {
        "wall_clock_only"
    };

    json!({
        "sample_count": samples.len(),
        "measurement_source": source,
        "wall_time_us": sample_stats_us(&wall_time_us),
        "user_cpu_ms": sample_stats_u64(&user_cpu_ms),
        "system_cpu_ms": sample_stats_u64(&system_cpu_ms),
        "cpu_burn_ms": sample_stats_u64(&cpu_burn_ms),
        "cpu_burn_effective_ms": sample_stats_f64(&cpu_burn_effective_ms),
        "cpu_burn_effective_source": "user_plus_system_time_when_nonzero_else_wall_time_times_cpu_percent",
        "cpu_burn_effective_source_counts": {
            "user_plus_system_time": direct_cpu_burn_samples,
            "wall_time_times_cpu_percent": estimated_cpu_burn_samples,
            "unavailable": unavailable_cpu_burn_samples
        },
        "cpu_burn_effective_quality": cpu_burn_effective_quality,
        "cpu_percent": sample_stats_f64(&cpu_percent),
        "max_rss_kb": sample_stats_u64(&max_rss_kb)
    })
}

fn process_comparison_summary(
    sample_count: u64,
    legacy_duration_us: u64,
    rust_duration_us: u64,
    legacy_samples: &[ProcessMetrics],
    rust_samples: &[ProcessMetrics],
) -> Value {
    json!({
        "wall_time_ratio_legacy_over_rust": speedup_ratio(legacy_duration_us, rust_duration_us),
        "cpu_burn_ratio_legacy_over_rust": optional_ratio(
            sum_cpu_burn_ms(legacy_samples),
            sum_cpu_burn_ms(rust_samples),
        ),
        "cpu_burn_effective_ratio_legacy_over_rust": optional_ratio_f64(
            sum_effective_cpu_burn_ms(legacy_samples),
            sum_effective_cpu_burn_ms(rust_samples),
        ),
        "peak_rss_ratio_rust_over_legacy_p95": optional_ratio(
            p95_max_rss_kb(rust_samples),
            p95_max_rss_kb(legacy_samples),
        ),
        "linear_scale_estimate_100k_units": {
            "basis": "linear estimate from average per-unit local process samples; process startup is included and this is not a production claim",
            "units": 100_000,
            "legacy_wall_seconds": scaled_wall_seconds(legacy_duration_us, sample_count, 100_000),
            "rust_wall_seconds": scaled_wall_seconds(rust_duration_us, sample_count, 100_000),
            "legacy_cpu_burn_seconds": scaled_cpu_seconds(legacy_samples, 100_000),
            "rust_cpu_burn_seconds": scaled_cpu_seconds(rust_samples, 100_000)
        }
    })
}

struct ProcessBenchmarkSamples {
    duration_us: u64,
    samples_us: Vec<u64>,
    sample_stats_us: Value,
    process_samples: Vec<ProcessMetrics>,
}

#[derive(Clone, Debug)]
struct LegacyResourcesCliBaseline {
    path: PathBuf,
    source: String,
    build_profile: String,
    cmake_build_type: Option<String>,
    dev_features: Option<bool>,
    known_non_debug: bool,
}

impl LegacyResourcesCliBaseline {
    fn evidence(&self) -> Value {
        json!({
            "path": self.path.display().to_string(),
            "source": self.source,
            "build_profile": self.build_profile,
            "cmake_build_type": self.cmake_build_type,
            "dev_features": self.dev_features,
            "known_non_debug": self.known_non_debug,
            "exists": self.path.exists()
        })
    }
}

fn run_process_benchmark_samples<Args, Validate>(
    executable: &Path,
    iterations: u64,
    output_root: &Path,
    side: &str,
    label: &str,
    args_for_iteration: Args,
    validate_first_iteration: Validate,
) -> Result<ProcessBenchmarkSamples>
where
    Args: Fn(u64, &Path) -> Vec<OsString>,
    Validate: Fn(&Path) -> Result<()>,
{
    run_process_benchmark_samples_with_prepare(
        executable,
        iterations,
        output_root,
        side,
        label,
        |_| Ok(()),
        args_for_iteration,
        validate_first_iteration,
    )
}

fn run_process_benchmark_samples_with_prepare<Prepare, Args, Validate>(
    executable: &Path,
    iterations: u64,
    output_root: &Path,
    side: &str,
    label: &str,
    prepare_iteration: Prepare,
    args_for_iteration: Args,
    validate_first_iteration: Validate,
) -> Result<ProcessBenchmarkSamples>
where
    Prepare: Fn(&Path) -> Result<()>,
    Args: Fn(u64, &Path) -> Vec<OsString>,
    Validate: Fn(&Path) -> Result<()>,
{
    let mut samples_us = Vec::new();
    let mut process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..iterations {
        let iteration_dir = output_root.join(format!("{side}-{iteration}"));
        fs::create_dir_all(&iteration_dir)
            .with_context(|| format!("creating {}", iteration_dir.display()))?;
        prepare_iteration(&iteration_dir)?;
        let args = args_for_iteration(iteration, &iteration_dir);
        let measured = run_timed_process(executable, &args, Some(&iteration_dir))
            .with_context(|| format!("running {label}"))?;
        samples_us.push(measured.metrics.wall_time_us);
        process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, label)?;
        if iteration == 0 {
            validate_first_iteration(&iteration_dir)?;
        }
    }

    let duration_us = started.elapsed().as_micros() as u64;
    let sample_stats_us = sample_stats_us(&samples_us);
    Ok(ProcessBenchmarkSamples {
        duration_us,
        samples_us,
        sample_stats_us,
        process_samples,
    })
}

fn assert_file_bytes_match(expected_path: &Path, actual_path: &Path, label: &str) -> Result<u64> {
    let expected =
        fs::read(expected_path).with_context(|| format!("reading {}", expected_path.display()))?;
    let actual =
        fs::read(actual_path).with_context(|| format!("reading {}", actual_path.display()))?;
    if actual != expected {
        bail!(
            "{label} wrote {} but it did not match {}",
            actual_path.display(),
            expected_path.display()
        );
    }
    Ok(actual.len() as u64)
}

fn resolve_legacy_resources_cli_baseline(
    repo_root: &Path,
    env_var: &str,
    default_relative_path: &str,
    default_debug_profile: &str,
) -> Result<LegacyResourcesCliBaseline> {
    let (path, source) = match env::var_os(env_var) {
        Some(value) if !value.is_empty() => {
            let path = PathBuf::from(value);
            let path = if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            };
            (path, format!("env:{env_var}"))
        }
        _ => (
            repo_root.join(default_relative_path),
            String::from("workspace_default_debug_baseline"),
        ),
    };

    let baseline =
        legacy_resources_cli_baseline_metadata(path, source, Some(default_debug_profile));
    if !baseline.path.exists() {
        bail!(
            "legacy resources CLI is missing at {}; set {env_var} to a compatible legacy resources binary or build the default baseline",
            baseline.path.display()
        );
    }
    Ok(baseline)
}

fn legacy_resources_cli_baseline_metadata(
    path: PathBuf,
    source: String,
    default_profile: Option<&str>,
) -> LegacyResourcesCliBaseline {
    let cache = find_ancestor_cmake_cache(&path);
    let cmake_build_type = cache
        .as_deref()
        .and_then(|cache| cmake_cache_value(cache, "CMAKE_BUILD_TYPE"));
    let dev_features = cache
        .as_deref()
        .and_then(|cache| cmake_cache_value(cache, "DEV_FEATURES"))
        .and_then(|value| cmake_bool(&value));
    let file_name = path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let build_type_release = cmake_build_type
        .as_deref()
        .is_some_and(is_release_like_cmake_build_type);
    let known_non_debug = build_type_release && !file_name.contains("debug");
    let build_profile = match (
        default_profile,
        known_non_debug,
        cmake_build_type.as_deref(),
    ) {
        (Some(profile), false, _) => profile.to_string(),
        (_, true, Some(build_type)) => {
            format!("legacy_resources_{}", build_type_label(build_type))
        }
        (_, false, Some(build_type)) if build_type.eq_ignore_ascii_case("debug") => {
            String::from("legacy_resources_debug")
        }
        _ if file_name.contains("debug") => String::from("legacy_resources_debug"),
        _ => String::from("legacy_resources_unknown"),
    };

    LegacyResourcesCliBaseline {
        path,
        source,
        build_profile,
        cmake_build_type,
        dev_features,
        known_non_debug,
    }
}

fn find_ancestor_cmake_cache(path: &Path) -> Option<PathBuf> {
    let mut current = path.parent();
    while let Some(dir) = current {
        let cache = dir.join("CMakeCache.txt");
        if cache.exists() {
            return Some(cache);
        }
        current = dir.parent();
    }
    None
}

fn cmake_cache_value(cache: &Path, key: &str) -> Option<String> {
    let text = fs::read_to_string(cache).ok()?;
    let prefix = format!("{key}:");
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(&prefix)?;
        rest.split_once('=')
            .map(|(_, value)| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn cmake_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_uppercase().as_str() {
        "ON" | "TRUE" | "1" | "YES" => Some(true),
        "OFF" | "FALSE" | "0" | "NO" => Some(false),
        _ => None,
    }
}

fn is_release_like_cmake_build_type(build_type: &str) -> bool {
    matches!(
        build_type.to_ascii_lowercase().as_str(),
        "release" | "relwithdebinfo" | "minsizerel" | "trinitydev"
    )
}

fn build_type_label(build_type: &str) -> String {
    build_type
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn find_legacy_resources_cli_candidates(repo_root: &Path) -> Vec<LegacyResourcesCliBaseline> {
    let resources_dir = repo_root.join("carbonengine/resources");
    let mut candidates = Vec::new();
    let Ok(entries) = fs::read_dir(&resources_dir) else {
        return candidates;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(".cmake-build") {
            continue;
        }
        let cli_dir = entry.path().join("cli");
        let Ok(cli_entries) = fs::read_dir(cli_dir) else {
            continue;
        };
        for cli_entry in cli_entries.flatten() {
            let path = cli_entry.path();
            let Ok(file_type) = cli_entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
                continue;
            };
            if file_name.starts_with("resources") {
                candidates.push(legacy_resources_cli_baseline_metadata(
                    path,
                    String::from("workspace_scan"),
                    None,
                ));
            }
        }
    }
    candidates.sort_by(|left, right| left.path.cmp(&right.path));
    candidates
}

fn benchmark_optimization_readiness(
    rust_native_release_ready: bool,
    normal_legacy_rows: usize,
    dev_legacy_rows: usize,
    normal_legacy: &LegacyResourcesCliBaseline,
    dev_legacy: &LegacyResourcesCliBaseline,
    candidates: &[LegacyResourcesCliBaseline],
) -> Value {
    let candidate_non_debug_count = candidates
        .iter()
        .filter(|candidate| candidate.known_non_debug)
        .count();
    let eligible_comparisons = if rust_native_release_ready {
        (if normal_legacy.known_non_debug {
            normal_legacy_rows
        } else {
            0
        }) + (if dev_legacy.known_non_debug {
            dev_legacy_rows
        } else {
            0
        })
    } else {
        0
    };
    let total_comparisons = normal_legacy_rows + dev_legacy_rows;
    let legacy_optimized_selected = normal_legacy.known_non_debug && dev_legacy.known_non_debug;
    let legacy_detection_status = if legacy_optimized_selected {
        "selected_optimized_legacy_baseline"
    } else if candidate_non_debug_count > 0 {
        "optimized_candidate_available_not_selected"
    } else {
        "blocked_no_optimized_legacy_baseline_detected"
    };
    let blocker = if !rust_native_release_ready {
        "rerun scripts/carbon-native-bench.sh bench so Rust rows use release-native, target-cpu=native, and debug assertions off"
    } else if !legacy_optimized_selected {
        "build or provide non-debug legacy resources CLI binaries and rerun scripts/carbon-native-bench.sh bench with CARBON_LEGACY_RESOURCES_CLI and CARBON_LEGACY_RESOURCES_DEV_CLI when needed"
    } else {
        "none for optimized baseline eligibility; broader parity/report gates still apply"
    };

    json!({
        "rust_native_release_ready": rust_native_release_ready,
        "legacy_optimized_baseline_ready": legacy_optimized_selected,
        "total_comparable_process_comparisons": total_comparisons,
        "speedup_claim_eligible_comparisons": eligible_comparisons,
        "observed_only_comparisons": total_comparisons.saturating_sub(eligible_comparisons),
        "speedup_claims_allowed": rust_native_release_ready && legacy_optimized_selected,
        "blocked_reason": blocker,
        "legacy_optimized_baseline_detection": {
            "status": legacy_detection_status,
            "non_debug_candidate_count": candidate_non_debug_count,
            "selected_default_cli": normal_legacy.evidence(),
            "selected_devfeatures_cli": dev_legacy.evidence(),
            "candidate_binaries": candidates.iter().map(LegacyResourcesCliBaseline::evidence).collect::<Vec<_>>()
        }
    })
}

fn bench_tier_local() -> Result<()> {
    let xtask_build_profile = inferred_xtask_build_profile();
    let rust_build = rust_build_metadata();
    let target_cpu_native = rust_build
        .get("target_cpu_native")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let debug_assertions = rust_build
        .get("debug_assertions")
        .and_then(Value::as_bool)
        .unwrap_or(cfg!(debug_assertions));
    let xtask_bench_command = if xtask_build_profile == "release-native" {
        "scripts/carbon-native-bench.sh bench"
    } else {
        "cargo run -p xtask -- bench"
    };
    let xtask_bench_release_command = "scripts/carbon-native-bench.sh bench";
    let rust_runner = env::current_exe().context("locating current xtask executable")?;
    let scheduler_fixture =
        carbon_scheduler_trace::load_fixture("fixtures/scheduler/run_order.json")?;
    let scheduler_iterations = 20_000_u64;
    let started = Instant::now();
    for _ in 0..scheduler_iterations {
        let trace = run_scenario(&scheduler_fixture.scenario)
            .map_err(|error| anyhow!("scheduler benchmark scenario failed: {error}"))?;
        black_box(trace.events.len());
    }
    let scheduler_duration_ms = started.elapsed().as_millis() as u64;
    let scheduler_events = scheduler_iterations * scheduler_fixture.events.len() as u64;
    let scheduler_process_iterations = 20_000_u64;
    let scheduler_process_sample_count = 1_u64;
    let scheduler_process_args = vec![
        OsString::from("bench-scheduler-core"),
        OsString::from("fixtures/scheduler/run_order.json"),
        OsString::from(scheduler_process_iterations.to_string()),
    ];
    let scheduler_process_command_template = command_line(&rust_runner, &scheduler_process_args);
    let mut scheduler_process_metrics = Vec::new();
    let mut scheduler_process_runs = Vec::new();
    for _ in 0..scheduler_process_sample_count {
        let measured = run_timed_process(&rust_runner, &scheduler_process_args, None)
            .context("running Rust scheduler process benchmark")?;
        let stdout = String::from_utf8_lossy(&measured.output.stdout);
        let stderr = String::from_utf8_lossy(&measured.output.stderr);
        if !measured.output.status.success() {
            bail!(
                "Rust scheduler process benchmark failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                measured.output.status.code(),
                stdout,
                stderr
            );
        }
        let run: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing scheduler process benchmark JSON: {stdout}"))?;
        scheduler_process_metrics.push(measured.metrics.clone());
        scheduler_process_runs.push(run);
    }
    let scheduler_process_duration_us = scheduler_process_metrics
        .iter()
        .map(|sample| sample.wall_time_us)
        .sum::<u64>();
    let scheduler_process_events = scheduler_process_runs
        .iter()
        .filter_map(|run| run.get("events").and_then(Value::as_u64))
        .sum::<u64>();
    let scheduler_process_iterations_total = scheduler_process_runs
        .iter()
        .filter_map(|run| run.get("iterations").and_then(Value::as_u64))
        .sum::<u64>();
    let scheduler_process_latency_stats = scheduler_process_runs
        .first()
        .and_then(|run| run.get("sample_stats_us"))
        .cloned()
        .unwrap_or_else(|| json!({"count": 0}));

    let resource_fixture =
        fs::read("carbonengine/resources/tests/testData/resourcesOnBranch/introMovie.txt")
            .context("reading resources benchmark fixture")?;
    let md5_iterations = 50_000_u64;
    let started = Instant::now();
    for _ in 0..md5_iterations {
        black_box(md5_hex(&resource_fixture));
    }
    let md5_duration_ms = started.elapsed().as_millis() as u64;

    let gzip_iterations = 5_000_u64;
    let started = Instant::now();
    for _ in 0..gzip_iterations {
        black_box(gzip_compress(&resource_fixture).context("gzip benchmark compression failed")?);
    }
    let gzip_duration_ms = started.elapsed().as_millis() as u64;

    let filter = parse_legacy_filter_ini(
        r#"
[DEFAULT]
prefixmap = prefix1:.

[demo]
filter = ![ blocked ]
respaths =
    prefix1:/...
"#,
    )
    .map_err(|error| anyhow!("filter benchmark parse failed: {error}"))?;
    let filter_paths = [
        "materials/a.txt",
        "materials/nested/b.txt",
        "textures/stone.png",
        "textures/blocked.png",
        "models/ship.fbx",
    ];
    let filter_iterations = 100_000_u64;
    let started = Instant::now();
    let mut filter_matches = 0_u64;
    for _ in 0..filter_iterations {
        for path in filter_paths.iter().copied() {
            if black_box(filter.check_path(black_box(path))) {
                filter_matches += 1;
            }
        }
    }
    black_box(filter_matches);
    let filter_duration_ms = started.elapsed().as_millis() as u64;
    let filter_checks = filter_iterations * filter_paths.len() as u64;

    let repo_root = env::current_dir().context("locating repository root")?;
    let legacy_resources_cli_info = resolve_legacy_resources_cli_baseline(
        &repo_root,
        "CARBON_LEGACY_RESOURCES_CLI",
        "carbonengine/resources/.cmake-build-linux-vcpkg-probe/cli/resources_debug",
        "legacy_cmake_vcpkg_probe_resources_debug",
    )?;
    let legacy_resources_dev_cli_info = resolve_legacy_resources_cli_baseline(
        &repo_root,
        "CARBON_LEGACY_RESOURCES_DEV_CLI",
        "carbonengine/resources/.cmake-build-linux-vcpkg-devfeatures/cli/resources_debug",
        "legacy_cmake_vcpkg_devfeatures_resources_debug",
    )?;
    let legacy_resources_cli = legacy_resources_cli_info.path.clone();
    let legacy_resources_dev_cli = legacy_resources_dev_cli_info.path.clone();
    let legacy_resources_cli_profile = legacy_resources_cli_info.build_profile.clone();
    let legacy_resources_dev_cli_profile = legacy_resources_dev_cli_info.build_profile.clone();
    let legacy_resources_cli_known_non_debug = legacy_resources_cli_info.known_non_debug;
    let legacy_resources_dev_cli_known_non_debug = legacy_resources_dev_cli_info.known_non_debug;
    let legacy_resources_cli_candidates = find_legacy_resources_cli_candidates(&repo_root);
    let rust_native_release_ready =
        xtask_build_profile == "release-native" && target_cpu_native && !debug_assertions;
    let optimization_readiness = benchmark_optimization_readiness(
        rust_native_release_ready,
        7,
        2,
        &legacy_resources_cli_info,
        &legacy_resources_dev_cli_info,
        &legacy_resources_cli_candidates,
    );

    let create_group_input =
        Path::new("carbonengine/resources/tests/testData/CreateResourceFiles/ResourceFiles");
    let create_group_expected = fs::read_to_string(
        "carbonengine/resources/tests/testData/CreateResourceFiles/ResourceGroupLinux.yaml",
    )
    .context("reading create-group golden")?;
    let create_group_bench_dir = Path::new("target/carbon/bench/create-group");
    fs::remove_dir_all(create_group_bench_dir).ok();
    fs::create_dir_all(create_group_bench_dir)
        .with_context(|| format!("creating {}", create_group_bench_dir.display()))?;
    let create_group_iterations = 20_u64;

    let mut legacy_create_group_samples_us = Vec::new();
    let mut legacy_create_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..create_group_iterations {
        let output_file = create_group_bench_dir.join(format!("legacy-{iteration}.yaml"));
        let args = vec![
            OsString::from("create-group"),
            create_group_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&legacy_resources_cli, &args, None)
            .context("running legacy create-group benchmark")?;
        legacy_create_group_samples_us.push(measured.metrics.wall_time_us);
        legacy_create_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "legacy create-group benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != create_group_expected {
                bail!("legacy create-group benchmark output did not match the Linux golden");
            }
        }
    }
    let legacy_create_group_duration_us = started.elapsed().as_micros() as u64;

    let mut rust_create_group_samples_us = Vec::new();
    let mut rust_create_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..create_group_iterations {
        let output_file = create_group_bench_dir.join(format!("rust-{iteration}.yaml"));
        let args = vec![
            OsString::from("rust-create-group"),
            create_group_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&rust_runner, &args, None)
            .context("running Rust create-group process benchmark")?;
        rust_create_group_samples_us.push(measured.metrics.wall_time_us);
        rust_create_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "Rust create-group process benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != create_group_expected {
                bail!("Rust create-group benchmark output did not match the Linux golden");
            }
        }
    }
    let rust_create_group_duration_us = started.elapsed().as_micros() as u64;
    let create_group_speedup = speedup_ratio(
        legacy_create_group_duration_us,
        rust_create_group_duration_us,
    );
    let legacy_create_group_sample_stats = sample_stats_us(&legacy_create_group_samples_us);
    let rust_create_group_sample_stats = sample_stats_us(&rust_create_group_samples_us);

    let create_filter_mapping =
        Path::new("carbonengine/resources/tests/testData/FilterFiles/resFilterIndexMapping.yaml");
    let create_filter_base = Path::new("carbonengine/resources/tests/testData/FilterFiles");
    let create_filter_prefix_base =
        Path::new("carbonengine/resources/tests/testData/CreateResourceFiles/ResourceFiles");
    let create_filter_expected = create_group_expected.clone();
    let create_filter_bench_dir = Path::new("target/carbon/bench/create-group-from-filter");
    fs::remove_dir_all(create_filter_bench_dir).ok();
    fs::create_dir_all(create_filter_bench_dir)
        .with_context(|| format!("creating {}", create_filter_bench_dir.display()))?;
    let create_filter_iterations = 20_u64;
    let create_filter_mapping_abs = repo_root.join(create_filter_mapping);
    let create_filter_base_abs = repo_root.join(create_filter_base);
    let create_filter_prefix_base_abs = repo_root.join(create_filter_prefix_base);

    let mut legacy_create_filter_samples_us = Vec::new();
    let mut legacy_create_filter_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..create_filter_iterations {
        let output_dir = create_filter_bench_dir.join(format!("legacy-{iteration}"));
        fs::create_dir_all(&output_dir)
            .with_context(|| format!("creating {}", output_dir.display()))?;
        let args = vec![
            OsString::from("create-group-from-filter"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            OsString::from("--filter-index-mapping-file"),
            create_filter_mapping_abs.as_os_str().to_os_string(),
            OsString::from("--filter-file-basepath"),
            create_filter_base_abs.as_os_str().to_os_string(),
            OsString::from("--prefix-map-basepath"),
            create_filter_prefix_base_abs.as_os_str().to_os_string(),
            OsString::from("--number-of-threads"),
            OsString::from("0"),
        ];
        let measured = run_timed_process(&legacy_resources_cli, &args, Some(&output_dir))
            .context("running legacy create-group-from-filter benchmark")?;
        legacy_create_filter_samples_us.push(measured.metrics.wall_time_us);
        legacy_create_filter_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "legacy create-group-from-filter benchmark")?;
        if iteration == 0 {
            let output_file = output_dir.join("ResourceGroup.yaml");
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != create_filter_expected {
                bail!(
                    "legacy create-group-from-filter benchmark output did not match the Linux golden"
                );
            }
        }
    }
    let legacy_create_filter_duration_us = started.elapsed().as_micros() as u64;

    let mut rust_create_filter_samples_us = Vec::new();
    let mut rust_create_filter_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..create_filter_iterations {
        let output_dir = create_filter_bench_dir.join(format!("rust-{iteration}"));
        fs::create_dir_all(&output_dir)
            .with_context(|| format!("creating {}", output_dir.display()))?;
        let args = vec![
            OsString::from("rust-create-group-from-filter"),
            OsString::from("--filter-index-mapping-file"),
            create_filter_mapping.as_os_str().to_os_string(),
            OsString::from("--filter-file-basepath"),
            create_filter_base.as_os_str().to_os_string(),
            OsString::from("--prefix-map-basepath"),
            create_filter_prefix_base.as_os_str().to_os_string(),
            OsString::from("--output-directory"),
            output_dir.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&rust_runner, &args, None)
            .context("running Rust create-group-from-filter process benchmark")?;
        rust_create_filter_samples_us.push(measured.metrics.wall_time_us);
        rust_create_filter_process_samples.push(measured.metrics.clone());
        ensure_success(
            measured.output,
            "Rust create-group-from-filter process benchmark",
        )?;
        if iteration == 0 {
            let output_file = output_dir.join("ResourceGroup.yaml");
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != create_filter_expected {
                bail!(
                    "Rust create-group-from-filter benchmark output did not match the Linux golden"
                );
            }
        }
    }
    let rust_create_filter_duration_us = started.elapsed().as_micros() as u64;
    let create_filter_speedup = speedup_ratio(
        legacy_create_filter_duration_us,
        rust_create_filter_duration_us,
    );
    let legacy_create_filter_sample_stats = sample_stats_us(&legacy_create_filter_samples_us);
    let rust_create_filter_sample_stats = sample_stats_us(&rust_create_filter_samples_us);

    let merge_group_base = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/BaseResourceGroup.yaml",
    );
    let merge_group_input = Path::new(
        "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/MergeResourceGroup.yaml",
    );
    let merge_group_expected = fs::read_to_string(
        "carbonengine/resources/tests/testData/MergeGroups/YamlAdditive/ExpectedMergedResourceGroup.yaml",
    )
    .context("reading merge-group golden")?;
    let merge_group_bench_dir = Path::new("target/carbon/bench/merge-group");
    fs::remove_dir_all(merge_group_bench_dir).ok();
    fs::create_dir_all(merge_group_bench_dir)
        .with_context(|| format!("creating {}", merge_group_bench_dir.display()))?;
    let merge_group_iterations = 20_u64;

    let mut legacy_merge_group_samples_us = Vec::new();
    let mut legacy_merge_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..merge_group_iterations {
        let output_file = merge_group_bench_dir.join(format!("legacy-{iteration}.yaml"));
        let args = vec![
            OsString::from("merge-group"),
            merge_group_base.as_os_str().to_os_string(),
            merge_group_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&legacy_resources_cli, &args, None)
            .context("running legacy merge-group benchmark")?;
        legacy_merge_group_samples_us.push(measured.metrics.wall_time_us);
        legacy_merge_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "legacy merge-group benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != merge_group_expected {
                bail!("legacy merge-group benchmark output did not match the YAML additive golden");
            }
        }
    }
    let legacy_merge_group_duration_us = started.elapsed().as_micros() as u64;

    let mut rust_merge_group_samples_us = Vec::new();
    let mut rust_merge_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..merge_group_iterations {
        let output_file = merge_group_bench_dir.join(format!("rust-{iteration}.yaml"));
        let args = vec![
            OsString::from("rust-merge-group"),
            merge_group_base.as_os_str().to_os_string(),
            merge_group_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&rust_runner, &args, None)
            .context("running Rust merge-group process benchmark")?;
        rust_merge_group_samples_us.push(measured.metrics.wall_time_us);
        rust_merge_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "Rust merge-group process benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != merge_group_expected {
                bail!("Rust merge-group benchmark output did not match the YAML additive golden");
            }
        }
    }
    let rust_merge_group_duration_us = started.elapsed().as_micros() as u64;
    let merge_group_speedup =
        speedup_ratio(legacy_merge_group_duration_us, rust_merge_group_duration_us);
    let legacy_merge_group_sample_stats = sample_stats_us(&legacy_merge_group_samples_us);
    let rust_merge_group_sample_stats = sample_stats_us(&rust_merge_group_samples_us);

    let diff_group_base =
        Path::new("carbonengine/resources/tests/testData/DiffGroups/resFileIndex.txt");
    let diff_group_target =
        Path::new("carbonengine/resources/tests/testData/DiffGroups/resFileIndexWithAdditions.txt");
    let diff_group_expected = fs::read_to_string(
        "carbonengine/resources/tests/testData/DiffGroups/ExpectedDiffWithAdditions.txt",
    )
    .context("reading diff-group golden")?;
    let diff_group_bench_dir = Path::new("target/carbon/bench/diff-group");
    fs::remove_dir_all(diff_group_bench_dir).ok();
    fs::create_dir_all(diff_group_bench_dir)
        .with_context(|| format!("creating {}", diff_group_bench_dir.display()))?;
    let diff_group_iterations = 20_u64;

    let mut legacy_diff_group_samples_us = Vec::new();
    let mut legacy_diff_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..diff_group_iterations {
        let output_file = diff_group_bench_dir.join(format!("legacy-{iteration}.txt"));
        let args = vec![
            OsString::from("diff-group"),
            diff_group_base.as_os_str().to_os_string(),
            diff_group_target.as_os_str().to_os_string(),
            OsString::from("--diff-output-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&legacy_resources_cli, &args, None)
            .context("running legacy diff-group benchmark")?;
        legacy_diff_group_samples_us.push(measured.metrics.wall_time_us);
        legacy_diff_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "legacy diff-group benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != diff_group_expected {
                bail!("legacy diff-group benchmark output did not match the additions golden");
            }
        }
    }
    let legacy_diff_group_duration_us = started.elapsed().as_micros() as u64;

    let mut rust_diff_group_samples_us = Vec::new();
    let mut rust_diff_group_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..diff_group_iterations {
        let output_file = diff_group_bench_dir.join(format!("rust-{iteration}.txt"));
        let args = vec![
            OsString::from("rust-diff-group"),
            diff_group_base.as_os_str().to_os_string(),
            diff_group_target.as_os_str().to_os_string(),
            OsString::from("--diff-output-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&rust_runner, &args, None)
            .context("running Rust diff-group process benchmark")?;
        rust_diff_group_samples_us.push(measured.metrics.wall_time_us);
        rust_diff_group_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "Rust diff-group process benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != diff_group_expected {
                bail!("Rust diff-group benchmark output did not match the additions golden");
            }
        }
    }
    let rust_diff_group_duration_us = started.elapsed().as_micros() as u64;
    let diff_group_speedup =
        speedup_ratio(legacy_diff_group_duration_us, rust_diff_group_duration_us);
    let legacy_diff_group_sample_stats = sample_stats_us(&legacy_diff_group_samples_us);
    let rust_diff_group_sample_stats = sample_stats_us(&rust_diff_group_samples_us);

    let remove_resources_base =
        Path::new("carbonengine/resources/tests/testData/RemoveResource/BaseResourceGroup.yaml");
    let remove_resources_list =
        Path::new("carbonengine/resources/tests/testData/RemoveResource/ResourcesToRemoveList.txt");
    let remove_resources_expected = fs::read_to_string(
        "carbonengine/resources/tests/testData/RemoveResource/ResourceGroupAfterRemove.yaml",
    )
    .context("reading remove-resources golden")?;
    let remove_resources_bench_dir = Path::new("target/carbon/bench/remove-resources");
    fs::remove_dir_all(remove_resources_bench_dir).ok();
    fs::create_dir_all(remove_resources_bench_dir)
        .with_context(|| format!("creating {}", remove_resources_bench_dir.display()))?;
    let remove_resources_iterations = 20_u64;

    let mut legacy_remove_resources_samples_us = Vec::new();
    let mut legacy_remove_resources_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..remove_resources_iterations {
        let output_file = remove_resources_bench_dir.join(format!("legacy-{iteration}.yaml"));
        let args = vec![
            OsString::from("remove-resources"),
            remove_resources_base.as_os_str().to_os_string(),
            remove_resources_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&legacy_resources_cli, &args, None)
            .context("running legacy remove-resources benchmark")?;
        legacy_remove_resources_samples_us.push(measured.metrics.wall_time_us);
        legacy_remove_resources_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "legacy remove-resources benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != remove_resources_expected {
                bail!("legacy remove-resources benchmark output did not match the remove golden");
            }
        }
    }
    let legacy_remove_resources_duration_us = started.elapsed().as_micros() as u64;

    let mut rust_remove_resources_samples_us = Vec::new();
    let mut rust_remove_resources_process_samples = Vec::new();
    let started = Instant::now();
    for iteration in 0..remove_resources_iterations {
        let output_file = remove_resources_bench_dir.join(format!("rust-{iteration}.yaml"));
        let args = vec![
            OsString::from("rust-remove-resources"),
            remove_resources_base.as_os_str().to_os_string(),
            remove_resources_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            output_file.as_os_str().to_os_string(),
        ];
        let measured = run_timed_process(&rust_runner, &args, None)
            .context("running Rust remove-resources process benchmark")?;
        rust_remove_resources_samples_us.push(measured.metrics.wall_time_us);
        rust_remove_resources_process_samples.push(measured.metrics.clone());
        ensure_success(measured.output, "Rust remove-resources process benchmark")?;
        if iteration == 0 {
            let actual = fs::read_to_string(&output_file)
                .with_context(|| format!("reading {}", output_file.display()))?;
            if actual != remove_resources_expected {
                bail!("Rust remove-resources benchmark output did not match the remove golden");
            }
        }
    }
    let rust_remove_resources_duration_us = started.elapsed().as_micros() as u64;
    let remove_resources_speedup = speedup_ratio(
        legacy_remove_resources_duration_us,
        rust_remove_resources_duration_us,
    );
    let legacy_remove_resources_sample_stats = sample_stats_us(&legacy_remove_resources_samples_us);
    let rust_remove_resources_sample_stats = sample_stats_us(&rust_remove_resources_samples_us);

    let create_bundle_resource_group =
        repo_root.join("carbonengine/resources/tests/testData/Bundle/resFileIndexShort.txt");
    let create_bundle_source = repo_root.join("carbonengine/resources/tests/testData/Bundle/Res");
    let create_bundle_expected_manifest =
        Path::new("carbonengine/resources/tests/testData/CreateBundle/BundleResourceGroup.yaml");
    let create_bundle_expected_directory =
        Path::new("carbonengine/resources/tests/testData/CreateBundle/CreateBundleOut");
    let create_bundle_bench_dir = Path::new("target/carbon/bench/create-bundle-local");
    fs::remove_dir_all(create_bundle_bench_dir).ok();
    fs::create_dir_all(create_bundle_bench_dir)
        .with_context(|| format!("creating {}", create_bundle_bench_dir.display()))?;
    let create_bundle_iterations = 10_u64;
    let validate_create_bundle = |iteration_dir: &Path| -> Result<()> {
        assert_file_bytes_match(
            create_bundle_expected_manifest,
            &iteration_dir.join("BundleOut/BundleResourceGroup.yaml"),
            "create-bundle benchmark",
        )?;
        assert_directory_subset_bytes(
            create_bundle_expected_directory,
            &iteration_dir.join("CreateBundleOut"),
        )?;
        Ok(())
    };
    let legacy_create_bundle_benchmark = run_process_benchmark_samples(
        &legacy_resources_cli,
        create_bundle_iterations,
        create_bundle_bench_dir,
        "legacy",
        "legacy create-bundle benchmark",
        |_, _| {
            vec![
                OsString::from("create-bundle"),
                OsString::from("--verbosity-level"),
                OsString::from("-1"),
                create_bundle_resource_group.as_os_str().to_os_string(),
                OsString::from("--resource-source-path"),
                create_bundle_source.as_os_str().to_os_string(),
                OsString::from("--bundle-resourcegroup-relative-path"),
                OsString::from("BundleResourceGroup.yaml"),
                OsString::from("--bundle-resourcegroup-destination-path"),
                OsString::from("BundleOut/"),
                OsString::from("--bundle-resourcegroup-destination-type"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--chunk-destination-path"),
                OsString::from("CreateBundleOut"),
                OsString::from("--chunk-destination-type"),
                OsString::from("LOCAL_CDN"),
                OsString::from("--chunk-size"),
                OsString::from("1000"),
            ]
        },
        validate_create_bundle,
    )?;
    let rust_create_bundle_benchmark = run_process_benchmark_samples(
        &rust_runner,
        create_bundle_iterations,
        create_bundle_bench_dir,
        "rust",
        "Rust create-bundle process benchmark",
        |_, _| {
            vec![
                OsString::from("rust-create-bundle"),
                create_bundle_resource_group.as_os_str().to_os_string(),
                OsString::from("--resource-source-path"),
                create_bundle_source.as_os_str().to_os_string(),
                OsString::from("--bundle-resourcegroup-relative-path"),
                OsString::from("BundleResourceGroup.yaml"),
                OsString::from("--bundle-resourcegroup-destination-path"),
                OsString::from("BundleOut/"),
                OsString::from("--bundle-resourcegroup-destination-type"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--chunk-destination-path"),
                OsString::from("CreateBundleOut"),
                OsString::from("--chunk-destination-type"),
                OsString::from("LOCAL_CDN"),
                OsString::from("--chunk-size"),
                OsString::from("1000"),
            ]
        },
        validate_create_bundle,
    )?;
    let create_bundle_speedup = speedup_ratio(
        legacy_create_bundle_benchmark.duration_us,
        rust_create_bundle_benchmark.duration_us,
    );

    let create_patch_previous_group = repo_root
        .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_previous.txt");
    let create_patch_next_group = repo_root
        .join("carbonengine/resources/tests/testData/Patch/resFileIndexShort_build_next.txt");
    let create_patch_previous_resources =
        repo_root.join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources");
    let create_patch_next_resources =
        repo_root.join("carbonengine/resources/tests/testData/Patch/NextBuildResources");
    let create_patch_expected_manifest =
        Path::new("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml");
    let create_patch_expected_directory =
        Path::new("carbonengine/resources/tests/testData/Patch/LocalCDNPatches");
    let create_patch_bench_dir = Path::new("target/carbon/bench/create-patch-local");
    fs::remove_dir_all(create_patch_bench_dir).ok();
    fs::create_dir_all(create_patch_bench_dir)
        .with_context(|| format!("creating {}", create_patch_bench_dir.display()))?;
    let create_patch_iterations = 10_u64;
    let validate_create_patch = |iteration_dir: &Path| -> Result<()> {
        assert_file_bytes_match(
            create_patch_expected_manifest,
            &iteration_dir.join("PatchOut/PatchResourceGroup.yaml"),
            "create-patch benchmark",
        )?;
        assert_directory_subset_bytes(
            create_patch_expected_directory,
            &iteration_dir.join("PatchOut/Patches"),
        )?;
        Ok(())
    };
    let legacy_create_patch_benchmark = run_process_benchmark_samples(
        &legacy_resources_cli,
        create_patch_iterations,
        create_patch_bench_dir,
        "legacy",
        "legacy create-patch benchmark",
        |_, _| {
            vec![
                OsString::from("create-patch"),
                OsString::from("--verbosity-level"),
                OsString::from("-1"),
                create_patch_previous_group.as_os_str().to_os_string(),
                create_patch_next_group.as_os_str().to_os_string(),
                OsString::from("--resource-source-type-previous"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--resource-source-base-path-next"),
                create_patch_next_resources.as_os_str().to_os_string(),
                OsString::from("--resource-source-base-path-previous"),
                create_patch_previous_resources.as_os_str().to_os_string(),
                OsString::from("--patch-resourcegroup-destination-path"),
                OsString::from("PatchOut"),
                OsString::from("--patch-destination-base-path"),
                OsString::from("PatchOut/Patches"),
                OsString::from("--patch-destination-type"),
                OsString::from("LOCAL_CDN"),
                OsString::from("--chunk-size"),
                OsString::from("50000000"),
            ]
        },
        validate_create_patch,
    )?;
    let rust_create_patch_benchmark = run_process_benchmark_samples(
        &rust_runner,
        create_patch_iterations,
        create_patch_bench_dir,
        "rust",
        "Rust create-patch process benchmark",
        |_, _| {
            vec![
                OsString::from("rust-create-patch"),
                create_patch_previous_group.as_os_str().to_os_string(),
                create_patch_next_group.as_os_str().to_os_string(),
                OsString::from("--resource-source-type-previous"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--resource-source-base-path-previous"),
                create_patch_previous_resources.as_os_str().to_os_string(),
                OsString::from("--resource-source-base-path-next"),
                create_patch_next_resources.as_os_str().to_os_string(),
                OsString::from("--patch-resourcegroup-destination-path"),
                OsString::from("PatchOut"),
                OsString::from("--patch-destination-base-path"),
                OsString::from("PatchOut/Patches"),
                OsString::from("--patch-destination-type"),
                OsString::from("LOCAL_CDN"),
                OsString::from("--chunk-size"),
                OsString::from("50000000"),
            ]
        },
        validate_create_patch,
    )?;
    let create_patch_speedup = speedup_ratio(
        legacy_create_patch_benchmark.duration_us,
        rust_create_patch_benchmark.duration_us,
    );

    let unpack_bundle_manifest =
        repo_root.join("carbonengine/resources/tests/testData/Bundle/BundleResourceGroup.yaml");
    let unpack_bundle_chunks =
        repo_root.join("carbonengine/resources/tests/testData/Bundle/LocalRemoteChunks");
    let unpack_bundle_expected_directory =
        Path::new("carbonengine/resources/tests/testData/Bundle/Res");
    let unpack_bundle_bench_dir = Path::new("target/carbon/bench/unpack-bundle-local");
    fs::remove_dir_all(unpack_bundle_bench_dir).ok();
    fs::create_dir_all(unpack_bundle_bench_dir)
        .with_context(|| format!("creating {}", unpack_bundle_bench_dir.display()))?;
    let unpack_bundle_iterations = 10_u64;
    let validate_unpack_bundle = |iteration_dir: &Path| -> Result<()> {
        assert_directory_subset_bytes(
            unpack_bundle_expected_directory,
            &iteration_dir.join("UnpackBundleOut"),
        )?;
        if !iteration_dir
            .join("UnpackBundleOut/ResourceGroup.yaml")
            .exists()
        {
            bail!(
                "unpack-bundle benchmark did not write {}",
                iteration_dir
                    .join("UnpackBundleOut/ResourceGroup.yaml")
                    .display()
            );
        }
        Ok(())
    };
    let legacy_unpack_bundle_benchmark = run_process_benchmark_samples(
        &legacy_resources_dev_cli,
        unpack_bundle_iterations,
        unpack_bundle_bench_dir,
        "legacy",
        "legacy unpack-bundle benchmark",
        |_, _| {
            vec![
                OsString::from("unpack-bundle"),
                OsString::from("--verbosity-level"),
                OsString::from("-1"),
                unpack_bundle_manifest.as_os_str().to_os_string(),
                OsString::from("--chunk-source-base-path"),
                unpack_bundle_chunks.as_os_str().to_os_string(),
                OsString::from("--resource-destination-type"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--resource-destination-base-path"),
                OsString::from("UnpackBundleOut"),
            ]
        },
        validate_unpack_bundle,
    )?;
    let rust_unpack_bundle_benchmark = run_process_benchmark_samples(
        &rust_runner,
        unpack_bundle_iterations,
        unpack_bundle_bench_dir,
        "rust",
        "Rust unpack-bundle process benchmark",
        |_, _| {
            vec![
                OsString::from("rust-unpack-bundle"),
                unpack_bundle_manifest.as_os_str().to_os_string(),
                OsString::from("--chunk-source-base-path"),
                unpack_bundle_chunks.as_os_str().to_os_string(),
                OsString::from("--resource-destination-type"),
                OsString::from("LOCAL_RELATIVE"),
                OsString::from("--output-base-path"),
                OsString::from("UnpackBundleOut"),
            ]
        },
        validate_unpack_bundle,
    )?;
    let unpack_bundle_speedup = speedup_ratio(
        legacy_unpack_bundle_benchmark.duration_us,
        rust_unpack_bundle_benchmark.duration_us,
    );

    let apply_patch_manifest =
        repo_root.join("carbonengine/resources/tests/testData/Patch/PatchResourceGroup.yaml");
    let apply_patch_binaries =
        repo_root.join("carbonengine/resources/tests/testData/Patch/LocalCDNPatches");
    let apply_patch_previous_resources =
        repo_root.join("carbonengine/resources/tests/testData/Patch/PreviousBuildResources");
    let apply_patch_next_resources =
        repo_root.join("carbonengine/resources/tests/testData/Patch/NextBuildResources");
    let apply_patch_expected_directory =
        Path::new("carbonengine/resources/tests/testData/Patch/NextBuildResources");
    let apply_patch_bench_dir = Path::new("target/carbon/bench/apply-patch-local");
    fs::remove_dir_all(apply_patch_bench_dir).ok();
    fs::create_dir_all(apply_patch_bench_dir)
        .with_context(|| format!("creating {}", apply_patch_bench_dir.display()))?;
    let apply_patch_iterations = 10_u64;
    let validate_apply_patch = |iteration_dir: &Path| -> Result<()> {
        let output_dir = iteration_dir.join("ApplyPatchOut");
        assert_directory_subset_bytes(apply_patch_expected_directory, &output_dir)?;
        let removed_path = output_dir.join("testResource.txt");
        if removed_path.exists() {
            bail!(
                "apply-patch benchmark left removed resource {}",
                removed_path.display()
            );
        }
        Ok(())
    };
    let legacy_apply_patch_benchmark = run_process_benchmark_samples_with_prepare(
        &legacy_resources_dev_cli,
        apply_patch_iterations,
        apply_patch_bench_dir,
        "legacy",
        "legacy apply-patch benchmark",
        |iteration_dir| {
            copy_directory_recursive(
                &apply_patch_previous_resources,
                &iteration_dir.join("ApplyPatchOut"),
            )
        },
        |_, _| {
            vec![
                OsString::from("apply-patch"),
                OsString::from("--verbosity-level"),
                OsString::from("-1"),
                apply_patch_manifest.as_os_str().to_os_string(),
                OsString::from("--patch-binaries-base-path"),
                apply_patch_binaries.as_os_str().to_os_string(),
                OsString::from("--resources-to-patch-base-path"),
                apply_patch_previous_resources.as_os_str().to_os_string(),
                OsString::from("--next-resources-base-path"),
                apply_patch_next_resources.as_os_str().to_os_string(),
                OsString::from("--output-base-path"),
                OsString::from("ApplyPatchOut"),
            ]
        },
        validate_apply_patch,
    )?;
    let rust_apply_patch_benchmark = run_process_benchmark_samples(
        &rust_runner,
        apply_patch_iterations,
        apply_patch_bench_dir,
        "rust",
        "Rust apply-patch process benchmark",
        |_, _| {
            vec![
                OsString::from("rust-apply-patch"),
                apply_patch_manifest.as_os_str().to_os_string(),
                OsString::from("--patch-binaries-base-path"),
                apply_patch_binaries.as_os_str().to_os_string(),
                OsString::from("--resources-to-patch-base-path"),
                apply_patch_previous_resources.as_os_str().to_os_string(),
                OsString::from("--next-resources-base-path"),
                apply_patch_next_resources.as_os_str().to_os_string(),
                OsString::from("--output-base-path"),
                OsString::from("ApplyPatchOut"),
            ]
        },
        validate_apply_patch,
    )?;
    let apply_patch_speedup = speedup_ratio(
        legacy_apply_patch_benchmark.duration_us,
        rust_apply_patch_benchmark.duration_us,
    );

    let rust_only_not_comparable_reason = "Rust-only in-process microbenchmark in the xtask process; this evidence file does not run a legacy implementation with the same harness, sample shape, or process-resource measurement.";
    let scheduler_microbench_command =
        format!("{xtask_bench_command} (in-process scheduler fixture loop)");
    let md5_microbench_command = format!("{xtask_bench_command} (in-process Rust md5 loop)");
    let gzip_microbench_command = format!("{xtask_bench_command} (in-process Rust gzip loop)");
    let filter_microbench_command =
        format!("{xtask_bench_command} (in-process Rust legacy-filter matching loop)");
    let create_group_legacy_command_template = command_line(
        &legacy_resources_cli,
        &[
            OsString::from("create-group"),
            create_group_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            OsString::from("target/carbon/bench/create-group/legacy-{iteration}.yaml"),
        ],
    );
    let create_group_rust_command_template = command_line(
        &rust_runner,
        &[
            OsString::from("rust-create-group"),
            create_group_input.as_os_str().to_os_string(),
            OsString::from("--output-file"),
            OsString::from("target/carbon/bench/create-group/rust-{iteration}.yaml"),
        ],
    );
    let create_filter_legacy_command_template = command_line_with_current_dir(
        &legacy_resources_cli,
        &[
            OsString::from("create-group-from-filter"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            OsString::from("--filter-index-mapping-file"),
            create_filter_mapping_abs.as_os_str().to_os_string(),
            OsString::from("--filter-file-basepath"),
            create_filter_base_abs.as_os_str().to_os_string(),
            OsString::from("--prefix-map-basepath"),
            create_filter_prefix_base_abs.as_os_str().to_os_string(),
            OsString::from("--number-of-threads"),
            OsString::from("0"),
        ],
        Some(Path::new(
            "target/carbon/bench/create-group-from-filter/legacy-{iteration}",
        )),
    );
    let create_filter_rust_command_template = command_line(
        &rust_runner,
        &[
            OsString::from("rust-create-group-from-filter"),
            OsString::from("--filter-index-mapping-file"),
            create_filter_mapping.as_os_str().to_os_string(),
            OsString::from("--filter-file-basepath"),
            create_filter_base.as_os_str().to_os_string(),
            OsString::from("--prefix-map-basepath"),
            create_filter_prefix_base.as_os_str().to_os_string(),
            OsString::from("--output-directory"),
            OsString::from("target/carbon/bench/create-group-from-filter/rust-{iteration}"),
        ],
    );
    let merge_group_legacy_command_template = command_line(
        &legacy_resources_cli,
        &[
            OsString::from("merge-group"),
            merge_group_base.as_os_str().to_os_string(),
            merge_group_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            OsString::from("target/carbon/bench/merge-group/legacy-{iteration}.yaml"),
        ],
    );
    let merge_group_rust_command_template = command_line(
        &rust_runner,
        &[
            OsString::from("rust-merge-group"),
            merge_group_base.as_os_str().to_os_string(),
            merge_group_input.as_os_str().to_os_string(),
            OsString::from("--merge-output-resource-group-path"),
            OsString::from("target/carbon/bench/merge-group/rust-{iteration}.yaml"),
        ],
    );
    let diff_group_legacy_command_template = command_line(
        &legacy_resources_cli,
        &[
            OsString::from("diff-group"),
            diff_group_base.as_os_str().to_os_string(),
            diff_group_target.as_os_str().to_os_string(),
            OsString::from("--diff-output-path"),
            OsString::from("target/carbon/bench/diff-group/legacy-{iteration}.txt"),
        ],
    );
    let diff_group_rust_command_template = command_line(
        &rust_runner,
        &[
            OsString::from("rust-diff-group"),
            diff_group_base.as_os_str().to_os_string(),
            diff_group_target.as_os_str().to_os_string(),
            OsString::from("--diff-output-path"),
            OsString::from("target/carbon/bench/diff-group/rust-{iteration}.txt"),
        ],
    );
    let remove_resources_legacy_command_template = command_line(
        &legacy_resources_cli,
        &[
            OsString::from("remove-resources"),
            remove_resources_base.as_os_str().to_os_string(),
            remove_resources_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            OsString::from("target/carbon/bench/remove-resources/legacy-{iteration}.yaml"),
        ],
    );
    let remove_resources_rust_command_template = command_line(
        &rust_runner,
        &[
            OsString::from("rust-remove-resources"),
            remove_resources_base.as_os_str().to_os_string(),
            remove_resources_list.as_os_str().to_os_string(),
            OsString::from("--output-resource-group-path"),
            OsString::from("target/carbon/bench/remove-resources/rust-{iteration}.yaml"),
        ],
    );
    let create_bundle_legacy_command_template = command_line_with_current_dir(
        &legacy_resources_cli,
        &[
            OsString::from("create-bundle"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            create_bundle_resource_group.as_os_str().to_os_string(),
            OsString::from("--resource-source-path"),
            create_bundle_source.as_os_str().to_os_string(),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroup.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut/"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOut"),
            OsString::from("--chunk-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("1000"),
        ],
        Some(Path::new(
            "target/carbon/bench/create-bundle-local/legacy-{iteration}",
        )),
    );
    let create_bundle_rust_command_template = command_line_with_current_dir(
        &rust_runner,
        &[
            OsString::from("rust-create-bundle"),
            create_bundle_resource_group.as_os_str().to_os_string(),
            OsString::from("--resource-source-path"),
            create_bundle_source.as_os_str().to_os_string(),
            OsString::from("--bundle-resourcegroup-relative-path"),
            OsString::from("BundleResourceGroup.yaml"),
            OsString::from("--bundle-resourcegroup-destination-path"),
            OsString::from("BundleOut/"),
            OsString::from("--bundle-resourcegroup-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--chunk-destination-path"),
            OsString::from("CreateBundleOut"),
            OsString::from("--chunk-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("1000"),
        ],
        Some(Path::new(
            "target/carbon/bench/create-bundle-local/rust-{iteration}",
        )),
    );
    let create_patch_legacy_command_template = command_line_with_current_dir(
        &legacy_resources_cli,
        &[
            OsString::from("create-patch"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            create_patch_previous_group.as_os_str().to_os_string(),
            create_patch_next_group.as_os_str().to_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-next"),
            create_patch_next_resources.as_os_str().to_os_string(),
            OsString::from("--resource-source-base-path-previous"),
            create_patch_previous_resources.as_os_str().to_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        Some(Path::new(
            "target/carbon/bench/create-patch-local/legacy-{iteration}",
        )),
    );
    let create_patch_rust_command_template = command_line_with_current_dir(
        &rust_runner,
        &[
            OsString::from("rust-create-patch"),
            create_patch_previous_group.as_os_str().to_os_string(),
            create_patch_next_group.as_os_str().to_os_string(),
            OsString::from("--resource-source-type-previous"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-source-base-path-previous"),
            create_patch_previous_resources.as_os_str().to_os_string(),
            OsString::from("--resource-source-base-path-next"),
            create_patch_next_resources.as_os_str().to_os_string(),
            OsString::from("--patch-resourcegroup-destination-path"),
            OsString::from("PatchOut"),
            OsString::from("--patch-destination-base-path"),
            OsString::from("PatchOut/Patches"),
            OsString::from("--patch-destination-type"),
            OsString::from("LOCAL_CDN"),
            OsString::from("--chunk-size"),
            OsString::from("50000000"),
        ],
        Some(Path::new(
            "target/carbon/bench/create-patch-local/rust-{iteration}",
        )),
    );
    let unpack_bundle_legacy_command_template = command_line_with_current_dir(
        &legacy_resources_dev_cli,
        &[
            OsString::from("unpack-bundle"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            unpack_bundle_manifest.as_os_str().to_os_string(),
            OsString::from("--chunk-source-base-path"),
            unpack_bundle_chunks.as_os_str().to_os_string(),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--resource-destination-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        Some(Path::new(
            "target/carbon/bench/unpack-bundle-local/legacy-{iteration}",
        )),
    );
    let unpack_bundle_rust_command_template = command_line_with_current_dir(
        &rust_runner,
        &[
            OsString::from("rust-unpack-bundle"),
            unpack_bundle_manifest.as_os_str().to_os_string(),
            OsString::from("--chunk-source-base-path"),
            unpack_bundle_chunks.as_os_str().to_os_string(),
            OsString::from("--resource-destination-type"),
            OsString::from("LOCAL_RELATIVE"),
            OsString::from("--output-base-path"),
            OsString::from("UnpackBundleOut"),
        ],
        Some(Path::new(
            "target/carbon/bench/unpack-bundle-local/rust-{iteration}",
        )),
    );
    let apply_patch_legacy_command_template = command_line_with_current_dir(
        &legacy_resources_dev_cli,
        &[
            OsString::from("apply-patch"),
            OsString::from("--verbosity-level"),
            OsString::from("-1"),
            apply_patch_manifest.as_os_str().to_os_string(),
            OsString::from("--patch-binaries-base-path"),
            apply_patch_binaries.as_os_str().to_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            apply_patch_previous_resources.as_os_str().to_os_string(),
            OsString::from("--next-resources-base-path"),
            apply_patch_next_resources.as_os_str().to_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        Some(Path::new(
            "target/carbon/bench/apply-patch-local/legacy-{iteration}",
        )),
    );
    let apply_patch_rust_command_template = command_line_with_current_dir(
        &rust_runner,
        &[
            OsString::from("rust-apply-patch"),
            apply_patch_manifest.as_os_str().to_os_string(),
            OsString::from("--patch-binaries-base-path"),
            apply_patch_binaries.as_os_str().to_os_string(),
            OsString::from("--resources-to-patch-base-path"),
            apply_patch_previous_resources.as_os_str().to_os_string(),
            OsString::from("--next-resources-base-path"),
            apply_patch_next_resources.as_os_str().to_os_string(),
            OsString::from("--output-base-path"),
            OsString::from("ApplyPatchOut"),
        ],
        Some(Path::new(
            "target/carbon/bench/apply-patch-local/rust-{iteration}",
        )),
    );

    let evidence = json!({
        "schema": "carbon.evidence.benchmark.v1",
        "gate": "bench-tier-local",
        "status": "pass",
        "report_ready": false,
        "command": xtask_bench_command,
        "recommended_comparable_command": xtask_bench_release_command,
        "xtask_executable": rust_runner.display().to_string(),
        "build_profile": xtask_build_profile,
        "target_cpu_native": target_cpu_native,
        "debug_assertions": debug_assertions,
        "build_profile_source": "current_exe_parent_when_available_else_debug_assertions",
        "coverage": "initial_microbenchmarks_with_process_measured_scheduler_core_and_comparable_catalog_bundle_patch_create_apply_ops_resource_stats",
        "comparability": "mixed_with_preliminary_comparable_catalog_bundle_patch_create_apply_ops",
        "comparability_summary": {
            "comparable_process_to_process_comparisons": 9,
            "comparable_process_workload_rows": 18,
            "rust_only_not_comparable_workload_rows": 5,
            "scheduler_process_resource_rows": 1,
            "speedup_claim_scope": "only comparable_process_to_process comparison rows with parity_status=pass"
        },
        "optimization_readiness": optimization_readiness,
        "not_report_ready_reason": "Create-group, create-group-from-filter, merge-group, diff-group, remove-resources, create-bundle, create-patch, unpack-bundle, and apply-patch have parity-checked process-to-process samples with wall-time, latency, CPU, CPU-burn, and peak-RSS stats. Scheduler core now has process-measured Rust target resource evidence, but no legacy scheduler process baseline. Other rows are Rust-only or wall-time-only, and legacy scheduler Python/IO semantic comparisons plus broader parity-complete workloads are not implemented yet.",
        "command_templates": {
            "run_order_fixture_rust_core_process": {
                "rust": scheduler_process_command_template
            },
            "create_group_directory_yaml": {
                "legacy": create_group_legacy_command_template,
                "rust": create_group_rust_command_template
            },
            "create_group_from_filter_yaml": {
                "legacy": create_filter_legacy_command_template,
                "rust": create_filter_rust_command_template
            },
            "merge_group_yaml_additive": {
                "legacy": merge_group_legacy_command_template,
                "rust": merge_group_rust_command_template
            },
            "diff_group_csv_additions": {
                "legacy": diff_group_legacy_command_template,
                "rust": diff_group_rust_command_template
            },
            "remove_resources_yaml": {
                "legacy": remove_resources_legacy_command_template,
                "rust": remove_resources_rust_command_template
            },
            "create_bundle_local_cdn": {
                "legacy": create_bundle_legacy_command_template,
                "rust": create_bundle_rust_command_template
            },
            "create_patch_local_cdn": {
                "legacy": create_patch_legacy_command_template,
                "rust": create_patch_rust_command_template
            },
            "unpack_bundle_local_cdn": {
                "legacy": unpack_bundle_legacy_command_template,
                "rust": unpack_bundle_rust_command_template
            },
            "apply_patch_local_cdn": {
                "legacy": apply_patch_legacy_command_template,
                "rust": apply_patch_rust_command_template
            }
        },
        "host": {
            "os": env::consts::OS,
            "arch": env::consts::ARCH,
            "cpu_model": host_cpu_model().unwrap_or_else(|| String::from("unknown")),
            "logical_cpus": std::thread::available_parallelism().map(|value| value.get()).unwrap_or_default(),
            "ram_kb": host_mem_total_kb(),
            "rustc": command_stdout("rustc", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "cargo": command_stdout("cargo", &["--version"]).unwrap_or_else(|_| String::from("unknown")),
            "rust_build": rust_build.clone(),
            "process_resource_measurement": if Path::new("/usr/bin/time").exists() { "external_time_v" } else { "wall_clock_only" },
        },
        "resource_concurrency": {
            "logical_cpus": std::thread::available_parallelism().map(|value| value.get()).unwrap_or_default(),
            "legacy_create_group_from_filter_threads": "0 (legacy auto)",
            "rust_create_group_from_filter_threads": "single xtask process for current slice",
            "scheduler_process_samples": scheduler_process_sample_count,
            "io_workload_concurrency": "recorded in io-workloads.json as server thread plus client tasklet or baseline client loop"
        },
        "workloads": [
            {
                "component": "scheduler",
                "workload": "run_order_fixture_rust_core",
                "implementation": "rust",
                "command": scheduler_microbench_command,
                "build_profile": xtask_build_profile,
                "iterations": scheduler_iterations,
                "duration_ms": scheduler_duration_ms,
                "events": scheduler_events,
                "throughput_events_per_sec": rate_per_second(scheduler_events, scheduler_duration_ms),
                "target_cpu_native": target_cpu_native,
                "debug_assertions": debug_assertions,
                "comparability": "rust_only_in_process_not_legacy_comparable",
                "not_comparable_reason": rust_only_not_comparable_reason,
                "parity_status": "partial_pass",
                "parity_gate": "scheduler-fixtures.json",
                "claim": "no_speedup_claim"
            },
            {
                "component": "scheduler",
                "workload": "run_order_fixture_rust_core_process",
                "implementation": "rust_xtask_process",
                "command_template": scheduler_process_command_template,
                "build_profile": xtask_build_profile,
                "target_cpu_native": target_cpu_native,
                "debug_assertions": debug_assertions,
                "iterations": scheduler_process_iterations_total,
                "process_sample_count": scheduler_process_sample_count,
                "duration_ms": duration_ms_from_us(scheduler_process_duration_us),
                "duration_us": scheduler_process_duration_us,
                "rust_duration_us": scheduler_process_duration_us,
                "rust_sample_stats_us": scheduler_process_latency_stats,
                "process_runs": scheduler_process_runs,
                "process_samples": process_metrics_sample_json(&scheduler_process_metrics),
                "rust_process_stats": process_metrics_summary(&scheduler_process_metrics),
                "process_stats": process_metrics_summary(&scheduler_process_metrics),
                "events": scheduler_process_events,
                "throughput_events_per_sec": rate_per_second_us(scheduler_process_events, scheduler_process_duration_us),
                "resource_comparison": {
                    "linear_scale_estimate_100k_units": {
                        "basis": "linear estimate from process-measured Rust scheduler fixture runs; process startup is included and this is not a production claim",
                        "units": 100_000,
                        "rust_wall_seconds": scaled_wall_seconds(scheduler_process_duration_us, scheduler_process_iterations_total, 100_000),
                        "rust_cpu_burn_seconds": scale_optional_ms_seconds(
                            sum_effective_cpu_burn_ms(&scheduler_process_metrics),
                            scheduler_process_iterations_total,
                            100_000.0
                        )
                    }
                },
                "comparability": "rust_scheduler_process_not_legacy_comparable",
                "not_comparable_reason": "Rust scheduler process resource evidence has no matched legacy scheduler process baseline in this row; it supports CPU/RSS/native-build reporting only.",
                "parity_status": "partial_pass",
                "parity_gate": "scheduler-fixtures.json",
                "claim": "scheduler_resource_efficiency_evidence_only_no_speedup_claim"
            },
            {
                "component": "resources",
                "workload": "md5_intro_movie_fixture",
                "implementation": "rust",
                "command": md5_microbench_command,
                "build_profile": xtask_build_profile,
                "iterations": md5_iterations,
                "duration_ms": md5_duration_ms,
                "bytes_processed": resource_fixture.len() as u64 * md5_iterations,
                "throughput_bytes_per_sec": rate_per_second(resource_fixture.len() as u64 * md5_iterations, md5_duration_ms),
                "comparability": "rust_only_in_process_not_legacy_comparable",
                "not_comparable_reason": rust_only_not_comparable_reason,
                "parity_status": "partial_pass",
                "parity_gate": "rust-resources.json",
                "claim": "no_speedup_claim"
            },
            {
                "component": "resources",
                "workload": "gzip_intro_movie_fixture",
                "implementation": "rust",
                "command": gzip_microbench_command,
                "build_profile": xtask_build_profile,
                "iterations": gzip_iterations,
                "duration_ms": gzip_duration_ms,
                "bytes_processed": resource_fixture.len() as u64 * gzip_iterations,
                "throughput_bytes_per_sec": rate_per_second(resource_fixture.len() as u64 * gzip_iterations, gzip_duration_ms),
                "comparability": "rust_only_in_process_not_legacy_comparable",
                "not_comparable_reason": rust_only_not_comparable_reason,
                "parity_status": "partial_pass",
                "parity_gate": "rust-resources.json",
                "claim": "no_speedup_claim"
            },
            {
                "component": "resources",
                "workload": "legacy_filter_path_matching",
                "implementation": "rust",
                "command": filter_microbench_command,
                "build_profile": xtask_build_profile,
                "iterations": filter_iterations,
                "duration_ms": filter_duration_ms,
                "paths_checked": filter_checks,
                "matched_paths": filter_matches,
                "throughput_paths_per_sec": rate_per_second(filter_checks, filter_duration_ms),
                "comparability": "rust_only_in_process_not_legacy_comparable",
                "not_comparable_reason": rust_only_not_comparable_reason,
                "parity_status": "partial_pass",
                "parity_gate": "rust-resources.json",
                "claim": "no_speedup_claim"
            },
            {
                "component": "resources",
                "workload": "create_group_directory_yaml_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "create_group_directory_yaml",
                "command_template": create_group_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": create_group_iterations,
                "duration_ms": duration_ms_from_us(legacy_create_group_duration_us),
                "duration_us": legacy_create_group_duration_us,
                "samples_us": legacy_create_group_samples_us,
                "sample_stats_us": legacy_create_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&legacy_create_group_process_samples),
                "process_stats": process_metrics_summary(&legacy_create_group_process_samples),
                "directories_processed": create_group_iterations,
                "throughput_directories_per_sec": rate_per_second_us(create_group_iterations, legacy_create_group_duration_us),
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "create_group_directory_yaml_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "create_group_directory_yaml",
                "command_template": create_group_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": create_group_iterations,
                "duration_ms": duration_ms_from_us(rust_create_group_duration_us),
                "duration_us": rust_create_group_duration_us,
                "samples_us": rust_create_group_samples_us,
                "sample_stats_us": rust_create_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&rust_create_group_process_samples),
                "process_stats": process_metrics_summary(&rust_create_group_process_samples),
                "directories_processed": create_group_iterations,
                "throughput_directories_per_sec": rate_per_second_us(create_group_iterations, rust_create_group_duration_us),
                "speedup_vs_legacy": create_group_speedup,
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "create_group_from_filter_yaml_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "create_group_from_filter_yaml",
                "command_template": create_filter_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": create_filter_iterations,
                "duration_ms": duration_ms_from_us(legacy_create_filter_duration_us),
                "duration_us": legacy_create_filter_duration_us,
                "samples_us": legacy_create_filter_samples_us,
                "sample_stats_us": legacy_create_filter_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&legacy_create_filter_process_samples),
                "process_stats": process_metrics_summary(&legacy_create_filter_process_samples),
                "filter_mappings_processed": create_filter_iterations,
                "throughput_filter_mappings_per_sec": rate_per_second_us(create_filter_iterations, legacy_create_filter_duration_us),
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "create_group_from_filter_yaml_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "create_group_from_filter_yaml",
                "command_template": create_filter_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": create_filter_iterations,
                "duration_ms": duration_ms_from_us(rust_create_filter_duration_us),
                "duration_us": rust_create_filter_duration_us,
                "samples_us": rust_create_filter_samples_us,
                "sample_stats_us": rust_create_filter_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&rust_create_filter_process_samples),
                "process_stats": process_metrics_summary(&rust_create_filter_process_samples),
                "filter_mappings_processed": create_filter_iterations,
                "throughput_filter_mappings_per_sec": rate_per_second_us(create_filter_iterations, rust_create_filter_duration_us),
                "speedup_vs_legacy": create_filter_speedup,
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "merge_group_yaml_additive_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "merge_group_yaml_additive",
                "command_template": merge_group_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": merge_group_iterations,
                "duration_ms": duration_ms_from_us(legacy_merge_group_duration_us),
                "duration_us": legacy_merge_group_duration_us,
                "samples_us": legacy_merge_group_samples_us,
                "sample_stats_us": legacy_merge_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&legacy_merge_group_process_samples),
                "process_stats": process_metrics_summary(&legacy_merge_group_process_samples),
                "groups_merged": merge_group_iterations,
                "throughput_groups_per_sec": rate_per_second_us(merge_group_iterations, legacy_merge_group_duration_us),
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "merge_group_yaml_additive_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "merge_group_yaml_additive",
                "command_template": merge_group_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": merge_group_iterations,
                "duration_ms": duration_ms_from_us(rust_merge_group_duration_us),
                "duration_us": rust_merge_group_duration_us,
                "samples_us": rust_merge_group_samples_us,
                "sample_stats_us": rust_merge_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&rust_merge_group_process_samples),
                "process_stats": process_metrics_summary(&rust_merge_group_process_samples),
                "groups_merged": merge_group_iterations,
                "throughput_groups_per_sec": rate_per_second_us(merge_group_iterations, rust_merge_group_duration_us),
                "speedup_vs_legacy": merge_group_speedup,
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "diff_group_csv_additions_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "diff_group_csv_additions",
                "command_template": diff_group_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": diff_group_iterations,
                "duration_ms": duration_ms_from_us(legacy_diff_group_duration_us),
                "duration_us": legacy_diff_group_duration_us,
                "samples_us": legacy_diff_group_samples_us,
                "sample_stats_us": legacy_diff_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&legacy_diff_group_process_samples),
                "process_stats": process_metrics_summary(&legacy_diff_group_process_samples),
                "diffs_processed": diff_group_iterations,
                "throughput_diffs_per_sec": rate_per_second_us(diff_group_iterations, legacy_diff_group_duration_us),
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "diff_group_csv_additions_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "diff_group_csv_additions",
                "command_template": diff_group_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": diff_group_iterations,
                "duration_ms": duration_ms_from_us(rust_diff_group_duration_us),
                "duration_us": rust_diff_group_duration_us,
                "samples_us": rust_diff_group_samples_us,
                "sample_stats_us": rust_diff_group_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&rust_diff_group_process_samples),
                "process_stats": process_metrics_summary(&rust_diff_group_process_samples),
                "diffs_processed": diff_group_iterations,
                "throughput_diffs_per_sec": rate_per_second_us(diff_group_iterations, rust_diff_group_duration_us),
                "speedup_vs_legacy": diff_group_speedup,
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "remove_resources_yaml_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "remove_resources_yaml",
                "command_template": remove_resources_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": remove_resources_iterations,
                "duration_ms": duration_ms_from_us(legacy_remove_resources_duration_us),
                "duration_us": legacy_remove_resources_duration_us,
                "samples_us": legacy_remove_resources_samples_us,
                "sample_stats_us": legacy_remove_resources_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&legacy_remove_resources_process_samples),
                "process_stats": process_metrics_summary(&legacy_remove_resources_process_samples),
                "removes_processed": remove_resources_iterations,
                "throughput_removes_per_sec": rate_per_second_us(remove_resources_iterations, legacy_remove_resources_duration_us),
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "remove_resources_yaml_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "remove_resources_yaml",
                "command_template": remove_resources_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": remove_resources_iterations,
                "duration_ms": duration_ms_from_us(rust_remove_resources_duration_us),
                "duration_us": rust_remove_resources_duration_us,
                "samples_us": rust_remove_resources_samples_us,
                "sample_stats_us": rust_remove_resources_sample_stats.clone(),
                "process_samples": process_metrics_sample_json(&rust_remove_resources_process_samples),
                "process_stats": process_metrics_summary(&rust_remove_resources_process_samples),
                "removes_processed": remove_resources_iterations,
                "throughput_removes_per_sec": rate_per_second_us(remove_resources_iterations, rust_remove_resources_duration_us),
                "speedup_vs_legacy": remove_resources_speedup,
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "create_bundle_local_cdn_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "create_bundle_local_cdn",
                "command_template": create_bundle_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": create_bundle_iterations,
                "duration_ms": duration_ms_from_us(legacy_create_bundle_benchmark.duration_us),
                "duration_us": legacy_create_bundle_benchmark.duration_us,
                "samples_us": legacy_create_bundle_benchmark.samples_us,
                "sample_stats_us": legacy_create_bundle_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&legacy_create_bundle_benchmark.process_samples),
                "process_stats": process_metrics_summary(&legacy_create_bundle_benchmark.process_samples),
                "bundles_processed": create_bundle_iterations,
                "throughput_bundles_per_sec": rate_per_second_us(create_bundle_iterations, legacy_create_bundle_benchmark.duration_us),
                "artifact_manifest": "BundleOut/BundleResourceGroup.yaml",
                "artifact_directory": "CreateBundleOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "create_bundle_local_cdn_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "create_bundle_local_cdn",
                "command_template": create_bundle_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": create_bundle_iterations,
                "duration_ms": duration_ms_from_us(rust_create_bundle_benchmark.duration_us),
                "duration_us": rust_create_bundle_benchmark.duration_us,
                "samples_us": rust_create_bundle_benchmark.samples_us,
                "sample_stats_us": rust_create_bundle_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&rust_create_bundle_benchmark.process_samples),
                "process_stats": process_metrics_summary(&rust_create_bundle_benchmark.process_samples),
                "bundles_processed": create_bundle_iterations,
                "throughput_bundles_per_sec": rate_per_second_us(create_bundle_iterations, rust_create_bundle_benchmark.duration_us),
                "speedup_vs_legacy": create_bundle_speedup,
                "artifact_manifest": "BundleOut/BundleResourceGroup.yaml",
                "artifact_directory": "CreateBundleOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "create_patch_local_cdn_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "create_patch_local_cdn",
                "command_template": create_patch_legacy_command_template,
                "build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "iterations": create_patch_iterations,
                "duration_ms": duration_ms_from_us(legacy_create_patch_benchmark.duration_us),
                "duration_us": legacy_create_patch_benchmark.duration_us,
                "samples_us": legacy_create_patch_benchmark.samples_us,
                "sample_stats_us": legacy_create_patch_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&legacy_create_patch_benchmark.process_samples),
                "process_stats": process_metrics_summary(&legacy_create_patch_benchmark.process_samples),
                "patches_processed": create_patch_iterations,
                "throughput_patches_per_sec": rate_per_second_us(create_patch_iterations, legacy_create_patch_benchmark.duration_us),
                "artifact_manifest": "PatchOut/PatchResourceGroup.yaml",
                "artifact_directory": "PatchOut/Patches",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "create_patch_local_cdn_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "create_patch_local_cdn",
                "command_template": create_patch_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": create_patch_iterations,
                "duration_ms": duration_ms_from_us(rust_create_patch_benchmark.duration_us),
                "duration_us": rust_create_patch_benchmark.duration_us,
                "samples_us": rust_create_patch_benchmark.samples_us,
                "sample_stats_us": rust_create_patch_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&rust_create_patch_benchmark.process_samples),
                "process_stats": process_metrics_summary(&rust_create_patch_benchmark.process_samples),
                "patches_processed": create_patch_iterations,
                "throughput_patches_per_sec": rate_per_second_us(create_patch_iterations, rust_create_patch_benchmark.duration_us),
                "speedup_vs_legacy": create_patch_speedup,
                "artifact_manifest": "PatchOut/PatchResourceGroup.yaml",
                "artifact_directory": "PatchOut/Patches",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "unpack_bundle_local_cdn_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "unpack_bundle_local_cdn",
                "command_template": unpack_bundle_legacy_command_template,
                "build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_binary": legacy_resources_dev_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_dev_cli_known_non_debug,
                "iterations": unpack_bundle_iterations,
                "duration_ms": duration_ms_from_us(legacy_unpack_bundle_benchmark.duration_us),
                "duration_us": legacy_unpack_bundle_benchmark.duration_us,
                "samples_us": legacy_unpack_bundle_benchmark.samples_us,
                "sample_stats_us": legacy_unpack_bundle_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&legacy_unpack_bundle_benchmark.process_samples),
                "process_stats": process_metrics_summary(&legacy_unpack_bundle_benchmark.process_samples),
                "bundles_unpacked": unpack_bundle_iterations,
                "throughput_bundles_unpacked_per_sec": rate_per_second_us(unpack_bundle_iterations, legacy_unpack_bundle_benchmark.duration_us),
                "artifact_manifest": "UnpackBundleOut/ResourceGroup.yaml",
                "artifact_directory": "UnpackBundleOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "unpack_bundle_local_cdn_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "unpack_bundle_local_cdn",
                "command_template": unpack_bundle_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": unpack_bundle_iterations,
                "duration_ms": duration_ms_from_us(rust_unpack_bundle_benchmark.duration_us),
                "duration_us": rust_unpack_bundle_benchmark.duration_us,
                "samples_us": rust_unpack_bundle_benchmark.samples_us,
                "sample_stats_us": rust_unpack_bundle_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&rust_unpack_bundle_benchmark.process_samples),
                "process_stats": process_metrics_summary(&rust_unpack_bundle_benchmark.process_samples),
                "bundles_unpacked": unpack_bundle_iterations,
                "throughput_bundles_unpacked_per_sec": rate_per_second_us(unpack_bundle_iterations, rust_unpack_bundle_benchmark.duration_us),
                "speedup_vs_legacy": unpack_bundle_speedup,
                "artifact_manifest": "UnpackBundleOut/ResourceGroup.yaml",
                "artifact_directory": "UnpackBundleOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            },
            {
                "component": "resources",
                "workload": "apply_patch_local_cdn_legacy_cli",
                "implementation": "legacy_cpp_cli",
                "comparison_group": "apply_patch_local_cdn",
                "command_template": apply_patch_legacy_command_template,
                "build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_binary": legacy_resources_dev_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_dev_cli_known_non_debug,
                "iterations": apply_patch_iterations,
                "duration_ms": duration_ms_from_us(legacy_apply_patch_benchmark.duration_us),
                "duration_us": legacy_apply_patch_benchmark.duration_us,
                "samples_us": legacy_apply_patch_benchmark.samples_us,
                "sample_stats_us": legacy_apply_patch_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&legacy_apply_patch_benchmark.process_samples),
                "process_stats": process_metrics_summary(&legacy_apply_patch_benchmark.process_samples),
                "patches_applied": apply_patch_iterations,
                "throughput_patches_applied_per_sec": rate_per_second_us(apply_patch_iterations, legacy_apply_patch_benchmark.duration_us),
                "artifact_directory": "ApplyPatchOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "baseline"
            },
            {
                "component": "resources",
                "workload": "apply_patch_local_cdn_rust_xtask_process",
                "implementation": "rust_xtask_process",
                "comparison_group": "apply_patch_local_cdn",
                "command_template": apply_patch_rust_command_template,
                "build_profile": xtask_build_profile,
                "iterations": apply_patch_iterations,
                "duration_ms": duration_ms_from_us(rust_apply_patch_benchmark.duration_us),
                "duration_us": rust_apply_patch_benchmark.duration_us,
                "samples_us": rust_apply_patch_benchmark.samples_us,
                "sample_stats_us": rust_apply_patch_benchmark.sample_stats_us.clone(),
                "process_samples": process_metrics_sample_json(&rust_apply_patch_benchmark.process_samples),
                "process_stats": process_metrics_summary(&rust_apply_patch_benchmark.process_samples),
                "patches_applied": apply_patch_iterations,
                "throughput_patches_applied_per_sec": rate_per_second_us(apply_patch_iterations, rust_apply_patch_benchmark.duration_us),
                "speedup_vs_legacy": apply_patch_speedup,
                "artifact_directory": "ApplyPatchOut",
                "parity_status": "pass",
                "parity_gate": "rust-resources.json",
                "comparability": "comparable_process_to_process",
                "claim": "preliminary_process_level_speedup"
            }
        ],
        "comparisons": [
            {
                "component": "resources",
                "workload": "create_group_directory_yaml",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": create_group_legacy_command_template,
                "rust_command_template": create_group_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": create_group_iterations,
                "legacy_duration_us": legacy_create_group_duration_us,
                "rust_duration_us": rust_create_group_duration_us,
                "legacy_sample_stats_us": legacy_create_group_sample_stats,
                "rust_sample_stats_us": rust_create_group_sample_stats,
                "legacy_process_stats": process_metrics_summary(&legacy_create_group_process_samples),
                "rust_process_stats": process_metrics_summary(&rust_create_group_process_samples),
                "resource_comparison": process_comparison_summary(create_group_iterations, legacy_create_group_duration_us, rust_create_group_duration_us, &legacy_create_group_process_samples, &rust_create_group_process_samples),
                "legacy_throughput_directories_per_sec": rate_per_second_us(create_group_iterations, legacy_create_group_duration_us),
                "rust_throughput_directories_per_sec": rate_per_second_us(create_group_iterations, rust_create_group_duration_us),
                "speedup": create_group_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level create-group benchmark only"
            },
            {
                "component": "resources",
                "workload": "create_group_from_filter_yaml",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": create_filter_legacy_command_template,
                "rust_command_template": create_filter_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": create_filter_iterations,
                "legacy_duration_us": legacy_create_filter_duration_us,
                "rust_duration_us": rust_create_filter_duration_us,
                "legacy_sample_stats_us": legacy_create_filter_sample_stats,
                "rust_sample_stats_us": rust_create_filter_sample_stats,
                "legacy_process_stats": process_metrics_summary(&legacy_create_filter_process_samples),
                "rust_process_stats": process_metrics_summary(&rust_create_filter_process_samples),
                "resource_comparison": process_comparison_summary(create_filter_iterations, legacy_create_filter_duration_us, rust_create_filter_duration_us, &legacy_create_filter_process_samples, &rust_create_filter_process_samples),
                "legacy_throughput_filter_mappings_per_sec": rate_per_second_us(create_filter_iterations, legacy_create_filter_duration_us),
                "rust_throughput_filter_mappings_per_sec": rate_per_second_us(create_filter_iterations, rust_create_filter_duration_us),
                "speedup": create_filter_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level create-group-from-filter benchmark only"
            },
            {
                "component": "resources",
                "workload": "merge_group_yaml_additive",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": merge_group_legacy_command_template,
                "rust_command_template": merge_group_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": merge_group_iterations,
                "legacy_duration_us": legacy_merge_group_duration_us,
                "rust_duration_us": rust_merge_group_duration_us,
                "legacy_sample_stats_us": legacy_merge_group_sample_stats,
                "rust_sample_stats_us": rust_merge_group_sample_stats,
                "legacy_process_stats": process_metrics_summary(&legacy_merge_group_process_samples),
                "rust_process_stats": process_metrics_summary(&rust_merge_group_process_samples),
                "resource_comparison": process_comparison_summary(merge_group_iterations, legacy_merge_group_duration_us, rust_merge_group_duration_us, &legacy_merge_group_process_samples, &rust_merge_group_process_samples),
                "legacy_throughput_groups_per_sec": rate_per_second_us(merge_group_iterations, legacy_merge_group_duration_us),
                "rust_throughput_groups_per_sec": rate_per_second_us(merge_group_iterations, rust_merge_group_duration_us),
                "speedup": merge_group_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level merge-group benchmark only"
            },
            {
                "component": "resources",
                "workload": "diff_group_csv_additions",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": diff_group_legacy_command_template,
                "rust_command_template": diff_group_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": diff_group_iterations,
                "legacy_duration_us": legacy_diff_group_duration_us,
                "rust_duration_us": rust_diff_group_duration_us,
                "legacy_sample_stats_us": legacy_diff_group_sample_stats,
                "rust_sample_stats_us": rust_diff_group_sample_stats,
                "legacy_process_stats": process_metrics_summary(&legacy_diff_group_process_samples),
                "rust_process_stats": process_metrics_summary(&rust_diff_group_process_samples),
                "resource_comparison": process_comparison_summary(diff_group_iterations, legacy_diff_group_duration_us, rust_diff_group_duration_us, &legacy_diff_group_process_samples, &rust_diff_group_process_samples),
                "legacy_throughput_diffs_per_sec": rate_per_second_us(diff_group_iterations, legacy_diff_group_duration_us),
                "rust_throughput_diffs_per_sec": rate_per_second_us(diff_group_iterations, rust_diff_group_duration_us),
                "speedup": diff_group_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level diff-group benchmark only"
            },
            {
                "component": "resources",
                "workload": "remove_resources_yaml",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": remove_resources_legacy_command_template,
                "rust_command_template": remove_resources_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": remove_resources_iterations,
                "legacy_duration_us": legacy_remove_resources_duration_us,
                "rust_duration_us": rust_remove_resources_duration_us,
                "legacy_sample_stats_us": legacy_remove_resources_sample_stats,
                "rust_sample_stats_us": rust_remove_resources_sample_stats,
                "legacy_process_stats": process_metrics_summary(&legacy_remove_resources_process_samples),
                "rust_process_stats": process_metrics_summary(&rust_remove_resources_process_samples),
                "resource_comparison": process_comparison_summary(remove_resources_iterations, legacy_remove_resources_duration_us, rust_remove_resources_duration_us, &legacy_remove_resources_process_samples, &rust_remove_resources_process_samples),
                "legacy_throughput_removes_per_sec": rate_per_second_us(remove_resources_iterations, legacy_remove_resources_duration_us),
                "rust_throughput_removes_per_sec": rate_per_second_us(remove_resources_iterations, rust_remove_resources_duration_us),
                "speedup": remove_resources_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level remove-resources benchmark only"
            },
            {
                "component": "resources",
                "workload": "create_bundle_local_cdn",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": create_bundle_legacy_command_template,
                "rust_command_template": create_bundle_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": create_bundle_iterations,
                "legacy_duration_us": legacy_create_bundle_benchmark.duration_us,
                "rust_duration_us": rust_create_bundle_benchmark.duration_us,
                "legacy_sample_stats_us": legacy_create_bundle_benchmark.sample_stats_us,
                "rust_sample_stats_us": rust_create_bundle_benchmark.sample_stats_us,
                "legacy_process_stats": process_metrics_summary(&legacy_create_bundle_benchmark.process_samples),
                "rust_process_stats": process_metrics_summary(&rust_create_bundle_benchmark.process_samples),
                "resource_comparison": process_comparison_summary(create_bundle_iterations, legacy_create_bundle_benchmark.duration_us, rust_create_bundle_benchmark.duration_us, &legacy_create_bundle_benchmark.process_samples, &rust_create_bundle_benchmark.process_samples),
                "legacy_throughput_bundles_per_sec": rate_per_second_us(create_bundle_iterations, legacy_create_bundle_benchmark.duration_us),
                "rust_throughput_bundles_per_sec": rate_per_second_us(create_bundle_iterations, rust_create_bundle_benchmark.duration_us),
                "speedup": create_bundle_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level create-bundle benchmark only"
            },
            {
                "component": "resources",
                "workload": "create_patch_local_cdn",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": create_patch_legacy_command_template,
                "rust_command_template": create_patch_rust_command_template,
                "legacy_build_profile": legacy_resources_cli_profile.as_str(),
                "legacy_binary": legacy_resources_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": create_patch_iterations,
                "legacy_duration_us": legacy_create_patch_benchmark.duration_us,
                "rust_duration_us": rust_create_patch_benchmark.duration_us,
                "legacy_sample_stats_us": legacy_create_patch_benchmark.sample_stats_us,
                "rust_sample_stats_us": rust_create_patch_benchmark.sample_stats_us,
                "legacy_process_stats": process_metrics_summary(&legacy_create_patch_benchmark.process_samples),
                "rust_process_stats": process_metrics_summary(&rust_create_patch_benchmark.process_samples),
                "resource_comparison": process_comparison_summary(create_patch_iterations, legacy_create_patch_benchmark.duration_us, rust_create_patch_benchmark.duration_us, &legacy_create_patch_benchmark.process_samples, &rust_create_patch_benchmark.process_samples),
                "legacy_throughput_patches_per_sec": rate_per_second_us(create_patch_iterations, legacy_create_patch_benchmark.duration_us),
                "rust_throughput_patches_per_sec": rate_per_second_us(create_patch_iterations, rust_create_patch_benchmark.duration_us),
                "speedup": create_patch_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level create-patch benchmark only"
            },
            {
                "component": "resources",
                "workload": "unpack_bundle_local_cdn",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": unpack_bundle_legacy_command_template,
                "rust_command_template": unpack_bundle_rust_command_template,
                "legacy_build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_binary": legacy_resources_dev_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_dev_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": unpack_bundle_iterations,
                "legacy_duration_us": legacy_unpack_bundle_benchmark.duration_us,
                "rust_duration_us": rust_unpack_bundle_benchmark.duration_us,
                "legacy_sample_stats_us": legacy_unpack_bundle_benchmark.sample_stats_us,
                "rust_sample_stats_us": rust_unpack_bundle_benchmark.sample_stats_us,
                "legacy_process_stats": process_metrics_summary(&legacy_unpack_bundle_benchmark.process_samples),
                "rust_process_stats": process_metrics_summary(&rust_unpack_bundle_benchmark.process_samples),
                "resource_comparison": process_comparison_summary(unpack_bundle_iterations, legacy_unpack_bundle_benchmark.duration_us, rust_unpack_bundle_benchmark.duration_us, &legacy_unpack_bundle_benchmark.process_samples, &rust_unpack_bundle_benchmark.process_samples),
                "legacy_throughput_bundles_unpacked_per_sec": rate_per_second_us(unpack_bundle_iterations, legacy_unpack_bundle_benchmark.duration_us),
                "rust_throughput_bundles_unpacked_per_sec": rate_per_second_us(unpack_bundle_iterations, rust_unpack_bundle_benchmark.duration_us),
                "speedup": unpack_bundle_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level unpack-bundle benchmark only"
            },
            {
                "component": "resources",
                "workload": "apply_patch_local_cdn",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_command_template": apply_patch_legacy_command_template,
                "rust_command_template": apply_patch_rust_command_template,
                "legacy_build_profile": legacy_resources_dev_cli_profile.as_str(),
                "legacy_binary": legacy_resources_dev_cli.display().to_string(),
                "legacy_known_non_debug": legacy_resources_dev_cli_known_non_debug,
                "rust_build_profile": xtask_build_profile,
                "sample_count": apply_patch_iterations,
                "legacy_duration_us": legacy_apply_patch_benchmark.duration_us,
                "rust_duration_us": rust_apply_patch_benchmark.duration_us,
                "legacy_sample_stats_us": legacy_apply_patch_benchmark.sample_stats_us,
                "rust_sample_stats_us": rust_apply_patch_benchmark.sample_stats_us,
                "legacy_process_stats": process_metrics_summary(&legacy_apply_patch_benchmark.process_samples),
                "rust_process_stats": process_metrics_summary(&rust_apply_patch_benchmark.process_samples),
                "resource_comparison": process_comparison_summary(apply_patch_iterations, legacy_apply_patch_benchmark.duration_us, rust_apply_patch_benchmark.duration_us, &legacy_apply_patch_benchmark.process_samples, &rust_apply_patch_benchmark.process_samples),
                "legacy_throughput_patches_applied_per_sec": rate_per_second_us(apply_patch_iterations, legacy_apply_patch_benchmark.duration_us),
                "rust_throughput_patches_applied_per_sec": rate_per_second_us(apply_patch_iterations, rust_apply_patch_benchmark.duration_us),
                "speedup": apply_patch_speedup,
                "parity_status": "pass",
                "comparability": "comparable_process_to_process",
                "claim_scope": "preliminary local process-level apply-patch benchmark only"
            }
        ]
    });
    let evidence_path = evidence_path("bench-tier-local.json");
    write_json(&evidence_path, &evidence)?;
    println!(
        "bench-tier-local: pass (preliminary resource process comparisons including catalog/filter/bundle/patch create/apply/unpack; final report not ready); evidence {}",
        evidence_path.display()
    );
    Ok(())
}

fn final_report() -> Result<()> {
    let required = [
        "scheduler-fixtures.json",
        "legacy-scheduler.json",
        "rust-scheduler-python.json",
        "io-workloads.json",
        "legacy-resources.json",
        "rust-resources.json",
        "bench-tier-local.json",
    ];

    let mut missing_or_failed = Vec::new();
    let mut evidence = Vec::new();
    for file in required {
        match read_evidence(file) {
            Ok(value) if report_ready_blockers(file, &value).is_empty() => {
                evidence.push((file, value));
            }
            Ok(value) => {
                let blockers = report_ready_blockers(file, &value);
                missing_or_failed.push(format!(
                    "{file}: {}",
                    readiness_blocker_summary(&value, &blockers)
                ));
            }
            Err(_) => missing_or_failed.push(format!("{file}: missing")),
        }
    }

    if !missing_or_failed.is_empty() {
        return Err(anyhow!(
            "final HTML report is blocked until all evidence gates pass and validate report-ready:\n{}",
            missing_or_failed.join("\n")
        ));
    }

    let report_path = Path::new("target/carbon/report/index.html");
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating report dir {}", parent.display()))?;
    }
    fs::write(report_path, render_html_report(&evidence)?)
        .with_context(|| format!("writing report {}", report_path.display()))?;
    println!("final HTML report written to {}", report_path.display());
    Ok(())
}

fn progress_report() -> Result<()> {
    let evidence_files = [
        "scheduler-fixtures.json",
        "legacy-scheduler.json",
        "rust-scheduler-python.json",
        "io-workloads.json",
        "legacy-resources.json",
        "rust-resources.json",
        "bench-tier-local.json",
        "scalability-matrix.json",
    ];
    let evidence = evidence_files
        .iter()
        .map(|file| (*file, read_evidence(file).ok()))
        .collect::<Vec<_>>();

    let report_path = Path::new("target/carbon/report/progress.html");
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating report dir {}", parent.display()))?;
    }
    fs::write(report_path, render_progress_report(&evidence)?)
        .with_context(|| format!("writing report {}", report_path.display()))?;
    println!(
        "current progress HTML report written to {}",
        report_path.display()
    );
    Ok(())
}

fn readiness_line(file: &str) -> String {
    match read_evidence(file) {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let report_ready = value
                .get("report_ready")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let coverage = value
                .get("coverage")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let blockers = report_ready_blockers(file, &value);
            if blockers.is_empty() {
                format!("{status} ({coverage}, report_ready=true)")
            } else {
                format!(
                    "{status} ({coverage}, report_ready=false, claimed_report_ready={report_ready}, blockers={})",
                    readiness_blocker_codes_summary(&blockers)
                )
            }
        }
        Err(_) => String::from("missing"),
    }
}

fn report_ready_blockers(file: &str, value: &Value) -> Vec<Value> {
    let mut blockers = Vec::new();
    if value.get("status").and_then(Value::as_str) != Some("pass") {
        blockers.push(readiness_blocker(
            "status_not_pass",
            format!(
                "{} status is {}",
                file,
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            "rerun the gate until it records status=pass",
        ));
    }
    if value.get("report_ready").and_then(Value::as_bool) != Some(true) {
        blockers.push(readiness_blocker(
            "report_ready_false",
            format!("{file} is not marked report_ready"),
            "finish the listed semantic parity work before promoting report_ready",
        ));
    }
    if value
        .get("remaining_before_report_ready")
        .and_then(Value::as_array)
        .is_some_and(|remaining| !remaining.is_empty())
    {
        blockers.push(readiness_blocker(
            "remaining_report_ready_work",
            format!("{file} still lists remaining report-ready work"),
            "clear remaining_before_report_ready only after the covered parity tests are complete",
        ));
    }
    if value
        .get("not_report_ready_reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| !reason.trim().is_empty())
    {
        blockers.push(readiness_blocker(
            "not_report_ready_reason_present",
            format!("{file} still records a not_report_ready_reason"),
            "remove not_report_ready_reason only when the evidence is genuinely final-report eligible",
        ));
    }

    match file {
        "legacy-scheduler.json" => {
            blockers.extend(legacy_scheduler_report_ready_blockers(value));
        }
        "rust-scheduler-python.json" => {
            blockers.extend(rust_scheduler_python_report_ready_blockers(value));
        }
        _ => {}
    }
    blockers.extend(performance_report_ready_blockers(file, value));
    blockers
}

fn legacy_scheduler_report_ready_blockers(value: &Value) -> Vec<Value> {
    let mut blockers = Vec::new();
    let build_status = value.get("legacy_build_status");
    if build_status
        .and_then(|status| status.get("baseline_complete"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        blockers.push(readiness_blocker(
            "legacy_scheduler_baseline_incomplete",
            "legacy scheduler evidence does not record a complete passing CTest baseline",
            "configure, build, and run the legacy scheduler Python/C API CTest baseline successfully",
        ));
    }
    if build_status
        .and_then(|status| status.get("ctest"))
        .and_then(Value::as_str)
        != Some("pass")
    {
        blockers.push(readiness_blocker(
            "legacy_scheduler_ctest_not_pass",
            "legacy scheduler evidence does not record ctest=pass",
            "run the legacy scheduler CTest baseline and record a passing result",
        ));
    }
    match build_status
        .and_then(|status| status.get("tests_total"))
        .and_then(Value::as_u64)
    {
        Some(total) if total > 0 => {}
        Some(_) => blockers.push(readiness_blocker(
            "legacy_scheduler_ctest_no_tests",
            "legacy scheduler CTest summary reported zero tests",
            "ensure CTest discovers the legacy scheduler Python and C API tests",
        )),
        None => blockers.push(readiness_blocker(
            "legacy_scheduler_ctest_summary_missing",
            "legacy scheduler evidence is missing parsed CTest test counts",
            "record parsed tests_passed, tests_failed, and tests_total from CTest output",
        )),
    }
    if let Some(failed) = build_status
        .and_then(|status| status.get("tests_failed"))
        .and_then(Value::as_u64)
    {
        if failed != 0 {
            blockers.push(readiness_blocker(
                "legacy_scheduler_ctest_failed_tests",
                "legacy scheduler CTest summary includes failed tests",
                "fix the failing legacy scheduler baseline tests before promoting report_ready",
            ));
        }
    }
    blockers
}

fn rust_scheduler_python_report_ready_blockers(value: &Value) -> Vec<Value> {
    let mut blockers = Vec::new();
    let subset_len = value
        .get("unchanged_legacy_subset")
        .and_then(Value::as_array)
        .map(Vec::len);
    let subset_count = value
        .get("unchanged_legacy_subset_count")
        .and_then(Value::as_u64)
        .map(|count| count as usize);
    if subset_len != subset_count {
        blockers.push(readiness_blocker(
            "rust_scheduler_subset_count_mismatch",
            "rust scheduler unchanged legacy suite count does not match the listed tests",
            "update unchanged_legacy_subset_count together with the unchanged legacy suite list",
        ));
    }
    let core_ownership_status = value
        .get("core_ownership_status")
        .and_then(|status| status.get("status"))
        .and_then(Value::as_str);
    if core_ownership_status != Some("complete") {
        blockers.push(readiness_blocker(
            "scheduler_core_ownership_not_complete",
            format!(
                "rust scheduler Python bridge core ownership status is {}",
                core_ownership_status.unwrap_or("missing")
            ),
            "move tasklet/channel/scheduler lifecycle ownership into Rust core/FFI handles before promoting the Python bridge to final report readiness",
        ));
    }
    blockers
}

fn performance_report_ready_blockers(file: &str, value: &Value) -> Vec<Value> {
    if !has_comparable_speedup_claim(value)
        || value.get("report_ready").and_then(Value::as_bool) != Some(true)
    {
        return Vec::new();
    }

    let mut blockers = Vec::new();
    let build_profile = value
        .get("build_profile")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .pointer("/host/rust_build/build_profile")
                .and_then(Value::as_str)
        })
        .unwrap_or("unknown");
    if build_profile != "release-native" {
        blockers.push(readiness_blocker(
            "performance_evidence_debug_build",
            format!("{file} contains comparable speedup claims from build_profile={build_profile}"),
            "rerun comparable performance evidence with scripts/carbon-native-bench.sh so build_profile=release-native",
        ));
    }
    if value
        .pointer("/host/rust_build/target_cpu_native")
        .and_then(Value::as_bool)
        != Some(true)
    {
        blockers.push(readiness_blocker(
            "performance_evidence_not_native",
            format!("{file} contains comparable speedup claims without target-cpu=native evidence"),
            "rerun comparable performance evidence with RUSTFLAGS including -C target-cpu=native",
        ));
    }
    if value
        .pointer("/host/rust_build/debug_assertions")
        .and_then(Value::as_bool)
        == Some(true)
    {
        blockers.push(readiness_blocker(
            "performance_evidence_debug_assertions",
            format!("{file} contains comparable speedup claims from a debug-assertions build"),
            "rerun comparable performance evidence with a release-native build without debug assertions",
        ));
    }
    if value
        .get("comparisons")
        .and_then(Value::as_array)
        .is_some_and(|comparisons| {
            comparisons.iter().any(|comparison| {
                comparison.get("speedup").is_some()
                    && comparison.get("comparability").and_then(Value::as_str)
                        == Some("comparable_process_to_process")
                    && !has_known_non_debug_legacy_performance_baseline(comparison)
            })
        })
    {
        blockers.push(readiness_blocker(
            "performance_legacy_baseline_not_optimized",
            format!("{file} contains comparable speedup ratios without a known non-debug legacy baseline"),
            "rerun comparable performance evidence against an optimized legacy C++ baseline or keep the row as an observed ratio only",
        ));
    }
    blockers
}

fn has_comparable_speedup_claim(value: &Value) -> bool {
    value
        .get("comparisons")
        .and_then(Value::as_array)
        .is_some_and(|comparisons| {
            comparisons.iter().any(|comparison| {
                comparison.get("speedup").is_some()
                    && comparison.get("comparability").and_then(Value::as_str)
                        == Some("comparable_process_to_process")
            })
        })
}

fn readiness_blocker_summary(value: &Value, blockers: &[Value]) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let claimed_report_ready = value
        .get("report_ready")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    format!(
        "{status}, report_ready=false, claimed_report_ready={claimed_report_ready}, blockers={}",
        readiness_blocker_codes_summary(blockers)
    )
}

fn readiness_blocker_codes_summary(blockers: &[Value]) -> String {
    let codes = blocker_codes(blockers);
    if codes.is_empty() {
        String::from("[]")
    } else {
        format!("[{}]", codes.join(","))
    }
}

fn render_progress_gate_blocker_rows(evidence: &[(&str, Option<Value>)]) -> String {
    let mut rows = String::new();
    for (file, value) in evidence {
        match value {
            Some(value) => {
                let gate = value.get("gate").and_then(Value::as_str).unwrap_or(file);
                let blockers = report_ready_blockers(file, value);
                let blocker_class = progress_blocker_class(file, value, &blockers);
                let codes = blocker_codes(&blockers);
                let code_html = if codes.is_empty() {
                    String::from("<span class=\"chip\">clear</span>")
                } else {
                    codes
                        .iter()
                        .map(|code| {
                            format!("<span class=\"blocker-code\">{}</span>", escape_html(code))
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                rows.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    escape_html(gate),
                    escape_html(&blocker_class),
                    code_html,
                    render_progress_remaining_items(value, blockers.is_empty())
                ));
            }
            None => {
                rows.push_str(&format!(
                    "<tr><td>{}</td><td>local evidence</td><td><span class=\"blocker-code\">missing_evidence</span></td><td><ul class=\"compact-list\"><li>run the gate and write target/carbon/evidence/{}</li></ul></td></tr>",
                    escape_html(file),
                    escape_html(file)
                ));
            }
        }
    }
    rows
}

fn progress_blocker_class(file: &str, value: &Value, blockers: &[Value]) -> String {
    if blockers.is_empty() {
        return String::from("clear");
    }
    let codes = blocker_codes(blockers);
    if file == "legacy-scheduler.json"
        || codes.iter().any(|code| {
            code == "legacy_scheduler_baseline_incomplete" || code.starts_with("legacy_scheduler_")
        })
    {
        return String::from("environment/baseline");
    }
    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status != "pass")
    {
        return String::from("local failing gate");
    }
    if file == "bench-tier-local.json" {
        return String::from("benchmark scope");
    }
    String::from("local parity/report gate")
}

fn render_progress_remaining_items(value: &Value, clear: bool) -> String {
    let items = value
        .get("remaining_before_report_ready")
        .and_then(Value::as_array)
        .map(|remaining| {
            remaining
                .iter()
                .filter_map(Value::as_str)
                .filter(|item| !item.trim().is_empty())
                .map(|item| format!("<li>{}</li>", escape_html(item)))
                .collect::<String>()
        })
        .unwrap_or_default();
    if !items.is_empty() {
        format!("<ul class=\"compact-list\">{items}</ul>")
    } else if clear {
        String::from("<span class=\"chip\">no blockers</span>")
    } else if let Some(reason) = value
        .get("not_report_ready_reason")
        .and_then(Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
    {
        format!(
            "<ul class=\"compact-list\"><li>{}</li></ul>",
            escape_html(reason)
        )
    } else {
        String::from("<span class=\"chip\">see blocker codes</span>")
    }
}

fn read_evidence(file: &str) -> Result<Value> {
    let path = evidence_path(file);
    let text = fs::read_to_string(&path)
        .with_context(|| format!("reading evidence {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing evidence {}", path.display()))
}

fn evidence_path(file: &str) -> PathBuf {
    Path::new(EVIDENCE_DIR).join(file)
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating evidence dir {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(value)? + "\n")
        .with_context(|| format!("writing evidence {}", path.display()))
}

fn ensure_success(output: Output, label: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    bail!(
        "{label} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn parse_ctest_summary(stdout: &str) -> (Option<u64>, Option<u64>) {
    parse_ctest_summary_from_text(stdout)
        .map(|summary| (Some(summary.passed), Some(summary.failed)))
        .unwrap_or((None, None))
}

fn parse_ctest_summary_from_text(stdout: &str) -> Option<CTestSummary> {
    for line in stdout.lines().rev() {
        if line.contains("No tests were found") {
            return Some(CTestSummary {
                passed: 0,
                failed: 0,
                total: 0,
            });
        }
        if let Some(summary) = parse_ctest_summary_line(line) {
            return Some(summary);
        }
    }
    None
}

fn parse_ctest_summary_line(line: &str) -> Option<CTestSummary> {
    let failed_marker = " tests failed out of ";
    let failed_index = line.find(failed_marker)?;
    let failed_prefix = &line[..failed_index];
    let failed = failed_prefix
        .rsplit_once(' ')
        .and_then(|(_, value)| value.parse::<u64>().ok())?;
    let total = line[failed_index + failed_marker.len()..]
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(CTestSummary {
        passed: total.saturating_sub(failed),
        failed,
        total,
    })
}

fn tail_lines(text: &str, count: usize) -> Vec<String> {
    let mut lines = text
        .lines()
        .rev()
        .take(count)
        .map(str::to_string)
        .collect::<Vec<_>>();
    lines.reverse();
    lines
}

fn parse_python_unittest_summary(stdout: &str, stderr: &str) -> Value {
    let combined = format!("{stdout}\n{stderr}");
    let ran = combined.lines().find_map(|line| {
        let line = line.trim();
        line.strip_prefix("Ran ")
            .and_then(|rest| rest.split_once(" tests"))
            .and_then(|(count, _)| count.parse::<u64>().ok())
    });
    let skipped = combined.lines().find_map(|line| {
        let line = line.trim();
        line.strip_prefix("OK (skipped=")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|count| count.parse::<u64>().ok())
    });
    let ok = combined
        .lines()
        .map(str::trim)
        .any(|line| line == "OK" || line.starts_with("OK ("));

    json!({
        "ran": ran,
        "skipped": skipped.unwrap_or(0),
        "ok": ok
    })
}

fn command_stdout(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        bail!("{program} failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn rate_per_second(units: u64, duration_ms: u64) -> u64 {
    if duration_ms == 0 {
        return 0;
    }
    ((units as u128) * 1000 / duration_ms as u128) as u64
}

fn rate_per_second_us(units: u64, duration_us: u64) -> u64 {
    if duration_us == 0 {
        return 0;
    }
    ((units as u128) * 1_000_000 / duration_us as u128) as u64
}

fn duration_ms_from_us(duration_us: u64) -> u64 {
    duration_us.div_ceil(1000)
}

fn speedup_ratio(legacy_duration_us: u64, rust_duration_us: u64) -> f64 {
    if rust_duration_us == 0 {
        return 0.0;
    }
    legacy_duration_us as f64 / rust_duration_us as f64
}

fn sample_stats_us(samples: &[u64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0
        });
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().sum::<u64>();
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "mean": sum as f64 / sorted.len() as f64,
        "p50": percentile_nearest_rank(&sorted, 50),
        "p95": percentile_nearest_rank(&sorted, 95),
        "p99": percentile_nearest_rank(&sorted, 99),
        "max": sorted[sorted.len() - 1]
    })
}

fn mean_u64_rounded(samples: &[u64]) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    ((samples.iter().sum::<u64>() as f64) / samples.len() as f64).round() as u64
}

fn percentile_nearest_rank(sorted_samples: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted_samples.len()).div_ceil(100);
    let index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    sorted_samples[index]
}

fn sample_stats_u64(samples: &[u64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0
        });
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().sum::<u64>();
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "mean": sum as f64 / sorted.len() as f64,
        "p50": percentile_nearest_rank(&sorted, 50),
        "p95": percentile_nearest_rank(&sorted, 95),
        "p99": percentile_nearest_rank(&sorted, 99),
        "max": sorted[sorted.len() - 1]
    })
}

fn sample_stats_f64(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0
        });
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let sum = sorted.iter().sum::<f64>();
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "mean": sum / sorted.len() as f64,
        "p50": percentile_nearest_rank_f64(&sorted, 50),
        "p95": percentile_nearest_rank_f64(&sorted, 95),
        "p99": percentile_nearest_rank_f64(&sorted, 99),
        "max": sorted[sorted.len() - 1]
    })
}

fn percentile_nearest_rank_f64(sorted_samples: &[f64], percentile: usize) -> f64 {
    let rank = (percentile * sorted_samples.len()).div_ceil(100);
    let index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    sorted_samples[index]
}

fn optional_ratio(numerator: Option<u64>, denominator: Option<u64>) -> Value {
    match (numerator, denominator) {
        (Some(_), Some(0)) => Value::Null,
        (Some(numerator), Some(denominator)) => json!(numerator as f64 / denominator as f64),
        _ => Value::Null,
    }
}

fn optional_ratio_f64(numerator: Option<f64>, denominator: Option<f64>) -> Value {
    match (numerator, denominator) {
        (Some(_), Some(0.0)) => Value::Null,
        (Some(numerator), Some(denominator)) => json!(numerator / denominator),
        _ => Value::Null,
    }
}

fn sum_cpu_burn_ms(samples: &[ProcessMetrics]) -> Option<u64> {
    let values = samples
        .iter()
        .map(ProcessMetrics::cpu_burn_ms)
        .collect::<Option<Vec<_>>>()?;
    Some(values.iter().sum())
}

fn sum_effective_cpu_burn_ms(samples: &[ProcessMetrics]) -> Option<f64> {
    let values = samples
        .iter()
        .map(ProcessMetrics::effective_cpu_burn_ms)
        .collect::<Option<Vec<_>>>()?;
    Some(values.iter().sum())
}

fn p95_max_rss_kb(samples: &[ProcessMetrics]) -> Option<u64> {
    let mut values = samples
        .iter()
        .map(|sample| sample.max_rss_kb)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(percentile_nearest_rank(&values, 95))
}

fn scaled_wall_seconds(duration_us: u64, sample_count: u64, scaled_units: u64) -> f64 {
    if sample_count == 0 {
        return 0.0;
    }
    (duration_us as f64 / sample_count as f64) * scaled_units as f64 / 1_000_000.0
}

fn scaled_cpu_seconds(samples: &[ProcessMetrics], scaled_units: u64) -> Value {
    let Some(cpu_burn_ms) = sum_effective_cpu_burn_ms(samples) else {
        return Value::Null;
    };
    if samples.is_empty() {
        return Value::Null;
    }
    json!((cpu_burn_ms as f64 / samples.len() as f64) * scaled_units as f64 / 1000.0)
}

fn host_cpu_model() -> Option<String> {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        line.strip_prefix("model name")
            .and_then(|rest| rest.split_once(':'))
            .map(|(_, value)| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn host_mem_total_kb() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?;
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })
}

fn rust_build_metadata() -> Value {
    let rustflags = env::var("RUSTFLAGS").ok();
    let rustflags_text = rustflags.as_deref().unwrap_or("");
    let rustflags_args = rustflags_text
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let target_cpu_native = rustflags_args
        .windows(2)
        .any(|window| window[0] == "-C" && window[1] == "target-cpu=native")
        || rustflags_args
            .iter()
            .any(|flag| flag == "-Ctarget-cpu=native" || flag == "target-cpu=native");
    let current_exe = env::current_exe().ok();
    json!({
        "build_profile": inferred_xtask_build_profile(),
        "build_profile_source": "current_exe_parent_when_available_else_debug_assertions",
        "xtask_profile": if cfg!(debug_assertions) { "debug_or_dev" } else { "release_or_custom" },
        "debug_assertions": cfg!(debug_assertions),
        "current_executable": current_exe.map(|path| path.display().to_string()),
        "rustc_verbose": command_stdout("rustc", &["-vV"]).ok(),
        "rustflags": rustflags,
        "rustflags_args": rustflags_args,
        "target_cpu_native": target_cpu_native,
        "workspace_release_lto": "thin",
        "workspace_release_native_lto": "fat",
        "workspace_profiles": {
            "release": {
                "codegen_units": 1,
                "lto": "thin"
            },
            "release-native": {
                "inherits": "release",
                "codegen_units": 1,
                "lto": "fat"
            }
        },
        "native_benchmark_wrapper": "scripts/carbon-native-bench.sh"
    })
}

fn inferred_xtask_build_profile() -> String {
    env::current_exe()
        .ok()
        .as_deref()
        .and_then(|path| {
            path.parent()
                .and_then(|parent| parent.file_name())
                .and_then(OsStr::to_str)
                .map(String::from)
        })
        .unwrap_or_else(|| {
            if cfg!(debug_assertions) {
                String::from("debug_or_dev")
            } else {
                String::from("release_or_custom")
            }
        })
}

fn command_line(program: &Path, args: &[OsString]) -> String {
    std::iter::once(shell_quote_os(program.as_os_str()))
        .chain(args.iter().map(|arg| shell_quote_os(arg.as_os_str())))
        .collect::<Vec<_>>()
        .join(" ")
}

fn command_line_with_current_dir(
    program: &Path,
    args: &[OsString],
    current_dir: Option<&Path>,
) -> String {
    let command = command_line(program, args);
    match current_dir {
        Some(current_dir) => format!(
            "cd {} && {command}",
            shell_quote_os(current_dir.as_os_str())
        ),
        None => command,
    }
}

fn shell_quote_os(value: &OsStr) -> String {
    shell_quote(&value.to_string_lossy())
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "_-./:=+{},".contains(ch))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p xtask -- <command>\n\ncommands:\n  scheduler-fixtures [fixtures/scheduler]\n  scheduler-trace <fixture-path>\n  legacy-scheduler [build|run|build-run|native-linux]\n  legacy-scheduler import <artifact.json|ctest.log> [--host-os <windows|macos>] [--host-arch <x86_64|aarch64>]\n  rust-scheduler-python\n  io-workloads\n  legacy-resources\n  rust-resources\n  bench\n  bench-scalability [--tier quick|full] [--families scheduler,io,data] [--samples N]\n  bench-scheduler-core [fixture-path] [iterations]\n  report-readiness\n  report-progress\n  report\n  rust-resources-cli <legacy-resource-command> [options]\n  rust-create-group [options] <input-directory>\n  rust-create-group-from-filter [options]\n  rust-create-bundle [options] <resource-group-path>\n  rust-unpack-bundle [options] <bundle-resource-group-path>\n  rust-create-patch [options] <previous-resource-group-path> <next-resource-group-path>\n  rust-apply-patch [options] <patch-resource-group-path>\n  rust-merge-group [options] <base-resource-group-path> <merge-resource-group-path>\n  rust-diff-group [options] <base-resource-group-path> <diff-resource-group-path>\n  rust-remove-resources [options] <resource-group-path> <resource-list-path>"
    );
}

fn render_html_report(evidence: &[(&str, Value)]) -> Result<String> {
    let tasks = fs::read_to_string("reviews/tasks.md").unwrap_or_default();
    let readiness = fs::read_to_string("reviews/report-readiness.md").unwrap_or_default();
    let optimization = fs::read_to_string("reviews/optimization-map.md").unwrap_or_default();
    let optimization_evidence = evidence
        .iter()
        .map(|(file, value)| (*file, Some(value.clone())))
        .collect::<Vec<_>>();

    let mut gate_rows = String::new();
    for (file, value) in evidence {
        let gate = value.get("gate").and_then(Value::as_str).unwrap_or(*file);
        let status = value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        gate_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(gate),
            escape_html(status),
            escape_html(file)
        ));
    }

    let scheduler = evidence
        .iter()
        .find(|(file, _)| *file == "scheduler-fixtures.json")
        .map(|(_, value)| value)
        .ok_or_else(|| anyhow!("scheduler fixture evidence missing"))?;
    let scheduler_passed = scheduler
        .get("passed")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let scheduler_total = scheduler
        .get("fixture_count")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let performance_entries =
        performance_entries_from_sources(evidence.iter().map(|(file, value)| (*file, value)));
    let performance_summary = performance_comparability_summary(&performance_entries);
    let performance_rows = render_performance_rows(&performance_entries);
    let feature_summary = render_feature_performance_summary(&performance_entries);
    let performance_chart_data = render_performance_chart_data(&performance_entries);
    let optimization_tables = render_optimization_tables(&optimization_evidence);
    let scheduler_ownership_boundary = render_scheduler_ownership_boundary(&optimization_evidence);

    Ok(format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Carbon Rust Migration Evidence Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #1f2933; }}
    h1, h2 {{ color: #102a43; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
    th, td {{ border: 1px solid #bcccdc; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    code, pre {{ background: #f0f4f8; padding: 2px 4px; }}
    pre {{ overflow: auto; padding: 12px; }}
    tbody tr.not-comparable {{ background: #fff8e6; }}
    tbody tr.is-hidden {{ display: none; }}
    .panel {{ border: 1px solid #bcccdc; background: #ffffff; padding: 16px; border-radius: 6px; margin: 12px 0 18px; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: end; margin-bottom: 14px; }}
    .toolbar label {{ display: grid; gap: 4px; font-size: 0.85rem; font-weight: 700; color: #334e68; }}
    select {{ border: 1px solid #9fb3c8; border-radius: 4px; padding: 6px 8px; background: #ffffff; color: #102a43; }}
    .dashboard-grid {{ display: grid; grid-template-columns: minmax(360px, 2fr) minmax(280px, 1fr); gap: 16px; align-items: start; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 14px; }}
    .kpi-card {{ border: 1px solid #d9e2ec; background: #f5f7fa; padding: 10px; border-radius: 6px; }}
    .kpi-card span {{ display: block; color: #52606d; font-size: 0.78rem; font-weight: 700; text-transform: uppercase; }}
    .kpi-card strong {{ display: block; margin-top: 3px; color: #102a43; font-size: 1.18rem; }}
    .chart {{ display: grid; gap: 10px; }}
    .bar-row {{ display: grid; grid-template-columns: minmax(180px, 1.2fr) minmax(240px, 3fr); gap: 12px; align-items: center; border: 1px solid transparent; border-radius: 6px; padding: 7px; }}
    .bar-row:hover {{ background: #f8fafc; border-color: #d9e2ec; cursor: pointer; }}
    .bar-row.is-selected {{ background: #edf7ed; border-color: #9ae6b4; }}
    .bar-label {{ font-weight: 700; color: #102a43; overflow-wrap: anywhere; }}
    .bar-meta {{ display: block; margin-top: 3px; color: #52606d; font-size: 0.78rem; font-weight: 500; }}
    .bar-stack {{ display: grid; gap: 5px; }}
    .bar-line {{ display: grid; grid-template-columns: 70px minmax(120px, 1fr) minmax(86px, auto); gap: 8px; align-items: center; font-size: 0.86rem; }}
    .bar-track {{ height: 12px; background: #e6eef5; border-radius: 3px; overflow: hidden; }}
    .bar-fill {{ height: 100%; min-width: 2px; }}
    .bar-fill.legacy {{ background: #486581; }}
    .bar-fill.rust {{ background: #2f855a; }}
    .detail-panel {{ border: 1px solid #d9e2ec; background: #fbfdff; padding: 12px; border-radius: 6px; }}
    .detail-panel h3 {{ margin: 0 0 10px; color: #102a43; font-size: 1rem; }}
    .detail-grid {{ display: grid; gap: 8px; }}
    .detail-item {{ border-top: 1px solid #e6eef5; padding-top: 8px; }}
    .detail-item span {{ display: block; color: #52606d; font-size: 0.78rem; font-weight: 700; text-transform: uppercase; }}
    .detail-item strong {{ display: block; color: #102a43; overflow-wrap: anywhere; }}
    .cell-note {{ display: block; margin-top: 4px; color: #52606d; font-size: 0.82rem; }}
    .status-badge {{ display: inline-block; border: 1px solid #9fb3c8; border-radius: 999px; padding: 2px 8px; font-size: 0.78rem; font-weight: 700; white-space: nowrap; }}
    .status-badge.claim-ready {{ border-color: #2f855a; color: #276749; background: #f0fff4; }}
    .status-badge.observed-only {{ border-color: #d9a441; color: #7c4a03; background: #fff8e6; }}
    .status-badge.not-comparable {{ border-color: #9fb3c8; color: #334e68; background: #f8fafc; }}
    .feature-summary-table td {{ vertical-align: middle; }}
    .feature-summary-table strong {{ color: #102a43; }}
    .mini-bar {{ width: 100%; max-width: 170px; height: 18px; display: block; margin-bottom: 4px; }}
    .mini-bar-bg {{ fill: #e6eef5; }}
    .mini-bar-fill.claim-ready {{ fill: #2f855a; }}
    .mini-bar-fill.observed-only {{ fill: #486581; }}
    .mini-bar-fill.not-comparable {{ fill: #d9a441; }}
    .mini-bar-fill.rust {{ fill: #2f855a; }}
    .evidence-pill {{ display: inline-block; border: 1px solid #d9e2ec; border-radius: 4px; padding: 2px 6px; margin: 1px 3px 1px 0; background: #f8fafc; color: #334e68; font-size: 0.78rem; }}
    .table-wrap {{ overflow-x: auto; }}
    .metric-note {{ color: #52606d; margin: 4px 0 10px; }}
    @media (max-width: 880px) {{
      .dashboard-grid {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 1fr; }}
      .bar-line {{ grid-template-columns: 56px minmax(110px, 1fr) minmax(74px, auto); }}
    }}
  </style>
</head>
<body>
  <h1>Carbon Rust Migration Evidence Report</h1>
  <h2>Executive Summary</h2>
  <p>All required evidence gates are present and passing. Scheduler semantic fixtures passed {scheduler_passed}/{scheduler_total}. Performance claims in this report are limited to workloads with passing parity evidence.</p>

  <h2>Current Evidence Gates</h2>
  <table>
    <thead><tr><th>Gate</th><th>Status</th><th>Evidence File</th></tr></thead>
    <tbody>{gate_rows}</tbody>
  </table>

  <h2>Test Coverage</h2>
  <table>
    <thead><tr><th>Feature Area</th><th>Legacy Tests</th><th>Rust Tests</th><th>Realistic Workloads</th><th>Status</th><th>Gaps</th></tr></thead>
    <tbody>
      <tr><td>Scheduler</td><td>legacy-scheduler.json</td><td>scheduler-fixtures.json and rust-scheduler-python.json</td><td>io-workloads.json</td><td>pass</td><td>See backlog for deferred non-claimed behavior.</td></tr>
      <tr><td>Resources</td><td>legacy-resources.json</td><td>rust-resources.json</td><td>bench-tier-local.json</td><td>pass</td><td>See backlog for deferred non-claimed behavior.</td></tr>
    </tbody>
  </table>

  <h2>Feature Parity</h2>
  <table>
    <thead><tr><th>Legacy Behavior</th><th>Rust Status</th><th>Compatibility Risk</th><th>Linked Tasks</th></tr></thead>
    <tbody>
      <tr><td>Scheduler semantic fixture slice</td><td>pass</td><td>Low for covered symbolic fixtures; Python/C API compatibility is separately gated.</td><td>reviews/tasks.md</td></tr>
      <tr><td>Resources parity slice</td><td>pass</td><td>Limited to workloads in evidence files.</td><td>reviews/tasks.md</td></tr>
    </tbody>
  </table>

  <h2>Scheduler Ownership Boundary</h2>
  {scheduler_ownership_boundary}

  <h2>Performance Parity And Gain</h2>
  <p>{performance_summary}</p>
  {feature_summary}
  <section class="panel">
    <div class="toolbar">
      <label>Feature
        <select id="feature-filter"></select>
      </label>
      <label>Comparability
        <select id="comparability-filter"></select>
      </label>
      <label>Metric
        <select id="metric-filter">
          <option value="speedup">Observed ratio</option>
          <option value="throughput">Throughput</option>
          <option value="bestObservedThroughput">Best observed throughput</option>
          <option value="wall">Wall time</option>
          <option value="p95">p95 latency</option>
          <option value="cpuBurn">CPU burn</option>
          <option value="cpuPercent">CPU percent</option>
          <option value="rss">Peak RSS</option>
          <option value="scaledWall">100k wall estimate</option>
        </select>
      </label>
    </div>
    <div id="performance-kpis" class="kpi-grid"></div>
    <div class="dashboard-grid">
      <div>
        <p id="metric-note" class="metric-note"></p>
        <div id="performance-chart" class="chart"></div>
      </div>
      <aside class="detail-panel">
        <h3>Selected Workload</h3>
        <div id="selected-detail" class="detail-grid"></div>
      </aside>
    </div>
  </section>
  <table id="performance-table">
    <thead><tr><th>Evidence</th><th>Feature</th><th>Workload</th><th>Baseline/Legacy Throughput</th><th>Rust/Target Throughput</th><th>Observed Ratio</th><th>Wall Time</th><th>Latency p50/p95</th><th>CPU Burn</th><th>CPU %</th><th>Peak RSS</th><th>100k Estimate</th><th>Comparable?</th><th>Claim / Reason</th><th>Command</th></tr></thead>
    <tbody>{performance_rows}</tbody>
  </table>

  <h2>Architecture Improvements</h2>
  <table>
    <thead><tr><th>Change</th><th>Reason</th><th>Effect</th><th>Evidence / Status</th></tr></thead>
    <tbody>
      <tr><td>Pure Rust scheduler core</td><td>Separate deterministic scheduler semantics from Python object lifetime.</td><td>Reliability improvement for covered state-machine semantics.</td><td>scheduler-fixtures.json; measured for current fixtures</td></tr>
      <tr><td>PyO3 scheduler bridge as compatibility boundary</td><td>Keep old `_scheduler`, `scheduler`, and `scheduler._C_API` imports working while logic moves behind Rust crates.</td><td>Compatibility-preserving migration path; not the final ownership model.</td><td>rust-scheduler-python.json; partial measured boundary evidence</td></tr>
      <tr><td>Scheduler core ownership drain</td><td>Move tasklet/channel/scheduler lifecycle decisions out of PyO3 types and behind Rust-owned IDs and handles.</td><td>Clearer final architecture with Python limited to compatibility, callables, exceptions, and GIL/refcount translation.</td><td>rust-scheduler-python.json core_ownership_status; open blocker, with live PyO3 tasklet/channel objects now carrying CoreScheduler handles for mirrored unbuffered channel state, core-owned live run-queue FIFO/count/remove/pop and scheduled-state authority, explicit core pause/resume for covered bind/remove/insert/switch pause paths, core-ID selected send/receive transfers, core-owned immediate peer handoff, core-selected queue-front introspection, and tasklet lifecycle snapshots</td></tr>
      <tr><td>Sampled local IO evidence lane</td><td>Compare realistic local socket/TLS request loops using latency, throughput, CPU burn, CPU percent, and RSS when legacy Carbon IO is unavailable.</td><td>More credible local resource-consumption data without claiming Carbon IO speedup.</td><td>io-workloads.json; measured baseline-vs-bridge only</td></tr>
      <tr><td>Evidence-gated speedup suppression</td><td>Prevent unsupported feature and speedup claims.</td><td>Reliability improvement for the final report and progress report.</td><td>xtask report-readiness/report; measured gate behavior</td></tr>
      <tr><td>Native release benchmark lane</td><td>Record Rust `release-native`, LTO, debug assertion state, and `target-cpu=native` context.</td><td>Required context for any future optimized Rust performance claim.</td><td>bench-tier-local.json and io-workloads.json; measured current native Rust context</td></tr>
      <tr><td>Rayon/Tokio/bitset/hash/SIMD lanes</td><td>Candidate architecture improvements for CPU pipeline stages, remote IO, membership sets, indexing, and codecs.</td><td>Expected only until byte parity and benchmark rows exist.</td><td>reviews/optimization-map.md; unmeasured opportunities</td></tr>
    </tbody>
  </table>

  <h2>Build And Optimization Readiness</h2>
  {optimization_tables}

  <h2>Raw Optimization Notes</h2>
  <pre>{optimization}</pre>

  <h2>Remaining Blockers</h2>
  <pre>{readiness}</pre>

  <h2>Next Task List</h2>
  <pre>{tasks}</pre>
  <script>
    const performanceComparisons = {performance_chart_data};
    const featureFilter = document.getElementById('feature-filter');
    const comparabilityFilter = document.getElementById('comparability-filter');
    const metricFilter = document.getElementById('metric-filter');
    const chart = document.getElementById('performance-chart');
    const metricNote = document.getElementById('metric-note');
    const kpiRoot = document.getElementById('performance-kpis');
    const detailRoot = document.getElementById('selected-detail');
    const performanceTable = document.getElementById('performance-table');
    let selectedRowKey = performanceComparisons.length ? rowKey(performanceComparisons[0]) : null;

    function numeric(value) {{
      return typeof value === 'number' && Number.isFinite(value) ? value : null;
    }}
    function getPath(row, path) {{
      let value = row;
      for (const key of path) {{
        if (value === null || value === undefined || value[key] === undefined) return null;
        value = value[key];
      }}
      return numeric(value);
    }}
    function getRawPath(row, path) {{
      let value = row;
      for (const key of path) {{
        if (value === null || value === undefined || value[key] === undefined) return null;
        value = value[key];
      }}
      return value;
    }}
    function rowKey(row) {{
      return row.row_key || [row.evidence_file || 'unknown', row.row_kind || 'comparison', row.workload || 'unknown'].join(':');
    }}
    function rowName(row) {{
      return (row.workload || 'unknown').replace(/_/g, ' ');
    }}
    function fmt(value, unit) {{
      if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
      const abs = Math.abs(value);
      const rendered = abs >= 1000 ? Math.round(value).toLocaleString() : (abs >= 10 ? value.toFixed(1) : value.toFixed(2));
      return unit ? rendered + ' ' + unit : rendered;
    }}
    function throughput(row, prefix) {{
      const marker = prefix + '_throughput_';
      for (const key of Object.keys(row)) if (key.indexOf(marker) === 0) return numeric(row[key]);
      if (prefix === 'rust') {{
        for (const key of Object.keys(row)) if (key.indexOf('throughput_') === 0) return numeric(row[key]);
      }}
      return null;
    }}
    function throughputUnit(row) {{
      for (const key of Object.keys(row)) {{
        if (key.indexOf('legacy_throughput_') === 0 || key.indexOf('rust_throughput_') === 0) return key.replace(/^legacy_throughput_/, '').replace(/^rust_throughput_/, '').replace(/_per_sec$/, '/sec').replace(/_/g, ' ');
        if (key.indexOf('throughput_') === 0) return key.replace(/^throughput_/, '').replace(/_per_sec$/, '/sec').replace(/_/g, ' ');
      }}
      return 'units/sec';
    }}
    function durationMs(row, prefix) {{
      const prefixed = numeric(row[prefix + '_duration_us']);
      if (prefixed !== null) return prefixed / 1000;
      return prefix === 'rust' ? numeric(row.duration_ms) : null;
    }}
    function isComparable(row) {{
      return row.comparability === 'comparable_process_to_process';
    }}
    function isNativeRelease(row) {{
      const profile = row.rust_build_profile || row.build_profile || row.evidence_build_profile;
      const nativeCpu = row.target_cpu_native === true || row.evidence_target_cpu_native === true;
      const debugAssertions = row.debug_assertions === true || row.evidence_debug_assertions === true;
      return profile === 'release-native' && nativeCpu && !debugAssertions;
    }}
    function legacyBuildProfile(row) {{
      return String(row.legacy_build_profile || row.evidence_legacy_build_profile || '').toLowerCase();
    }}
    function hasLegacyDebugBaseline(row) {{
      return legacyBuildProfile(row).indexOf('debug') !== -1;
    }}
    function hasKnownNonDebugLegacyBaseline(row) {{
      const profile = legacyBuildProfile(row);
      return profile.length > 0 && profile.indexOf('unknown') === -1 && profile.indexOf('debug') === -1;
    }}
    function isSpeedupClaimEligible(row) {{
      return isComparable(row) && isNativeRelease(row) && hasKnownNonDebugLegacyBaseline(row);
    }}
    function comparabilityLabel(value) {{
      if (value === 'comparable_process_to_process') return 'comparable: legacy vs Rust';
      if (value === 'rust_only_in_process_not_legacy_comparable') return 'not comparable: Rust-only observation';
      if (value === 'same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io') return 'not comparable: baseline vs bridge';
      if (value === 'rust_scheduler_process_not_legacy_comparable') return 'not comparable: scheduler process resource evidence';
      return value || 'unknown';
    }}
    const metricDefs = {{
      speedup: {{ note: 'Higher is better. This is an observed local ratio; it becomes a speedup claim only when Rust is release-native target-cpu=native and the legacy baseline is non-debug.', unit: function() {{ return 'x'; }}, legacy: function(row) {{ return numeric(row.speedup) === null ? null : 1; }}, rust: function(row) {{ return numeric(row.speedup); }} }},
      throughput: {{ note: 'Higher is better.', unit: throughputUnit, legacy: function(row) {{ return throughput(row, 'legacy'); }}, rust: function(row) {{ return throughput(row, 'rust'); }} }},
      wall: {{ note: 'Lower is better.', unit: function() {{ return 'ms'; }}, legacy: function(row) {{ return durationMs(row, 'legacy'); }}, rust: function(row) {{ return durationMs(row, 'rust'); }} }},
      p95: {{ note: 'Lower is better.', unit: function() {{ return 'ms'; }}, legacy: function(row) {{ const value = getPath(row, ['legacy_sample_stats_us', 'p95']); return value === null ? null : value / 1000; }}, rust: function(row) {{ const value = getPath(row, ['rust_sample_stats_us', 'p95']); return value === null ? null : value / 1000; }} }},
      cpuBurn: {{ note: 'Lower is better. Effective mean process CPU burn; selected-row details show whether samples used direct user/system time or wall-time times CPU percent.', unit: function() {{ return 'ms'; }}, legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'cpu_burn_effective_ms', 'mean']); }}, rust: function(row) {{ return getPath(row, ['rust_process_stats', 'cpu_burn_effective_ms', 'mean']); }} }},
      cpuPercent: {{ note: 'Observed process CPU utilization.', unit: function() {{ return '%'; }}, legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'cpu_percent', 'mean']); }}, rust: function(row) {{ return getPath(row, ['rust_process_stats', 'cpu_percent', 'mean']); }} }},
      rss: {{ note: 'Lower is better. p95 maximum resident set size.', unit: function() {{ return 'KB'; }}, legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'max_rss_kb', 'p95']); }}, rust: function(row) {{ return getPath(row, ['rust_process_stats', 'max_rss_kb', 'p95']); }} }},
      scaledWall: {{ note: 'Lower is better. Linear 100k-unit estimate from local samples.', unit: function() {{ return 's'; }}, legacy: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'legacy_wall_seconds']); }}, rust: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'rust_wall_seconds']); }} }}
    }};
    function addOption(select, value, label) {{
      const option = document.createElement('option');
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }}
    function initFilters() {{
      addOption(featureFilter, 'all', 'All features');
      Array.from(new Set(performanceComparisons.map(function(row) {{ return row.component || 'unknown'; }}))).sort().forEach(function(value) {{ addOption(featureFilter, value, value); }});
      addOption(comparabilityFilter, 'all', 'All comparability');
      Array.from(new Set(performanceComparisons.map(function(row) {{ return row.comparability || 'unknown'; }}))).sort().forEach(function(value) {{ addOption(comparabilityFilter, value, comparabilityLabel(value)); }});
    }}
    function visibleRows() {{
      const selectedFeature = featureFilter.value || 'all';
      const selectedComparability = comparabilityFilter.value || 'all';
      return performanceComparisons.filter(function(row) {{
        return (selectedFeature === 'all' || (row.component || 'unknown') === selectedFeature) &&
          (selectedComparability === 'all' || (row.comparability || 'unknown') === selectedComparability);
      }});
    }}
    function appendKpi(label, value, note) {{
      const card = document.createElement('div');
      card.className = 'kpi-card';
      const labelEl = document.createElement('span');
      labelEl.textContent = label;
      const valueEl = document.createElement('strong');
      valueEl.textContent = value;
      card.appendChild(labelEl);
      card.appendChild(valueEl);
      if (note) {{
        const small = document.createElement('small');
        small.textContent = note;
        card.appendChild(small);
      }}
      kpiRoot.appendChild(card);
    }}
    function renderKpis(rows) {{
      kpiRoot.innerHTML = '';
      const comparable = rows.filter(isComparable);
      const schedulerResourceRows = rows.filter(function(row) {{ return row.comparability === 'rust_scheduler_process_not_legacy_comparable'; }});
      const claimEligible = rows.filter(isSpeedupClaimEligible);
      const speeds = claimEligible.map(function(row) {{ return numeric(row.speedup); }}).filter(function(value) {{ return value !== null; }});
      const mean = speeds.length ? speeds.reduce(function(a, b) {{ return a + b; }}, 0) / speeds.length : null;
      appendKpi('Visible rows', String(rows.length), 'filtered performance rows');
      appendKpi('Comparable rows', String(comparable.length), 'legacy vs Rust');
      appendKpi('Claim-ready mean', fmt(mean, 'x'), 'requires non-debug legacy baseline');
      appendKpi('Non-comparable rows', String(rows.length - comparable.length), 'shown with reasons');
      appendKpi('Scheduler resource rows', String(schedulerResourceRows.length), 'CPU/RSS/latency only');
    }}
    function appendBar(stack, label, className, value, max, unit) {{
      const line = document.createElement('div');
      line.className = 'bar-line';
      const name = document.createElement('span');
      name.textContent = label;
      const track = document.createElement('div');
      track.className = 'bar-track';
      const fill = document.createElement('div');
      fill.className = 'bar-fill ' + className;
      fill.style.width = max > 0 && value !== null ? Math.max(2, (value / max) * 100) + '%' : '0';
      track.appendChild(fill);
      const valueEl = document.createElement('span');
      valueEl.textContent = fmt(value, unit);
      line.appendChild(name);
      line.appendChild(track);
      line.appendChild(valueEl);
      stack.appendChild(line);
    }}
    function addDetail(label, value) {{
      const item = document.createElement('div');
      item.className = 'detail-item';
      const labelEl = document.createElement('span');
      labelEl.textContent = label;
      const valueEl = document.createElement('strong');
      valueEl.textContent = value;
      item.appendChild(labelEl);
      item.appendChild(valueEl);
      detailRoot.appendChild(item);
    }}
    function cpuBurnQualityText(row, prefix) {{
      const statsKey = prefix + '_process_stats';
      const quality = getRawPath(row, [statsKey, 'cpu_burn_effective_quality']);
      const counts = getRawPath(row, [statsKey, 'cpu_burn_effective_source_counts']);
      if (!quality) return 'n/a';
      if (!counts) return String(quality);
      return String(quality) + ' (direct=' + (counts.user_plus_system_time || 0) + ', estimated=' + (counts.wall_time_times_cpu_percent || 0) + ', missing=' + (counts.unavailable || 0) + ')';
    }}
    function renderDetail(row) {{
      detailRoot.innerHTML = '';
      if (!row) {{
        addDetail('State', 'No rows for current filters');
        return;
      }}
      addDetail('Workload', rowName(row));
      addDetail('Evidence', (row.row_kind || 'comparison') + ': ' + (row.evidence_file || 'unknown'));
      let comparableText = comparabilityLabel(row.comparability);
      if (isComparable(row)) {{
        if (isSpeedupClaimEligible(row)) {{
          comparableText = 'yes: optimized legacy vs Rust release-native';
        }} else if (isNativeRelease(row) && hasLegacyDebugBaseline(row)) {{
          comparableText = 'yes: legacy debug process vs Rust release-native; observed ratio only';
        }} else {{
          comparableText = 'yes: observed ratio, not speedup-claim eligible';
        }}
      }}
      addDetail('Comparable', comparableText);
      addDetail('Claim', row.claim_scope || row.not_comparable_reason || row.claim || 'n/a');
      addDetail('Throughput', fmt(throughput(row, 'legacy'), throughputUnit(row)) + ' vs ' + fmt(throughput(row, 'rust'), throughputUnit(row)));
      addDetail('Wall time', fmt(durationMs(row, 'legacy'), 'ms') + ' vs ' + fmt(durationMs(row, 'rust'), 'ms'));
      addDetail('CPU burn', fmt(getPath(row, ['legacy_process_stats', 'cpu_burn_effective_ms', 'mean']), 'ms') + ' vs ' + fmt(getPath(row, ['rust_process_stats', 'cpu_burn_effective_ms', 'mean']), 'ms'));
      addDetail('CPU burn quality', cpuBurnQualityText(row, 'legacy') + ' vs ' + cpuBurnQualityText(row, 'rust'));
      addDetail('Peak RSS', fmt(getPath(row, ['legacy_process_stats', 'max_rss_kb', 'p95']), 'KB') + ' vs ' + fmt(getPath(row, ['rust_process_stats', 'max_rss_kb', 'p95']), 'KB'));
    }}
    function ensureSelection(rows) {{
      if (!rows.length) {{
        selectedRowKey = null;
        return null;
      }}
      const selected = rows.find(function(row) {{ return rowKey(row) === selectedRowKey; }});
      if (selected) return selected;
      selectedRowKey = rowKey(rows[0]);
      return rows[0];
    }}
    function renderChart(rows) {{
      const metric = metricDefs[metricFilter.value] || metricDefs.speedup;
      metricNote.textContent = metric.note;
      chart.innerHTML = '';
      const values = [];
      rows.forEach(function(row) {{
        const legacyValue = metric.legacy(row);
        const rustValue = metric.rust(row);
        if (legacyValue !== null) values.push(legacyValue);
        if (rustValue !== null) values.push(rustValue);
      }});
      const max = Math.max(0, ...values);
      rows.forEach(function(row) {{
        const unit = metric.unit(row);
        const outer = document.createElement('div');
        outer.className = 'bar-row' + (rowKey(row) === selectedRowKey ? ' is-selected' : '');
        outer.addEventListener('click', function() {{ selectedRowKey = rowKey(row); renderDashboard(); }});
        const label = document.createElement('div');
        label.className = 'bar-label';
        label.textContent = rowName(row);
        const meta = document.createElement('span');
        meta.className = 'bar-meta';
        meta.textContent = (row.component || 'unknown') + ' | ' + comparabilityLabel(row.comparability);
        label.appendChild(meta);
        const stack = document.createElement('div');
        stack.className = 'bar-stack';
        appendBar(stack, isComparable(row) ? 'legacy' : 'baseline', 'legacy', metric.legacy(row), max, unit);
        appendBar(stack, isComparable(row) ? 'rust' : 'target', 'rust', metric.rust(row), max, unit);
        outer.appendChild(label);
        outer.appendChild(stack);
        chart.appendChild(outer);
      }});
    }}
    function syncTable(rows) {{
      const visible = new Set(rows.map(rowKey));
      performanceTable.querySelectorAll('tbody tr').forEach(function(tableRow) {{
        tableRow.classList.toggle('is-hidden', !visible.has(tableRow.dataset.rowKey));
      }});
    }}
    function renderDashboard() {{
      const rows = visibleRows();
      const selected = ensureSelection(rows);
      renderKpis(rows);
      renderChart(rows);
      renderDetail(selected);
      syncTable(rows);
    }}
    initFilters();
    featureFilter.addEventListener('change', renderDashboard);
    comparabilityFilter.addEventListener('change', renderDashboard);
    metricFilter.addEventListener('change', renderDashboard);
    renderDashboard();
  </script>
</body>
</html>
"#,
        scheduler_passed = scheduler_passed,
        scheduler_total = scheduler_total,
        gate_rows = gate_rows,
        performance_summary = escape_html(&performance_summary),
        feature_summary = feature_summary,
        scheduler_ownership_boundary = scheduler_ownership_boundary,
        performance_rows = performance_rows,
        performance_chart_data = performance_chart_data,
        optimization_tables = optimization_tables,
        readiness = escape_html(&readiness),
        optimization = escape_html(&optimization),
        tasks = escape_html(&tasks)
    ))
}

fn render_progress_report(evidence: &[(&str, Option<Value>)]) -> Result<String> {
    let tasks = fs::read_to_string("reviews/tasks.md").unwrap_or_default();
    let readiness = fs::read_to_string("reviews/report-readiness.md").unwrap_or_default();
    let optimization = fs::read_to_string("reviews/optimization-map.md").unwrap_or_default();

    let mut gate_rows = String::new();
    for (file, value) in evidence {
        match value {
            Some(value) => {
                let gate = value.get("gate").and_then(Value::as_str).unwrap_or(file);
                let status = value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let coverage = value
                    .get("coverage")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let report_ready = value
                    .get("report_ready")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                gate_rows.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    escape_html(gate),
                    escape_html(status),
                    escape_html(if report_ready { "yes" } else { "no" }),
                    escape_html(coverage),
                    escape_html(file)
                ));
            }
            None => {
                gate_rows.push_str(&format!(
                    "<tr><td>{}</td><td>missing</td><td>no</td><td>not run</td><td>{}</td></tr>",
                    escape_html(file),
                    escape_html(file)
                ));
            }
        }
    }
    let gate_blocker_rows = render_progress_gate_blocker_rows(evidence);

    let scheduler_summary = evidence_value(evidence, "scheduler-fixtures.json")
        .map(|value| {
            let passed = value
                .get("passed")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let total = value
                .get("fixture_count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let invariant_checked = value
                .get("reports")
                .and_then(Value::as_array)
                .map(|reports| {
                    !reports.is_empty()
                        && reports.iter().all(|report| {
                            report.get("invariants_checked").and_then(Value::as_bool) == Some(true)
                        })
                })
                .unwrap_or(false);
            if invariant_checked {
                format!("{passed}/{total} current semantic fixtures pass with invariant checks")
            } else {
                format!("{passed}/{total} current semantic fixtures pass")
            }
        })
        .unwrap_or_else(|| String::from("missing"));
    let legacy_resources_summary = evidence_value(evidence, "legacy-resources.json")
        .map(|value| {
            let passed = value
                .get("tests_passed")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let failed = value
                .get("tests_failed")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            format!("{passed} passed, {failed} failed")
        })
        .unwrap_or_else(|| String::from("missing"));
    let rust_resources_summary = evidence_value(evidence, "rust-resources.json")
        .and_then(|value| value.get("covered_behaviors"))
        .and_then(Value::as_array)
        .map(|behaviors| format!("{} covered behaviors", behaviors.len()))
        .unwrap_or_else(|| String::from("missing"));
    let io_summary = evidence_value(evidence, "io-workloads.json")
        .map(|value| {
            let workloads = value
                .get("workloads")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            let comparisons = value
                .get("comparisons")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            let semantic_smoke = value
                .get("scheduler_capi_semantic_smoke")
                .and_then(|smoke| smoke.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("missing");
            let semantic_fixtures = value
                .get("semantic_trace_fixtures")
                .and_then(|fixtures| fixtures.get("fixture_count"))
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let carbonio_trace_status = value
                .get("legacy_carbonio_semantic_traces")
                .and_then(|trace| trace.get("legacy_carbonio_trace_status"))
                .and_then(Value::as_str)
                .unwrap_or("missing");
            format!(
                "{workloads} workload runs, {comparisons} loopback comparisons, {semantic_fixtures} IO semantic fixtures, Scheduler.h IO channel smoke {semantic_smoke}, Carbon IO trace {carbonio_trace_status}"
            )
        })
        .unwrap_or_else(|| String::from("missing"));
    let scheduler_python_subset_count = evidence_value(evidence, "rust-scheduler-python.json")
        .and_then(|value| {
            value
                .get("unchanged_legacy_subset_count")
                .and_then(Value::as_u64)
                .or_else(|| {
                    value
                        .get("unchanged_legacy_subset")
                        .and_then(Value::as_array)
                        .map(|subset| subset.len() as u64)
                })
        })
        .map(|count| count.to_string())
        .unwrap_or_else(|| String::from("unknown"));

    let performance_entries = progress_performance_entries(evidence);
    let performance_summary = performance_comparability_summary(&performance_entries);
    let performance_rows = render_performance_rows(&performance_entries);
    let feature_summary = render_feature_performance_summary(&performance_entries);
    let performance_chart_data = render_performance_chart_data(&performance_entries);
    let run_context = render_progress_run_context(evidence);
    let scheduler_ownership_boundary = render_scheduler_ownership_boundary(evidence);
    let optimization_tables = render_optimization_tables(evidence);
    let legacy_scheduler_diagnosis = evidence_value(evidence, "legacy-scheduler.json")
        .and_then(|value| value.get("local_probe_diagnosis"))
        .and_then(Value::as_str)
        .unwrap_or("legacy scheduler baseline is not report-ready")
        .to_string();
    let scheduler_python_blocker = format!(
        "Rust scheduler Python/C API bridge is partial: initial PyO3 cdylib build-flavor import/package/QueueChannel C API constructor/property/counter/simple setup-run smoke, active Greenlet/channel/callback/thread-cleanup/error smoke paths, the {scheduler_python_subset_count}-test unchanged legacy scheduler Python suite including all 10 QueueChannel legacy tests, an expanded C++ Scheduler.h tasklet lifecycle/run-control/channel-preference/invalid-argument/inside-tasklet-send smoke, real legacy capiTest/Tasklet.cpp, Channel.cpp, and Scheduler.cpp per-test child-process source-slice runs, an installed release-wheel smoke, and an IO-facing Scheduler.h channel wake/send_throw semantic smoke pass. The new in-process source-slice probe records the current repeated-interpreter failure after the first test in each slice: the next embedded scheduler import sees _scheduler.channel as None. Full in-process legacy SchedulerCapiTest/capiTest binary compatibility, broader wheel flavor/dependency/install matrices, broader failure-mode hardening, and legacy carbonio semantic traces remain open."
    );
    let blockers = vec![
        format!("Legacy scheduler Python/C API baseline is not report-ready: {legacy_scheduler_diagnosis}"),
        scheduler_python_blocker,
        String::from("Realistic IO evidence is sampled: loopback TCP/TLS workload stats now aggregate five process samples per implementation by default plus a validated fixture-only normalized semantic trace corpus for socket recv/send wake, SSL read/write wake, and SSL send_throw error wake, and a compiled Scheduler.h channel balance/send_throw semantic smoke; the legacy carbonio/_socket/_ssl semantic trace gate is structured but blocked on a supported Windows/macOS legacy carbonio+legacy scheduler run or supplied prebuilt legacy artifacts."),
        String::from("Rust resources evidence is partial: catalog import/export now includes Linux/macOS/Windows create-group YAML, skip-compression YAML, CSV, and prefixed CSV golden roundtrips with platform-specific BinaryOperation values, plus the large Indicies normal and binary-operation v0 CSV to v0.1 YAML corpus and YAML round-trips; ResourceTools coverage now includes legacy FileDataStreamIn chunked reads, FileDataStreamOut byte output, CompressedFileDataStreamOut gzip roundtrip, MD5 stream checksums, chunked gzip stream decompression checks, FindMatchingChunks string cases, FindMatchingChunk file offset cases, CountMatchingChunks patch fixture offsets, generated/persisted ChunkIndex lookup, checksum-filtered ChunkIndex lookup, and bundle stream splitting for many files into many uncompressed chunks and one compressed chunk with exact reconstruction; filter coverage now includes the legacy named cases plus generated wildcard/ellipsis path and include/exclude section-property matrices; local bundle create/unpack, a process-level local create-and-unpack bundle roundtrip with stable YAML and payload byte checks, remote-CDN compressed bundle create/unpack, create-bundle zero-chunk and missing-resource-source failure cleanup, 42-chunk local unpack boundary evidence, missing-chunk unpack failure cleanup, remote-requested-local chunk failure cleanup, local-requested-remote compressed chunk failure cleanup, local remote-CDN mirror/cache retrieval, CLI remote-CDN first-run download and second-run cache-hit stats, patch payload read/generation/local apply byte coverage including old-layout apply preserving legacy no-removal semantics, low-level BSDIFF corruption rejection, create-patch zero-chunk and missing previous/next resource-source failure cleanup, apply-patch missing previous/next resource input and missing/corrupt patch payload failure cleanup, malformed local apply manifest rejection for zero apply chunk size, target offset overflow, source range overflow, and overlapping copy ranges, copy-only patch records without generated binary payloads, and 70 process-level Rust resources CLI parity cases exist including top-level legacy help-shape output, operation-specific help/usage-shape output, dispatcher exit-code/status classes, local and remote-CDN create-bundle, local create-and-unpack bundle, local and remote-CDN unpack-bundle, bundle failure cleanup, normal, no-change, chunked, old-layout, copy-only, zero-chunk, missing-source create-patch, and missing-previous/missing-next/missing-payload/corrupt-payload apply-patch behavior, but broader bundle corpus, broader patch temp-file cleanup modes, broader filter-file corpus/fuzz coverage, network-backed remote/catalog behavior, cross-host create-group generation, broader detailed CLI output/error text compatibility, and broader apply/unpack corpus coverage remain open."),
        String::from("Benchmarks are mixed: create-group, create-group-from-filter, merge-group, diff-group, remove-resources, create-bundle, create-patch, unpack-bundle, and apply-patch have preliminary parity-checked process-level speed, latency, CPU, CPU-burn, peak-RSS, and 100k linear estimates; TCP/TLS loopback has baseline-vs-bridge stats, but legacy scheduler/IO semantic comparisons and broader comparable rows are still missing."),
    ];
    let blocker_items = blockers
        .iter()
        .map(|blocker| format!("<li>{}</li>", escape_html(blocker)))
        .collect::<String>();

    Ok(format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Carbon Rust Migration Progress Report</title>
  <style>
    :root {{
      --ink: #1f2933;
      --muted: #52606d;
      --line: #bcccdc;
      --panel: #ffffff;
      --soft: #f5f7fa;
      --legacy: #486581;
      --rust: #2f855a;
      --warn: #d9a441;
    }}
    body {{ font-family: system-ui, sans-serif; margin: 0; color: var(--ink); line-height: 1.45; background: #edf2f7; }}
    main {{ max-width: 1480px; margin: 0 auto; padding: 28px; }}
    h1, h2 {{ color: #102a43; letter-spacing: 0; }}
    h1 {{ margin: 0 0 8px; }}
    h2 {{ margin-top: 30px; }}
    .hero-band {{ background: #ffffff; border-bottom: 1px solid var(--line); }}
    .hero-inner {{ max-width: 1480px; margin: 0 auto; padding: 28px; }}
    .hero-meta {{ color: var(--muted); max-width: 960px; }}
    .notice {{ border: 1px solid var(--warn); background: #fff8e6; padding: 12px 14px; margin: 12px 0 0; }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 18px 0 26px; }}
    .summary-card {{ border: 1px solid var(--line); background: var(--panel); padding: 12px; border-radius: 6px; box-shadow: 0 1px 1px rgba(15, 23, 42, 0.04); }}
    .summary-card strong {{ display: block; color: #102a43; font-size: 0.92rem; margin-bottom: 4px; }}
    .summary-card span {{ display: block; overflow-wrap: anywhere; }}
    .context-grid {{ margin-top: 8px; }}
    .panel {{ border: 1px solid var(--line); background: var(--panel); padding: 16px; border-radius: 6px; margin: 12px 0 18px; box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05); }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: end; margin-bottom: 14px; }}
    .toolbar label {{ display: grid; gap: 4px; font-size: 0.85rem; font-weight: 700; color: #334e68; }}
    select {{ border: 1px solid #9fb3c8; border-radius: 4px; padding: 6px 8px; background: #ffffff; color: #102a43; }}
    .dashboard-grid {{ display: grid; grid-template-columns: minmax(360px, 2.1fr) minmax(280px, 1fr); gap: 16px; align-items: start; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 14px; }}
    .kpi-card {{ border: 1px solid #d9e2ec; background: var(--soft); padding: 10px; border-radius: 6px; }}
    .kpi-card span {{ display: block; color: var(--muted); font-size: 0.78rem; font-weight: 700; text-transform: uppercase; }}
    .kpi-card strong {{ display: block; margin-top: 3px; color: #102a43; font-size: 1.18rem; }}
    .chart {{ display: grid; gap: 10px; }}
    .chart-shell {{ min-width: 0; }}
    .bar-row {{ display: grid; grid-template-columns: minmax(180px, 1.2fr) minmax(240px, 3fr); gap: 12px; align-items: center; border: 1px solid transparent; border-radius: 6px; padding: 7px; }}
    .bar-row:hover {{ background: #f8fafc; border-color: #d9e2ec; cursor: pointer; }}
    .bar-row.is-selected {{ background: #edf7ed; border-color: #9ae6b4; }}
    .bar-label {{ font-weight: 700; color: #102a43; overflow-wrap: anywhere; }}
    .bar-meta {{ display: block; margin-top: 3px; color: var(--muted); font-size: 0.78rem; font-weight: 500; }}
    .bar-stack {{ display: grid; gap: 5px; }}
    .bar-line {{ display: grid; grid-template-columns: 70px minmax(120px, 1fr) minmax(86px, auto); gap: 8px; align-items: center; font-size: 0.86rem; }}
    .bar-track {{ height: 12px; background: #e6eef5; border-radius: 3px; overflow: hidden; }}
    .bar-fill {{ height: 100%; min-width: 2px; }}
    .bar-fill.legacy {{ background: var(--legacy); }}
    .bar-fill.rust {{ background: var(--rust); }}
    .detail-panel {{ border: 1px solid #d9e2ec; background: #fbfdff; padding: 12px; border-radius: 6px; }}
    .detail-panel h3 {{ margin: 0 0 10px; color: #102a43; font-size: 1rem; }}
    .detail-grid {{ display: grid; gap: 8px; }}
    .detail-item {{ border-top: 1px solid #e6eef5; padding-top: 8px; }}
    .detail-item span {{ display: block; color: var(--muted); font-size: 0.78rem; font-weight: 700; text-transform: uppercase; }}
    .detail-item strong {{ display: block; color: #102a43; overflow-wrap: anywhere; }}
    .cell-note {{ display: block; margin-top: 4px; color: var(--muted); font-size: 0.82rem; }}
    .status-badge {{ display: inline-block; border: 1px solid #9fb3c8; border-radius: 999px; padding: 2px 8px; font-size: 0.78rem; font-weight: 700; white-space: nowrap; }}
    .status-badge.claim-ready {{ border-color: #2f855a; color: #276749; background: #f0fff4; }}
    .status-badge.observed-only {{ border-color: var(--warn); color: #7c4a03; background: #fff8e6; }}
    .status-badge.not-comparable {{ border-color: #9fb3c8; color: #334e68; background: #f8fafc; }}
    .feature-summary-table td {{ vertical-align: middle; }}
    .feature-summary-table strong {{ color: #102a43; }}
    .mini-bar {{ width: 100%; max-width: 170px; height: 18px; display: block; margin-bottom: 4px; }}
    .mini-bar-bg {{ fill: #e6eef5; }}
    .mini-bar-fill.claim-ready {{ fill: var(--rust); }}
    .mini-bar-fill.observed-only {{ fill: var(--legacy); }}
    .mini-bar-fill.not-comparable {{ fill: var(--warn); }}
    .mini-bar-fill.rust {{ fill: var(--rust); }}
    .evidence-pill {{ display: inline-block; border: 1px solid #d9e2ec; border-radius: 4px; padding: 2px 6px; margin: 1px 3px 1px 0; background: #f8fafc; color: #334e68; font-size: 0.78rem; }}
    .chip {{ display: inline-block; border: 1px solid #9fb3c8; border-radius: 999px; padding: 2px 8px; color: #334e68; background: #f8fafc; font-size: 0.78rem; font-weight: 700; }}
    .blocker-code {{ display: inline-block; border: 1px solid #d9a441; border-radius: 4px; padding: 2px 6px; margin: 1px 3px 1px 0; color: #7c4a03; background: #fff8e6; font-size: 0.78rem; font-weight: 700; }}
    .compact-list {{ margin: 0; padding-left: 18px; }}
    .compact-list li + li {{ margin-top: 4px; }}
    .table-wrap {{ overflow-x: auto; }}
    .metric-note {{ color: var(--muted); margin: 4px 0 10px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
    th, td {{ border: 1px solid var(--line); padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    tbody tr.not-comparable {{ background: #fff8e6; }}
    tbody tr.is-hidden {{ display: none; }}
    code, pre {{ background: #f0f4f8; padding: 2px 4px; }}
    pre {{ overflow: auto; padding: 12px; }}
    @media (max-width: 880px) {{
      main, .hero-inner {{ padding: 18px; }}
      .dashboard-grid {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 1fr; }}
      .bar-line {{ grid-template-columns: 56px minmax(110px, 1fr) minmax(74px, auto); }}
    }}
  </style>
</head>
<body>
  <header class="hero-band">
    <div class="hero-inner">
      <h1>Carbon Rust Migration Progress Report</h1>
      <p class="hero-meta">Interactive local evidence dashboard for scheduler, IO, and resources parity work. Use the controls to inspect throughput, latency, CPU burn, RSS, and 100k linear scale estimates by feature.</p>
      <div class="notice">This is a progress report, not the final parity/performance report. Final HTML remains blocked until every required evidence gate is present, passing, and report-ready.</div>
    </div>
  </header>
  <main>

  <h2>Executive Summary</h2>
  <p>Scheduler: {scheduler_summary}. Legacy resources: {legacy_resources_summary}. Rust resources: {rust_resources_summary}. IO workloads: {io_summary}. Current benchmark rows include preliminary parity-checked process-level resource comparisons plus initial TCP/TLS loopback workload stats, fixture-backed IO semantic trace shape evidence, and IO-facing C API channel semantic evidence; broader speedup claims remain blocked.</p>
  <div class="summary-grid">
    <div class="summary-card"><strong>Scheduler Gate</strong>{scheduler_summary}</div>
    <div class="summary-card"><strong>Legacy Resources</strong>{legacy_resources_summary}</div>
    <div class="summary-card"><strong>Rust Resources</strong>{rust_resources_summary}</div>
    <div class="summary-card"><strong>IO Workloads</strong>{io_summary}</div>
    <div class="summary-card"><strong>Comparable Stats</strong>{performance_summary}</div>
  </div>
  <section class="panel">
    <h2>Run Context</h2>
    <div class="summary-grid context-grid">{run_context}</div>
  </section>

  <h2>Scheduler Ownership Boundary</h2>
  {scheduler_ownership_boundary}

  <h2>Current Evidence Gates</h2>
  <table>
    <thead><tr><th>Gate</th><th>Status</th><th>Report Ready</th><th>Coverage</th><th>Evidence File</th></tr></thead>
    <tbody>{gate_rows}</tbody>
  </table>

  <h2>Report Blocker Details</h2>
  <table>
    <thead><tr><th>Gate</th><th>Blocker Class</th><th>Readiness Blocker Codes</th><th>Remaining Work Before Final Report</th></tr></thead>
    <tbody>{gate_blocker_rows}</tbody>
  </table>

  <h2>Test Coverage</h2>
  <table>
    <thead><tr><th>Feature Area</th><th>Legacy Tests</th><th>Rust Tests</th><th>Realistic Workload Tests</th><th>Status</th><th>Gaps</th></tr></thead>
    <tbody>
      <tr><td>Scheduler semantic core</td><td>Mapped from legacy scheduler Python tests and C++ implementation notes</td><td>{scheduler_summary}</td><td>Not yet promoted</td><td>partial</td><td>More lifecycle, channel, switch-trap, callback, nested-timeout, and teardown fixtures needed.</td></tr>
      <tr><td>Scheduler Python/C API bridge</td><td>{scheduler_python_subset_count} unchanged legacy Python unittest cases plus expanded Scheduler.h C++ smoke, real legacy capiTest/Tasklet.cpp, Channel.cpp, and Scheduler.cpp child-process source slices, an explicit failing in-process source-slice probe, and installed release-wheel smoke pass against Rust extension</td><td>PyO3 smoke, C API capsule smoke, direct run/order smoke, active Greenlet continuation, channel exception/send_throw, schedule/channel callback, thread cleanup, tasklet lifecycle, switch/raise/kill, invalid C API argument rejection, channel preference get/set/clamping, inside-tasklet C API send blocking, real tasklet/channel/scheduler C API source tests, installed wheel import/package smoke, and all 10 QueueChannel legacy tests through the wrapper path</td><td>{io_summary}</td><td>partial</td><td>In-process source-slice probe currently fails after first test because repeated embedded imports see _scheduler.channel as None; full legacy SchedulerCapiTest/capiTest binary coverage, broader wheel flavor/dependency/install matrices, broader failure-mode hardening, and legacy carbonio semantic trace comparison remain open.</td></tr>
      <tr><td>Resources</td><td>{legacy_resources_summary}</td><td>{rust_resources_summary}</td><td>Tier 1 local process benchmarks for selected catalog, bundle, patch create/apply, and unpack ops</td><td>partial</td><td>Broader bundle/patch failure modes, broader filter-file corpus/fuzz coverage, network-backed remote/catalog behavior, broader detailed CLI output/error text compatibility, and broader apply/unpack corpus coverage.</td></tr>
      <tr><td>Benchmarks</td><td>Legacy process samples for selected resource catalog/bundle/patch create/apply/unpack ops; Python stdlib baseline for IO loopback</td><td>Rust samples for scheduler/resource micro, selected resource catalog/bundle/patch create/apply/unpack ops, and scheduler-bridge TCP/TLS loopback</td><td>Local dev box only</td><td>partial</td><td>Legacy Carbon IO extension comparison, broader comparable rows, allocation counters, and production-size validation.</td></tr>
    </tbody>
  </table>

  <h2>Feature Parity</h2>
  <table>
    <thead><tr><th>Area</th><th>Current Evidence</th><th>Status</th><th>Remaining Work</th></tr></thead>
    <tbody>
      <tr><td>Scheduler core</td><td>{scheduler_summary}</td><td>partial</td><td>Expand fixtures, unblock legacy scheduler, complete Python/C API bridge, and promote IO semantic trace parity.</td></tr>
      <tr><td>Resources legacy</td><td>{legacy_resources_summary}</td><td>green legacy baseline</td><td>Use as comparison gate for Rust parity.</td></tr>
      <tr><td>Resources Rust</td><td>{rust_resources_summary}</td><td>partial</td><td>Broader bundle corpus, broader patch temp-file cleanup modes, broader filter-file corpus/fuzz coverage, network-backed remote/catalog behavior, broader detailed CLI output/error text compatibility, and broader apply/unpack corpus coverage.</td></tr>
    </tbody>
  </table>

  <h2>Performance Data</h2>
  {feature_summary}
  <section class="panel">
    <div class="toolbar">
      <label>Feature
        <select id="feature-filter"></select>
      </label>
      <label>Family
        <select id="family-filter"></select>
      </label>
      <label>Comparability
        <select id="comparability-filter"></select>
      </label>
      <label>Row Kind
        <select id="row-kind-filter"></select>
      </label>
      <label>Build
        <select id="build-filter"></select>
      </label>
      <label>Metric
        <select id="metric-filter">
          <option value="speedup">Observed ratio</option>
          <option value="throughput">Throughput</option>
          <option value="wall">Wall time</option>
          <option value="wallRatio">Wall ratio</option>
          <option value="p50">p50 latency</option>
          <option value="p95">p95 latency</option>
          <option value="p99">p99 latency</option>
          <option value="latencySpread">p95-p50 spread</option>
          <option value="cpuBurn">CPU burn</option>
          <option value="cpuRatio">CPU burn ratio</option>
          <option value="cpuPercent">CPU percent</option>
          <option value="rss">Peak RSS</option>
          <option value="rssRatio">RSS ratio</option>
          <option value="sampleCount">Sample count</option>
          <option value="scaledWall">100k wall estimate</option>
          <option value="scaledCpu">100k CPU estimate</option>
        </select>
      </label>
      <label>Sort
        <select id="sort-filter">
          <option value="metric-desc">Metric high first</option>
          <option value="metric-asc">Metric low first</option>
          <option value="workload">Workload</option>
          <option value="feature">Feature</option>
          <option value="comparability">Comparability</option>
        </select>
      </label>
    </div>
    <div id="performance-kpis" class="kpi-grid"></div>
    <div class="dashboard-grid">
      <div class="chart-shell">
        <p id="metric-note" class="metric-note"></p>
        <div id="performance-chart" class="chart"></div>
      </div>
      <aside class="detail-panel">
        <h3>Selected Workload</h3>
        <div id="selected-detail" class="detail-grid"></div>
      </aside>
    </div>
  </section>
  <div class="table-wrap">
    <table id="performance-table">
      <thead><tr><th>Evidence</th><th>Feature</th><th>Workload</th><th>Baseline/Legacy Throughput</th><th>Rust/Target Throughput</th><th>Observed Ratio</th><th>Wall Time</th><th>Latency p50/p95</th><th>CPU Burn</th><th>CPU %</th><th>Peak RSS</th><th>100k Estimate</th><th>Comparable?</th><th>Claim / Reason</th><th>Command</th></tr></thead>
      <tbody>{performance_rows}</tbody>
    </table>
  </div>

  <h2>Architecture Improvements</h2>
  <table>
    <thead><tr><th>Change</th><th>Reason</th><th>Evidence</th><th>Status</th></tr></thead>
    <tbody>
      <tr><td>Pure Rust scheduler core</td><td>Separates deterministic scheduler semantics from Python and Greenlet object lifetime.</td><td>scheduler-fixtures.json</td><td>measured for current fixtures</td></tr>
      <tr><td>PyO3 scheduler bridge as compatibility boundary</td><td>Runs unchanged legacy Python and C API tests while the owned scheduler state keeps moving into Rust crates.</td><td>rust-scheduler-python.json and docs/functionality-matrix.md</td><td>partial boundary evidence; not the final core ownership model</td></tr>
      <tr><td>Scheduler CoreScheduler handle API</td><td>Introduces Rust-owned CoreTaskletId/CoreChannelId/CoreRunQueueId state for unbuffered channel rendezvous, scheduler run-queue FIFO/count/remove/pop behavior, scheduled-state authority, explicit pause/resume lifecycle transitions, balance signs, blocked sender/receiver queues, preference clamping, close/open/clear, block-trap no-mutation checks, operation-result selected send/receive transfers, core-owned immediate peer handoff, core-selected queue-front introspection, and tasklet runtime snapshots.</td><td>carbon-scheduler-core tests and rust-scheduler-python.json core_ownership_status</td><td>measured core target now wired into the PyO3 bridge for live handle allocation, core-owned bridge run queues and scheduled-state authority, explicit core pause/resume for covered bind/remove/insert/switch pause paths, mirrored unbuffered channel balance/queue transitions, core-ID selected matched sender/receiver transfer plus immediate peer handoff, core-selected channel.queue results in covered paths, and mirrored tasklet alive/paused/switch-count snapshots; Python payload storage is still bridge-owned</td></tr>
      <tr><td>Scheduler core ownership drain</td><td>Moves tasklet/channel/scheduler lifecycle decisions out of PyO3 types and behind Rust-owned IDs and handles.</td><td>rust-scheduler-python.json core_ownership_status and reviews/tasks.md</td><td>open blocker; Python bridge is compatibility, not destination architecture</td></tr>
      <tr><td>Evidence-gated report generation</td><td>Prevents unsupported feature and speedup claims.</td><td>xtask report blocks on non-report-ready evidence</td><td>measured</td></tr>
      <tr><td>Rust resources model/tools/catalog-corpus/filter/create-group/create-from-filter/merge/diff/remove/malformed-imports/local-and-remote-CDN-bundle-byte/remote-cache-stats/create-bundle-failure-cleanup/missing-chunk-and-source-type-failure-cleanup/patch-manifest-payload-generation/local-apply/create-patch-failure-cleanup/CLI-artifact slice</td><td>Moves checksum, compression with legacy gzip header normalization, rolling checksum, ResourceTools stream checks for FileDataStreamIn chunked reads, FileDataStreamOut byte output, CompressedFileDataStreamOut gzip roundtrip, MD5 stream checksums, chunked gzip stream decompression, FindMatchingChunks string cases, FindMatchingChunk file offset cases, CountMatchingChunks patch fixture offsets, generated/persisted ChunkIndex lookup, and checksum-filtered ChunkIndex lookup, catalog parsing/export including the large Indicies normal and binary-operation v0 CSV to v0.1 YAML corpus plus YAML round-trips, malformed import result mapping, legacy filter matching with generated wildcard/ellipsis path and include/exclude section-property matrix coverage, CreateFromFilter fixture output, directory ResourceGroup creation, catalog merge, catalog diff, resource removal, BundleGroup local create/unpack byte checks, remote-CDN compressed BundleGroup create/unpack byte checks, process-level local create-and-unpack bundle roundtrip checks, create-bundle zero-chunk and missing-resource-source failure cleanup, 42-chunk local unpack boundary evidence, missing-chunk unpack failure cleanup, remote-requested-local chunk failure cleanup, local-requested-remote compressed chunk failure cleanup, local remote-CDN mirror/cache hit and bad-cache replacement checks, process-level remote-CDN first-run download and second-run cache-hit stats, BundleGroup/PatchGroup manifest parsing/export, PatchGroup local CDN payload read/generation/local apply byte checks including old-layout apply preserving legacy no-removal semantics, low-level BSDIFF corruption rejection, create-patch zero-chunk and missing previous/next resource-source failure cleanup, apply-patch missing previous/next resource input and missing/corrupt payload failure cleanup, malformed local apply manifest rejection for zero apply chunk size, target offset overflow, source range overflow, and overlapping copy ranges, copy-only patch records without generated binary payloads, and 70 process-level Rust resources CLI parity cases including top-level legacy help-shape output, operation-specific help/usage-shape output, dispatcher exit-code/status classes, local and remote-CDN create-bundle, local create-and-unpack bundle, create-bundle failure cleanup, local and remote-CDN unpack-bundle, missing-chunk and source-type mismatch unpack failure cleanup, normal, no-change, chunked, old-layout, copy-only, zero-chunk, missing-source create-patch, and missing-previous/missing-next/missing-payload/corrupt-payload apply-patch behavior into measured Rust evidence.</td><td>rust-resources.json</td><td>measured for current slice</td></tr>
      <tr><td>Report-ready flag on evidence</td><td>Separates passing partial work from final report-eligible parity.</td><td>evidence JSON schema</td><td>measured</td></tr>
    </tbody>
  </table>

  <h2>Build And Optimization Readiness</h2>
  {optimization_tables}

  <h2>Raw Optimization Notes</h2>
  <pre>{optimization}</pre>

  <h2>Remaining Blockers</h2>
  <ul>{blocker_items}</ul>

  <h2>Readiness Notes</h2>
  <pre>{readiness}</pre>

  <h2>Backlog</h2>
  <pre>{tasks}</pre>
  </main>
  <script>
    const performanceComparisons = {performance_chart_data};
    const featureFilter = document.getElementById('feature-filter');
    const familyFilter = document.getElementById('family-filter');
    const comparabilityFilter = document.getElementById('comparability-filter');
    const rowKindFilter = document.getElementById('row-kind-filter');
    const buildFilter = document.getElementById('build-filter');
    const metricFilter = document.getElementById('metric-filter');
    const sortFilter = document.getElementById('sort-filter');
    const chart = document.getElementById('performance-chart');
    const metricNote = document.getElementById('metric-note');
    const kpiRoot = document.getElementById('performance-kpis');
    const detailRoot = document.getElementById('selected-detail');
    const performanceTable = document.getElementById('performance-table');
    let selectedRowKey = performanceComparisons.length ? rowKey(performanceComparisons[0]) : null;

    function numeric(value) {{
      return typeof value === 'number' && Number.isFinite(value) ? value : null;
    }}

    function getPath(row, path) {{
      let value = row;
      for (const key of path) {{
        if (value === null || value === undefined || value[key] === undefined) {{
          return null;
        }}
        value = value[key];
      }}
      return numeric(value);
    }}
    function getRawPath(row, path) {{
      let value = row;
      for (const key of path) {{
        if (value === null || value === undefined || value[key] === undefined) {{
          return null;
        }}
        value = value[key];
      }}
      return value;
    }}

    function throughput(row, prefix) {{
      const marker = prefix + '_throughput_';
      for (const key of Object.keys(row)) {{
        if (key.indexOf(marker) === 0) {{
          return numeric(row[key]);
        }}
      }}
      if (prefix === 'rust') {{
        for (const key of Object.keys(row)) {{
          if (key.indexOf('throughput_') === 0) {{
            return numeric(row[key]);
          }}
        }}
      }}
      return null;
    }}

    function bestObservedThroughput(row, prefix) {{
      const minUs = getPath(row, [prefix + '_sample_stats_us', 'min']);
      return minUs === null || minUs <= 0 ? null : 1000000 / minUs;
    }}

    function throughputUnit(row) {{
      for (const key of Object.keys(row)) {{
        if (key.indexOf('legacy_throughput_') === 0 || key.indexOf('rust_throughput_') === 0) {{
          return key.replace(/^legacy_throughput_/, '').replace(/^rust_throughput_/, '').replace(/_per_sec$/, '/sec').replace(/_/g, ' ');
        }}
        if (key.indexOf('throughput_') === 0) {{
          return key.replace(/^throughput_/, '').replace(/_per_sec$/, '/sec').replace(/_/g, ' ');
        }}
      }}
      return 'units/sec';
    }}

    function durationMs(row, prefix) {{
      const prefixed = numeric(row[prefix + '_duration_us']);
      if (prefixed !== null) {{
        return prefixed / 1000;
      }}
      if (prefix === 'rust') {{
        return numeric(row.duration_ms);
      }}
      return null;
    }}

    function isLegacyComparable(row) {{
      return row.comparability === 'comparable_process_to_process';
    }}

    function isNativeRelease(row) {{
      const profile = row.rust_build_profile || row.build_profile || row.evidence_build_profile;
      const nativeCpu = row.target_cpu_native === true || row.evidence_target_cpu_native === true;
      const debugAssertions = row.debug_assertions === true || row.evidence_debug_assertions === true;
      return profile === 'release-native' && nativeCpu && !debugAssertions;
    }}

    function evidenceLabel(row) {{
      return (row.row_kind || 'comparison') + ': ' + (row.evidence_file || 'unknown');
    }}

    function commandText(row) {{
      if (row.legacy_command_template && row.rust_command_template) {{
        return 'legacy: ' + row.legacy_command_template + '; rust: ' + row.rust_command_template;
      }}
      return row.command_template || row.command || 'n/a';
    }}

    function fmt(value, unit) {{
      if (value === null || value === undefined || Number.isNaN(value)) {{
        return 'n/a';
      }}
      const abs = Math.abs(value);
      let rendered;
      if (abs >= 1000) {{
        rendered = Math.round(value).toLocaleString();
      }} else if (abs >= 10) {{
        rendered = value.toFixed(1);
      }} else {{
        rendered = value.toFixed(2);
      }}
      return unit ? rendered + ' ' + unit : rendered;
    }}

    function rowName(row) {{
      return (row.workload || 'unknown').replace(/_/g, ' ');
    }}

    function rowKey(row) {{
      return row.row_key || [
        row.evidence_file || 'unknown',
        row.row_kind || 'comparison',
        row.workload || 'unknown',
        row.legacy_implementation || row.implementation || 'baseline',
        row.rust_implementation || 'target'
      ].join(':');
    }}

    function workloadFamily(row) {{
      if (row.kind) {{
        return row.kind;
      }}
      const workload = row.workload || '';
      if (workload.indexOf('socket') >= 0) return 'socket';
      if (workload.indexOf('ssl') >= 0) return 'ssl';
      if (workload.indexOf('create_group') >= 0) return 'create group';
      if (workload.indexOf('merge') >= 0) return 'merge';
      if (workload.indexOf('diff') >= 0) return 'diff';
      if (workload.indexOf('remove') >= 0) return 'remove';
      if (workload.indexOf('md5') >= 0) return 'hashing';
      if (workload.indexOf('gzip') >= 0) return 'compression';
      if (workload.indexOf('filter') >= 0) return 'filter';
      if (workload.indexOf('scheduler') >= 0 || workload.indexOf('tasklet') >= 0 || workload.indexOf('channel') >= 0) return 'scheduler';
      return row.component || 'unknown';
    }}

    function rowKind(row) {{
      return row.row_kind || 'comparison';
    }}

    function buildLabel(row) {{
      return row.rust_build_profile || row.build_profile || row.evidence_build_profile || 'unknown';
    }}

    function comparabilityLabel(value) {{
      if (value === 'comparable_process_to_process') {{
        return 'comparable: legacy vs Rust';
      }}
      if (value === 'rust_only_in_process_not_legacy_comparable') {{
        return 'not comparable: Rust-only in-process';
      }}
      if (value === 'same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io') {{
        return 'not comparable: baseline vs bridge';
      }}
      if (value === 'rust_scheduler_process_not_legacy_comparable') {{
        return 'not comparable: scheduler process resource evidence';
      }}
      return value || 'unknown';
    }}

    function baselineLabel(row) {{
      return isLegacyComparable(row) ? 'legacy' : 'baseline';
    }}

    function targetLabel(row) {{
      if (row.comparability === 'rust_only_in_process_not_legacy_comparable') {{
        return 'observed';
      }}
      return isLegacyComparable(row) ? 'rust' : 'target';
    }}

    const metricDefs = {{
      speedup: {{
        note: 'Higher is better. This is an observed local ratio; it becomes a speedup claim only when Rust is release-native target-cpu=native and the legacy baseline is non-debug.',
        unit: function() {{ return 'x'; }},
        legacy: function(row) {{ return numeric(row.speedup) === null ? null : 1; }},
        rust: function(row) {{ return numeric(row.speedup); }}
      }},
      throughput: {{
        note: 'Higher is better.',
        unit: throughputUnit,
        legacy: function(row) {{ return throughput(row, 'legacy'); }},
        rust: function(row) {{ return throughput(row, 'rust'); }}
      }},
      bestObservedThroughput: {{
        note: 'Higher is better. Best observed single local sample, not sustained max throughput.',
        unit: throughputUnit,
        legacy: function(row) {{ return bestObservedThroughput(row, 'legacy'); }},
        rust: function(row) {{ return bestObservedThroughput(row, 'rust'); }}
      }},
      wall: {{
        note: 'Lower is better.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ return durationMs(row, 'legacy'); }},
        rust: function(row) {{ return durationMs(row, 'rust'); }}
      }},
      wallRatio: {{
        note: 'Higher is better for legacy-over-Rust rows; values below 1 mean the target was slower than the baseline.',
        unit: function() {{ return 'x'; }},
        legacy: function() {{ return 1; }},
        rust: wallRatio
      }},
      p50: {{
        note: 'Lower is better.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ const value = getPath(row, ['legacy_sample_stats_us', 'p50']); return value === null ? null : value / 1000; }},
        rust: function(row) {{ const value = getPath(row, ['rust_sample_stats_us', 'p50']); return value === null ? null : value / 1000; }}
      }},
      p95: {{
        note: 'Lower is better.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ const value = getPath(row, ['legacy_sample_stats_us', 'p95']); return value === null ? null : value / 1000; }},
        rust: function(row) {{ const value = getPath(row, ['rust_sample_stats_us', 'p95']); return value === null ? null : value / 1000; }}
      }},
      p99: {{
        note: 'Lower is better. Uses p99 when evidence records it; otherwise n/a.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ const value = getPath(row, ['legacy_sample_stats_us', 'p99']); return value === null ? null : value / 1000; }},
        rust: function(row) {{ const value = getPath(row, ['rust_sample_stats_us', 'p99']); return value === null ? null : value / 1000; }}
      }},
      latencySpread: {{
        note: 'Lower is better. p95 minus p50 shows per-workload latency spread.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ return latencySpreadMs(row, 'legacy'); }},
        rust: function(row) {{ return latencySpreadMs(row, 'rust'); }}
      }},
      cpuBurn: {{
        note: 'Lower is better. Mean per process sample; tiny samples use wall time times CPU percent when user/system time rounds to zero.',
        unit: function() {{ return 'ms'; }},
        legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'cpu_burn_effective_ms', 'mean']); }},
        rust: function(row) {{ return getPath(row, ['rust_process_stats', 'cpu_burn_effective_ms', 'mean']); }}
      }},
      cpuRatio: {{
        note: 'Higher is better for legacy-over-Rust rows; values below 1 mean the target burned more CPU than the baseline.',
        unit: function() {{ return 'x'; }},
        legacy: function() {{ return 1; }},
        rust: cpuRatio
      }},
      cpuPercent: {{
        note: 'Observed process CPU utilization from /usr/bin/time.',
        unit: function() {{ return '%'; }},
        legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'cpu_percent', 'mean']); }},
        rust: function(row) {{ return getPath(row, ['rust_process_stats', 'cpu_percent', 'mean']); }}
      }},
      rss: {{
        note: 'Lower is better. p95 maximum resident set size.',
        unit: function() {{ return 'KB'; }},
        legacy: function(row) {{ return getPath(row, ['legacy_process_stats', 'max_rss_kb', 'p95']); }},
        rust: function(row) {{ return getPath(row, ['rust_process_stats', 'max_rss_kb', 'p95']); }}
      }},
      rssRatio: {{
        note: 'Lower is better. Target-over-baseline p95 RSS ratio; 1 means equal memory use.',
        unit: function() {{ return 'x'; }},
        legacy: function() {{ return 1; }},
        rust: rssRatio
      }},
      sampleCount: {{
        note: 'Higher means more repeated samples or requests in the evidence row.',
        unit: function() {{ return 'samples'; }},
        legacy: function(row) {{ return getPath(row, ['legacy_sample_stats_us', 'count']) || getPath(row, ['legacy_process_stats', 'sample_count']); }},
        rust: function(row) {{ return getPath(row, ['rust_sample_stats_us', 'count']) || getPath(row, ['rust_process_stats', 'sample_count']) || numeric(row.sample_count); }}
      }},
      scaledWall: {{
        note: 'Lower is better. Linear estimate from local samples, not a production claim.',
        unit: function() {{ return 's'; }},
        legacy: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'legacy_wall_seconds']); }},
        rust: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'rust_wall_seconds']); }}
      }},
      scaledCpu: {{
        note: 'Lower is better. Linear estimate from local CPU-burn samples, not a production claim.',
        unit: function() {{ return 's'; }},
        legacy: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'legacy_cpu_burn_seconds']); }},
        rust: function(row) {{ return getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'rust_cpu_burn_seconds']); }}
      }}
    }};

    function addOption(select, value, label) {{
      const option = document.createElement('option');
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }}

    function initFilters() {{
      featureFilter.innerHTML = '';
      addOption(featureFilter, 'all', 'All features');
      Array.from(new Set(performanceComparisons.map(function(row) {{ return row.component || 'unknown'; }}))).sort().forEach(function(feature) {{
        addOption(featureFilter, feature, feature);
      }});

      familyFilter.innerHTML = '';
      addOption(familyFilter, 'all', 'All families');
      Array.from(new Set(performanceComparisons.map(workloadFamily))).sort().forEach(function(family) {{
        addOption(familyFilter, family, family);
      }});

      comparabilityFilter.innerHTML = '';
      addOption(comparabilityFilter, 'all', 'All comparability');
      Array.from(new Set(performanceComparisons.map(function(row) {{ return row.comparability || 'unknown'; }}))).sort().forEach(function(value) {{
        addOption(comparabilityFilter, value, comparabilityLabel(value));
      }});

      rowKindFilter.innerHTML = '';
      addOption(rowKindFilter, 'all', 'All row kinds');
      Array.from(new Set(performanceComparisons.map(rowKind))).sort().forEach(function(value) {{
        addOption(rowKindFilter, value, value);
      }});

      buildFilter.innerHTML = '';
      addOption(buildFilter, 'all', 'All builds');
      Array.from(new Set(performanceComparisons.map(buildLabel))).sort().forEach(function(value) {{
        addOption(buildFilter, value, value);
      }});
    }}

    function visibleRows() {{
      const selectedFeature = featureFilter.value || 'all';
      const selectedFamily = familyFilter.value || 'all';
      const selectedComparability = comparabilityFilter.value || 'all';
      const selectedRowKind = rowKindFilter.value || 'all';
      const selectedBuild = buildFilter.value || 'all';
      const rows = performanceComparisons.filter(function(row) {{
        const featureMatch = selectedFeature === 'all' || (row.component || 'unknown') === selectedFeature;
        const familyMatch = selectedFamily === 'all' || workloadFamily(row) === selectedFamily;
        const comparabilityMatch = selectedComparability === 'all' || (row.comparability || 'unknown') === selectedComparability;
        const rowKindMatch = selectedRowKind === 'all' || rowKind(row) === selectedRowKind;
        const buildMatch = selectedBuild === 'all' || buildLabel(row) === selectedBuild;
        return featureMatch && familyMatch && comparabilityMatch && rowKindMatch && buildMatch;
      }});
      return sortedRows(rows);
    }}

    function metricSortValue(row) {{
      const metric = metricDefs[metricFilter.value] || metricDefs.speedup;
      const rustValue = metric.rust(row);
      const legacyValue = metric.legacy(row);
      return rustValue !== null ? rustValue : legacyValue;
    }}

    function sortedRows(rows) {{
      const ordered = rows.slice();
      const sortValue = sortFilter.value || 'metric-desc';
      ordered.sort(function(a, b) {{
        if (sortValue === 'workload') {{
          return rowName(a).localeCompare(rowName(b));
        }}
        if (sortValue === 'feature') {{
          return (a.component || '').localeCompare(b.component || '') || rowName(a).localeCompare(rowName(b));
        }}
        if (sortValue === 'comparability') {{
          return (a.comparability || '').localeCompare(b.comparability || '') || rowName(a).localeCompare(rowName(b));
        }}
        const av = metricSortValue(a);
        const bv = metricSortValue(b);
        if (av === null && bv === null) return rowName(a).localeCompare(rowName(b));
        if (av === null) return 1;
        if (bv === null) return -1;
        return sortValue === 'metric-asc' ? av - bv : bv - av;
      }});
      return ordered;
    }}

    function appendKpi(label, value, note) {{
      const card = document.createElement('div');
      card.className = 'kpi-card';
      const labelEl = document.createElement('span');
      labelEl.textContent = label;
      const valueEl = document.createElement('strong');
      valueEl.textContent = value;
      card.appendChild(labelEl);
      card.appendChild(valueEl);
      if (note) {{
        const noteEl = document.createElement('small');
        noteEl.textContent = note;
        card.appendChild(noteEl);
      }}
      kpiRoot.appendChild(card);
    }}

    function mean(values) {{
      const filtered = values.filter(function(value) {{ return value !== null; }});
      if (!filtered.length) {{
        return null;
      }}
      return filtered.reduce(function(sum, value) {{ return sum + value; }}, 0) / filtered.length;
    }}

    function bestRow(rows) {{
      let best = null;
      for (const row of rows) {{
        const value = numeric(row.speedup);
        if (value !== null && (best === null || value > best.value)) {{
          best = {{ row: row, value: value }};
        }}
      }}
      return best;
    }}

    function maxBy(rows, valueFn) {{
      let best = null;
      for (const row of rows) {{
        const value = numeric(valueFn(row));
        if (value !== null && (best === null || value > best.value)) {{
          best = {{ row: row, value: value }};
        }}
      }}
      return best;
    }}

    function rssRatio(row) {{
      return getPath(row, ['resource_comparison', 'peak_rss_ratio_rust_over_legacy_p95']) ||
        getPath(row, ['resource_comparison', 'peak_rss_ratio_scheduler_bridge_over_baseline_p95']);
    }}

    function wallRatio(row) {{
      return getPath(row, ['resource_comparison', 'wall_time_ratio_legacy_over_rust']) ||
        getPath(row, ['resource_comparison', 'wall_time_ratio_baseline_over_scheduler_bridge']);
    }}

    function cpuRatio(row) {{
      return getPath(row, ['resource_comparison', 'cpu_burn_effective_ratio_legacy_over_rust']) ||
        getPath(row, ['resource_comparison', 'cpu_burn_effective_ratio_baseline_over_scheduler_bridge']);
    }}

    function cpuBurnQualityText(row, prefix) {{
      const statsKey = prefix + '_process_stats';
      const quality = getRawPath(row, [statsKey, 'cpu_burn_effective_quality']);
      const counts = getRawPath(row, [statsKey, 'cpu_burn_effective_source_counts']);
      if (!quality) return 'n/a';
      if (!counts) return String(quality);
      return String(quality) + ' (direct=' + (counts.user_plus_system_time || 0) + ', estimated=' + (counts.wall_time_times_cpu_percent || 0) + ', missing=' + (counts.unavailable || 0) + ')';
    }}

    function latencySpreadMs(row, prefix) {{
      const p95 = getPath(row, [prefix + '_sample_stats_us', 'p95']);
      const p50 = getPath(row, [prefix + '_sample_stats_us', 'p50']);
      return p95 === null || p50 === null ? null : (p95 - p50) / 1000;
    }}

    function renderKpis(rows) {{
      kpiRoot.innerHTML = '';
      const comparableRows = rows.filter(isLegacyComparable);
      const claimEligibleRows = comparableRows.filter(isSpeedupClaimEligible);
      const nonComparableRows = rows.filter(function(row) {{ return !isLegacyComparable(row); }});
      const ioRows = rows.filter(function(row) {{ return row.component === 'io'; }});
      const schedulerResourceRows = rows.filter(function(row) {{ return row.comparability === 'rust_scheduler_process_not_legacy_comparable'; }});
      const averageComparableSpeedup = mean(claimEligibleRows.map(function(row) {{ return numeric(row.speedup); }}));
      const averageRssRatio = mean(rows.map(rssRatio));
      const best = bestRow(comparableRows.length ? comparableRows : rows);
      const worstSpeed = maxBy(comparableRows, function(row) {{
        const speedup = numeric(row.speedup);
        return speedup === null || speedup === 0 ? null : 1 / speedup;
      }});
      const highestP95 = maxBy(rows, function(row) {{
        return getPath(row, ['rust_sample_stats_us', 'p95']) || getPath(row, ['legacy_sample_stats_us', 'p95']);
      }});
      const highestRss = maxBy(rows, rssRatio);
      const highestCpuBurn = maxBy(rows, function(row) {{ return getPath(row, ['rust_process_stats', 'cpu_burn_effective_ms', 'mean']); }});
      appendKpi('Visible rows', String(rows.length), 'filtered benchmark rows');
      appendKpi('Comparable rows', String(comparableRows.length), 'legacy vs Rust process rows');
      appendKpi('Non-comparable rows', String(nonComparableRows.length), 'shown with reasons, no speedup claim');
      appendKpi('Scheduler resource rows', String(schedulerResourceRows.length), 'CPU/RSS/latency only');
      appendKpi('Claim-ready mean', fmt(averageComparableSpeedup, 'x'), 'requires non-debug legacy baseline');
      appendKpi('Best observed ratio', best ? fmt(best.value, 'x') : 'n/a', best ? rowName(best.row) : '');
      appendKpi('Worst observed ratio', worstSpeed ? fmt(worstSpeed.value, 'x') : 'n/a', worstSpeed ? rowName(worstSpeed.row) : '');
      appendKpi('Highest p95 latency', highestP95 ? fmt(highestP95.value / 1000, 'ms') : 'n/a', highestP95 ? rowName(highestP95.row) : '');
      appendKpi('Mean RSS ratio', fmt(averageRssRatio, 'x'), 'target over baseline p95 RSS');
      appendKpi('Highest RSS ratio', highestRss ? fmt(highestRss.value, 'x') : 'n/a', highestRss ? rowName(highestRss.row) : '');
      appendKpi('Highest CPU burn', highestCpuBurn ? fmt(highestCpuBurn.value, 'ms') : 'n/a', highestCpuBurn ? rowName(highestCpuBurn.row) : '');
      appendKpi('IO loopback rows', String(ioRows.length), 'baseline vs bridge, not final Carbon IO');
    }}

    function appendBar(stack, label, className, value, max, unit) {{
      const line = document.createElement('div');
      line.className = 'bar-line';
      const name = document.createElement('span');
      name.textContent = label;
      const track = document.createElement('div');
      track.className = 'bar-track';
      const fill = document.createElement('div');
      fill.className = 'bar-fill ' + className;
      fill.style.width = max > 0 && value !== null ? Math.max(2, (value / max) * 100) + '%' : '0';
      track.appendChild(fill);
      const valueEl = document.createElement('span');
      valueEl.textContent = fmt(value, unit);
      line.appendChild(name);
      line.appendChild(track);
      line.appendChild(valueEl);
      stack.appendChild(line);
    }}

    function ensureSelection(rows) {{
      if (!rows.length) {{
        selectedRowKey = null;
        return null;
      }}
      const selected = rows.find(function(row) {{ return rowKey(row) === selectedRowKey; }});
      if (selected) {{
        return selected;
      }}
      selectedRowKey = rowKey(rows[0]);
      return rows[0];
    }}

    function addDetail(label, value) {{
      const item = document.createElement('div');
      item.className = 'detail-item';
      const labelEl = document.createElement('span');
      labelEl.textContent = label;
      const valueEl = document.createElement('strong');
      valueEl.textContent = value;
      item.appendChild(labelEl);
      item.appendChild(valueEl);
      detailRoot.appendChild(item);
    }}

    function renderDetail(row) {{
      detailRoot.innerHTML = '';
      if (!row) {{
        addDetail('State', 'No comparison data for the current filters');
        return;
      }}
      addDetail('Workload', rowName(row));
      addDetail('Evidence', evidenceLabel(row));
      addDetail('Feature', row.component || 'unknown');
      let comparableText = comparabilityLabel(row.comparability);
      if (isLegacyComparable(row)) {{
        if (isSpeedupClaimEligible(row)) {{
          comparableText = 'yes: optimized legacy vs Rust release-native process';
        }} else if (isNativeRelease(row) && hasLegacyDebugBaseline(row)) {{
          comparableText = 'yes: legacy debug process vs Rust release-native; observed ratio only';
        }} else {{
          comparableText = 'yes: observed ratio, not speedup-claim eligible';
        }}
      }}
      addDetail('Comparable', comparableText);
      if (!isLegacyComparable(row)) {{
        addDetail('Not comparable reason', row.not_comparable_reason || row.claim_scope || row.claim || 'no legacy-equivalent claim');
      }}
      addDetail('Claim scope', row.claim_scope || 'not specified');
      addDetail('Build profile', row.rust_build_profile ? ('legacy ' + (row.legacy_build_profile || 'unknown') + '; rust ' + row.rust_build_profile) : (row.build_profile || 'n/a'));
      addDetail('Family / row kind', workloadFamily(row) + ' / ' + rowKind(row));
      addDetail('Command', commandText(row));
      addDetail('Throughput', fmt(throughput(row, 'legacy'), throughputUnit(row)) + ' vs ' + fmt(throughput(row, 'rust'), throughputUnit(row)));
      addDetail('Best observed throughput', fmt(bestObservedThroughput(row, 'legacy'), throughputUnit(row)) + ' vs ' + fmt(bestObservedThroughput(row, 'rust'), throughputUnit(row)) + ' (single local sample)');
      addDetail('Wall time', fmt(durationMs(row, 'legacy'), 'ms') + ' vs ' + fmt(durationMs(row, 'rust'), 'ms'));
      addDetail('Wall ratio', fmt(wallRatio(row), 'x'));
      addDetail('Latency p95', fmt(getPath(row, ['legacy_sample_stats_us', 'p95']) === null ? null : getPath(row, ['legacy_sample_stats_us', 'p95']) / 1000, 'ms') + ' vs ' + fmt(getPath(row, ['rust_sample_stats_us', 'p95']) === null ? null : getPath(row, ['rust_sample_stats_us', 'p95']) / 1000, 'ms'));
      addDetail('Latency p99', fmt(getPath(row, ['legacy_sample_stats_us', 'p99']) === null ? null : getPath(row, ['legacy_sample_stats_us', 'p99']) / 1000, 'ms') + ' vs ' + fmt(getPath(row, ['rust_sample_stats_us', 'p99']) === null ? null : getPath(row, ['rust_sample_stats_us', 'p99']) / 1000, 'ms'));
      addDetail('Samples', fmt(getPath(row, ['legacy_sample_stats_us', 'count']) || getPath(row, ['legacy_process_stats', 'sample_count']), 'samples') + ' vs ' + fmt(getPath(row, ['rust_sample_stats_us', 'count']) || getPath(row, ['rust_process_stats', 'sample_count']) || numeric(row.sample_count), 'samples'));
      addDetail('CPU burn', fmt(getPath(row, ['legacy_process_stats', 'cpu_burn_effective_ms', 'mean']), 'ms') + ' vs ' + fmt(getPath(row, ['rust_process_stats', 'cpu_burn_effective_ms', 'mean']), 'ms'));
      addDetail('CPU burn quality', cpuBurnQualityText(row, 'legacy') + ' vs ' + cpuBurnQualityText(row, 'rust'));
      addDetail('CPU burn ratio', fmt(cpuRatio(row), 'x'));
      addDetail('Peak RSS', fmt(getPath(row, ['legacy_process_stats', 'max_rss_kb', 'p95']), 'KB') + ' vs ' + fmt(getPath(row, ['rust_process_stats', 'max_rss_kb', 'p95']), 'KB'));
      addDetail('RSS ratio', fmt(rssRatio(row), 'x'));
      addDetail('Workload parameters', ['iterations=' + (row.iterations || 'n/a'), 'requests=' + (row.requests || 'n/a'), 'payload_bytes=' + (row.payload_bytes || 'n/a'), 'bytes_transferred=' + (row.bytes_transferred || 'n/a')].join('; '));
      addDetail('100k wall estimate', fmt(getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'legacy_wall_seconds']), 's') + ' vs ' + fmt(getPath(row, ['resource_comparison', 'linear_scale_estimate_100k_units', 'rust_wall_seconds']), 's'));
    }}

    function renderChart(rows) {{
      const metric = metricDefs[metricFilter.value] || metricDefs.speedup;
      metricNote.textContent = metric.note;
      chart.innerHTML = '';
      if (!rows.length) {{
        chart.textContent = 'No comparison data.';
        return;
      }}
      const values = [];
      for (const row of rows) {{
        const legacyValue = metric.legacy(row);
        const rustValue = metric.rust(row);
        if (legacyValue !== null) values.push(legacyValue);
        if (rustValue !== null) values.push(rustValue);
      }}
      const max = Math.max(0, ...values);
      for (const row of rows) {{
        const unit = metric.unit(row);
        const legacyValue = metric.legacy(row);
        const rustValue = metric.rust(row);
        const outer = document.createElement('div');
        outer.className = 'bar-row' + (rowKey(row) === selectedRowKey ? ' is-selected' : '');
        outer.setAttribute('role', 'button');
        outer.setAttribute('tabindex', '0');
        outer.addEventListener('click', function() {{
          selectedRowKey = rowKey(row);
          renderDashboard();
        }});
        outer.addEventListener('keydown', function(event) {{
          if (event.key === 'Enter' || event.key === ' ') {{
            selectedRowKey = rowKey(row);
            renderDashboard();
          }}
        }});
        const label = document.createElement('div');
        label.className = 'bar-label';
        const name = document.createElement('span');
        name.textContent = rowName(row);
        const meta = document.createElement('span');
        meta.className = 'bar-meta';
        meta.textContent = (row.component || 'unknown') + ' | ' + comparabilityLabel(row.comparability);
        label.appendChild(name);
        label.appendChild(meta);
        const stack = document.createElement('div');
        stack.className = 'bar-stack';
        appendBar(stack, baselineLabel(row), 'legacy', legacyValue, max, unit);
        appendBar(stack, targetLabel(row), 'rust', rustValue, max, unit);
        outer.appendChild(label);
        outer.appendChild(stack);
        chart.appendChild(outer);
      }}
    }}

    function syncTable(rows) {{
      if (!performanceTable) {{
        return;
      }}
      const visible = new Set(rows.map(rowKey));
      for (const tableRow of performanceTable.querySelectorAll('tbody tr')) {{
        const show = visible.has(tableRow.dataset.rowKey);
        tableRow.classList.toggle('is-hidden', !show);
      }}
    }}

    function renderDashboard() {{
      const rows = visibleRows();
      const selected = ensureSelection(rows);
      renderKpis(rows);
      renderChart(rows);
      renderDetail(selected);
      syncTable(rows);
    }}

    initFilters();
    featureFilter.addEventListener('change', renderDashboard);
    familyFilter.addEventListener('change', renderDashboard);
    comparabilityFilter.addEventListener('change', renderDashboard);
    rowKindFilter.addEventListener('change', renderDashboard);
    buildFilter.addEventListener('change', renderDashboard);
    metricFilter.addEventListener('change', renderDashboard);
    sortFilter.addEventListener('change', renderDashboard);
    renderDashboard();
  </script>
</body>
</html>
"#,
        scheduler_summary = escape_html(&scheduler_summary),
        legacy_resources_summary = escape_html(&legacy_resources_summary),
        rust_resources_summary = escape_html(&rust_resources_summary),
        io_summary = escape_html(&io_summary),
        scheduler_python_subset_count = escape_html(&scheduler_python_subset_count),
        performance_summary = escape_html(&performance_summary),
        feature_summary = feature_summary,
        run_context = run_context,
        scheduler_ownership_boundary = scheduler_ownership_boundary,
        gate_rows = gate_rows,
        gate_blocker_rows = gate_blocker_rows,
        performance_rows = performance_rows,
        performance_chart_data = performance_chart_data,
        optimization_tables = optimization_tables,
        blocker_items = blocker_items,
        optimization = escape_html(&optimization),
        readiness = escape_html(&readiness),
        tasks = escape_html(&tasks)
    ))
}

fn evidence_value<'a>(evidence: &'a [(&str, Option<Value>)], file: &str) -> Option<&'a Value> {
    evidence
        .iter()
        .find(|(candidate, _)| *candidate == file)
        .and_then(|(_, value)| value.as_ref())
}

fn render_progress_run_context(evidence: &[(&str, Option<Value>)]) -> String {
    let bench = evidence_value(evidence, "bench-tier-local.json");
    let io = evidence_value(evidence, "io-workloads.json");
    let mut cards = Vec::new();

    if let Some(bench) = bench {
        cards.push(context_card(
            "Benchmark Command",
            json_pointer_string(bench, "/command").unwrap_or_else(|| String::from("missing")),
        ));
        cards.push(context_card(
            "Recommended Comparable Command",
            json_pointer_string(bench, "/recommended_comparable_command")
                .unwrap_or_else(|| String::from("scripts/carbon-native-bench.sh bench")),
        ));
        cards.push(context_card(
            "Build Profile",
            json_pointer_string(bench, "/build_profile").unwrap_or_else(|| String::from("unknown")),
        ));
        cards.push(context_card(
            "Native CPU",
            match json_pointer_bool(bench, "/host/rust_build/target_cpu_native") {
                Some(true) => String::from("target-cpu=native recorded"),
                Some(false) => String::from("not native; current evidence is debug/non-native"),
                None => String::from("unknown"),
            },
        ));
        cards.push(context_card(
            "RUSTFLAGS",
            json_pointer_string(bench, "/host/rust_build/rustflags")
                .unwrap_or_else(|| String::from("unset")),
        ));
        let cpu = json_pointer_string(bench, "/host/cpu_model")
            .unwrap_or_else(|| String::from("unknown CPU"));
        let logical = json_pointer_u64(bench, "/host/logical_cpus")
            .map(|value| value.to_string())
            .unwrap_or_else(|| String::from("?"));
        let ram = json_pointer_u64(bench, "/host/ram_kb")
            .map(|kb| format!("{:.1} GB RAM", kb as f64 / 1024.0 / 1024.0))
            .unwrap_or_else(|| String::from("RAM unknown"));
        cards.push(context_card(
            "Host",
            format!("{cpu}; {logical} logical CPUs; {ram}"),
        ));
        cards.push(context_card(
            "Toolchain",
            format!(
                "{}; {}",
                json_pointer_string(bench, "/host/rustc")
                    .unwrap_or_else(|| String::from("rustc unknown")),
                json_pointer_string(bench, "/host/cargo")
                    .unwrap_or_else(|| String::from("cargo unknown"))
            ),
        ));
        cards.push(context_card(
            "Comparable Rows",
            format!(
                "{} comparable comparisons; {} Rust-only rows; {} scheduler resource-only rows",
                json_pointer_u64(
                    bench,
                    "/comparability_summary/comparable_process_to_process_comparisons",
                )
                .unwrap_or(0),
                json_pointer_u64(
                    bench,
                    "/comparability_summary/rust_only_not_comparable_workload_rows",
                )
                .unwrap_or(0),
                json_pointer_u64(
                    bench,
                    "/comparability_summary/scheduler_process_resource_rows",
                )
                .unwrap_or(0)
            ),
        ));
    } else {
        cards.push(context_card(
            "Benchmark Evidence",
            "bench-tier-local.json missing".to_string(),
        ));
    }

    if let Some(io) = io {
        let workloads = io
            .get("workloads")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        let comparisons = io
            .get("comparisons")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        cards.push(context_card(
            "IO Workload Rows",
            format!("{workloads} workloads; {comparisons} baseline-vs-bridge comparisons"),
        ));
        cards.push(context_card(
            "IO Build Profile",
            json_pointer_string(io, "/host/rust_build/build_profile")
                .unwrap_or_else(|| String::from("unknown")),
        ));
        cards.push(context_card(
            "IO Native CPU",
            match json_pointer_bool(io, "/host/rust_build/target_cpu_native") {
                Some(true) => String::from("target-cpu=native recorded"),
                Some(false) => String::from("not native; IO evidence is debug/non-native"),
                None => String::from("unknown"),
            },
        ));
        cards.push(context_card(
            "IO Measurement",
            json_pointer_string(io, "/host/process_resource_measurement")
                .unwrap_or_else(|| String::from("unknown")),
        ));
    }

    cards.join("")
}

fn render_scheduler_ownership_boundary(evidence: &[(&str, Option<Value>)]) -> String {
    let Some(value) = evidence_value(evidence, "rust-scheduler-python.json") else {
        return String::from(
            r#"<section class="panel"><p>rust-scheduler-python.json missing; scheduler ownership boundary cannot be evaluated.</p></section>"#,
        );
    };
    let Some(ownership) = value.get("core_ownership_status") else {
        return String::from(
            r#"<section class="panel"><p>rust-scheduler-python.json has no core_ownership_status; scheduler ownership boundary cannot be evaluated.</p></section>"#,
        );
    };

    let status = ownership
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let target_state = ownership
        .get("target_state")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let migration_blocker = ownership
        .get("migration_blocker")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let rust_owned = render_string_list(
        ownership.get("already_rust_owned"),
        "No Rust-owned scheduler boundary evidence recorded.",
    );
    let bridge_owned = render_string_list(
        ownership.get("still_bridge_owned"),
        "No bridge-owned scheduler state list recorded.",
    );

    format!(
        r#"<section class="panel">
    <div class="summary-grid">
      <div class="summary-card"><strong>Boundary Status</strong><span>{}</span></div>
      <div class="summary-card"><strong>Target State</strong><span>{}</span></div>
      <div class="summary-card"><strong>Migration Blocker</strong><span>{}</span></div>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Already Rust-Owned</th><th>Still Bridge-Owned</th></tr></thead>
        <tbody><tr><td>{}</td><td>{}</td></tr></tbody>
      </table>
    </div>
    <p class="metric-note">PyO3 remains a compatibility adapter for unchanged legacy Python and C API tests. It is not the desired final owner of tasklet, channel, or scheduler lifecycle state.</p>
  </section>"#,
        escape_html(status),
        escape_html(target_state),
        escape_html(migration_blocker),
        rust_owned,
        bridge_owned
    )
}

fn render_string_list(value: Option<&Value>, empty: &str) -> String {
    let Some(items) = value.and_then(Value::as_array) else {
        return escape_html(empty);
    };
    if items.is_empty() {
        return escape_html(empty);
    }

    let items = items
        .iter()
        .filter_map(Value::as_str)
        .map(|item| format!("<li>{}</li>", escape_html(item)))
        .collect::<String>();
    if items.is_empty() {
        escape_html(empty)
    } else {
        format!(r#"<ul class="compact-list">{items}</ul>"#)
    }
}

fn render_optimization_tables(evidence: &[(&str, Option<Value>)]) -> String {
    let bench = evidence_value(evidence, "bench-tier-local.json");
    let io = evidence_value(evidence, "io-workloads.json");
    let manifests = workspace_manifest_text();

    let bench_profile = bench
        .and_then(|value| json_pointer_string(value, "/build_profile"))
        .unwrap_or_else(|| String::from("missing"));
    let bench_native = bench
        .and_then(|value| json_pointer_bool(value, "/host/rust_build/target_cpu_native"))
        .unwrap_or(false);
    let bench_debug_assertions = bench
        .and_then(|value| json_pointer_bool(value, "/host/rust_build/debug_assertions"))
        .unwrap_or(true);
    let bench_rustflags = bench
        .and_then(|value| json_pointer_string(value, "/host/rust_build/rustflags"))
        .unwrap_or_else(|| String::from("unset"));
    let io_profile = io
        .and_then(|value| json_pointer_string(value, "/host/rust_build/build_profile"))
        .unwrap_or_else(|| String::from("missing"));
    let io_native = io
        .and_then(|value| json_pointer_bool(value, "/host/rust_build/target_cpu_native"))
        .unwrap_or(false);

    let comparisons = bench
        .and_then(|value| value.get("comparisons"))
        .and_then(Value::as_array);
    let comparable_rows = comparisons.map(Vec::len).unwrap_or_default();
    let claim_ready_rows = bench
        .and_then(|value| {
            value
                .get("comparisons")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .cloned()
                        .map(|row| {
                            performance_entry_with_metadata(
                                row,
                                "bench-tier-local.json",
                                "comparison",
                                value,
                            )
                        })
                        .filter(has_speedup_claim_eligible_performance_evidence)
                        .count()
                })
        })
        .unwrap_or_default();
    let evidence_claim_ready_rows = bench
        .and_then(|value| {
            json_pointer_u64(
                value,
                "/optimization_readiness/speedup_claim_eligible_comparisons",
            )
        })
        .unwrap_or(claim_ready_rows as u64);
    let observed_only_rows = bench
        .and_then(|value| {
            json_pointer_u64(value, "/optimization_readiness/observed_only_comparisons")
        })
        .unwrap_or_else(|| comparable_rows.saturating_sub(claim_ready_rows) as u64);
    let optimized_detection_status = bench
        .and_then(|value| {
            json_pointer_string(
                value,
                "/optimization_readiness/legacy_optimized_baseline_detection/status",
            )
        })
        .unwrap_or_else(|| String::from("missing"));
    let optimized_candidate_count = bench
        .and_then(|value| {
            json_pointer_u64(
                value,
                "/optimization_readiness/legacy_optimized_baseline_detection/non_debug_candidate_count",
            )
        })
        .unwrap_or_default();
    let optimization_blocked_reason = bench
        .and_then(|value| json_pointer_string(value, "/optimization_readiness/blocked_reason"))
        .unwrap_or_else(|| {
            String::from("rerun benchmark evidence to populate optimization_readiness")
        });
    let legacy_profiles = comparisons
        .map(|rows| {
            let mut profiles = rows
                .iter()
                .filter_map(|row| row.get("legacy_build_profile").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>();
            profiles.sort();
            profiles.dedup();
            profiles.join(", ")
        })
        .filter(|profiles| !profiles.is_empty())
        .unwrap_or_else(|| String::from("missing"));

    let has_rayon = manifest_mentions_dependency(&manifests, "rayon");
    let has_tokio = manifest_mentions_dependency(&manifests, "tokio");
    let has_object_store = manifest_mentions_dependency(&manifests, "object_store");
    let has_bitvec = manifest_mentions_dependency(&manifests, "bitvec");
    let has_fixedbitset = manifest_mentions_dependency(&manifests, "fixedbitset");
    let has_roaring = manifest_mentions_dependency(&manifests, "roaring");
    let has_hashbrown = manifest_mentions_dependency(&manifests, "hashbrown");
    let has_ahash = manifest_mentions_dependency(&manifests, "ahash");
    let has_indexmap = manifest_mentions_dependency(&manifests, "indexmap");
    let has_globset = manifest_mentions_dependency(&manifests, "globset");
    let has_blake3 = manifest_mentions_dependency(&manifests, "blake3");
    let has_zstd = manifest_mentions_dependency(&manifests, "zstd");
    let has_flate2 = manifest_mentions_dependency(&manifests, "flate2");
    let has_md5 = manifest_mentions_dependency(&manifests, "md5");

    let build_rows = [
        optimization_table_row(
            "Rust native benchmark lane",
            &format!(
                "bench-tier-local.json build_profile={bench_profile}, target_cpu_native={bench_native}, debug_assertions={bench_debug_assertions}, RUSTFLAGS={bench_rustflags}"
            ),
            "Measured for current resource benchmark rows; required before any Rust-side speedup claim.",
            if bench_profile == "release-native" && bench_native && !bench_debug_assertions {
                "partial pass"
            } else {
                "missing native evidence"
            },
        ),
        optimization_table_row(
            "Legacy optimized baseline",
            &format!(
                "{comparable_rows} comparable rows use legacy profiles: {legacy_profiles}; {evidence_claim_ready_rows} optimized-baseline claim-eligible, {observed_only_rows} observed-only; non-debug candidates detected={optimized_candidate_count}; detector={optimized_detection_status}"
            ),
            &optimization_blocked_reason,
            if evidence_claim_ready_rows > 0 {
                "partial claim-ready rows"
            } else {
                "blocked"
            },
        ),
        optimization_table_row(
            "IO native benchmark lane",
            &format!("io-workloads.json build_profile={io_profile}, target_cpu_native={io_native}"),
            "IO rows are baseline-vs-bridge observations and are not legacy Carbon IO comparable yet.",
            if io_profile == "release-native" && io_native {
                "partial"
            } else {
                "not native"
            },
        ),
    ]
    .join("");

    let opportunity_rows = [
        optimization_table_row(
            "Rayon CPU stages",
            &dependency_state(has_rayon, "rayon"),
            "Candidate for hashing, compression, patch generation/application, and filter evaluation after byte-parity tests are stable.",
            if has_rayon { "dependency present; unmeasured" } else { "not implemented" },
        ),
        optimization_table_row(
            "Tokio/object-store remote IO",
            &format!(
                "tokio: {}; object_store: {}",
                dependency_state(has_tokio, "tokio"),
                dependency_state(has_object_store, "object_store")
            ),
            "Candidate for downloader, remote catalog probes, retry/cancel/error parity, and local object-store simulation.",
            if has_tokio || has_object_store {
                "dependency present; unmeasured"
            } else {
                "not implemented"
            },
        ),
        optimization_table_row(
            "Dense/sparse membership sets",
            &format!(
                "bitvec: {}; fixedbitset: {}; roaring: {}",
                dependency_state(has_bitvec, "bitvec"),
                dependency_state(has_fixedbitset, "fixedbitset"),
                dependency_state(has_roaring, "roaring")
            ),
            "Candidate for resource group membership, filter result sets, duplicate detection, and catalog diffs.",
            if has_bitvec || has_fixedbitset || has_roaring {
                "dependency present; unmeasured"
            } else {
                "not implemented"
            },
        ),
        optimization_table_row(
            "Optimized hash/index structures",
            &format!(
                "hashbrown: {}; ahash: {}; indexmap: {}; globset: {}",
                dependency_state(has_hashbrown, "hashbrown"),
                dependency_state(has_ahash, "ahash"),
                dependency_state(has_indexmap, "indexmap"),
                dependency_state(has_globset, "globset")
            ),
            "Candidate for resource path lookup, filter matching, duplicate reduction, and deterministic manifest export.",
            if has_hashbrown || has_ahash || has_indexmap || has_globset {
                "dependency present; unmeasured"
            } else {
                "not implemented"
            },
        ),
        optimization_table_row(
            "SIMD and low-level codecs",
            &format!(
                "target-cpu=native evidence: {bench_native}; md5: {}; flate2: {}; blake3: {}; zstd: {}",
                dependency_state(has_md5, "md5"),
                dependency_state(has_flate2, "flate2"),
                dependency_state(has_blake3, "blake3"),
                dependency_state(has_zstd, "zstd")
            ),
            "Current native build context can help autovectorization, but no app-level SIMD kernel or SIMD crate gain is proven.",
            "native build measured; SIMD unmeasured",
        ),
        optimization_table_row(
            "Scheduler core ownership drain",
            "PyO3 bridge is present for compatibility; core fixtures and live PyO3 tasklet/channel objects now carry CoreScheduler handles for mirrored unbuffered channel state, explicit core pause/resume in covered bind/remove/insert/switch pause paths, core-ID selected send/receive transfers, core-owned immediate peer handoff, core-selected queue-front introspection in covered paths, and tasklet runtime snapshots, but Python payload/Greenlet behavior remains bridge-owned.",
            "Make CoreScheduler tasklet/channel snapshots authoritative for remaining lifecycle decisions, payload handoff tokens, and queue identity adapters while keeping PyO3 as a wrapper.",
            "partial architecture work",
        ),
    ]
    .join("");

    format!(
        r#"<section class="panel">
    <h3>Native Build And Claim Readiness</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Area</th><th>Current Evidence</th><th>Report Treatment</th><th>Status</th></tr></thead>
        <tbody>{build_rows}</tbody>
      </table>
    </div>
  </section>
  <section class="panel">
    <h3>Optimization Opportunity Matrix</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Opportunity</th><th>Current Workspace State</th><th>Required Proof Before Claiming</th><th>Status</th></tr></thead>
        <tbody>{opportunity_rows}</tbody>
      </table>
    </div>
  </section>"#
    )
}

fn workspace_manifest_text() -> String {
    let mut text = fs::read_to_string("Cargo.toml").unwrap_or_default();
    if let Ok(entries) = fs::read_dir("crates") {
        for entry in entries.flatten() {
            let path = entry.path().join("Cargo.toml");
            if let Ok(manifest) = fs::read_to_string(path) {
                text.push('\n');
                text.push_str(&manifest);
            }
        }
    }
    text
}

fn manifest_mentions_dependency(manifests: &str, crate_name: &str) -> bool {
    manifests.lines().any(|line| {
        let line = line.trim();
        !line.starts_with('#')
            && (line.starts_with(&format!("{crate_name} ="))
                || line.starts_with(&format!("{crate_name}="))
                || line.starts_with(&format!("\"{crate_name}\" ="))
                || line.starts_with(&format!("\"{crate_name}\"=")))
    })
}

fn dependency_state(present: bool, crate_name: &str) -> String {
    if present {
        format!("{crate_name} direct dependency present")
    } else {
        format!("{crate_name} not a direct workspace dependency")
    }
}

fn optimization_table_row(area: &str, current: &str, treatment: &str, status: &str) -> String {
    format!(
        "<tr><td>{}</td><td>{}</td><td>{}</td><td><span class=\"chip\">{}</span></td></tr>",
        escape_html(area),
        escape_html(current),
        escape_html(treatment),
        escape_html(status)
    )
}

fn context_card(label: &str, value: String) -> String {
    format!(
        "<div class=\"summary-card\"><strong>{}</strong><span>{}</span></div>",
        escape_html(label),
        escape_html(&value)
    )
}

fn json_pointer_string(value: &Value, pointer: &str) -> Option<String> {
    match value.pointer(pointer)? {
        Value::String(text) => Some(text.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn json_pointer_bool(value: &Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(Value::as_bool)
}

fn json_pointer_u64(value: &Value, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(Value::as_u64)
}

fn render_feature_performance_summary(entries: &[Value]) -> String {
    if entries.is_empty() {
        return String::from(
            r#"<section class="panel"><h3>Feature Performance Summary</h3><p>No benchmark evidence rows are available yet.</p></section>"#,
        );
    }

    let mut by_feature = BTreeMap::<String, Vec<&Value>>::new();
    for entry in entries {
        let feature = entry
            .get("component")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        by_feature.entry(feature).or_default().push(entry);
    }

    let max_rows = by_feature.values().map(Vec::len).max().unwrap_or(1) as f64;
    let max_speedup = by_feature
        .values()
        .flat_map(|rows| {
            rows.iter()
                .filter_map(|entry| entry.get("speedup").and_then(json_number))
        })
        .fold(1.0_f64, f64::max);

    let rows = by_feature
        .iter()
        .map(|(feature, rows)| {
            let total = rows.len();
            let comparable = rows
                .iter()
                .filter(|entry| is_legacy_comparable_entry(entry))
                .count();
            let claim_ready = rows
                .iter()
                .filter(|entry| {
                    is_legacy_comparable_entry(entry)
                        && entry.get("speedup").and_then(json_number).is_some()
                        && has_speedup_claim_eligible_performance_evidence(entry)
                })
                .count();
            let non_comparable = total.saturating_sub(comparable);
            let observed_only = comparable.saturating_sub(claim_ready);
            let speedups = rows
                .iter()
                .filter(|entry| is_legacy_comparable_entry(entry))
                .filter_map(|entry| entry.get("speedup").and_then(json_number))
                .collect::<Vec<_>>();
            let best_speedup = max_number(speedups.iter().copied());
            let mean_speedup = mean_number(speedups.iter().copied());
            let target_p95_ms = mean_number(rows.iter().filter_map(|entry| {
                number_at(entry, &["rust_sample_stats_us", "p95"])
                    .or_else(|| number_at(entry, &["legacy_sample_stats_us", "p95"]))
                    .map(|value| value / 1000.0)
            }));
            let cpu_ratio = mean_number(rows.iter().filter_map(|entry| performance_cpu_ratio(entry)));
            let rss_ratio = mean_number(rows.iter().filter_map(|entry| performance_rss_ratio(entry)));
            let evidence_files = render_evidence_pills(rows);
            let workload_note = feature_workload_note(rows);
            let status_badge = feature_claim_status_badge(claim_ready, comparable);
            format!(
                r#"<tr>
  <td><strong>{}</strong><span class="cell-note">{}</span></td>
  <td>{}<span class="cell-note">{} claim-ready, {} observed-only comparable, {} non-comparable</span></td>
  <td>{}<span class="cell-note">{} total rows</span></td>
  <td>{}<strong>{}</strong><span class="cell-note">mean comparable {}</span></td>
  <td><strong>{}</strong><span class="cell-note">mean target/baseline p95</span></td>
  <td><strong>CPU {}</strong><span class="cell-note">legacy-over-target mean</span><strong>RSS {}</strong><span class="cell-note">target-over-baseline mean</span></td>
  <td>{}</td>
</tr>"#,
                escape_html(feature),
                workload_note,
                status_badge,
                claim_ready,
                observed_only,
                non_comparable,
                render_row_mix_bar(total, claim_ready, observed_only, non_comparable, max_rows),
                total,
                render_metric_bar(best_speedup, max_speedup, "best observed ratio", "rust"),
                format_amount(best_speedup, "x"),
                format_amount(mean_speedup, "x"),
                format_amount(target_p95_ms, "ms"),
                format_amount(cpu_ratio, "x"),
                format_amount(rss_ratio, "x"),
                evidence_files
            )
        })
        .collect::<String>();

    format!(
        r#"<section class="panel feature-summary-panel">
  <h3>Feature Performance Summary</h3>
  <div class="table-wrap">
    <table class="feature-summary-table">
      <thead><tr><th>Feature</th><th>Claim Status</th><th>Row Mix</th><th>Observed Ratio</th><th>Latency</th><th>CPU / RSS</th><th>Evidence</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="metric-note">Feature badges are presentation-only summaries from existing evidence rows. Observed-only and non-comparable rows remain blocked from speedup claims by the same readiness gates.</p>
</section>"#
    )
}

fn render_evidence_pills(rows: &[&Value]) -> String {
    let mut files = rows
        .iter()
        .filter_map(|entry| entry.get("evidence_file").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    if files.is_empty() {
        return String::from("<span class=\"evidence-pill\">unknown</span>");
    }
    files
        .iter()
        .map(|file| format!("<span class=\"evidence-pill\">{}</span>", escape_html(file)))
        .collect::<String>()
}

fn feature_workload_note(rows: &[&Value]) -> String {
    let mut workloads = rows
        .iter()
        .filter_map(|entry| entry.get("workload").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    workloads.sort();
    workloads.dedup();
    let shown = workloads
        .iter()
        .take(3)
        .map(|workload| workload.replace('_', " "))
        .collect::<Vec<_>>();
    let mut note = if shown.is_empty() {
        String::from("no named workloads")
    } else {
        shown.join(", ")
    };
    if workloads.len() > shown.len() {
        note.push_str(&format!(" +{} more", workloads.len() - shown.len()));
    }
    escape_html(&note)
}

fn feature_claim_status_badge(claim_ready: usize, comparable: usize) -> String {
    if claim_ready > 0 {
        status_badge("claim-ready", "speedup claim-ready")
    } else if comparable > 0 {
        status_badge("observed-only", "observed only")
    } else {
        status_badge("not-comparable", "not comparable")
    }
}

fn performance_claim_badge(entry: &Value) -> String {
    if is_legacy_comparable_entry(entry)
        && entry.get("speedup").and_then(json_number).is_some()
        && has_speedup_claim_eligible_performance_evidence(entry)
    {
        status_badge("claim-ready", "speedup claim")
    } else if is_legacy_comparable_entry(entry) {
        status_badge("observed-only", "observed only")
    } else {
        status_badge("not-comparable", "no speedup claim")
    }
}

fn performance_comparable_badge(entry: &Value) -> String {
    if is_legacy_comparable_entry(entry) {
        if has_speedup_claim_eligible_performance_evidence(entry) {
            status_badge("claim-ready", "comparable")
        } else {
            status_badge("observed-only", "comparable")
        }
    } else {
        status_badge("not-comparable", "not comparable")
    }
}

fn status_badge(class_name: &str, label: &str) -> String {
    format!(
        "<span class=\"status-badge {}\">{}</span>",
        escape_html(class_name),
        escape_html(label)
    )
}

fn render_row_mix_bar(
    total: usize,
    claim_ready: usize,
    observed_only: usize,
    non_comparable: usize,
    max_rows: f64,
) -> String {
    let scale = if max_rows > 0.0 {
        total as f64 / max_rows
    } else {
        0.0
    };
    let bar_width = (116.0 * scale).clamp(0.0, 116.0);
    let total = total.max(1) as f64;
    let claim_width = bar_width * claim_ready as f64 / total;
    let observed_width = bar_width * observed_only as f64 / total;
    let non_comparable_width = bar_width * non_comparable as f64 / total;
    let observed_x = 2.0 + claim_width;
    let non_comparable_x = observed_x + observed_width;
    let label = format!(
        "{claim_ready} claim-ready rows, {observed_only} observed-only comparable rows, {non_comparable} non-comparable rows"
    );
    format!(
        r#"<svg class="mini-bar" viewBox="0 0 120 16" role="img" aria-label="{}">
  <title>{}</title>
  <rect class="mini-bar-bg" x="2" y="4" width="116" height="8" rx="2"></rect>
  <rect class="mini-bar-fill claim-ready" x="2" y="4" width="{:.1}" height="8" rx="2"></rect>
  <rect class="mini-bar-fill observed-only" x="{:.1}" y="4" width="{:.1}" height="8"></rect>
  <rect class="mini-bar-fill not-comparable" x="{:.1}" y="4" width="{:.1}" height="8" rx="2"></rect>
</svg>"#,
        escape_html(&label),
        escape_html(&label),
        claim_width,
        observed_x,
        observed_width,
        non_comparable_x,
        non_comparable_width
    )
}

fn render_metric_bar(value: Option<f64>, max: f64, label: &str, fill_class: &str) -> String {
    let width = value
        .filter(|value| value.is_finite() && *value > 0.0 && max > 0.0)
        .map(|value| ((value / max).clamp(0.0, 1.0) * 116.0).max(2.0))
        .unwrap_or(0.0);
    let label = match value {
        Some(value) => format!("{label}: {}", format_amount(Some(value), "x")),
        None => format!("{label}: n/a"),
    };
    format!(
        r#"<svg class="mini-bar" viewBox="0 0 120 16" role="img" aria-label="{}">
  <title>{}</title>
  <rect class="mini-bar-bg" x="2" y="4" width="116" height="8" rx="2"></rect>
  <rect class="mini-bar-fill {}" x="2" y="4" width="{:.1}" height="8" rx="2"></rect>
</svg>"#,
        escape_html(&label),
        escape_html(&label),
        escape_html(fill_class),
        width
    )
}

fn mean_number<I>(values: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    let mut count = 0_u64;
    let mut sum = 0.0;
    for value in values {
        if value.is_finite() {
            count += 1;
            sum += value;
        }
    }
    (count > 0).then_some(sum / count as f64)
}

fn max_number<I>(values: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    values
        .into_iter()
        .filter(|value| value.is_finite())
        .reduce(f64::max)
}

fn render_performance_rows(entries: &[Value]) -> String {
    if entries.is_empty() {
        return String::from("<tr><td colspan=\"15\">No benchmark evidence yet.</td></tr>");
    }

    entries
        .iter()
        .map(|entry| {
            let component = entry
                .get("component")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let name = entry
                .get("workload")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let row_comparability = entry
                .get("comparability")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let row_kind = entry
                .get("row_kind")
                .and_then(Value::as_str)
                .unwrap_or("comparison");
            let evidence_file = entry
                .get("evidence_file")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let row_key = entry
                .get("row_key")
                .and_then(Value::as_str)
                .unwrap_or(name);
            let speedup_text = performance_speedup_text(entry);
            let claim = performance_claim_text(entry);
            let comparable_text = performance_comparable_text(entry);
            let command = performance_command_text(entry);
            let comparable_badge = performance_comparable_badge(entry);
            let claim_badge = performance_claim_badge(entry);
            let row_class = if is_legacy_comparable_entry(entry) {
                "comparable"
            } else {
                "not-comparable"
            };
            format!(
                "<tr class=\"{}\" data-component=\"{}\" data-comparability=\"{}\" data-workload=\"{}\" data-row-key=\"{}\"><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><code>{}</code></td></tr>",
                row_class,
                escape_html(component),
                escape_html(row_comparability),
                escape_html(name),
                escape_html(row_key),
                escape_html(&format!("{row_kind}: {evidence_file}")),
                escape_html(component),
                escape_html(name),
                escape_html(&comparison_throughput(entry, "legacy")),
                escape_html(&comparison_throughput(entry, "rust")),
                escape_html(&speedup_text),
                escape_html(&duration_pair(entry)),
                escape_html(&latency_pair(entry)),
                escape_html(&cpu_burn_pair(entry)),
                escape_html(&cpu_percent_pair(entry)),
                escape_html(&rss_pair(entry)),
                escape_html(&scaled_pair(entry)),
                format!(
                    "{}<span class=\"cell-note\">{}</span>",
                    comparable_badge,
                    escape_html(&comparable_text)
                ),
                format!(
                    "{}<span class=\"cell-note\">{}</span>",
                    claim_badge,
                    escape_html(&claim)
                ),
                escape_html(&command)
            )
        })
        .collect::<String>()
}

fn render_performance_chart_data(entries: &[Value]) -> String {
    serde_json::to_string(entries).unwrap_or_else(|_| String::from("[]"))
}

fn progress_performance_entries(evidence: &[(&str, Option<Value>)]) -> Vec<Value> {
    performance_entries_from_sources(
        evidence
            .iter()
            .filter_map(|(file, value)| value.as_ref().map(|value| (*file, value))),
    )
}

fn performance_entries_from_sources<'a, I>(sources: I) -> Vec<Value>
where
    I: IntoIterator<Item = (&'a str, &'a Value)>,
{
    let mut entries = Vec::new();
    for (file, value) in sources {
        if let Some(comparisons) = value.get("comparisons").and_then(Value::as_array) {
            entries.extend(
                comparisons
                    .iter()
                    .cloned()
                    .map(|entry| performance_entry_with_metadata(entry, file, "comparison", value)),
            );
        }

        if file == "bench-tier-local.json" {
            if let Some(workloads) = value.get("workloads").and_then(Value::as_array) {
                entries.extend(
                    workloads
                        .iter()
                        .filter(|entry| {
                            entry
                                .get("comparability")
                                .and_then(Value::as_str)
                                .is_some_and(|comparability| {
                                    comparability != "comparable_process_to_process"
                                })
                        })
                        .cloned()
                        .map(|entry| {
                            performance_entry_with_metadata(entry, file, "workload", value)
                        }),
                );
            }
        }

        if file == "scalability-matrix.json" {
            if let Some(rows) = value.get("rows").and_then(Value::as_array) {
                entries.extend(rows.iter().cloned().map(|entry| {
                    let normalized = normalize_scalability_performance_entry(entry);
                    performance_entry_with_metadata(normalized, file, "pressure", value)
                }));
            }
        }
    }
    entries
}

fn normalize_scalability_performance_entry(mut entry: Value) -> Value {
    if let Some(object) = entry.as_object_mut() {
        if !object.contains_key("rust_sample_stats_us") {
            if let Some(latency) = object.get("latency_us_extended").cloned() {
                object.insert(String::from("rust_sample_stats_us"), latency);
            }
        }
        if !object.contains_key("rust_process_stats") {
            if let Some(process_stats) = object.get("process_stats").cloned() {
                object.insert(String::from("rust_process_stats"), process_stats);
            }
        }
        if !object.contains_key("rust_duration_us") {
            if let Some(duration_us) = object.get("duration_us").cloned() {
                object.insert(String::from("rust_duration_us"), duration_us);
            }
        }
        for (source, target) in [
            (
                "throughput_operations_per_sec",
                "rust_throughput_operations_per_sec",
            ),
            (
                "throughput_events_per_sec",
                "rust_throughput_events_per_sec",
            ),
            (
                "throughput_network_bytes_per_sec",
                "rust_throughput_network_bytes_per_sec",
            ),
            (
                "throughput_data_bytes_per_sec",
                "rust_throughput_data_bytes_per_sec",
            ),
            ("throughput_rows_per_sec", "rust_throughput_rows_per_sec"),
        ] {
            if !object.contains_key(target) {
                if let Some(value) = object.get(source).cloned() {
                    object.insert(String::from(target), value);
                }
            }
        }
        if !object.contains_key("not_comparable_reason") {
            if let Some(claim_scope) = object.get("claim_scope").cloned() {
                object.insert(String::from("not_comparable_reason"), claim_scope);
            }
        }
        if !object.contains_key("claim") {
            if let Some(claim) = object.get("claim_eligibility").cloned() {
                object.insert(String::from("claim"), claim);
            }
        }
    }
    entry
}

fn performance_entry_with_metadata(
    mut entry: Value,
    evidence_file: &str,
    row_kind: &str,
    evidence: &Value,
) -> Value {
    if let Some(object) = entry.as_object_mut() {
        let workload = object
            .get("workload")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let legacy = object
            .get("legacy_implementation")
            .or_else(|| object.get("implementation"))
            .and_then(Value::as_str)
            .unwrap_or("baseline")
            .to_string();
        let rust = object
            .get("rust_implementation")
            .and_then(Value::as_str)
            .unwrap_or("target")
            .to_string();
        object.insert(String::from("evidence_file"), json!(evidence_file));
        object.insert(String::from("row_kind"), json!(row_kind));
        object.insert(
            String::from("row_key"),
            json!(format!(
                "{evidence_file}:{row_kind}:{workload}:{legacy}:{rust}"
            )),
        );
        if !object.contains_key("evidence_build_profile") {
            let build_profile = evidence
                .get("build_profile")
                .and_then(Value::as_str)
                .or_else(|| {
                    evidence
                        .pointer("/host/rust_build/build_profile")
                        .and_then(Value::as_str)
                });
            object.insert(String::from("evidence_build_profile"), json!(build_profile));
        }
        if !object.contains_key("evidence_target_cpu_native") {
            object.insert(
                String::from("evidence_target_cpu_native"),
                evidence
                    .pointer("/host/rust_build/target_cpu_native")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        if !object.contains_key("target_cpu_native") {
            object.insert(
                String::from("target_cpu_native"),
                evidence
                    .get("target_cpu_native")
                    .or_else(|| evidence.pointer("/host/rust_build/target_cpu_native"))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        if !object.contains_key("evidence_debug_assertions") {
            object.insert(
                String::from("evidence_debug_assertions"),
                evidence
                    .get("debug_assertions")
                    .or_else(|| evidence.pointer("/host/rust_build/debug_assertions"))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        if !object.contains_key("debug_assertions") {
            object.insert(
                String::from("debug_assertions"),
                evidence
                    .get("debug_assertions")
                    .or_else(|| evidence.pointer("/host/rust_build/debug_assertions"))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        if !object.contains_key("evidence_command") {
            object.insert(
                String::from("evidence_command"),
                evidence.get("command").cloned().unwrap_or(Value::Null),
            );
        }
    }
    entry
}

fn performance_comparability_summary(entries: &[Value]) -> String {
    let comparable = entries
        .iter()
        .filter(|entry| is_legacy_comparable_entry(entry))
        .count();
    let non_comparable = entries.len().saturating_sub(comparable);
    let scheduler_resource_only = entries
        .iter()
        .filter(|entry| {
            entry.get("comparability").and_then(Value::as_str)
                == Some("rust_scheduler_process_not_legacy_comparable")
        })
        .count();
    let scheduler_pressure_only = entries
        .iter()
        .filter(|entry| {
            entry.get("comparability").and_then(Value::as_str)
                == Some("rust_only_generated_pressure_not_legacy_comparable")
        })
        .count();
    let io_bridge_only = entries
        .iter()
        .filter(|entry| {
            entry.get("comparability").and_then(Value::as_str)
                == Some(
                    "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
                )
        })
        .count();
    let rust_only = entries
        .iter()
        .filter(|entry| {
            entry.get("comparability").and_then(Value::as_str)
                == Some("rust_only_in_process_not_legacy_comparable")
        })
        .count();
    let speedup_claims = entries
        .iter()
        .filter(|entry| {
            is_legacy_comparable_entry(entry)
                && entry.get("speedup").is_some()
                && has_speedup_claim_eligible_performance_evidence(entry)
        })
        .count();
    format!(
        "{comparable} comparable legacy-vs-Rust rows; {non_comparable} non-comparable rows shown with reasons ({scheduler_resource_only} scheduler resource-only, {scheduler_pressure_only} scheduler pressure, {io_bridge_only} IO baseline-vs-bridge, {rust_only} Rust-only); {speedup_claims} optimized-baseline speedup claim rows."
    )
}

fn is_legacy_comparable_entry(entry: &Value) -> bool {
    entry.get("comparability").and_then(Value::as_str) == Some("comparable_process_to_process")
}

fn has_native_release_performance_evidence(entry: &Value) -> bool {
    let build_profile = entry
        .get("rust_build_profile")
        .or_else(|| entry.get("build_profile"))
        .or_else(|| entry.get("evidence_build_profile"))
        .and_then(Value::as_str);
    let target_cpu_native = entry
        .get("target_cpu_native")
        .or_else(|| entry.get("evidence_target_cpu_native"))
        .and_then(Value::as_bool);
    let debug_assertions = entry
        .get("debug_assertions")
        .or_else(|| entry.get("evidence_debug_assertions"))
        .and_then(Value::as_bool);

    build_profile == Some("release-native")
        && target_cpu_native == Some(true)
        && debug_assertions == Some(false)
}

fn legacy_build_profile(entry: &Value) -> Option<&str> {
    entry
        .get("legacy_build_profile")
        .or_else(|| entry.get("evidence_legacy_build_profile"))
        .and_then(Value::as_str)
}

fn has_legacy_debug_performance_baseline(entry: &Value) -> bool {
    legacy_build_profile(entry)
        .is_some_and(|profile| profile.to_ascii_lowercase().contains("debug"))
}

fn has_known_non_debug_legacy_performance_baseline(entry: &Value) -> bool {
    if entry.get("legacy_known_non_debug").and_then(Value::as_bool) == Some(true) {
        return true;
    }
    legacy_build_profile(entry).is_some_and(|profile| {
        let profile = profile.to_ascii_lowercase();
        !profile.is_empty() && !profile.contains("unknown") && !profile.contains("debug")
    })
}

fn has_speedup_claim_eligible_performance_evidence(entry: &Value) -> bool {
    has_native_release_performance_evidence(entry)
        && has_known_non_debug_legacy_performance_baseline(entry)
}

fn performance_speedup_text(entry: &Value) -> String {
    let speedup = entry.get("speedup").and_then(json_number);
    if speedup.is_some() {
        format_amount(speedup, "x")
    } else {
        String::from("n/a")
    }
}

fn performance_comparable_text(entry: &Value) -> String {
    if is_legacy_comparable_entry(entry) {
        if has_speedup_claim_eligible_performance_evidence(entry) {
            String::from("yes: optimized legacy vs Rust release-native process")
        } else if has_native_release_performance_evidence(entry)
            && has_legacy_debug_performance_baseline(entry)
        {
            String::from("yes: legacy debug process vs Rust release-native; observed ratio only")
        } else {
            String::from("yes: legacy vs Rust process; not speedup-claim eligible")
        }
    } else {
        let comparability = entry
            .get("comparability")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let native_suffix = if has_native_release_performance_evidence(entry) {
            "; release-native target-cpu=native"
        } else {
            ""
        };
        format!(
            "no: {}{}",
            comparability_label(comparability),
            native_suffix
        )
    }
}

fn performance_claim_text(entry: &Value) -> String {
    let speedup = entry.get("speedup").and_then(json_number);
    if is_legacy_comparable_entry(entry) {
        if !has_native_release_performance_evidence(entry) {
            return String::from(
                "Observed comparable ratio only; not a speedup claim until release-native target-cpu=native evidence with debug assertions off",
            );
        }
        if !has_known_non_debug_legacy_performance_baseline(entry) {
            return String::from(
                "Observed comparable ratio only; not a speedup claim until the legacy baseline is a known non-debug optimized process",
            );
        }
        return match speedup {
            Some(speedup) => {
                format!(
                    "{speedup:.2}x optimized-baseline process-level speedup; CPU/RSS stats observed"
                )
            }
            None => String::from("Comparable baseline row; no speedup value recorded"),
        };
    }

    entry
        .get("not_comparable_reason")
        .and_then(Value::as_str)
        .or_else(|| entry.get("claim_scope").and_then(Value::as_str))
        .or_else(|| entry.get("claim").and_then(Value::as_str))
        .map(|reason| format!("Not comparable: {reason}"))
        .unwrap_or_else(|| String::from("Not comparable: no legacy-equivalent claim"))
}

fn performance_command_text(entry: &Value) -> String {
    if let (Some(legacy), Some(rust)) = (
        entry.get("legacy_command_template").and_then(Value::as_str),
        entry.get("rust_command_template").and_then(Value::as_str),
    ) {
        return format!("legacy: {legacy}; rust: {rust}");
    }
    entry
        .get("command_template")
        .and_then(Value::as_str)
        .or_else(|| entry.get("command").and_then(Value::as_str))
        .unwrap_or("n/a")
        .to_string()
}

fn performance_cpu_ratio(entry: &Value) -> Option<f64> {
    number_at(
        entry,
        &[
            "resource_comparison",
            "cpu_burn_effective_ratio_legacy_over_rust",
        ],
    )
    .or_else(|| {
        number_at(
            entry,
            &[
                "resource_comparison",
                "cpu_burn_effective_ratio_baseline_over_scheduler_bridge",
            ],
        )
    })
}

fn performance_rss_ratio(entry: &Value) -> Option<f64> {
    number_at(
        entry,
        &["resource_comparison", "peak_rss_ratio_rust_over_legacy_p95"],
    )
    .or_else(|| {
        number_at(
            entry,
            &[
                "resource_comparison",
                "peak_rss_ratio_scheduler_bridge_over_baseline_p95",
            ],
        )
    })
}

fn comparability_label(value: &str) -> &'static str {
    match value {
        "comparable_process_to_process" => "legacy vs Rust",
        "rust_only_in_process_not_legacy_comparable" => {
            "Rust-only in-process, no matched legacy sample"
        }
        "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io" => {
            "baseline vs bridge, not legacy Carbon IO"
        }
        "rust_scheduler_process_not_legacy_comparable" => {
            "Rust scheduler process resource evidence, no matched legacy sample"
        }
        "rust_only_generated_pressure_not_legacy_comparable" => {
            "Rust scheduler pressure evidence, no matched legacy sample"
        }
        _ => "unknown comparability",
    }
}

fn comparison_throughput(comparison: &Value, prefix: &str) -> String {
    comparison_throughput_value(comparison, prefix)
        .map(|(unit, value)| format_amount(Some(value), &unit))
        .unwrap_or_else(|| String::from("n/a"))
}

fn comparison_throughput_value(comparison: &Value, prefix: &str) -> Option<(String, f64)> {
    let preferred_suffixes = [
        "requests_per_sec",
        "operations_per_sec",
        "directories_per_sec",
        "filter_mappings_per_sec",
        "groups_per_sec",
        "diffs_per_sec",
        "removes_per_sec",
        "bundles_per_sec",
        "patches_per_sec",
        "bundles_unpacked_per_sec",
        "patches_applied_per_sec",
        "events_per_sec",
        "network_bytes_per_sec",
        "data_bytes_per_sec",
        "rows_per_sec",
        "paths_per_sec",
        "bytes_per_sec",
    ];
    for suffix in preferred_suffixes {
        let key = format!("{prefix}_throughput_{suffix}");
        if let Some(value) = comparison.get(&key).and_then(json_number) {
            return Some((
                suffix.trim_end_matches("_per_sec").replace('_', " ") + "/sec",
                value,
            ));
        }
    }
    if prefix == "rust" {
        for suffix in preferred_suffixes {
            let key = format!("throughput_{suffix}");
            if let Some(value) = comparison.get(&key).and_then(json_number) {
                return Some((
                    suffix.trim_end_matches("_per_sec").replace('_', " ") + "/sec",
                    value,
                ));
            }
        }
    }
    None
}

fn duration_pair(comparison: &Value) -> String {
    if comparison.get("row_kind").and_then(Value::as_str) == Some("workload") {
        let observed = comparison.get("duration_ms").and_then(json_number);
        return format!("Observed {}", format_amount(observed, "ms"));
    }
    let legacy = comparison
        .get("legacy_duration_us")
        .and_then(json_number)
        .map(|value| value / 1000.0);
    let rust = comparison
        .get("rust_duration_us")
        .and_then(json_number)
        .map(|value| value / 1000.0);
    format_pair(comparison, legacy, rust, "ms")
}

fn latency_pair(comparison: &Value) -> String {
    let legacy_p50 =
        number_at(comparison, &["legacy_sample_stats_us", "p50"]).map(|value| value / 1000.0);
    let legacy_p95 =
        number_at(comparison, &["legacy_sample_stats_us", "p95"]).map(|value| value / 1000.0);
    let rust_p50 =
        number_at(comparison, &["rust_sample_stats_us", "p50"]).map(|value| value / 1000.0);
    let rust_p95 =
        number_at(comparison, &["rust_sample_stats_us", "p95"]).map(|value| value / 1000.0);
    let baseline = comparison_baseline_label(comparison);
    let target = comparison_target_label(comparison);
    format!(
        "{baseline} p50 {}, p95 {}; {target} p50 {}, p95 {}",
        format_amount(legacy_p50, "ms"),
        format_amount(legacy_p95, "ms"),
        format_amount(rust_p50, "ms"),
        format_amount(rust_p95, "ms")
    )
}

fn cpu_burn_pair(comparison: &Value) -> String {
    format_pair(
        comparison,
        number_at(
            comparison,
            &["legacy_process_stats", "cpu_burn_effective_ms", "mean"],
        ),
        number_at(
            comparison,
            &["rust_process_stats", "cpu_burn_effective_ms", "mean"],
        ),
        "ms effective mean",
    )
}

fn cpu_percent_pair(comparison: &Value) -> String {
    format_pair(
        comparison,
        number_at(comparison, &["legacy_process_stats", "cpu_percent", "mean"]),
        number_at(comparison, &["rust_process_stats", "cpu_percent", "mean"]),
        "%",
    )
}

fn rss_pair(comparison: &Value) -> String {
    format_pair(
        comparison,
        number_at(comparison, &["legacy_process_stats", "max_rss_kb", "p95"]),
        number_at(comparison, &["rust_process_stats", "max_rss_kb", "p95"]),
        "KB p95",
    )
}

fn scaled_pair(comparison: &Value) -> String {
    let legacy_wall = number_at(
        comparison,
        &[
            "resource_comparison",
            "linear_scale_estimate_100k_units",
            "legacy_wall_seconds",
        ],
    );
    let rust_wall = number_at(
        comparison,
        &[
            "resource_comparison",
            "linear_scale_estimate_100k_units",
            "rust_wall_seconds",
        ],
    );
    let legacy_cpu = number_at(
        comparison,
        &[
            "resource_comparison",
            "linear_scale_estimate_100k_units",
            "legacy_cpu_burn_seconds",
        ],
    );
    let rust_cpu = number_at(
        comparison,
        &[
            "resource_comparison",
            "linear_scale_estimate_100k_units",
            "rust_cpu_burn_seconds",
        ],
    );
    format!(
        "Wall {}; CPU {}",
        format_pair(comparison, legacy_wall, rust_wall, "s"),
        format_pair(comparison, legacy_cpu, rust_cpu, "s")
    )
}

fn format_pair(comparison: &Value, legacy: Option<f64>, rust: Option<f64>, unit: &str) -> String {
    let baseline = comparison_baseline_label(comparison);
    let target = comparison_target_label(comparison);
    format!(
        "{baseline} {}; {target} {}",
        format_amount(legacy, unit),
        format_amount(rust, unit)
    )
}

fn comparison_baseline_label(comparison: &Value) -> &'static str {
    match comparison.get("comparability").and_then(Value::as_str) {
        Some("comparable_process_to_process") => "Legacy",
        _ => "Baseline",
    }
}

fn comparison_target_label(comparison: &Value) -> &'static str {
    match comparison.get("comparability").and_then(Value::as_str) {
        Some("comparable_process_to_process") => "Rust",
        _ => "Target",
    }
}

fn format_amount(value: Option<f64>, unit: &str) -> String {
    match value {
        Some(value) if value.abs() >= 1000.0 => format!("{value:.0} {unit}"),
        Some(value) if value.abs() >= 10.0 => format!("{value:.1} {unit}"),
        Some(value) => format!("{value:.2} {unit}"),
        None => String::from("n/a"),
    }
}

fn number_at(value: &Value, path: &[&str]) -> Option<f64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    json_number(current)
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToOwned::to_owned)
}

fn json_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_u64().map(|value| value as f64))
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_semantic_trace_fixture_corpus_validates() {
        let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/io");
        let evidence = load_io_semantic_trace_fixture_evidence(&fixture_dir)
            .expect("load IO semantic trace fixtures");

        assert_eq!(evidence.get("status").and_then(Value::as_str), Some("pass"));
        assert_eq!(
            evidence.get("fixture_count").and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            evidence.get("parity_status").and_then(Value::as_str),
            Some("not_legacy_comparable")
        );
        assert_eq!(
            evidence
                .pointer("/kind_counts/socket")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            evidence.pointer("/kind_counts/ssl").and_then(Value::as_u64),
            Some(2)
        );
    }

    #[test]
    fn io_semantic_trace_fixture_rejects_timing_fields() {
        let fixture = json!({
            "schema": "carbon.io.semantic_trace_fixture.v1",
            "fixture": "timing_regression",
            "kind": "socket",
            "source_refs": ["carbonengine/io/src/socketmodule.cpp"],
            "expected_events": [
                {"event": "socket.recv.block", "duration_us": 10}
            ],
            "required_order": ["socket.recv.block"],
            "timings_excluded": true
        });

        let error =
            validate_io_semantic_trace_fixture(Path::new("fixtures/io/timing.json"), &fixture)
                .expect_err("timing fields must be rejected");

        assert!(error.to_string().contains("duration_us"));
    }

    #[test]
    fn comparison_throughput_value_recognizes_bundle_and_patch_units() {
        let comparison = json!({
            "legacy_throughput_bundles_per_sec": 12.5,
            "rust_throughput_bundles_per_sec": 20.0
        });
        assert_eq!(
            comparison_throughput_value(&comparison, "legacy"),
            Some((String::from("bundles/sec"), 12.5))
        );
        assert_eq!(
            comparison_throughput_value(&comparison, "rust"),
            Some((String::from("bundles/sec"), 20.0))
        );

        let patch_comparison = json!({
            "legacy_throughput_patches_applied_per_sec": 3.0,
            "rust_throughput_patches_applied_per_sec": 6.0
        });
        assert_eq!(
            comparison_throughput_value(&patch_comparison, "legacy"),
            Some((String::from("patches applied/sec"), 3.0))
        );
        assert_eq!(
            comparison_throughput_value(&patch_comparison, "rust"),
            Some((String::from("patches applied/sec"), 6.0))
        );
    }

    #[test]
    fn extended_sample_stats_record_p999_and_tail_ratio() {
        let stats = sample_stats_us_extended(&[1, 2, 3, 4, 1000]);

        assert_eq!(stats.get("count").and_then(Value::as_u64), Some(5));
        assert_eq!(stats.get("p50").and_then(Value::as_u64), Some(3));
        assert_eq!(stats.get("p99_9").and_then(Value::as_u64), Some(1000));
        assert_eq!(
            number_at(&stats, &["tail_ratio_p99_over_p50"]),
            Some(1000.0 / 3.0)
        );
    }

    #[test]
    fn throughput_stability_summary_records_cv_and_window_percentiles() {
        let summary = throughput_stability_summary(&[100.0, 110.0, 90.0, 100.0]);

        assert_eq!(summary.get("window_count").and_then(Value::as_u64), Some(4));
        assert_eq!(number_at(&summary, &["p5"]), Some(90.0));
        assert_eq!(number_at(&summary, &["p95"]), Some(110.0));
        assert!(number_at(&summary, &["coefficient_of_variation"])
            .is_some_and(|value| value > 0.07 && value < 0.08));
    }

    #[test]
    fn scalability_summary_extracts_peak_pressure_metrics() {
        let rows = vec![
            json!({
                "family": "io",
                "throughput_network_bytes_per_sec": 12_000_000,
                "latency_us_extended": {"p99": 700, "p99_9": 900},
                "process_stats": {
                    "max_rss_kb": {"p95": 20_000},
                    "cpu_percent": {"p95": 80.0}
                },
                "stability": {"coefficient_of_variation": 0.05}
            }),
            json!({
                "family": "data",
                "throughput_data_bytes_per_sec": 30_000_000,
                "throughput_operations_per_sec": 2_000,
                "latency_us_extended": {"p99": 1000, "p99_9": 1200},
                "process_stats": {
                    "max_rss_kb": {"p95": 40_000},
                    "cpu_percent": {"p95": 120.0}
                },
                "stability": {"coefficient_of_variation": 0.20}
            }),
        ];

        let summary = scalability_summary(&rows);

        assert_eq!(summary.get("row_count").and_then(Value::as_u64), Some(2));
        assert_eq!(
            number_at(&summary, &["peak_network_bytes_per_sec"]),
            Some(12_000_000.0)
        );
        assert_eq!(
            number_at(&summary, &["peak_data_bytes_per_sec"]),
            Some(30_000_000.0)
        );
        assert_eq!(
            number_at(&summary, &["worst_latency_p99_9_us"]),
            Some(1200.0)
        );
        assert_eq!(
            summary
                .get("stable_rows_cv_le_10_percent")
                .and_then(Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn parses_legacy_baseline_ctest_output() {
        let stdout = r#"
Test project /tmp/legacy-scheduler-build
      Start 1: capiTests.Scheduler.Run
1/2 Test #1: capiTests.Scheduler.Run ........   Passed    0.01 sec
      Start 2: test_scheduler.TestSchedule.test_schedule
2/2 Test #2: test_scheduler.TestSchedule.test_schedule ...   Passed    0.02 sec

100% tests passed, 0 tests failed out of 2

Total Test time (real) =   0.03 sec
"#;

        let summary = parse_ctest_summary_from_text(stdout).expect("parse CTest summary");

        assert_eq!(
            summary,
            CTestSummary {
                passed: 2,
                failed: 0,
                total: 2
            }
        );
        assert_eq!(parse_ctest_summary(stdout), (Some(2), Some(0)));
    }

    #[test]
    fn parses_python_unittest_summary() {
        let stderr = r#"
...............................ss.s..s..........................sss...............................
----------------------------------------------------------------------
Ran 210 tests in 0.435s

OK (skipped=7)
"#;

        let summary = parse_python_unittest_summary("", stderr);

        assert_eq!(summary.get("ran").and_then(Value::as_u64), Some(210));
        assert_eq!(summary.get("skipped").and_then(Value::as_u64), Some(7));
        assert_eq!(summary.get("ok").and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn progress_remaining_items_show_not_report_ready_reason() {
        let value = json!({
            "not_report_ready_reason": "benchmark scope is preliminary"
        });

        let html = render_progress_remaining_items(&value, false);

        assert!(html.contains("benchmark scope is preliminary"));
        assert!(!html.contains("see blocker codes"));
    }

    #[test]
    fn scheduler_cannot_be_report_ready_while_claimed_parity_gates_missing() {
        let evidence = json!({
            "gate": "rust-scheduler-python",
            "component": "scheduler",
            "status": "pass",
            "report_ready": true,
            "coverage": "claimed_scheduler_python_parity",
            "unchanged_legacy_subset": ["test_scheduler.TestCAPIExposure.test_has_capi_attribute"],
            "unchanged_legacy_subset_count": 1,
            "remaining_before_report_ready": [
                "remaining scheduler Python tests passing unchanged against the Rust extension"
            ]
        });

        let blockers = report_ready_blockers("rust-scheduler-python.json", &evidence);
        let codes = blocker_codes(&blockers);

        assert!(codes.contains(&String::from("remaining_report_ready_work")));
        assert!(codes.contains(&String::from("scheduler_core_ownership_not_complete")));
        assert!(!blockers.is_empty());
    }

    #[test]
    fn validates_native_release_evidence_for_speedup_claims() {
        let evidence = json!({
            "gate": "bench-tier-local",
            "status": "pass",
            "report_ready": true,
            "build_profile": "debug",
            "host": {
                "rust_build": {
                    "build_profile": "debug",
                    "target_cpu_native": false,
                    "debug_assertions": true
                }
            },
            "comparisons": [{
                "workload": "create_group_directory_yaml",
                "comparability": "comparable_process_to_process",
                "legacy_build_profile": "legacy_resources_debug",
                "speedup": 1.25
            }]
        });

        let codes = blocker_codes(&report_ready_blockers("bench-tier-local.json", &evidence));

        assert!(codes.contains(&String::from("performance_evidence_debug_build")));
        assert!(codes.contains(&String::from("performance_evidence_not_native")));
        assert!(codes.contains(&String::from("performance_evidence_debug_assertions")));
        assert!(codes.contains(&String::from("performance_legacy_baseline_not_optimized")));
    }

    #[test]
    fn speedup_claim_text_requires_native_release_metadata() {
        let row = json!({
            "component": "resources",
            "workload": "demo_workload",
            "comparability": "comparable_process_to_process",
            "legacy_implementation": "legacy_cpp_cli",
            "rust_implementation": "rust_xtask_process",
            "speedup": 12.0
        });
        let debug_evidence = json!({
            "build_profile": "debug",
            "target_cpu_native": false,
            "debug_assertions": true
        });
        let debug_row = performance_entry_with_metadata(
            row.clone(),
            "bench-tier-local.json",
            "comparison",
            &debug_evidence,
        );

        assert!(!has_native_release_performance_evidence(&debug_row));
        assert!(performance_claim_text(&debug_row).contains("not a speedup claim"));
        assert!(performance_comparability_summary(&[debug_row])
            .contains("0 optimized-baseline speedup claim rows"));

        let native_evidence = json!({
            "build_profile": "release-native",
            "target_cpu_native": true,
            "debug_assertions": false
        });
        let native_debug_legacy_row = performance_entry_with_metadata(
            json!({
                "component": "resources",
                "workload": "demo_workload",
                "comparability": "comparable_process_to_process",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_build_profile": "legacy_resources_debug",
                "speedup": 12.0
            }),
            "bench-tier-local.json",
            "comparison",
            &native_evidence,
        );
        assert!(has_native_release_performance_evidence(
            &native_debug_legacy_row
        ));
        assert!(performance_claim_text(&native_debug_legacy_row)
            .contains("legacy baseline is a known non-debug optimized process"));

        let native_row = performance_entry_with_metadata(
            json!({
                "component": "resources",
                "workload": "demo_workload",
                "comparability": "comparable_process_to_process",
                "legacy_implementation": "legacy_cpp_cli",
                "rust_implementation": "rust_xtask_process",
                "legacy_build_profile": "legacy_resources_release",
                "speedup": 12.0
            }),
            "bench-tier-local.json",
            "comparison",
            &native_evidence,
        );

        assert!(has_native_release_performance_evidence(&native_row));
        assert!(has_speedup_claim_eligible_performance_evidence(&native_row));
        assert!(performance_claim_text(&native_row)
            .contains("12.00x optimized-baseline process-level speedup"));
    }

    #[test]
    fn legacy_resources_cli_detector_gates_optimized_baseline_claims() {
        let root = PathBuf::from("target/carbon/xtask-tests/legacy-resources-cli-detector");
        fs::remove_dir_all(&root).ok();
        let debug_cli = root.join(".cmake-build-debug/cli/resources_debug");
        let release_cli = root.join(".cmake-build-release/cli/resources");
        fs::create_dir_all(debug_cli.parent().unwrap()).expect("create debug cli dir");
        fs::create_dir_all(release_cli.parent().unwrap()).expect("create release cli dir");
        fs::write(&debug_cli, "").expect("write debug cli");
        fs::write(&release_cli, "").expect("write release cli");
        fs::write(
            root.join(".cmake-build-debug/CMakeCache.txt"),
            "CMAKE_BUILD_TYPE:STRING=Debug\nDEV_FEATURES:BOOL=OFF\n",
        )
        .expect("write debug cache");
        fs::write(
            root.join(".cmake-build-release/CMakeCache.txt"),
            "CMAKE_BUILD_TYPE:STRING=Release\nDEV_FEATURES:BOOL=ON\n",
        )
        .expect("write release cache");

        let debug = legacy_resources_cli_baseline_metadata(
            debug_cli,
            String::from("workspace_scan"),
            Some("legacy_default_debug"),
        );
        let release = legacy_resources_cli_baseline_metadata(
            release_cli,
            String::from("workspace_scan"),
            None,
        );

        assert!(!debug.known_non_debug);
        assert_eq!(debug.build_profile, "legacy_default_debug");
        assert!(release.known_non_debug);
        assert_eq!(release.build_profile, "legacy_resources_release");

        let candidates = vec![release.clone(), debug.clone()];
        let readiness = benchmark_optimization_readiness(true, 7, 2, &release, &debug, &candidates);

        assert_eq!(
            readiness
                .get("speedup_claim_eligible_comparisons")
                .and_then(Value::as_u64),
            Some(7)
        );
        assert_eq!(
            readiness
                .get("observed_only_comparisons")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            readiness
                .pointer("/legacy_optimized_baseline_detection/status")
                .and_then(Value::as_str),
            Some("optimized_candidate_available_not_selected")
        );
    }

    #[test]
    fn performance_entries_preserve_native_scheduler_and_io_resource_rows() {
        let bench = json!({
            "gate": "bench-tier-local",
            "status": "pass",
            "build_profile": "release-native",
            "target_cpu_native": true,
            "debug_assertions": false,
            "host": {
                "rust_build": {
                    "build_profile": "release-native",
                    "target_cpu_native": true,
                    "debug_assertions": false
                }
            },
            "workloads": [{
                "component": "scheduler",
                "workload": "run_order_fixture_rust_core_process",
                "implementation": "rust_xtask_process",
                "build_profile": "release-native",
                "target_cpu_native": true,
                "debug_assertions": false,
                "duration_ms": 4,
                "rust_sample_stats_us": {"count": 3, "p50": 100, "p95": 200},
                "rust_process_stats": {
                    "cpu_burn_effective_ms": {"mean": 3.0},
                    "cpu_percent": {"mean": 98.0},
                    "max_rss_kb": {"p95": 4096}
                },
                "throughput_events_per_sec": 2000,
                "comparability": "rust_scheduler_process_not_legacy_comparable",
                "not_comparable_reason": "scheduler process row",
                "claim": "scheduler_resource_efficiency_evidence_only_no_speedup_claim"
            }]
        });
        let io = json!({
            "gate": "io-workloads",
            "status": "pass",
            "build_profile": "release-native",
            "target_cpu_native": true,
            "debug_assertions": false,
            "host": {
                "rust_build": {
                    "build_profile": "release-native",
                    "target_cpu_native": true,
                    "debug_assertions": false
                }
            },
            "comparisons": [{
                "component": "io",
                "workload": "socket_loopback_request_cycles",
                "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
                "legacy_implementation": "python_stdlib_baseline",
                "rust_implementation": "rust_scheduler_python_bridge",
                "rust_build_profile": "release-native",
                "target_cpu_native": true,
                "debug_assertions": false,
                "legacy_sample_stats_us": {"count": 1, "p50": 1000, "p95": 1000},
                "rust_sample_stats_us": {"count": 1, "p50": 1200, "p95": 1200},
                "legacy_process_stats": {
                    "cpu_burn_effective_ms": {"mean": 2.0},
                    "cpu_percent": {"mean": 80.0},
                    "max_rss_kb": {"p95": 9000}
                },
                "rust_process_stats": {
                    "cpu_burn_effective_ms": {"mean": 2.4},
                    "cpu_percent": {"mean": 95.0},
                    "max_rss_kb": {"p95": 10000}
                },
                "legacy_throughput_requests_per_sec": 100,
                "rust_throughput_requests_per_sec": 90
            }]
        });

        let entries = performance_entries_from_sources([
            ("bench-tier-local.json", &bench),
            ("io-workloads.json", &io),
        ]);
        let scheduler = entries
            .iter()
            .find(|entry| entry.get("component").and_then(Value::as_str) == Some("scheduler"))
            .expect("scheduler performance row");
        let io_row = entries
            .iter()
            .find(|entry| entry.get("component").and_then(Value::as_str) == Some("io"))
            .expect("io performance row");

        assert_eq!(
            scheduler.get("target_cpu_native").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            scheduler.get("debug_assertions").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            number_at(
                scheduler,
                &["rust_process_stats", "cpu_burn_effective_ms", "mean"]
            ),
            Some(3.0)
        );
        assert_eq!(
            io_row.get("target_cpu_native").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            io_row.get("rust_build_profile").and_then(Value::as_str),
            Some("release-native")
        );

        let rows = render_performance_rows(&entries);
        assert!(rows.contains("release-native target-cpu=native"));
        assert!(rows.contains("scheduler process resource evidence"));
        assert!(rows.contains("3.00 ms effective mean"));
        let summary = performance_comparability_summary(&entries);
        assert!(summary.contains("1 scheduler resource-only"));
        assert!(summary.contains("1 IO baseline-vs-bridge"));
    }

    #[test]
    fn legacy_build_status_controls_report_readiness() {
        let incomplete = json!({
            "gate": "legacy-scheduler",
            "status": "pass",
            "report_ready": true,
            "coverage": "legacy_scheduler_cmake_ctest",
            "legacy_build_status": {
                "configure": "pass",
                "build": "pass",
                "ctest": "pass",
                "baseline_complete": false
            }
        });

        let incomplete_codes =
            blocker_codes(&report_ready_blockers("legacy-scheduler.json", &incomplete));
        assert!(incomplete_codes.contains(&String::from("legacy_scheduler_baseline_incomplete")));
        assert!(incomplete_codes.contains(&String::from("legacy_scheduler_ctest_summary_missing")));

        let complete_status = legacy_scheduler_build_status(
            LegacySchedulerMode::BuildRun,
            true,
            true,
            true,
            Some(true),
            Some(true),
            Some(true),
            Some(CTestSummary {
                passed: 2,
                failed: 0,
                total: 2,
            }),
        );
        let complete = json!({
            "gate": "legacy-scheduler",
            "status": "pass",
            "report_ready": true,
            "coverage": "legacy_scheduler_cmake_ctest",
            "legacy_build_status": complete_status,
            "remaining_before_report_ready": []
        });

        assert!(report_ready_blockers("legacy-scheduler.json", &complete).is_empty());
    }

    #[test]
    fn imports_supported_legacy_scheduler_ctest_log_as_report_ready() {
        let args = LegacySchedulerImportArgs {
            artifact_path: PathBuf::from("legacy-ctest.log"),
            host_os: Some(String::from("windows")),
            host_arch: Some(String::from("x64")),
            source_command: Some(String::from("ctest --output-on-failure")),
        };
        let evidence = legacy_scheduler_import_evidence(
            &args,
            "Test project C:/carbon/scheduler\n100% tests passed, 0 tests failed out of 12\n",
            Instant::now(),
        )
        .expect("import evidence");

        assert_eq!(evidence.get("status").and_then(Value::as_str), Some("pass"));
        assert_eq!(
            evidence.get("report_ready").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            evidence
                .pointer("/legacy_build_status/tests_total")
                .and_then(Value::as_u64),
            Some(12)
        );
        assert!(report_ready_blockers("legacy-scheduler.json", &evidence).is_empty());
    }

    #[test]
    fn imported_legacy_scheduler_baseline_requires_supported_host() {
        let args = LegacySchedulerImportArgs {
            artifact_path: PathBuf::from("legacy-ctest.log"),
            host_os: Some(String::from("linux")),
            host_arch: Some(String::from("x86_64")),
            source_command: None,
        };
        let evidence = legacy_scheduler_import_evidence(
            &args,
            "100% tests passed, 0 tests failed out of 12\n",
            Instant::now(),
        )
        .expect("import evidence");
        let codes = blocker_codes(&report_ready_blockers("legacy-scheduler.json", &evidence));

        assert_eq!(evidence.get("status").and_then(Value::as_str), Some("pass"));
        assert_eq!(
            evidence.get("report_ready").and_then(Value::as_bool),
            Some(false)
        );
        assert!(codes.contains(&String::from("legacy_scheduler_baseline_incomplete")));
        assert!(codes.contains(&String::from("remaining_report_ready_work")));
    }

    #[test]
    fn unchanged_legacy_subset_count_must_match_list() {
        assert_eq!(
            rust_scheduler_unchanged_legacy_subset_count(),
            RUST_SCHEDULER_UNCHANGED_LEGACY_SUBSET.len()
        );

        let evidence = json!({
            "gate": "rust-scheduler-python",
            "status": "pass",
            "report_ready": true,
            "coverage": "unchanged_legacy_subset",
            "unchanged_legacy_subset": [
                "test_scheduler.TestCAPIExposure.test_has_capi_attribute",
                "test_tasklet.TestTasklets.test_run"
            ],
            "unchanged_legacy_subset_count": 1,
            "remaining_before_report_ready": []
        });

        let codes = blocker_codes(&report_ready_blockers(
            "rust-scheduler-python.json",
            &evidence,
        ));

        assert!(codes.contains(&String::from("rust_scheduler_subset_count_mismatch")));
        assert!(codes.contains(&String::from("scheduler_core_ownership_not_complete")));
    }

    #[test]
    fn final_html_report_includes_interactive_performance_dashboard() {
        let evidence = vec![
            (
                "scheduler-fixtures.json",
                json!({
                    "gate": "scheduler-fixtures",
                    "status": "pass",
                    "report_ready": true,
                    "passed": 1,
                    "fixture_count": 1
                }),
            ),
            (
                "bench-tier-local.json",
                json!({
                    "gate": "bench-tier-local",
                    "status": "pass",
                    "report_ready": true,
                    "build_profile": "release-native",
                    "target_cpu_native": true,
                    "debug_assertions": false,
                    "comparisons": [{
                        "component": "resources",
                        "workload": "demo_workload",
                        "comparability": "comparable_process_to_process",
                        "legacy_implementation": "legacy_cpp_cli",
                        "rust_implementation": "rust_xtask_process",
                        "legacy_build_profile": "legacy_resources_release",
                        "rust_build_profile": "release-native",
                        "target_cpu_native": true,
                        "debug_assertions": false,
                        "legacy_duration_us": 2000,
                        "rust_duration_us": 1000,
                        "legacy_throughput_groups_per_sec": 500,
                        "rust_throughput_groups_per_sec": 1000,
                        "speedup": 2.0,
                        "parity_status": "pass",
                        "legacy_sample_stats_us": {"count": 1, "p50": 2000, "p95": 2000},
                        "rust_sample_stats_us": {"count": 1, "p50": 1000, "p95": 1000},
                        "legacy_process_stats": {
                            "cpu_burn_effective_ms": {"mean": 2.0},
                            "cpu_percent": {"mean": 100.0},
                            "max_rss_kb": {"p95": 8000}
                        },
                        "rust_process_stats": {
                            "cpu_burn_effective_ms": {"mean": 1.0},
                            "cpu_percent": {"mean": 100.0},
                            "max_rss_kb": {"p95": 4000}
                        },
                        "resource_comparison": {
                            "linear_scale_estimate_100k_units": {
                                "legacy_wall_seconds": 200.0,
                                "rust_wall_seconds": 100.0
                            }
                        }
                    }]
                }),
            ),
        ];

        let html = render_html_report(&evidence).expect("render final HTML report");

        assert!(html.contains("performanceComparisons"));
        assert!(html.contains("feature-filter"));
        assert!(html.contains("performance-chart"));
        assert!(html.contains("Feature Performance Summary"));
        assert!(html.contains("feature-summary-table"));
        assert!(html.contains("speedup claim-ready"));
        assert!(html.contains("speedup claim"));
        assert!(html.contains("mini-bar"));
        assert!(html.contains("data-row-key="));
        assert!(html.contains("demo_workload"));
        assert!(html.contains("Architecture Improvements"));
        assert!(html.contains("Scheduler core ownership drain"));
        assert!(html.contains("Build And Optimization Readiness"));
        assert!(html.contains("Sampled local IO evidence lane"));
        assert!(html.contains("Rayon CPU stages"));
    }

    #[test]
    fn progress_html_report_includes_interactive_performance_dashboard() {
        let evidence = vec![
            (
                "scheduler-fixtures.json",
                Some(json!({
                    "gate": "scheduler-fixtures",
                    "status": "pass",
                    "report_ready": false,
                    "passed": 41,
                    "fixture_count": 41,
                    "remaining_before_report_ready": ["promote fixture coverage"]
                })),
            ),
            (
                "rust-resources.json",
                Some(json!({
                    "gate": "rust-resources",
                    "status": "pass",
                    "report_ready": false,
                    "covered_behaviors": ["catalog", "bundle", "patch"],
                    "remaining_before_report_ready": ["broader corpus"]
                })),
            ),
            (
                "rust-scheduler-python.json",
                Some(json!({
                    "gate": "rust-scheduler-python",
                    "status": "pass",
                    "report_ready": false,
                    "core_ownership_status": {
                        "status": "partial",
                        "target_state": "carbon-scheduler-core owns scheduler state; PyO3 holds compatibility wrappers",
                        "migration_blocker": "move lifecycle state behind Rust handles",
                        "already_rust_owned": [
                            "semantic fixture runner uses Rust-owned IDs",
                            "CoreScheduler tasklet snapshots mirror alive/scheduled/paused/times_switched_to"
                        ],
                        "still_bridge_owned": ["Python tasklet object still stores callable/args/kwargs and Greenlet continuation state"]
                    }
                })),
            ),
            (
                "io-workloads.json",
                Some(json!({
                    "gate": "io-workloads",
                    "status": "pass",
                    "report_ready": false,
                    "workloads": [],
                    "comparisons": [{
                        "component": "io",
                        "workload": "socket_loopback_request_cycles",
                        "comparability": "same_python_loopback_baseline_vs_rust_scheduler_bridge_not_legacy_carbon_io",
                        "baseline_implementation": "python_stdlib_baseline",
                        "target_implementation": "rust_scheduler_python_bridge",
                        "legacy_duration_us": 3000,
                        "rust_duration_us": 2500,
                        "legacy_throughput_requests_per_sec": 333,
                        "rust_throughput_requests_per_sec": 400,
                        "sample_count": 300,
                        "process_sample_count": 3,
                        "legacy_process_sample_count": 3,
                        "rust_process_sample_count": 3,
                        "requests": 300,
                        "requests_per_run": 100,
                        "payload_bytes": 128,
                        "legacy_sample_stats_us": {"count": 3, "p50": 30, "p95": 50, "p99": 60},
                        "rust_sample_stats_us": {"count": 3, "p50": 20, "p95": 40, "p99": 45},
                        "legacy_process_stats": {
                            "sample_count": 3,
                            "cpu_burn_effective_ms": {"mean": 3.0},
                            "cpu_percent": {"mean": 90.0},
                            "max_rss_kb": {"p95": 9000}
                        },
                        "rust_process_stats": {
                            "sample_count": 3,
                            "cpu_burn_effective_ms": {"mean": 2.5},
                            "cpu_percent": {"mean": 95.0},
                            "max_rss_kb": {"p95": 9500}
                        },
                        "resource_comparison": {
                            "linear_scale_estimate_100k_units": {
                                "legacy_wall_seconds": 300.0,
                                "rust_wall_seconds": 250.0,
                                "legacy_cpu_burn_seconds": 300.0,
                                "rust_cpu_burn_seconds": 250.0
                            }
                        }
                    }],
                    "scheduler_capi_semantic_smoke": {"status": "pass"},
                    "semantic_trace_fixtures": {
                        "status": "pass",
                        "fixture_count": 3,
                        "comparability": "fixture_schema_only_not_legacy_carbon_io"
                    },
                    "legacy_carbonio_semantic_traces": {"legacy_carbonio_trace_status": "blocked"},
                    "remaining_before_report_ready": ["legacy carbonio traces"]
                })),
            ),
            (
                "bench-tier-local.json",
                Some(json!({
                    "gate": "bench-tier-local",
                    "status": "pass",
                    "report_ready": false,
                    "build_profile": "release-native",
                    "target_cpu_native": true,
                    "debug_assertions": false,
                    "comparisons": [{
                        "component": "resources",
                        "workload": "create_group_directory_yaml",
                        "comparability": "comparable_process_to_process",
                        "row_kind": "comparison",
                        "legacy_implementation": "legacy_cpp_cli",
                        "rust_implementation": "rust_xtask_process",
                        "legacy_duration_us": 2000,
                        "rust_duration_us": 1000,
                        "legacy_throughput_groups_per_sec": 500,
                        "rust_throughput_groups_per_sec": 1000,
                        "speedup": 2.0,
                        "legacy_build_profile": "legacy_resources_debug",
                        "rust_build_profile": "release-native",
                        "target_cpu_native": true,
                        "debug_assertions": false,
                        "legacy_sample_stats_us": {"count": 5, "p50": 2000, "p95": 2200, "p99": 2300},
                        "rust_sample_stats_us": {"count": 5, "p50": 1000, "p95": 1200, "p99": 1300},
                        "legacy_process_stats": {
                            "cpu_burn_effective_ms": {"mean": 2.0},
                            "cpu_percent": {"mean": 100.0},
                            "max_rss_kb": {"p95": 8000}
                        },
                        "rust_process_stats": {
                            "cpu_burn_effective_ms": {"mean": 1.0},
                            "cpu_percent": {"mean": 100.0},
                            "max_rss_kb": {"p95": 4000}
                        },
                        "resource_comparison": {
                            "linear_scale_estimate_100k_units": {
                                "legacy_wall_seconds": 200.0,
                                "rust_wall_seconds": 100.0,
                                "legacy_cpu_burn_seconds": 200.0,
                                "rust_cpu_burn_seconds": 100.0
                            }
                        }
                    }],
                    "workloads": [{
                        "component": "scheduler",
                        "workload": "run_order_fixture_rust_core",
                        "comparability": "rust_only_in_process_not_legacy_comparable",
                        "row_kind": "workload",
                        "implementation": "rust",
                        "duration_ms": 10,
                        "throughput_events_per_sec": 1000,
                        "sample_count": 1,
                        "build_profile": "release-native",
                        "target_cpu_native": true,
                        "debug_assertions": false
                    }]
                })),
            ),
        ];

        let html = render_progress_report(&evidence).expect("render progress HTML report");

        assert!(html.contains("performanceComparisons"));
        assert!(html.contains("feature-filter"));
        assert!(html.contains("family-filter"));
        assert!(html.contains("row-kind-filter"));
        assert!(html.contains("build-filter"));
        assert!(html.contains("metric-filter"));
        assert!(html.contains("sort-filter"));
        assert!(html.contains("performance-chart"));
        assert!(html.contains("performance-kpis"));
        assert!(html.contains("selected-detail"));
        assert!(html.contains("performance-table"));
        assert!(html.contains("Feature Performance Summary"));
        assert!(html.contains("feature-summary-table"));
        assert!(html.contains("observed only"));
        assert!(html.contains("no speedup claim"));
        assert!(html.contains("mini-bar"));
        assert!(html.contains("bestObservedThroughput"));
        assert!(html.contains("scaledCpu"));
        assert!(html.contains("Scheduler resource rows"));
        assert!(html.contains("Workload parameters"));
        assert!(html.contains("Build And Optimization Readiness"));
        assert!(html.contains("Native Build And Claim Readiness"));
        assert!(html.contains("Optimization Opportunity Matrix"));
        assert!(html.contains("Rayon CPU stages"));
        assert!(html.contains("Scheduler Ownership Boundary"));
        assert!(html.contains("Boundary Status"));
        assert!(html.contains("semantic fixture runner uses Rust-owned IDs"));
        assert!(html.contains("CoreScheduler tasklet snapshots"));
        assert!(html.contains("Python tasklet object still stores callable/args/kwargs"));
        assert!(html.contains("PyO3 remains a compatibility adapter"));
        assert!(html.contains("PyO3 scheduler bridge as compatibility boundary"));
        assert!(html.contains("Scheduler CoreScheduler handle API"));
        assert!(html.contains("Scheduler core ownership drain"));
        assert!(html.contains("socket_loopback_request_cycles"));
        assert!(html.contains("3 IO semantic fixtures"));
        assert!(html.contains("process_sample_count"));
        assert!(html.contains("requests_per_run"));
        assert!(html.contains("create_group_directory_yaml"));
        assert!(html.contains("run_order_fixture_rust_core"));
        assert!(html.contains("release-native"));
        assert!(html.contains("data-row-key="));
    }
}
