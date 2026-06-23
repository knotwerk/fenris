use anyhow::{anyhow, Context, Result};
use carbon_scheduler_core::{
    run_scenario, run_scenario_with_teardown, MainStep, Scenario, SEMANTIC_TRACE_SCHEMA,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Fixture {
    pub schema: String,
    pub name: String,
    pub scenario: Scenario,
    #[serde(default)]
    pub teardown: Option<FixtureTeardown>,
    #[serde(default)]
    pub trace_expect: TraceExpect,
    #[serde(default = "default_check_events")]
    pub check_events: bool,
    #[serde(default)]
    pub events: Vec<Value>,
    #[serde(default)]
    pub expect: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FixtureTeardown {
    #[serde(default)]
    pub steps: Vec<MainStep>,
    #[serde(default)]
    pub expect: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TraceExpect {
    #[serde(default)]
    pub contains_in_order: Vec<Value>,
    #[serde(default)]
    pub match_counts: Vec<TraceMatchCount>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TraceMatchCount {
    pub event: Value,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FixtureReport {
    pub name: String,
    pub path: String,
    pub status: GateStatus,
    pub expected_events: usize,
    pub generated_events: usize,
    pub final_state_checked: bool,
    pub trace_expectations_checked: bool,
    pub trace_expectations: usize,
    pub teardown_checked: bool,
    pub teardown_steps: usize,
    pub invariants_checked: bool,
    pub invariant_checks: usize,
    pub differences: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FixtureDirReport {
    pub schema: &'static str,
    pub gate: &'static str,
    pub status: GateStatus,
    pub report_ready: bool,
    pub coverage: &'static str,
    pub fixture_count: usize,
    pub passed: usize,
    pub failed: usize,
    pub reports: Vec<FixtureReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Pass,
    Fail,
}

impl FixtureDirReport {
    pub fn is_pass(&self) -> bool {
        self.status == GateStatus::Pass
    }
}

pub fn run_fixture_dir(path: impl AsRef<Path>) -> Result<FixtureDirReport> {
    let path = path.as_ref();
    let mut fixture_paths = fs::read_dir(path)
        .with_context(|| format!("reading fixture dir {}", path.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<PathBuf>>>()
        .with_context(|| format!("listing fixture dir {}", path.display()))?;
    fixture_paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "json")
    });
    fixture_paths.sort();

    let mut reports = Vec::new();
    for fixture_path in fixture_paths {
        reports.push(run_fixture_path(&fixture_path)?);
    }

    let passed = reports
        .iter()
        .filter(|report| report.status == GateStatus::Pass)
        .count();
    let failed = reports.len() - passed;
    let status = if failed == 0 {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    };

    Ok(FixtureDirReport {
        schema: SEMANTIC_TRACE_SCHEMA,
        gate: "scheduler-fixtures",
        status,
        report_ready: status == GateStatus::Pass,
        coverage: "event_or_final_state_checked_trace_expectations_checked_event_run_count_calculated_run_count_invariant_checked_scheduler_semantic_fixtures_tasklet_run_front_plus_one_targeted_run_boundary_invalid_run_switch_no_mutation_bind_setup_rebind_schedule_back_reschedule_run_n_limited_schedule_order_timeout_counters_nested_timeout_remainder_scheduled_remove_nested_schedule_multi_level_blocked_yield_scheduler_switch_trap_no_mutation_switch_trap_error_counts_switch_trap_nested_level_channel_callback_preference_neutral_order_main_side_preference_fixture_teardown_blocked_channel_cleanup_close_clear_pending_teardown_cleanup_exception_throw_closed_channel_deadlock",
        fixture_count: reports.len(),
        passed,
        failed,
        reports,
    })
}

pub fn run_fixture_path(path: impl AsRef<Path>) -> Result<FixtureReport> {
    let path = path.as_ref();
    let fixture = load_fixture(path)?;
    let teardown_steps = fixture
        .teardown
        .as_ref()
        .map(|teardown| teardown.steps.len());
    let trace = if let Some(teardown) = &fixture.teardown {
        let run = run_scenario_with_teardown(&fixture.scenario, &teardown.steps)
            .with_context(|| format!("running fixture {}", fixture.name))?;
        TraceForFixture {
            events: run.events,
            final_state: run.final_state,
            expected_state: run.pre_teardown_state,
        }
    } else {
        let run = run_scenario(&fixture.scenario)
            .with_context(|| format!("running fixture {}", fixture.name))?;
        TraceForFixture {
            events: run.events,
            expected_state: run.final_state.clone(),
            final_state: run.final_state,
        }
    };

    let mut differences = Vec::new();
    if fixture.schema != SEMANTIC_TRACE_SCHEMA {
        differences.push(format!(
            "$.schema expected {SEMANTIC_TRACE_SCHEMA:?}, got {:?}",
            fixture.schema
        ));
    }

    if fixture.check_events {
        if fixture.events.len() != trace.events.len() {
            differences.push(format!(
                "$.events length expected {}, got {}",
                fixture.events.len(),
                trace.events.len()
            ));
        }

        for (index, expected) in fixture.events.iter().enumerate() {
            let Some(actual) = trace.events.get(index) else {
                break;
            };
            value_contains(
                actual,
                expected,
                &format!("$.events[{index}]"),
                &mut differences,
            );
        }
    }

    if !fixture.expect.is_null() {
        value_contains(
            &trace.expected_state,
            &fixture.expect,
            "$.expect",
            &mut differences,
        );
    }
    if let Some(teardown) = &fixture.teardown {
        if !teardown.expect.is_null() {
            value_contains(
                &trace.final_state,
                &teardown.expect,
                "$.teardown.expect",
                &mut differences,
            );
        }
    }
    let trace_expectations =
        validate_trace_expectations(&trace.events, &fixture.trace_expect, &mut differences);
    let invariant_checks =
        validate_trace_invariants(&trace.events, &trace.final_state, &mut differences);

    let status = if differences.is_empty() {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    };

    Ok(FixtureReport {
        name: fixture.name,
        path: path.display().to_string(),
        status,
        expected_events: if fixture.check_events {
            fixture.events.len()
        } else {
            0
        },
        generated_events: trace.events.len(),
        final_state_checked: !fixture.expect.is_null(),
        trace_expectations_checked: trace_expectations > 0,
        trace_expectations,
        teardown_checked: fixture
            .teardown
            .as_ref()
            .is_some_and(|teardown| !teardown.expect.is_null()),
        teardown_steps: teardown_steps.unwrap_or(0),
        invariants_checked: true,
        invariant_checks,
        differences,
    })
}

struct TraceForFixture {
    events: Vec<Value>,
    expected_state: Value,
    final_state: Value,
}

pub fn load_fixture(path: impl AsRef<Path>) -> Result<Fixture> {
    let path = path.as_ref();
    let text =
        fs::read_to_string(path).with_context(|| format!("reading fixture {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing fixture {}", path.display()))
}

fn value_contains(actual: &Value, expected: &Value, path: &str, differences: &mut Vec<String>) {
    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => {
            for (key, expected_value) in expected {
                let child_path = format!("{path}.{key}");
                match actual.get(key) {
                    Some(actual_value) => {
                        value_contains(actual_value, expected_value, &child_path, differences)
                    }
                    None => differences.push(format!("{child_path} missing")),
                }
            }
        }
        (Value::Array(actual), Value::Array(expected)) => {
            if actual.len() != expected.len() {
                differences.push(format!(
                    "{path} length expected {}, got {}",
                    expected.len(),
                    actual.len()
                ));
                return;
            }
            for (index, (actual_value, expected_value)) in
                actual.iter().zip(expected.iter()).enumerate()
            {
                value_contains(
                    actual_value,
                    expected_value,
                    &format!("{path}[{index}]"),
                    differences,
                );
            }
        }
        _ if actual == expected => {}
        _ => differences.push(format!(
            "{path} expected {}, got {}",
            render_value(expected),
            render_value(actual)
        )),
    }
}

fn render_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::from("<unrenderable>"))
}

fn validate_trace_expectations(
    events: &[Value],
    expected: &TraceExpect,
    differences: &mut Vec<String>,
) -> usize {
    let mut checks = 0;
    let mut next_index = 0usize;

    for (index, expected_event) in expected.contains_in_order.iter().enumerate() {
        checks += 1;
        let Some(found_index) = events.iter().enumerate().skip(next_index).find_map(
            |(candidate_index, actual_event)| {
                event_matches(actual_event, expected_event).then_some(candidate_index)
            },
        ) else {
            differences.push(format!(
                "$.trace_expect.contains_in_order[{index}] expected event containing {} after events[{next_index}]",
                render_value(expected_event)
            ));
            continue;
        };
        next_index = found_index + 1;
    }

    for (index, expected_count) in expected.match_counts.iter().enumerate() {
        checks += 1;
        let actual_count = events
            .iter()
            .filter(|event| event_matches(event, &expected_count.event))
            .count();
        if actual_count != expected_count.count {
            differences.push(format!(
                "$.trace_expect.match_counts[{index}] expected {} events containing {}, got {actual_count}",
                expected_count.count,
                render_value(&expected_count.event)
            ));
        }
    }

    checks
}

fn event_matches(actual: &Value, expected: &Value) -> bool {
    let mut differences = Vec::new();
    value_contains(actual, expected, "$.event", &mut differences);
    differences.is_empty()
}

fn validate_trace_invariants(
    events: &[Value],
    final_state: &Value,
    differences: &mut Vec<String>,
) -> usize {
    let mut checks = 0;

    for (index, event) in events.iter().enumerate() {
        checks += 1;
        let actual_seq = event.get("seq").and_then(Value::as_u64);
        if actual_seq != Some(index as u64) {
            differences.push(format!(
                "$.events[{index}].seq invariant expected {index}, got {}",
                actual_seq
                    .map(|seq| seq.to_string())
                    .unwrap_or_else(|| String::from("<missing>"))
            ));
        }

        checks += 1;
        let run_count = event.get("run_count").and_then(Value::as_u64);
        let calculated_run_count = event.get("calculated_run_count").and_then(Value::as_u64);
        if run_count != calculated_run_count {
            differences.push(format!(
                "$.events[{index}] run_count invariant expected calculated_run_count {}, got {}",
                calculated_run_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| String::from("<missing>")),
                run_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| String::from("<missing>"))
            ));
        }

        if let Some(runnable) = event.get("runnable").and_then(Value::as_array) {
            checks += 1;
            if run_count != Some(runnable.len() as u64) {
                differences.push(format!(
                    "$.events[{index}].run_count invariant expected runnable length {}, got {}",
                    runnable.len(),
                    run_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| String::from("<missing>"))
                ));
            }
        }
    }

    let Some(tasklets) = final_state.get("tasklets").and_then(Value::as_object) else {
        differences.push(String::from(
            "$.final_state.tasklets invariant missing object",
        ));
        return checks;
    };

    let scheduled_count = tasklets
        .values()
        .filter(|tasklet| bool_field(tasklet, "scheduled") == Some(true))
        .count();
    checks += 1;
    compare_usize_field(
        final_state,
        "run_count",
        scheduled_count,
        "$.final_state.run_count",
        differences,
    );

    let active_tasklet_count = tasklets
        .values()
        .filter(|tasklet| bool_field(tasklet, "alive") == Some(true))
        .count();
    checks += 1;
    compare_usize_field(
        final_state,
        "active_tasklet_count",
        active_tasklet_count,
        "$.final_state.active_tasklet_count",
        differences,
    );

    checks += 1;
    compare_usize_field(
        final_state,
        "all_time_tasklet_count",
        tasklets.len(),
        "$.final_state.all_time_tasklet_count",
        differences,
    );

    for (tasklet_id, tasklet) in tasklets {
        let alive = bool_field(tasklet, "alive");
        let scheduled = bool_field(tasklet, "scheduled");
        let blocked = bool_field(tasklet, "blocked");
        let blocked_on = tasklet.get("blocked_on").and_then(Value::as_str);
        let blocked_direction = tasklet.get("blocked_direction").and_then(Value::as_str);

        checks += 1;
        if alive == Some(false) && scheduled == Some(true) {
            differences.push(format!(
                "$.final_state.tasklets.{tasklet_id} invariant dead tasklet cannot be scheduled"
            ));
        }
        checks += 1;
        if blocked == Some(true) {
            if alive != Some(true) {
                differences.push(format!(
                    "$.final_state.tasklets.{tasklet_id} invariant blocked tasklet must be alive"
                ));
            }
            if scheduled == Some(true) {
                differences.push(format!(
                    "$.final_state.tasklets.{tasklet_id} invariant blocked tasklet cannot be scheduled"
                ));
            }
            if blocked_on.is_none() {
                differences.push(format!(
                    "$.final_state.tasklets.{tasklet_id} invariant blocked tasklet missing blocked_on"
                ));
            }
            if !matches!(blocked_direction, Some("send" | "receive")) {
                differences.push(format!(
                    "$.final_state.tasklets.{tasklet_id} invariant blocked_direction expected send/receive, got {}",
                    blocked_direction.unwrap_or("<missing>")
                ));
            }
        } else if blocked_on.is_some() || blocked_direction.is_some() {
            differences.push(format!(
                "$.final_state.tasklets.{tasklet_id} invariant unblocked tasklet has blocked_on/blocked_direction"
            ));
        }
    }
    let blocked_tasklet_count = tasklets
        .values()
        .filter(|tasklet| bool_field(tasklet, "blocked") == Some(true))
        .count();
    checks += 1;
    compare_usize_field(
        final_state,
        "blocked_tasklet_count",
        blocked_tasklet_count,
        "$.final_state.blocked_tasklet_count",
        differences,
    );

    let Some(channels) = final_state.get("channels").and_then(Value::as_object) else {
        differences.push(String::from(
            "$.final_state.channels invariant missing object",
        ));
        return checks;
    };

    for (channel_id, channel) in channels {
        let senders = string_array_field(channel, "blocked_senders");
        let receivers = string_array_field(channel, "blocked_receivers");
        checks += 1;
        if !senders.is_empty() && !receivers.is_empty() {
            differences.push(format!(
                "$.final_state.channels.{channel_id} invariant has both blocked senders and receivers"
            ));
        }

        checks += 1;
        let expected_balance = senders.len() as i64 - receivers.len() as i64;
        if channel.get("balance").and_then(Value::as_i64) != Some(expected_balance) {
            differences.push(format!(
                "$.final_state.channels.{channel_id}.balance invariant expected {expected_balance}, got {}",
                channel
                    .get("balance")
                    .map(render_value)
                    .unwrap_or_else(|| String::from("<missing>"))
            ));
        }

        checks += 1;
        let expected_front = receivers.first().or_else(|| senders.first());
        let actual_front = channel.get("queue_front").and_then(Value::as_str);
        if actual_front != expected_front.map(String::as_str) {
            differences.push(format!(
                "$.final_state.channels.{channel_id}.queue_front invariant expected {}, got {}",
                expected_front.map(|front| front.as_str()).unwrap_or("null"),
                channel
                    .get("queue_front")
                    .map(render_value)
                    .unwrap_or_else(|| String::from("<missing>"))
            ));
        }

        for sender in &senders {
            checks += 1;
            validate_channel_blocked_tasklet(
                tasklets,
                sender,
                channel_id,
                "send",
                "$.final_state.channels",
                differences,
            );
        }
        for receiver in &receivers {
            checks += 1;
            validate_channel_blocked_tasklet(
                tasklets,
                receiver,
                channel_id,
                "receive",
                "$.final_state.channels",
                differences,
            );
        }
    }
    let blocked_channel_count = channels
        .values()
        .filter(|channel| {
            !string_array_field(channel, "blocked_senders").is_empty()
                || !string_array_field(channel, "blocked_receivers").is_empty()
        })
        .count();
    checks += 1;
    compare_usize_field(
        final_state,
        "blocked_channel_count",
        blocked_channel_count,
        "$.final_state.blocked_channel_count",
        differences,
    );

    for (tasklet_id, tasklet) in tasklets {
        let Some(blocked_on) = tasklet.get("blocked_on").and_then(Value::as_str) else {
            continue;
        };
        let Some(direction) = tasklet.get("blocked_direction").and_then(Value::as_str) else {
            continue;
        };
        let Some(channel) = channels.get(blocked_on) else {
            checks += 1;
            differences.push(format!(
                "$.final_state.tasklets.{tasklet_id}.blocked_on invariant unknown channel {blocked_on}"
            ));
            continue;
        };
        let queue_name = match direction {
            "send" => "blocked_senders",
            "receive" => "blocked_receivers",
            _ => continue,
        };
        checks += 1;
        if !string_array_field(channel, queue_name)
            .iter()
            .any(|queued| queued == tasklet_id)
        {
            differences.push(format!(
                "$.final_state.tasklets.{tasklet_id} invariant missing from channel {blocked_on}.{queue_name}"
            ));
        }
    }

    checks
}

fn bool_field<'a>(value: &'a Value, field: &str) -> Option<bool> {
    value.get(field).and_then(Value::as_bool)
}

fn compare_usize_field(
    value: &Value,
    field: &str,
    expected: usize,
    path: &str,
    differences: &mut Vec<String>,
) {
    if value.get(field).and_then(Value::as_u64) != Some(expected as u64) {
        differences.push(format!(
            "{path} invariant expected {expected}, got {}",
            value
                .get(field)
                .map(render_value)
                .unwrap_or_else(|| String::from("<missing>"))
        ));
    }
}

fn string_array_field(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn validate_channel_blocked_tasklet(
    tasklets: &serde_json::Map<String, Value>,
    tasklet_id: &str,
    channel_id: &str,
    direction: &str,
    path: &str,
    differences: &mut Vec<String>,
) {
    let Some(tasklet) = tasklets.get(tasklet_id) else {
        differences.push(format!(
            "{path}.{channel_id} invariant queued tasklet {tasklet_id} is missing"
        ));
        return;
    };
    if bool_field(tasklet, "blocked") != Some(true) {
        differences.push(format!(
            "{path}.{channel_id} invariant queued tasklet {tasklet_id} is not blocked"
        ));
    }
    if tasklet.get("blocked_on").and_then(Value::as_str) != Some(channel_id) {
        differences.push(format!(
            "{path}.{channel_id} invariant queued tasklet {tasklet_id} blocked_on mismatch"
        ));
    }
    if tasklet.get("blocked_direction").and_then(Value::as_str) != Some(direction) {
        differences.push(format!(
            "{path}.{channel_id} invariant queued tasklet {tasklet_id} direction expected {direction}"
        ));
    }
}

fn default_check_events() -> bool {
    true
}

pub fn assert_report_pass(report: &FixtureDirReport) -> Result<()> {
    if report.is_pass() {
        return Ok(());
    }

    let mut message = format!(
        "{} failed: {} passed, {} failed",
        report.gate, report.passed, report.failed
    );
    for fixture in report
        .reports
        .iter()
        .filter(|fixture| fixture.status == GateStatus::Fail)
    {
        message.push_str(&format!("\n{}:", fixture.name));
        for difference in fixture.differences.iter().take(8) {
            message.push_str(&format!("\n  - {difference}"));
        }
    }
    Err(anyhow!(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_fixtures_pass() {
        let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/scheduler");
        let report = run_fixture_dir(fixture_dir).expect("fixture dir runs");
        assert_report_pass(&report).expect("scheduler fixtures pass");
    }
}
