#!/usr/bin/env python3
"""Render a CEO-ready comparative HTML report from Carbon evidence JSON."""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import math
import statistics
from pathlib import Path
from string import Template


DEFAULT_EVIDENCE_DIR = Path("target/carbon/evidence")
DEFAULT_OUTPUT = Path("target/carbon/report/blog.html")


WORKLOAD_LABELS = {
    "runnable_tasklets_128": (
        "Runnable tasklets, 128",
        "Schedule and drain 128 deterministic tasklets through the public scheduler API.",
    ),
    "runnable_tasklets_1024": (
        "Runnable tasklets, 1,024",
        "Scale runnable queue pressure through the same legacy and Rust Python API.",
    ),
    "runnable_tasklets_4096": (
        "Runnable tasklets, 4,096",
        "Large runnable queue drain pressure for the full lab tier.",
    ),
    "channel_rendezvous_32": (
        "Channel rendezvous, 32 pairs",
        "Pair blocked receivers and senders through scheduler channels.",
    ),
    "channel_rendezvous_256": (
        "Channel rendezvous, 256 pairs",
        "Scale channel handoff pressure to 256 sender/receiver pairs.",
    ),
    "channel_rendezvous_1024": (
        "Channel rendezvous, 1,024 pairs",
        "Large sender/receiver rendezvous pressure for the full lab tier.",
    ),
    "fanout_pipeline_256b": (
        "Fanout pipeline",
        "Synthetic message fanout across worker tasklets and scheduler channels.",
    ),
    "fanout_pipeline_4096b": (
        "Fanout pipeline, 4 KiB payloads",
        "Payload-heavy synthetic fanout across worker tasklets.",
    ),
    "zone_tick_study_small": (
        "Fake game zone tick",
        "Synthetic zones, entity updates, network-like messages, and aggregation.",
    ),
    "zone_tick_study_large": (
        "Fake game zone tick, large",
        "Larger synthetic game loop for tail and resource pressure.",
    ),
    "create_group_directory_yaml": (
        "Create resource group",
        "Scan a resource directory and write the group manifest.",
    ),
    "create_group_from_filter_yaml": (
        "Create groups from filters",
        "Apply filter mapping rules and generate filtered resource groups.",
    ),
    "merge_group_yaml_additive": (
        "Merge resource groups",
        "Combine additive resource group manifests.",
    ),
    "diff_group_csv_additions": (
        "Diff resource groups",
        "Compute additions and removals between manifests.",
    ),
    "remove_resources_yaml": (
        "Remove resources",
        "Remove listed resources and write an updated manifest.",
    ),
    "create_bundle_local_cdn": (
        "Create local bundle",
        "Package resource files into local CDN chunk output.",
    ),
    "create_patch_local_cdn": (
        "Create local patch",
        "Generate patch metadata and patch binary payloads.",
    ),
    "unpack_bundle_local_cdn": (
        "Unpack local bundle",
        "Restore resources from bundle metadata and chunks.",
    ),
    "apply_patch_local_cdn": (
        "Apply local patch",
        "Apply patch payloads to produce the next resource set.",
    ),
}


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def h(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def path_value(data: dict, path: str, default=None):
    current = data
    for part in path.strip("/").split("/"):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def ratio(old: object, rust: object) -> float | None:
    old_value = number(old)
    rust_value = number(rust)
    if old_value is None or rust_value in (None, 0):
        return None
    return old_value / rust_value


def reduction_percent(old: object, rust: object) -> float | None:
    old_value = number(old)
    rust_value = number(rust)
    if old_value in (None, 0) or rust_value is None:
        return None
    return (1.0 - rust_value / old_value) * 100.0


def fmt_int(value: object) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "n/a"


def fmt_ms_from_us(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    return f"{amount / 1000.0:.1f} ms"


def fmt_ratio(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    return f"{amount:.2f}x"


def fmt_directional_ratio(value: object, faster_label: str = "faster", slower_label: str = "slower") -> str:
    amount = number(value)
    if amount is None or amount == 0:
        return "n/a"
    if amount >= 1:
        return f"{amount:.2f}x {faster_label}"
    return f"{1.0 / amount:.2f}x {slower_label}"


def fmt_percent(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    return f"{amount:.0f}%"


def fmt_signed_percent(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    direction = "lower" if amount >= 0 else "higher"
    return f"{abs(amount):.0f}% {direction}"


def fmt_kb(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if amount >= 1024:
        return f"{amount / 1024:.1f} MB"
    return f"{amount:.0f} KB"


def fmt_us(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if amount >= 1000:
        return f"{amount / 1000.0:.2f} ms"
    return f"{amount:.0f} us"


def fmt_count(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if abs(amount) >= 1_000_000:
        return f"{amount / 1_000_000:.2f}M"
    if abs(amount) >= 1_000:
        return f"{amount / 1_000:.1f}k"
    return f"{amount:.0f}"


def fmt_rate(value: object, unit: str) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if "bytes" in unit:
        if amount >= 1024 * 1024:
            return f"{amount / (1024 * 1024):.1f} MB/s"
        if amount >= 1024:
            return f"{amount / 1024:.1f} KB/s"
        return f"{amount:.0f} B/s"
    return f"{fmt_count(amount)}/s"


def fmt_cv(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    return f"{amount * 100.0:.1f}%"


def short_text(text: object, limit: int = 140) -> str:
    value = "" if text is None else str(text)
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def throughput_pair(row: dict) -> tuple[str, object, object]:
    preferred_suffixes = [
        "operations_per_sec",
        "messages_per_sec",
        "requests_per_sec",
        "directories_per_sec",
        "resources_per_sec",
        "rows_per_sec",
        "data_bytes_per_sec",
        "bytes_per_sec",
    ]
    labels = {
        "operations_per_sec": "operations",
        "messages_per_sec": "messages",
        "requests_per_sec": "requests",
        "directories_per_sec": "directories",
        "resources_per_sec": "resources",
        "rows_per_sec": "rows",
        "data_bytes_per_sec": "data bytes",
        "bytes_per_sec": "bytes",
    }
    for suffix in preferred_suffixes:
        legacy_key = f"legacy_throughput_{suffix}"
        rust_key = f"rust_throughput_{suffix}"
        if legacy_key in row and rust_key in row:
            legacy_value = row[legacy_key]
            rust_value = row[rust_key]
            if number(legacy_value) or number(rust_value):
                return labels.get(suffix, suffix.replace("_", " ")), legacy_value, rust_value
    for key, value in row.items():
        if key.startswith("legacy_throughput_"):
            suffix = key.removeprefix("legacy_throughput_")
            rust_key = f"rust_throughput_{suffix}"
            if rust_key in row:
                return labels.get(suffix, suffix.replace("_", " ")), value, row[rust_key]
    return "ops / sec", None, None


def workload_label(workload: str) -> tuple[str, str]:
    return WORKLOAD_LABELS.get(
        workload,
        (workload.replace("_", " ").title(), "Equivalent old and Rust process workload."),
    )


def comparable_rows(bench: dict) -> list[dict]:
    return [
        row
        for row in bench.get("comparisons", []) or []
        if row.get("comparability") == "comparable_process_to_process"
    ]


def scheduler_comparable_rows(evidence: dict) -> list[dict]:
    return [
        row
        for row in evidence.get("comparisons", []) or []
        if row.get("comparability") == "comparable_scheduler_python_api_process_to_process"
    ]


COMPARABLE_PRESSURE_SHAPES = {
    "runnable_tasklets_128": {
        "axis": "tasklet_count",
        "tasklet_count": 128,
        "iterations_per_process": 40,
    },
    "runnable_tasklets_1024": {
        "axis": "tasklet_count",
        "tasklet_count": 1024,
        "iterations_per_process": 10,
    },
    "channel_rendezvous_32": {
        "axis": "channel_pair_count",
        "channel_pair_count": 32,
        "tasklet_count": 64,
        "iterations_per_process": 40,
    },
    "channel_rendezvous_256": {
        "axis": "channel_pair_count",
        "channel_pair_count": 256,
        "tasklet_count": 512,
        "iterations_per_process": 8,
    },
}


def scheduler_pressure_comparable_rows(rows: list[dict]) -> list[dict]:
    pressure_rows = []
    for row in rows:
        expected = COMPARABLE_PRESSURE_SHAPES.get(str(row.get("workload") or ""))
        pressure = row.get("pressure") or {}
        if not expected:
            continue
        if all(pressure.get(key) == value for key, value in expected.items()):
            pressure_rows.append(row)
    return pressure_rows


def report_is_publishable(bench: dict, rows: list[dict]) -> bool:
    readiness = bench.get("optimization_readiness") or {}
    return (
        bool(rows)
        and readiness.get("speedup_claims_allowed") is True
        and all(row.get("legacy_known_non_debug") is True for row in rows)
        and bench.get("build_profile") == "release-native"
        and bench.get("target_cpu_native") is True
        and bench.get("debug_assertions") is False
    )


def scheduler_report_is_publishable(evidence: dict, rows: list[dict]) -> bool:
    return (
        bool(rows)
        and evidence.get("status") == "pass"
        and evidence.get("report_ready") is True
        and number(evidence.get("samples_per_row")) is not None
        and number(evidence.get("samples_per_row")) >= 10
        and not evidence.get("rejected_comparisons")
        and all((row.get("semantic") or {}).get("mismatch_count") == 0 for row in rows)
    )


def build_summary(rows: list[dict]) -> dict:
    speedups = [float(row["speedup"]) for row in rows if number(row.get("speedup")) is not None]
    p99_reductions = [
        reduction_percent(
            path_value(row, "/legacy_sample_stats_us/p99"),
            path_value(row, "/rust_sample_stats_us/p99"),
        )
        for row in rows
    ]
    rss_reductions = [
        reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        for row in rows
    ]
    cpu_reductions = [
        reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        for row in rows
    ]
    p99_reductions = [value for value in p99_reductions if value is not None]
    rss_reductions = [value for value in rss_reductions if value is not None]
    cpu_reductions = [value for value in cpu_reductions if value is not None]
    materially_faster = sum(1 for value in speedups if value >= 1.05)
    equal_or_faster = sum(1 for value in speedups if value >= 0.995)
    slower = sum(1 for value in speedups if value < 0.995)
    return {
        "rows": len(rows),
        "median_speedup": statistics.median(speedups) if speedups else None,
        "geomean_speedup": math.prod(speedups) ** (1 / len(speedups)) if speedups else None,
        "best_speedup": max(speedups) if speedups else None,
        "equal_or_faster": equal_or_faster,
        "materially_faster": materially_faster,
        "slower": slower,
        "p99_better": sum(1 for value in p99_reductions if value > 0),
        "median_p99_reduction": statistics.median(p99_reductions) if p99_reductions else None,
        "rss_better": sum(1 for value in rss_reductions if value > 0),
        "median_rss_reduction": statistics.median(rss_reductions) if rss_reductions else None,
        "cpu_better": sum(1 for value in cpu_reductions if value > 0),
        "median_cpu_reduction": statistics.median(cpu_reductions) if cpu_reductions else None,
    }


def metric_card(label: str, value: str, note: str) -> str:
    return (
        '<div class="metric">'
        f"<span>{h(label)}</span>"
        f"<strong>{h(value)}</strong>"
        f"<small>{h(note)}</small>"
        "</div>"
    )


def executive_cards(summary: dict) -> str:
    return "\n".join(
        [
            metric_card(
                "Scheduler throughput",
                fmt_directional_ratio(summary["median_speedup"]),
                f"median old-vs-Rust result across {summary['rows']} lab workloads",
            ),
            metric_card(
                "Best result",
                fmt_directional_ratio(summary["best_speedup"]),
                "best measured scheduler workload",
            ),
            metric_card(
                "Tail latency",
                fmt_signed_percent(summary["median_p99_reduction"]),
                f"median p99 change; lower in {summary['p99_better']}/{summary['rows']}",
            ),
            metric_card(
                "Memory cost",
                fmt_signed_percent(summary["median_rss_reduction"]),
                f"median peak RSS; lower in {summary['rss_better']}/{summary['rows']}",
            ),
            metric_card(
                "CPU burn",
                fmt_signed_percent(summary["median_cpu_reduction"]),
                f"median effective CPU; lower in {summary['cpu_better']}/{summary['rows']}",
            ),
            metric_card(
                "Coverage",
                f"{summary['equal_or_faster']}/{summary['rows']}",
                f"Rust equal or faster wall time; {summary['materially_faster']} materially faster",
            ),
        ]
    )


def bar_width(value: object, max_value: float) -> float:
    amount = number(value)
    if amount is None or max_value <= 0:
        return 2.0
    return max(2.0, min(100.0, amount / max_value * 100.0))


def result_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="8">No old-vs-Rust comparison rows available.</td></tr>'
    max_speedup = max(float(row.get("speedup") or 0) for row in rows) or 1.0
    rendered = []
    for row in sorted(rows, key=lambda item: float(item.get("speedup") or 0), reverse=True):
        title, description = workload_label(str(row.get("workload") or ""))
        speedup = number(row.get("speedup"))
        p99_reduction = reduction_percent(
            path_value(row, "/legacy_sample_stats_us/p99"),
            path_value(row, "/rust_sample_stats_us/p99"),
        )
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        old_cpu_ms = path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean")
        rust_cpu_ms = path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean")
        old_cpu_percent = path_value(row, "/legacy_process_stats/cpu_percent/mean")
        rust_cpu_percent = path_value(row, "/rust_process_stats/cpu_percent/mean")
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        throughput_unit, old_throughput, rust_throughput = throughput_pair(row)
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(description)}</small></td>"
            f"<td><strong>{fmt_directional_ratio(speedup)}</strong><div class=\"bar\"><span style=\"width:{bar_width(speedup, max_speedup):.1f}%\"></span></div>"
            f"<small>{fmt_ms_from_us(row.get('legacy_duration_us'))} old vs {fmt_ms_from_us(row.get('rust_duration_us'))} Rust</small></td>"
            f"<td><strong>{fmt_signed_percent(p99_reduction)}</strong>"
            f"<small>{fmt_ms_from_us(path_value(row, '/legacy_sample_stats_us/p99'))} old vs {fmt_ms_from_us(path_value(row, '/rust_sample_stats_us/p99'))} Rust</small></td>"
            f"<td><strong>{h(fmt_int(old_throughput))} -> {h(fmt_int(rust_throughput))}</strong><small>{h(throughput_unit)}</small></td>"
            f"<td><strong>{fmt_signed_percent(cpu_reduction)}</strong>"
            f"<small>{fmt_ms_from_us(old_cpu_ms * 1000 if old_cpu_ms is not None else None)} old vs {fmt_ms_from_us(rust_cpu_ms * 1000 if rust_cpu_ms is not None else None)} Rust; CPU {fmt_percent(old_cpu_percent)} vs {fmt_percent(rust_cpu_percent)}</small></td>"
            f"<td><strong>{fmt_signed_percent(rss_reduction)}</strong>"
            f"<small>{fmt_kb(path_value(row, '/legacy_process_stats/max_rss_kb/p95'))} old vs {fmt_kb(path_value(row, '/rust_process_stats/max_rss_kb/p95'))} Rust</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def architecture_section() -> str:
    items = [
        (
            "Same scheduler API",
            "The comparison runs the legacy C++ extension and the Rust scheduler bridge through the same Python tasklet/channel API.",
        ),
        (
            "Rust bridge under test",
            "The measured Rust path keeps the legacy import surface while routing covered tasklet, channel, and run-queue behavior through Rust-owned scheduler state.",
        ),
        (
            "Native Linux baseline",
            "The legacy scheduler now builds and runs on this host, including Python tests and C API CTest, so old-vs-Rust scheduler comparisons can run locally.",
        ),
        (
            "Lab game study",
            "Synthetic tasklet, channel, fanout, and zone-tick workloads stress orchestration while the real game-environment run remains the next production gate.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in items
    )


SCHEDULER_GATES = [
    ("scheduler-fixtures.json", "Semantic fixtures"),
    ("legacy-scheduler.json", "Legacy Python/C API baseline"),
    ("rust-scheduler-python.json", "Rust Python/C API bridge"),
    ("io-workloads.json", "IO/socket/SSL orchestration"),
    ("scheduler-comparison.json", "Matched scheduler comparison"),
    ("scalability-matrix.json", "Pressure matrix"),
]


def optional_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return load_json(path)


def gate_items(evidence_dir: Path) -> list[dict]:
    items = []
    for filename, label in SCHEDULER_GATES:
        evidence = optional_json(evidence_dir / filename)
        remaining = evidence.get("remaining_before_report_ready") or []
        not_ready_reason = evidence.get("not_report_ready_reason")
        blockers = [str(item) for item in remaining[:3]]
        if not_ready_reason:
            blockers.insert(0, str(not_ready_reason))
        items.append(
            {
                "filename": filename,
                "label": label,
                "status": evidence.get("status", "missing"),
                "report_ready": evidence.get("report_ready") is True,
                "coverage": evidence.get("coverage", "missing"),
                "blockers": blockers,
            }
        )
    return items


def scheduler_story_section(items: list[dict]) -> str:
    open_count = sum(1 for item in items if not item["report_ready"])
    open_labels = [item["label"] for item in items if not item["report_ready"]]
    remaining_text = (
        f"Open gates: {', '.join(open_labels)}."
        if open_labels
        else "All scheduler evidence gates are report-ready."
    )
    cards = [
        (
            "Evidence status",
            f"This report publishes lab scheduler comparison evidence only. {open_count} scheduler-related evidence gates are still not report-ready for broader production claims.",
        ),
        (
            "What remains",
            remaining_text,
        ),
        (
            "Publish rule",
            "The measured rows can support lab conclusions; production scheduler claims wait for a real game-environment trace or harness.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in cards
    )


def scheduler_gate_rows(items: list[dict]) -> str:
    rows = []
    for item in items:
        blockers = item["blockers"] or ["no remaining work recorded"]
        rows.append(
            "<tr>"
            f"<td><strong>{h(item['label'])}</strong><small>{h(item['filename'])}</small></td>"
            f"<td>{h(item['status'])}</td>"
            f"<td>{'yes' if item['report_ready'] else 'no'}</td>"
            f"<td>{h(short_text(item['coverage'], 110))}</td>"
            f"<td>{h(short_text('; '.join(blockers), 180))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def evidence_status_cards(evidence_dir: Path) -> str:
    fixtures = optional_json(evidence_dir / "scheduler-fixtures.json")
    legacy = optional_json(evidence_dir / "legacy-scheduler.json")
    bridge = optional_json(evidence_dir / "rust-scheduler-python.json")
    comparison = optional_json(evidence_dir / "scheduler-comparison.json")
    cards = [
        (
            "Semantic fixtures",
            f"{fmt_int(fixtures.get('passed'))}/{fmt_int(fixtures.get('fixture_count'))} pass",
            "Deterministic scheduler state-machine fixtures; still not a full production parity gate.",
        ),
        (
            "Legacy baseline",
            "ready" if legacy.get("report_ready") is True else "not ready",
            "Native Linux legacy scheduler Python tests and C API CTest evidence.",
        ),
        (
            "Python/C API bridge",
            f"{fmt_int(bridge.get('unchanged_legacy_subset_count'))} legacy tests",
            "Rust bridge compatibility slice; core ownership is still partial.",
        ),
        (
            "Comparison rows",
            f"{fmt_int(path_value(comparison, '/summary/comparison_count'))} matched",
            "Legacy C++ scheduler extension vs Rust scheduler bridge through the same Python API.",
        ),
    ]
    return "\n".join(metric_card(label, value, note) for label, value, note in cards)


def summary_cards(summary: dict, *, subject: str) -> str:
    return "\n".join(
        [
            metric_card(
                f"{subject} median",
                fmt_directional_ratio(summary["median_speedup"]),
                f"wall/throughput ratio across {summary['rows']} comparable rows",
            ),
            metric_card(
                f"{subject} geomean",
                fmt_directional_ratio(summary["geomean_speedup"]),
                "aggregate ratio across all comparable rows",
            ),
            metric_card(
                "Best row",
                fmt_directional_ratio(summary["best_speedup"]),
                "largest measured old-vs-Rust ratio in this section",
            ),
            metric_card(
                "p99 tail",
                fmt_signed_percent(summary["median_p99_reduction"]),
                f"median p99 change; lower in {summary['p99_better']}/{summary['rows']}",
            ),
            metric_card(
                "CPU burn",
                fmt_signed_percent(summary["median_cpu_reduction"]),
                f"median effective CPU; lower in {summary['cpu_better']}/{summary['rows']}",
            ),
            metric_card(
                "Peak memory",
                fmt_signed_percent(summary["median_rss_reduction"]),
                f"median peak RSS; lower in {summary['rss_better']}/{summary['rows']}",
            ),
        ]
    )


def semantic_mismatch_count(rows: list[dict]) -> int | None:
    total = 0
    seen = False
    for row in rows:
        mismatch_count = path_value(row, "/semantic/mismatch_count")
        amount = number(mismatch_count)
        if amount is None:
            continue
        seen = True
        total += int(amount)
    return total if seen else None


def top_line_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_rows: list[dict],
) -> str:
    mismatch_count = semantic_mismatch_count(scheduler_rows)
    row_count = scheduler_summary["rows"]
    return "\n".join(
        [
            metric_card(
                "Scheduler parity",
                f"{fmt_int(row_count)}/{fmt_int(row_count)} rows",
                f"{fmt_int(mismatch_count)} semantic mismatches in matched lab workloads",
            ),
            metric_card(
                "Scheduler speed",
                fmt_directional_ratio(scheduler_summary["median_speedup"]),
                "median old-vs-Rust wall/throughput result today",
            ),
            metric_card(
                "Resources tools",
                fmt_directional_ratio(resource_summary["median_speedup"]),
                f"median across {resource_summary['rows']} separate CLI rows",
            ),
            metric_card(
                "Production gate",
                "open",
                "real game-environment scheduler workload still required",
            ),
        ]
    )


def performance_breakdown_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scalability_evidence: dict,
    io_rows_data: list[dict],
) -> str:
    pressure_summary = scalability_evidence.get("summary") or {}
    pressure_rows_count = path_value(pressure_summary, "/family_counts/scheduler", 0)
    stable_rows = pressure_summary.get("stable_rows_cv_le_10_percent")
    io_count = len(io_rows_data)
    return "\n".join(
        [
            metric_card(
                "Scheduler throughput",
                fmt_directional_ratio(scheduler_summary["median_speedup"]),
                f"legacy C++ vs Rust bridge; {scheduler_summary['rows']} comparable rows",
            ),
            metric_card(
                "Scheduler p99",
                fmt_signed_percent(scheduler_summary["median_p99_reduction"]),
                f"median tail change; lower in {scheduler_summary['p99_better']}/{scheduler_summary['rows']}",
            ),
            metric_card(
                "Scheduler resources",
                f"{fmt_signed_percent(scheduler_summary['median_cpu_reduction'])} CPU",
                f"peak memory {fmt_signed_percent(scheduler_summary['median_rss_reduction'])}",
            ),
            metric_card(
                "Pressure shape",
                fmt_rate(pressure_summary.get("peak_operations_per_sec"), "operations"),
                f"{fmt_int(stable_rows)}/{fmt_int(pressure_rows_count)} scheduler rows stable at CV <= 10%",
            ),
            metric_card(
                "Worst scheduler p99",
                fmt_us(pressure_summary.get("worst_latency_p99_us")),
                f"Rust-only pressure matrix; p99.9 {fmt_us(pressure_summary.get('worst_latency_p99_9_us'))}",
            ),
            metric_card(
                "Pressure memory",
                fmt_kb(pressure_summary.get("highest_peak_rss_kb_p95")),
                f"p95 peak RSS across {fmt_int(pressure_rows_count)} scheduler pressure rows",
            ),
            metric_card(
                "Resources throughput",
                fmt_directional_ratio(resource_summary["median_speedup"]),
                f"separate resource CLI comparison; {resource_summary['rows']} rows",
            ),
            metric_card(
                "Resources cost",
                f"{fmt_signed_percent(resource_summary['median_cpu_reduction'])} CPU",
                f"peak memory {fmt_signed_percent(resource_summary['median_rss_reduction'])}",
            ),
            metric_card(
                "IO loopback",
                f"{fmt_int(io_count)} rows",
                "request, byte, CPU, and memory stats; not legacy Carbon IO parity",
            ),
        ]
    )


def comparison_table_rows(rows: list[dict], *, empty_text: str) -> str:
    if not rows:
        return f'<tr><td colspan="8">{h(empty_text)}</td></tr>'
    max_speedup = max(float(row.get("speedup") or 0) for row in rows) or 1.0
    rendered = []
    for row in sorted(rows, key=lambda item: float(item.get("speedup") or 0), reverse=True):
        workload = str(row.get("workload") or row.get("comparison_group") or "")
        title, default_description = workload_label(workload)
        description = row.get("description") or default_description
        speedup = number(row.get("speedup"))
        p50_old = path_value(row, "/legacy_sample_stats_us/p50")
        p50_rust = path_value(row, "/rust_sample_stats_us/p50")
        p99_old = path_value(row, "/legacy_sample_stats_us/p99")
        p99_rust = path_value(row, "/rust_sample_stats_us/p99")
        p99_reduction = reduction_percent(p99_old, p99_rust)
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        old_cpu_percent = path_value(row, "/legacy_process_stats/cpu_percent/mean")
        rust_cpu_percent = path_value(row, "/rust_process_stats/cpu_percent/mean")
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        throughput_unit, old_throughput, rust_throughput = throughput_pair(row)
        parity = row.get("parity_status") or row.get("claim_eligibility") or "n/a"
        mismatch_count = path_value(row, "/semantic/mismatch_count")
        if mismatch_count is not None:
            parity = f"{parity}; mismatches={fmt_int(mismatch_count)}"
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(description)}</small></td>"
            f"<td><strong>{fmt_directional_ratio(speedup)}</strong><div class=\"bar\"><span style=\"width:{bar_width(speedup, max_speedup):.1f}%\"></span></div>"
            f"<small>{fmt_ms_from_us(row.get('legacy_duration_us'))} old vs {fmt_ms_from_us(row.get('rust_duration_us'))} Rust</small></td>"
            f"<td><strong>{fmt_us(p50_old)} -> {fmt_us(p50_rust)}</strong><small>legacy p50 vs Rust p50</small></td>"
            f"<td><strong>{fmt_signed_percent(p99_reduction)}</strong><small>{fmt_us(p99_old)} old p99 vs {fmt_us(p99_rust)} Rust p99</small></td>"
            f"<td><strong>{h(fmt_rate(old_throughput, throughput_unit))} -> {h(fmt_rate(rust_throughput, throughput_unit))}</strong><small>{h(throughput_unit)}</small></td>"
            f"<td><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>CPU {fmt_percent(old_cpu_percent)} old vs {fmt_percent(rust_cpu_percent)} Rust</small></td>"
            f"<td><strong>{fmt_signed_percent(rss_reduction)}</strong><small>{fmt_kb(path_value(row, '/legacy_process_stats/max_rss_kb/p95'))} old vs {fmt_kb(path_value(row, '/rust_process_stats/max_rss_kb/p95'))} Rust</small></td>"
            f"<td><small>{h(parity)}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def scheduler_pressure_comparison_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="9">No matched legacy-vs-Rust pressure rows available.</td></tr>'
    max_speedup = max(float(row.get("speedup") or 0) for row in rows) or 1.0
    rendered = []
    for row in sorted(rows, key=lambda item: str(item.get("workload") or "")):
        title, detail = pressure_label(row)
        speedup = number(row.get("speedup"))
        legacy_p99 = path_value(row, "/legacy_sample_stats_us/p99")
        rust_p99 = path_value(row, "/rust_sample_stats_us/p99")
        legacy_p999 = path_value(row, "/legacy_sample_stats_us/p99_9")
        rust_p999 = path_value(row, "/rust_sample_stats_us/p99_9")
        legacy_cpu = path_value(row, "/legacy_process_stats/cpu_percent/p95")
        rust_cpu = path_value(row, "/rust_process_stats/cpu_percent/p95")
        legacy_rss = path_value(row, "/legacy_process_stats/max_rss_kb/p95")
        rust_rss = path_value(row, "/rust_process_stats/max_rss_kb/p95")
        legacy_cv = path_value(row, "/legacy_throughput_stability/coefficient_of_variation")
        rust_cv = path_value(row, "/rust_throughput_stability/coefficient_of_variation")
        parity = row.get("parity_status") or "n/a"
        mismatch_count = path_value(row, "/semantic/mismatch_count")
        if mismatch_count is not None:
            parity = f"{parity}; mismatches={fmt_int(mismatch_count)}"
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(detail)}</small></td>"
            f"<td><strong>{fmt_directional_ratio(speedup)}</strong><div class=\"bar\"><span style=\"width:{bar_width(speedup, max_speedup):.1f}%\"></span></div><small>lab-only same-API ratio</small></td>"
            f"<td><strong>{h(fmt_rate(row.get('legacy_throughput_operations_per_sec'), 'operations'))} -> {h(fmt_rate(row.get('rust_throughput_operations_per_sec'), 'operations'))}</strong><small>operations/sec</small></td>"
            f"<td><strong>{fmt_us(legacy_p99)} -> {fmt_us(rust_p99)}</strong><small>legacy vs Rust p99</small></td>"
            f"<td><strong>{fmt_us(legacy_p999)} -> {fmt_us(rust_p999)}</strong><small>legacy vs Rust p99.9</small></td>"
            f"<td><strong>{fmt_percent(legacy_cpu)} -> {fmt_percent(rust_cpu)}</strong><small>CPU p95</small></td>"
            f"<td><strong>{fmt_kb(legacy_rss)} -> {fmt_kb(rust_rss)}</strong><small>peak RSS p95</small></td>"
            f"<td><strong>{fmt_cv(legacy_cv)} -> {fmt_cv(rust_cv)}</strong><small>throughput CV</small></td>"
            f"<td><small>{h(parity)}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def pressure_label(row: dict) -> tuple[str, str]:
    workload = str(row.get("workload") or "")
    pressure = row.get("pressure") or {}
    if pressure.get("axis") == "tasklet_count":
        count = fmt_int(pressure.get("tasklet_count"))
        return f"Runnable tasklets, {count}", f"{fmt_int(pressure.get('iterations_per_process'))} iterations per process"
    if pressure.get("axis") == "channel_pair_count":
        pairs = fmt_int(pressure.get("channel_pair_count"))
        tasklets = fmt_int(pressure.get("tasklet_count"))
        return f"Channel pairs, {pairs}", f"{tasklets} tasklets; {fmt_int(pressure.get('iterations_per_process'))} iterations per process"
    return workload.replace("_", " ").title(), "Rust scheduler pressure row"


def pressure_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="7">No scheduler pressure rows available.</td></tr>'
    rendered = []
    for row in rows:
        title, detail = pressure_label(row)
        p99 = path_value(row, "/latency_us_extended/p99")
        p99_9 = path_value(row, "/latency_us_extended/p99_9")
        throughput = row.get("throughput_operations_per_sec")
        cpu = path_value(row, "/process_stats/cpu_percent/p95")
        rss = path_value(row, "/process_stats/max_rss_kb/p95")
        cv = path_value(row, "/stability/coefficient_of_variation")
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(detail)}</small></td>"
            f"<td>{h(fmt_rate(throughput, 'operations'))}</td>"
            f"<td>{h(fmt_us(p99))}</td>"
            f"<td>{h(fmt_us(p99_9))}</td>"
            f"<td>{h(fmt_percent(cpu))}</td>"
            f"<td>{h(fmt_kb(rss))}</td>"
            f"<td>{h(fmt_cv(cv))}<small>throughput CV</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def io_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="7">No IO loopback rows available.</td></tr>'
    rendered = []
    for row in rows:
        workload = str(row.get("workload") or row.get("kind") or "io")
        title = workload.replace("_", " ").title()
        p99_old = path_value(row, "/legacy_sample_stats_us/p99")
        p99_rust = path_value(row, "/rust_sample_stats_us/p99")
        bytes_old = row.get("legacy_throughput_bytes_per_sec")
        bytes_rust = row.get("rust_throughput_bytes_per_sec")
        req_old = row.get("legacy_throughput_requests_per_sec")
        req_rust = row.get("rust_throughput_requests_per_sec")
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(row.get('not_comparable_reason') or row.get('claim_scope') or '')}</small></td>"
            f"<td>{h(fmt_us(p99_old))} -> {h(fmt_us(p99_rust))}<small>p99 request latency</small></td>"
            f"<td>{h(fmt_rate(req_old, 'requests'))} -> {h(fmt_rate(req_rust, 'requests'))}</td>"
            f"<td>{h(fmt_rate(bytes_old, 'bytes'))} -> {h(fmt_rate(bytes_rust, 'bytes'))}</td>"
            f"<td>{h(fmt_signed_percent(cpu_reduction))}</td>"
            f"<td>{h(fmt_signed_percent(rss_reduction))}</td>"
            f"<td><small>{h(row.get('parity_status') or 'not legacy comparable')}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def resources_methodology(bench: dict, evidence_dir: Path) -> str:
    readiness = bench.get("optimization_readiness") or {}
    return "\n".join(
        [
            "<p><strong>Resources baseline</strong><br>Legacy C++ resources CLI release binary vs Rust release-native xtask commands.</p>",
            f"<p><strong>Comparable rows</strong><br>{fmt_int(readiness.get('speedup_claim_eligible_comparisons'))} process-to-process resource rows; optimized legacy baseline selected={h(readiness.get('legacy_optimized_baseline_ready'))}.</p>",
            f"<p><strong>Evidence</strong><br>{h(evidence_dir / 'bench-tier-local.json')}</p>",
        ]
    )


def tested_workloads(rows: list[dict]) -> str:
    grouped = [
        ("Tasklet scheduling", [
            "runnable_tasklets_128",
            "runnable_tasklets_1024",
            "runnable_tasklets_4096",
        ]),
        ("Channel orchestration", [
            "channel_rendezvous_32",
            "channel_rendezvous_256",
            "channel_rendezvous_1024",
        ]),
        ("Fake game study", [
            "fanout_pipeline_256b",
            "fanout_pipeline_4096b",
            "zone_tick_study_small",
            "zone_tick_study_large",
        ]),
        ("Catalog and manifest operations", [
            "create_group_directory_yaml",
            "create_group_from_filter_yaml",
            "merge_group_yaml_additive",
            "diff_group_csv_additions",
            "remove_resources_yaml",
        ]),
        ("Bundle and patch operations", [
            "create_bundle_local_cdn",
            "create_patch_local_cdn",
            "unpack_bundle_local_cdn",
            "apply_patch_local_cdn",
        ]),
    ]
    available = {row.get("workload") for row in rows}
    cards = []
    for group, workloads in grouped:
        entries = []
        for workload in workloads:
            if workload in available:
                label, description = workload_label(workload)
                entries.append(f"<li><strong>{h(label)}</strong><span>{h(description)}</span></li>")
        if entries:
            cards.append(
                f"<article><h3>{h(group)}</h3><ul>{''.join(entries)}</ul></article>"
            )
    return "\n".join(cards)


def methodology(bench: dict, rows: list[dict], evidence_dir: Path) -> str:
    host = path_value(bench, "/host/cpu_model", "unknown host")
    logical_cpus = path_value(bench, "/host/logical_cpus", "unknown")
    build_profile = bench.get("build_profile") or path_value(bench, "/host/rust_build/build_profile", "unknown")
    target_cpu = bench.get("target_cpu_native") if "target_cpu_native" in bench else path_value(bench, "/host/rust_build/target_cpu_native", "unknown")
    debug_assertions = bench.get("debug_assertions") if "debug_assertions" in bench else path_value(bench, "/host/rust_build/debug_assertions", "unknown")
    workload_set = bench.get("workload_set") or "unknown"
    samples = bench.get("samples_per_row") or "unknown"
    return "\n".join(
        [
            "<p><strong>Old baseline</strong><br>Legacy C++ <code>_scheduler.so</code> native Linux build through the Python scheduler package.</p>",
            f"<p><strong>Rust baseline</strong><br>Rust scheduler Python bridge; build={h(build_profile)}; target-cpu=native={h(target_cpu)}; debug assertions={h(debug_assertions)}</p>",
            f"<p><strong>Pressure workload set</strong><br>{h(workload_set)}; {h(samples)} samples per matched row.</p>",
            f"<p><strong>Host</strong><br>{h(host)}; {h(logical_cpus)} logical CPUs</p>",
            f"<p><strong>Evidence</strong><br>{h(evidence_dir / 'scheduler-comparison.json')}</p>",
            "<p><strong>Claim boundary</strong><br>Lab scheduler orchestration comparison; real game-environment validation remains the production gate.</p>",
        ]
    )


def blocked_report(evidence_dir: Path, bench: dict, rows: list[dict]) -> str:
    readiness = bench.get("optimization_readiness") or {}
    remaining = bench.get("remaining_before_report_ready") or []
    reason = (
        readiness.get("blocked_reason")
        or bench.get("not_report_ready_reason")
        or ("; ".join(str(item) for item in remaining) if remaining else None)
        or "publishable scheduler comparison evidence is missing"
    )
    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    return Template(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Carbon Scheduler Rewrite: Comparative Report Blocked</title>
  <style>
    body { margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #17202a; background: #f4f7f9; line-height: 1.5; }
    main { max-width: 920px; margin: 0 auto; padding: 48px 24px; }
    section { background: #fff; border: 1px solid #d7dde5; border-radius: 8px; padding: 24px; }
    h1 { margin: 0 0 12px; font-size: 2.5rem; letter-spacing: 0; line-height: 1.05; }
    p { color: #3b4652; }
    code { background: #edf1f5; border: 1px solid #dce3ea; border-radius: 4px; padding: 2px 5px; }
  </style>
</head>
<body>
  <main>
    <section>
      <h1>Scheduler comparison report is blocked</h1>
      <p>This artifact is intentionally not publishable because the main report requires matched legacy-vs-Rust scheduler comparison rows.</p>
      <p><strong>Reason:</strong> $reason</p>
      <p><strong>Rows found:</strong> $row_count</p>
      <p><strong>Evidence directory:</strong> <code>$evidence_dir</code></p>
      <p><strong>Generated:</strong> $generated</p>
    </section>
  </main>
</body>
</html>
"""
    ).safe_substitute(
        reason=h(reason),
        row_count=fmt_int(len(rows)),
        evidence_dir=h(evidence_dir),
        generated=h(generated),
    )


def render(evidence_dir: Path) -> str:
    scheduler_path = evidence_dir / "scheduler-comparison.json"
    scheduler_evidence = load_json(scheduler_path) if scheduler_path.exists() else {}
    all_scheduler_rows = scheduler_comparable_rows(scheduler_evidence)
    matched_pressure_rows = scheduler_pressure_comparable_rows(all_scheduler_rows)
    if scheduler_report_is_publishable(scheduler_evidence, matched_pressure_rows):
        bench = scheduler_evidence
        rows = matched_pressure_rows
    else:
        bench = scheduler_evidence
        rows = matched_pressure_rows
        return blocked_report(evidence_dir, bench, rows)

    resources_evidence = optional_json(evidence_dir / "bench-tier-local.json")
    resource_rows = comparable_rows(resources_evidence)
    scalability_evidence = optional_json(evidence_dir / "scalability-matrix.json")
    scheduler_pressure_rows = [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "scheduler"
    ]
    io_evidence = optional_json(evidence_dir / "io-workloads.json")
    io_comparison_rows = io_evidence.get("comparisons", []) or []
    scheduler_gates = gate_items(evidence_dir)
    scheduler_summary = build_summary(rows)
    resource_summary = build_summary(resource_rows)
    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    page = Template(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Carbon Scheduler Rewrite: Performance Evidence</title>
  <style>
    :root {
      --ink: #17212b;
      --muted: #53606d;
      --line: #d8dee6;
      --paper: #ffffff;
      --wash: #f5f7f9;
      --wash-strong: #eaf0f4;
      --green: #236f4e;
      --blue: #235f8f;
      --amber: #9a6816;
      --red: #9b3d3d;
      --violet: #6652a3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: var(--wash);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.48;
    }
    header {
      background: var(--paper);
      border-bottom: 1px solid var(--line);
    }
    .hero {
      max-width: 1220px;
      margin: 0 auto;
      padding: 34px 24px 28px;
    }
    .eyebrow {
      margin: 0 0 10px;
      color: var(--blue);
      font-size: 0.78rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      max-width: 980px;
      font-size: clamp(2.05rem, 3.2vw, 3.45rem);
      line-height: 1.04;
      letter-spacing: 0;
    }
    .lead {
      max-width: 880px;
      margin: 14px 0 0;
      color: var(--muted);
      font-size: 1.08rem;
    }
    .hero .metric-grid {
      margin-top: 20px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    main {
      max-width: 1220px;
      margin: 0 auto;
      padding: 28px 24px 56px;
    }
    section { margin: 0 0 36px; }
    h2 {
      margin: 0 0 14px;
      font-size: 1.55rem;
      letter-spacing: 0;
    }
    h3 {
      margin: 0 0 8px;
      font-size: 1rem;
      letter-spacing: 0;
    }
    p { margin: 0 0 12px; }
    small { display: block; color: var(--muted); }
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin: 0 0 14px;
    }
    .section-head p {
      max-width: 620px;
      color: var(--muted);
      margin: 0;
    }
    .tag {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 4px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--paper);
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      white-space: nowrap;
    }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 12px;
    }
    .metric, .panel, .arch-grid article, .tested-grid article, .callout {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }
    .metric { padding: 15px; }
    .metric, .panel, .arch-grid article, .tested-grid article, .callout, .table-wrap {
      max-width: 100%;
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 0.74rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .metric strong {
      display: block;
      margin-top: 4px;
      font-size: 1.55rem;
      line-height: 1.05;
      overflow-wrap: anywhere;
    }
    .metric small {
      min-height: 2.7em;
    }
    .takeaways {
      display: grid;
      grid-template-columns: minmax(0, 1.05fr) minmax(300px, 0.95fr);
      gap: 18px;
      align-items: start;
    }
    .panel { padding: 18px; }
    .panel ul { margin: 0; padding-left: 20px; }
    .panel li + li { margin-top: 8px; }
    .callout {
      border-left: 5px solid var(--blue);
      padding: 16px 18px;
      color: var(--muted);
    }
    .callout strong { color: var(--ink); }
    .arch-grid, .tested-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .arch-grid article, .tested-grid article { padding: 16px; }
    .tested-grid ul { margin: 0; padding-left: 18px; }
    .tested-grid li + li { margin-top: 8px; }
    .tested-grid span { display: block; color: var(--muted); }
    .table-wrap {
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--paper);
    }
    table {
      width: 100%;
      min-width: 980px;
      border-collapse: collapse;
      background: var(--paper);
      font-size: 0.92rem;
    }
    th, td {
      padding: 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      background: var(--wash-strong);
      color: #304050;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      white-space: nowrap;
    }
    tr:last-child td { border-bottom: 0; }
    td strong { display: block; }
    td small { margin-top: 4px; }
    .bar {
      height: 9px;
      margin-top: 6px;
      border-radius: 3px;
      overflow: hidden;
      background: #e3e9ef;
    }
    .bar span {
      display: block;
      height: 100%;
      background: var(--green);
    }
    .bar span[style*="width:2.0"] { background: var(--amber); }
    code {
      background: #edf1f5;
      border: 1px solid #dce3ea;
      border-radius: 4px;
      padding: 1px 5px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.86em;
    }
    .methodology {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 10px;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .methodology p { overflow-wrap: anywhere; }
    @media (max-width: 820px) {
      .hero { padding: 32px 18px 24px; }
      main { padding: 22px 18px 44px; }
      .takeaways { grid-template-columns: 1fr; }
      .section-head { display: block; }
      .section-head .tag { margin-top: 8px; }
      h1 { font-size: 2.05rem; }
      .hero .metric-grid { grid-template-columns: 1fr 1fr; }
      .metric-grid { grid-template-columns: 1fr 1fr; }
      table { min-width: 760px; font-size: 0.84rem; }
      th, td { padding: 8px; }
    }
    @media (max-width: 520px) {
      .hero { padding: 28px 18px 22px; }
      h1 { font-size: 1.95rem; }
      .lead { font-size: 1rem; }
      .hero .metric-grid,
      .metric-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class="hero">
      <p class="eyebrow">Carbon scheduler rewrite evidence</p>
      <h1>Carbon scheduler parity is green in the lab; performance work remains.</h1>
      <p class="lead">This report is scheduler-first. It compares matched pressure rows for the legacy C++ scheduler extension and the Rust scheduler bridge through the same Python tasklet/channel API, then separates resource-tool wins and Rust-only pressure evidence so the claims stay clear.</p>
      <div class="metric-grid">
        $top_line_cards
      </div>
    </div>
  </header>
  <main>
    <section class="takeaways">
      <div>
        <div class="section-head">
          <div>
            <h2>Measured Breakdown</h2>
            <p>Use this as the stats read: scheduler comparison first, resource wins second, and pressure/IO evidence called out as non-comparable where appropriate.</p>
          </div>
          <span class="tag">Generated $generated</span>
        </div>
        <div class="metric-grid">
          $breakdown_cards
        </div>
      </div>
      <aside class="panel">
        <h2>What Changed</h2>
        <ul>
          <li>The scheduler path under test keeps the legacy Python API, but routes covered run-queue, tasklet, channel, and switch-trap decisions through Rust-owned scheduler state.</li>
          <li>Each matched pressure row records semantic checksum parity plus throughput, p99/p99.9 latency, CPU p95, peak RSS p95, and throughput CV.</li>
          <li>The next production gate is a real game-environment scheduler workload; the pressure table is lab orchestration evidence, not a production claim.</li>
        </ul>
      </aside>
    </section>

    <section>
      <div class="callout">
        <strong>Bottom line:</strong> this is no longer a file-copy benchmark or a Rust-language claim. The scheduler question is whether tasklet orchestration becomes cheaper, more predictable, and easier to scale across domains. Today the parity harness exists and shows zero semantic mismatches in the lab rows; the measured bridge still needs optimization before speedup claims.
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Scheduler Evidence Status</h2>
          <p>The scheduler repo remains the focus. These cards show the parity base behind the performance table.</p>
        </div>
        <span class="tag">Scheduler repo</span>
      </div>
      <div class="metric-grid">
        $evidence_status_cards
      </div>
    </section>

    <section>
      <h2>Architecture Change</h2>
      <div class="arch-grid">
        $architecture
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Scheduler Validation Gate</h2>
          <p>What can be said now, and what remains open before broad production claims.</p>
        </div>
        <span class="tag">Claim boundary</span>
      </div>
      <div class="arch-grid">
        $scheduler_story
      </div>
    </section>

    <section>
      <h2>Scheduler Gate Detail</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Gate</th>
              <th>Status</th>
              <th>Report ready</th>
              <th>Coverage</th>
              <th>Remaining blocker</th>
            </tr>
          </thead>
          <tbody>
            $scheduler_gate_rows
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <h2>What Was Tested</h2>
      <div class="tested-grid">
        $tested_workloads
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Comparable Pressure</h2>
          <p>Matched legacy C++ scheduler extension vs Rust scheduler bridge. Same Python tasklet/channel API, exact pressure workload set, release-native Rust build, zero semantic mismatches in accepted rows.</p>
        </div>
        <span class="tag">Lab-only speedup</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pressure</th>
              <th>Speedup</th>
              <th>Throughput</th>
              <th>p99 latency</th>
              <th>p99.9 latency</th>
              <th>CPU p95</th>
              <th>Peak RSS p95</th>
              <th>Stability</th>
              <th>Parity</th>
            </tr>
          </thead>
          <tbody>
            $scheduler_rows
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Resources CLI Results</h2>
          <p>Separate from scheduler work: optimized legacy resources CLI vs Rust release-native resource commands.</p>
        </div>
        <span class="tag">Resources comparison</span>
      </div>
      <div class="metric-grid">
        $resource_cards
      </div>
      <div class="table-wrap" style="margin-top:12px">
        <table>
          <thead>
            <tr>
              <th>Workload</th>
              <th>Wall latency</th>
              <th>p50 latency</th>
              <th>p99 tail</th>
              <th>Throughput</th>
              <th>CPU burn</th>
              <th>Peak memory</th>
              <th>Parity</th>
            </tr>
          </thead>
          <tbody>
            $resource_rows
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Rust-Only Pressure</h2>
          <p>Rust scheduler-core pressure evidence only. These rows show scaling shape and tail behavior, not old-vs-Rust speedups.</p>
        </div>
        <span class="tag">Rust-only pressure</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pressure</th>
              <th>Throughput</th>
              <th>p99 latency</th>
              <th>p99.9 latency</th>
              <th>CPU p95</th>
              <th>Peak RSS p95</th>
              <th>Stability</th>
            </tr>
          </thead>
          <tbody>
            $pressure_rows
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>IO And Network Loopback Rows</h2>
          <p>Socket/TLS request, byte, CPU, and memory stats are included for completeness. They are not legacy Carbon IO comparisons yet.</p>
        </div>
        <span class="tag">Not legacy comparable</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Workload</th>
              <th>p99 latency</th>
              <th>Requests/sec</th>
              <th>Bytes/sec</th>
              <th>CPU burn</th>
              <th>Peak memory</th>
              <th>Boundary</th>
            </tr>
          </thead>
          <tbody>
            $io_rows
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Methodology</h2>
      <div class="methodology">
        $methodology
        $resources_methodology
        <p><strong>Scheduler pressure evidence</strong><br>$scalability_evidence_path</p>
        <p><strong>IO loopback evidence</strong><br>$io_evidence_path</p>
      </div>
    </section>
  </main>
</body>
</html>
"""
    )
    return page.safe_substitute(
        top_line_cards=top_line_cards(scheduler_summary, resource_summary, rows),
        breakdown_cards=performance_breakdown_cards(
            scheduler_summary,
            resource_summary,
            scalability_evidence,
            io_comparison_rows,
        ),
        resource_cards=summary_cards(resource_summary, subject="Resources"),
        evidence_status_cards=evidence_status_cards(evidence_dir),
        architecture=architecture_section(),
        scheduler_story=scheduler_story_section(scheduler_gates),
        scheduler_gate_rows=scheduler_gate_rows(scheduler_gates),
        tested_workloads=tested_workloads(rows + resource_rows),
        scheduler_rows=scheduler_pressure_comparison_rows(
            rows,
        ),
        resource_rows=comparison_table_rows(
            resource_rows,
            empty_text="No comparable resource rows available.",
        ),
        pressure_rows=pressure_rows(scheduler_pressure_rows),
        io_rows=io_rows(io_comparison_rows),
        methodology=methodology(bench, rows, evidence_dir),
        resources_methodology=resources_methodology(resources_evidence, evidence_dir),
        scalability_evidence_path=h(evidence_dir / "scalability-matrix.json"),
        io_evidence_path=h(evidence_dir / "io-workloads.json"),
        generated=h(generated),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    html_text = render(args.evidence_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html_text, encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
