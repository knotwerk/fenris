#!/usr/bin/env python3
"""Render the Carbon to Rust migration test report from evidence JSON."""

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
DEFAULT_OUTPUT = Path("target/carbon/report/carbon-to-rust-migration-test.html")
DEFAULT_GUIDE_OUTPUT = Path("target/carbon/report/carbon-to-rust-reporting-guide.html")

# Report generator notes, intentionally not rendered:
# - The key stats table is the single place for performance stats.
# - Each workload row keeps the same contract: original path, Rust parity path,
#   Rust architecture path, speed/scale, p99/tail, CPU/RSS, batch/scale, and
#   readout/evidence status.
# - Group related Arrow IPC, Parquet, scheduler pressure, socket, and resource
#   tests into one summary row; split only when the decision differs.
# - If a lane has no measurement, leave a visible evidence-gap cell, especially for
#   Network / IO and architecture-only probes.
# - Do not render task lists, repeated benchmark dumps, collapsed duplicate
#   tables, or a speedup claim unless original and replacement measure the same
#   end-to-end function.


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
        "Synthetic zone tick",
        "Synthetic zones, entity updates, network-like messages, and aggregation.",
    ),
    "zone_tick_study_large": (
        "Synthetic zone tick, large",
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


def higher_is_better_ratio(old: object, rust: object) -> float | None:
    old_value = number(old)
    rust_value = number(rust)
    if old_value in (None, 0) or rust_value is None:
        return None
    return rust_value / old_value


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


def fmt_ms(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if amount >= 1000:
        return f"{amount / 1000.0:.2f} s"
    return f"{amount:.1f} ms"


def fmt_ratio(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    return f"{amount:.2f}x"


def fmt_multiplier(value: object) -> str:
    amount = number(value)
    if amount is None or amount == 0:
        return "n/a"
    if amount >= 100:
        return f"{amount:,.0f}x"
    if amount >= 10:
        return f"{amount:.1f}x"
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
    if abs(amount) < 0.5:
        return "0% change"
    direction = "lower" if amount >= 0 else "higher"
    return f"{abs(amount):.0f}% {direction}"


def fmt_kb(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if amount >= 1024:
        return f"{amount / 1024:.1f} MB"
    return f"{amount:.0f} KB"


def fmt_bytes(value: object) -> str:
    amount = number(value)
    if amount is None:
        return "n/a"
    if amount >= 1024 * 1024:
        return f"{amount / (1024 * 1024):.1f} MB"
    if amount >= 1024:
        return f"{amount / 1024:.1f} KB"
    return f"{amount:.0f} B"


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
    labels = {
        "directories": "dirs/s",
        "events": "events/s",
        "messages": "msg/s",
        "operations": "ops/s",
        "requests": "req/s",
        "resources": "resources/s",
        "rows": "rows/s",
    }
    suffix = labels.get(unit, f"{unit}/s")
    return f"{fmt_count(amount)} {suffix}"


def fmt_rate_range(values: list[object], unit: str) -> str:
    numeric = [float(value) for value in values if number(value) is not None]
    if not numeric:
        return "n/a"
    low = min(numeric)
    high = max(numeric)
    if abs(low - high) < 0.005:
        return fmt_rate(low, unit)
    return f"{fmt_rate(low, unit)} to {fmt_rate(high, unit)}"


def fmt_bytes_range(values: list[object]) -> str:
    numeric = [float(value) for value in values if number(value) is not None]
    if not numeric:
        return "n/a"
    low = min(numeric)
    high = max(numeric)
    if abs(low - high) < 0.5:
        return fmt_bytes(low)
    return f"{fmt_bytes(low)} to {fmt_bytes(high)}"


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


def merge_scheduler_architecture_rows(scalability_evidence: dict, architecture_evidence: dict) -> dict:
    if not architecture_evidence.get("native_rows"):
        return scalability_evidence
    merged = dict(scalability_evidence)
    rows = []
    architecture_comparisons = architecture_evidence.get("architecture_comparisons") or architecture_evidence.get("comparisons") or []
    comparison_by_native_row = {
        str(row.get("native_row") or ""): row for row in architecture_comparisons
    }
    architecture_rows_by_key = {}
    for row in architecture_evidence.get("native_rows", []) or []:
        enriched = dict(row)
        comparison = comparison_by_native_row.get(str(row.get("workload") or ""))
        if comparison:
            enriched["_architecture_comparison"] = comparison
        key = (
            str(enriched.get("component") or ""),
            str(enriched.get("family") or ""),
            str(enriched.get("workload") or ""),
            str(enriched.get("architecture_comparison_key") or ""),
        )
        architecture_rows_by_key[key] = enriched
    for row in list(merged.get("rows", []) or []):
        key = (
            str(row.get("component") or ""),
            str(row.get("family") or ""),
            str(row.get("workload") or ""),
            str(row.get("architecture_comparison_key") or ""),
        )
        rows.append(architecture_rows_by_key.pop(key, row))
    rows.extend(architecture_rows_by_key.values())
    merged["rows"] = rows
    return merged


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


def reconciliation_mismatch_count(rows: list[dict]) -> int | None:
    total = 0
    seen = False
    for row in rows:
        mismatch_count = path_value(row, "/reconciliation/mismatch_count")
        amount = number(mismatch_count)
        if amount is None:
            continue
        seen = True
        total += int(amount)
    return total if seen else None


def reconciliation_text(row: dict) -> str:
    status = path_value(row, "/reconciliation/status") or row.get("parity_status") or "n/a"
    mismatch_count = path_value(row, "/reconciliation/mismatch_count")
    checks = [
        ("spec", path_value(row, "/reconciliation/checks/workload_spec_match")),
        ("samples", path_value(row, "/reconciliation/checks/sample_count_match")),
        ("ops", path_value(row, "/reconciliation/checks/operation_count_match")),
        ("messages", path_value(row, "/reconciliation/checks/message_count_match")),
        ("bytes", path_value(row, "/reconciliation/checks/data_bytes_processed_match")),
        ("checksum", path_value(row, "/reconciliation/checks/semantic_checksum_match")),
    ]
    failed = [label for label, ok in checks if ok is False]
    if failed:
        return f"{status}; failed: {', '.join(failed)}"
    if mismatch_count is not None:
        return f"same workload/counts/checksum; {fmt_int(mismatch_count)} reconciliation mismatches"
    return f"{status}; reconciliation not recorded"


def top_line_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_rows: list[dict],
    fixtures_evidence: dict,
) -> str:
    mismatch_count = semantic_mismatch_count(scheduler_rows)
    reconciliation_count = reconciliation_mismatch_count(scheduler_rows)
    row_count = scheduler_summary["rows"]
    fixture_passed = fixtures_evidence.get("passed")
    fixture_count = fixtures_evidence.get("fixture_count")
    return "\n".join(
        [
            metric_card(
                "Scheduler parity",
                f"{fmt_int(row_count)}/{fmt_int(row_count)} rows",
                f"{fmt_int(mismatch_count)} semantic mismatches; {fmt_int(reconciliation_count)} reconciliation mismatches; {fmt_int(fixture_passed)}/{fmt_int(fixture_count)} semantic fixtures",
            ),
            metric_card(
                "Scheduler throughput",
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


def native_resource_rows_data(evidence: dict) -> list[dict]:
    return [
        row
        for row in evidence.get("rows", []) or []
        if row.get("component") == "resources"
        and row.get("serialization_format")
        and not str(row.get("workload") or "").startswith("data_catalog_interchange_")
    ]


def catalog_text_baseline_rows(evidence: dict) -> list[dict]:
    return [
        row
        for row in evidence.get("rows", []) or []
        if row.get("component") == "resources"
        and row.get("serialization_format") is None
        and str(row.get("workload") or "").startswith("data_catalog_roundtrip")
    ]


def catalog_text_baselines_by_record_count(evidence: dict) -> dict[int, dict]:
    baselines = {}
    for row in catalog_text_baseline_rows(evidence):
        record_count = number(path_value(row, "/pressure/record_count"))
        if record_count is not None:
            baselines[int(record_count)] = row
    return baselines


def native_resource_ratio_pairs(evidence: dict, metric: str) -> dict[str, list[float]]:
    baselines = catalog_text_baselines_by_record_count(evidence)
    grouped: dict[str, list[float]] = {}
    for row in native_resource_rows_data(evidence):
        record_count = number(path_value(row, "/pressure/record_count"))
        if record_count is None:
            continue
        baseline = baselines.get(int(record_count))
        if not baseline:
            continue
        baseline_value = number(baseline.get(metric))
        native_value = number(row.get(metric))
        if baseline_value in (None, 0) or native_value is None:
            continue
        grouped.setdefault(str(row.get("serialization_format") or "native"), []).append(
            native_value / baseline_value
        )
    return grouped


def fmt_ratio_range(values: list[float], *, faster_label: str = "faster", slower_label: str = "slower") -> str:
    numeric = [float(value) for value in values if number(value) is not None]
    if not numeric:
        return "n/a"
    low = min(numeric)
    high = max(numeric)
    if abs(low - high) < 0.005:
        return fmt_directional_ratio(low, faster_label=faster_label, slower_label=slower_label)
    return f"{fmt_directional_ratio(low, faster_label=faster_label, slower_label=slower_label)} to {fmt_directional_ratio(high, faster_label=faster_label, slower_label=slower_label)}"


def fmt_plain_ratio_range(values: list[object]) -> str:
    numeric = [float(value) for value in values if number(value) is not None]
    if not numeric:
        return "Not measured"
    low = min(numeric)
    high = max(numeric)
    if abs(low - high) < 0.005:
        return fmt_multiplier(low)
    return f"{fmt_multiplier(low)} to {fmt_multiplier(high)}"


def median_value(values: list[object]) -> float | None:
    numeric = [float(value) for value in values if number(value) is not None]
    return statistics.median(numeric) if numeric else None


def workload_rows(rows: list[dict], prefixes: tuple[str, ...]) -> list[dict]:
    return [
        row
        for row in rows
        if str(row.get("workload") or "").startswith(prefixes)
    ]


def comparison_summary_text(rows: list[dict]) -> str:
    if not rows:
        return "n/a"
    median_speedup = median_value([row.get("speedup") for row in rows])
    old_wall = median_value([row.get("legacy_duration_us") for row in rows])
    rust_wall = median_value([row.get("rust_duration_us") for row in rows])
    return (
        f"{fmt_directional_ratio(median_speedup)}"
        f"<small>{fmt_int(len(rows))} rows; median wall {fmt_ms_from_us(old_wall)} -> {fmt_ms_from_us(rust_wall)}</small>"
    )


def parity_text(rows: list[dict], *, fixture_count: object | None = None, fixture_passed: object | None = None) -> str:
    mismatch_count = semantic_mismatch_count(rows)
    reconciliation_count = reconciliation_mismatch_count(rows)
    pass_count = sum(1 for row in rows if row.get("parity_status") == "pass")
    row_count = len(rows)
    mismatch_text = (
        f"{fmt_int(mismatch_count)} mismatches"
        if mismatch_count is not None
        else "parity pass"
    )
    reconciliation_suffix = (
        f"; {fmt_int(reconciliation_count)} reconciliation mismatches"
        if reconciliation_count is not None
        else ""
    )
    if fixture_count is not None or fixture_passed is not None:
        return (
            f"<small>{fmt_int(fixture_passed)}/{fmt_int(fixture_count)} fixtures; "
            f"{fmt_int(row_count)} benchmark rows; {mismatch_text}{reconciliation_suffix}</small>"
        )
    return (
        f"<small>{fmt_int(pass_count)}/{fmt_int(row_count)} rows pass; {mismatch_text}{reconciliation_suffix}</small>"
    )


def median_wall_text(rows: list[dict], key: str) -> str:
    return fmt_ms_from_us(median_value([row.get(key) for row in rows]))


def catalog_baseline_text(scalability_evidence: dict) -> str:
    baselines = catalog_text_baseline_rows(scalability_evidence)
    if not baselines:
        return "No measured catalog text baseline"
    values = [row.get("throughput_rows_per_sec") for row in baselines]
    low = min(float(value) for value in values if number(value) is not None)
    high = max(float(value) for value in values if number(value) is not None)
    if low == high:
        return fmt_rate(low, "rows")
    return f"{fmt_rate(low, 'rows')} to {fmt_rate(high, 'rows')}"


def catalog_interchange_rows(scalability_evidence: dict) -> list[dict]:
    rows = []
    for row in scalability_evidence.get("rows", []) or []:
        workload = str(row.get("workload") or "")
        comparability = str(row.get("comparability") or "")
        if row.get("component") != "resources":
            continue
        if workload.startswith("data_catalog_interchange_") or comparability == "comparable_end_to_end_catalog_interchange_same_logical_records":
            rows.append(row)
    return rows


def bytes_ratio_text(old_bytes: object, new_bytes: object) -> str:
    old_value = number(old_bytes)
    new_value = number(new_bytes)
    if old_value in (None, 0) or new_value is None:
        return "wire bytes n/a"
    return fmt_directional_ratio(old_value / new_value, faster_label="fewer wire bytes", slower_label="more wire bytes")


def catalog_interchange_summary_text(scalability_evidence: dict) -> str | None:
    rows = catalog_interchange_rows(scalability_evidence)
    if not rows:
        return None
    parts = []
    for row in sorted(
        rows,
        key=lambda item: (
            str(item.get("serialization_format") or ""),
            int(number(path_value(item, "/pressure/record_count")) or 0),
        ),
    ):
        format_name = str(row.get("serialization_format") or "native").replace("_", " ").title()
        record_count = fmt_int(path_value(row, "/pressure/record_count"))
        speedup = fmt_directional_ratio(row.get("speedup"))
        legacy_rate = fmt_rate(row.get("legacy_throughput_rows_per_sec"), "rows")
        rust_rate = fmt_rate(row.get("rust_throughput_rows_per_sec"), "rows")
        wire_ratio = bytes_ratio_text(row.get("legacy_bytes_over_wire"), row.get("rust_bytes_over_wire"))
        parts.append(
            f"{format_name} {record_count}: {speedup}; rows {legacy_rate} -> {rust_rate}; {wire_ratio}"
        )
    return "Measured end-to-end catalog interchange. " + "; ".join(parts)


def catalog_format_label(format_name: str) -> str:
    labels = {
        "arrow_ipc": "Arrow IPC",
        "parquet_zstd": "Parquet/Zstd",
    }
    return labels.get(format_name, format_name.replace("_", " ").title())


def catalog_interchange_format_summaries(rows: list[dict]) -> list[str]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("serialization_format") or "native"), []).append(row)
    summaries = []
    for format_name in sorted(grouped):
        group = grouped[format_name]
        wire_ratios = [
            (number(row.get("legacy_bytes_over_wire")) or 0.0)
            / (number(row.get("rust_bytes_over_wire")) or 1.0)
            for row in group
            if number(row.get("legacy_bytes_over_wire")) not in (None, 0)
            and number(row.get("rust_bytes_over_wire")) not in (None, 0)
        ]
        summaries.append(
            f"{catalog_format_label(format_name)}: "
            f"{fmt_ratio_range([row.get('speedup') for row in group])}; "
            f"{fmt_rate_range([row.get('rust_throughput_rows_per_sec') for row in group], 'rows')}; "
            f"{fmt_ratio_range(wire_ratios, faster_label='fewer wire bytes', slower_label='more wire bytes')}"
        )
    return summaries


def native_resource_architecture_text(scalability_evidence: dict) -> str:
    catalog_interchange = catalog_interchange_summary_text(scalability_evidence)
    if catalog_interchange:
        return catalog_interchange
    rows = native_resource_rows_data(scalability_evidence)
    if not rows:
        return "No Arrow IPC or Parquet rows measured."
    parts = []
    for row in sorted(
        rows,
        key=lambda item: (
            str(item.get("serialization_format") or ""),
            int(number(path_value(item, "/pressure/record_count")) or 0),
        ),
    ):
        format_name = str(row.get("serialization_format") or "native").replace("_", " ").title()
        record_count = fmt_int(path_value(row, "/pressure/record_count"))
        parts.append(
            f"{format_name} {record_count}: {fmt_rate(row.get('throughput_rows_per_sec'), 'rows')}, {fmt_rate(row.get('throughput_data_bytes_per_sec'), 'bytes')}"
        )
    return (
        "Current evidence only measures standalone Rust native-format round-trips. "
        "The fair old-vs-new workload is still missing: YAML/text encode, compression/transmit, "
        "decompress/parse vs Arrow IPC or Parquet write, transmit, read for the same logical records. "
        + "; ".join(parts)
    )


def catalog_interchange_dashboard(scalability_evidence: dict, native_text: str) -> dict[str, str]:
    rows = catalog_interchange_rows(scalability_evidence)
    if not rows:
        return {
            "original_path": "YAML/text resource catalog",
            "original_metric": "Fair baseline missing: encode, compression/transmit, decompress, and parse for the same logical catalog records.",
            "parity_path": "No parity row yet",
            "parity_status": "<small>Current native rows are standalone Rust measurements only.</small>",
            "parity_metric": "End-to-end old-vs-new result missing",
            "architecture_change": "Arrow IPC / Parquet catalog path",
            "architecture_metric": native_text,
            "comment": "Do not use byte-to-byte transmit or standalone format rows as the comparison. Add the end-to-end catalog interchange benchmark before claiming a win.",
    }

    best = max(rows, key=lambda row: number(row.get("speedup")) or 0.0)
    format_summaries = catalog_interchange_format_summaries(rows)
    return {
        "original_path": "YAML/text resource catalog",
        "original_metric": (
            f"YAML+gzip end-to-end baseline: "
            f"{fmt_rate_range([row.get('legacy_throughput_rows_per_sec') for row in rows], 'rows')}; "
            f"wire {fmt_bytes_range([row.get('legacy_bytes_over_wire') for row in rows])}"
        ),
        "parity_path": "Same logical catalog workload",
        "parity_status": "<small>Measured end-to-end, same logical records.</small>",
        "parity_metric": "<br>".join(h(summary) for summary in format_summaries),
        "architecture_change": "Arrow IPC / Parquet catalog path",
        "architecture_metric": (
            f"Best row: {catalog_format_label(str(best.get('serialization_format') or 'native'))} "
            f"{fmt_directional_ratio(best.get('speedup'))}, "
            f"{fmt_rate(best.get('rust_throughput_rows_per_sec'), 'rows')}; "
            f"wire {fmt_bytes(best.get('rust_bytes_over_wire'))}"
        ),
        "comment": "This is the workload comparison: serialize, transmit bytes in memory, and deserialize on both paths.",
    }


def html_cell(title: str, note: str = "") -> str:
    suffix = f"<small>{note}</small>" if note else ""
    return f"<strong>{h(title)}</strong>{suffix}"


def todo_cell(note: str = "not measured yet") -> str:
    return f"<strong>Not measured</strong><small>{h(note)}</small>"


def median_path_value(rows: list[dict], path: str) -> float | None:
    return median_value([path_value(row, path) for row in rows])


def throughput_range_text(rows: list[dict], side: str) -> str:
    grouped: dict[str, list[object]] = {}
    for row in rows:
        unit, legacy_value, rust_value = throughput_pair(row)
        value = legacy_value if side == "legacy" else rust_value
        if number(value) is not None:
            grouped.setdefault(unit, []).append(value)
    if not grouped:
        return "n/a"
    if len(grouped) == 1:
        unit, values = next(iter(grouped.items()))
        return fmt_rate_range(values, unit)
    return f"{fmt_int(sum(len(values) for values in grouped.values()))} rows; mixed units"


def lane_speed_cell(rows: list[dict], side: str) -> str:
    if not rows:
        return todo_cell("no speed or scale row")
    duration_key = "legacy_duration_us" if side == "legacy" else "rust_duration_us"
    return (
        f"<strong>{h(throughput_range_text(rows, side))}</strong>"
        f"<small>median wall {median_wall_text(rows, duration_key)}</small>"
    )


def lane_tail_cell(rows: list[dict], side: str) -> str:
    if not rows:
        return todo_cell("no p99/tail row")
    root = "legacy_sample_stats_us" if side == "legacy" else "rust_sample_stats_us"
    p50 = median_path_value(rows, f"/{root}/p50")
    p99 = median_path_value(rows, f"/{root}/p99")
    p99_9 = median_path_value(rows, f"/{root}/p99_9")
    p999_text = f"; p99.9 {fmt_us(p99_9)}" if p99_9 is not None else ""
    return f"<strong>p99 {fmt_us(p99)}</strong><small>p50 {fmt_us(p50)}{p999_text}</small>"


def lane_process_cell(rows: list[dict], side: str) -> str:
    if not rows:
        return todo_cell("no CPU/RSS row")
    root = "legacy_process_stats" if side == "legacy" else "rust_process_stats"
    cpu = median_path_value(rows, f"/{root}/cpu_percent/p95")
    rss = median_path_value(rows, f"/{root}/max_rss_kb/p95")
    burn = median_path_value(rows, f"/{root}/cpu_burn_effective_ms/mean")
    return f"<strong>CPU {fmt_percent(cpu)}</strong><small>{fmt_kb(rss)} RSS p95; burn {fmt_ms(burn)}</small>"


def parity_multiple_cell(rows: list[dict]) -> str:
    if not rows:
        return todo_cell("no parity multiple")
    speedups = [row.get("speedup") for row in rows if number(row.get("speedup")) is not None]
    return (
        f"<strong>{h(fmt_ratio_range(speedups))}</strong>"
        f"<small>median {fmt_directional_ratio(median_value(speedups))}</small>"
    )


def native_scheduler_rows(scalability_evidence: dict) -> list[dict]:
    return [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "scheduler" and row.get("family") == "native-scheduler"
    ]


def native_scheduler_rows_for(rows: list[dict], prefixes: tuple[str, ...]) -> list[dict]:
    return [
        row
        for row in rows
        if str(row.get("architecture_comparison_key") or "").startswith(prefixes)
    ]


def architecture_comparable_baseline(comparison: dict, metric: str, side: str) -> object:
    prefix = "legacy" if side == "legacy" else "rust"
    if metric in ("native_messages_per_sec", "native_rendezvous_per_sec"):
        return comparison.get(f"{prefix}_throughput_messages_per_sec")
    return comparison.get(f"{prefix}_throughput_operations_per_sec")


def architecture_multiples(
    architecture_rows: list[dict],
    comparison_rows: list[dict],
    side: str,
) -> list[float]:
    comparison_by_workload = {
        str(row.get("workload") or ""): row for row in comparison_rows
    }
    multiples = []
    for row in architecture_rows:
        comparison = comparison_by_workload.get(str(row.get("architecture_comparison_key") or ""))
        if not comparison:
            continue
        joined = row.get("_architecture_comparison") or {}
        if side == "rust" and number(joined.get("native_over_bridge_comparable_throughput_ratio")) is not None:
            multiples.append(float(joined["native_over_bridge_comparable_throughput_ratio"]))
            continue
        metric = str(joined.get("native_comparable_metric") or "")
        arch_value = joined.get("native_comparable_throughput_per_sec")
        if number(arch_value) is None:
            arch_value = row.get("throughput_messages_per_sec") if metric == "native_messages_per_sec" else None
        if number(arch_value) is None:
            arch_value = row.get("throughput_completed_units_per_sec") if metric in ("native_rendezvous_per_sec", "native_tasklets_per_sec") else None
        if number(arch_value) is None:
            arch_value = row.get("throughput_operations_per_sec")
        baseline = architecture_comparable_baseline(comparison, metric, side)
        if number(baseline) not in (None, 0) and number(arch_value) is not None:
            multiples.append(float(arch_value) / float(baseline))
    return multiples


def architecture_speed_cell(architecture_rows: list[dict], comparison_rows: list[dict]) -> str:
    if not architecture_rows:
        return todo_cell("no architecture speed row")
    vs_original = architecture_multiples(architecture_rows, comparison_rows, "legacy")
    vs_parity = architecture_multiples(architecture_rows, comparison_rows, "rust")
    multiple_text = "multiple pending"
    if vs_original:
        multiple_text = f"{fmt_ratio_range(vs_original)} vs original"
        if vs_parity:
            multiple_text += f"; {fmt_ratio_range(vs_parity)} vs parity"
    return (
        f"<strong>{h(fmt_rate_range([row.get('throughput_operations_per_sec') for row in architecture_rows], 'operations'))}</strong>"
        f"<small>{h(multiple_text)}</small>"
    )


def headline_speedup_cell(
    parity_rows: list[dict],
    architecture_rows: list[dict],
    comparison_rows: list[dict],
    architecture_label: str = "No Python interop",
) -> str:
    parity_values = [
        row.get("speedup") for row in parity_rows if number(row.get("speedup")) is not None
    ]
    parity_text = fmt_plain_ratio_range(parity_values)
    vs_original = architecture_multiples(architecture_rows, comparison_rows, "legacy")
    vs_parity = architecture_multiples(architecture_rows, comparison_rows, "rust")
    if vs_original:
        architecture_text = f"{fmt_plain_ratio_range(vs_original)} vs old"
        if vs_parity:
            architecture_text += f"; {fmt_plain_ratio_range(vs_parity)} vs parity"
    elif architecture_rows:
        architecture_text = "measured; multiple pending"
    else:
        architecture_text = "Not measured"
    return (
        "<div class=\"headline-speedup\">"
        f"<strong><span>Parity</span>{h(parity_text)}</strong>"
        f"<strong><span>{h(architecture_label)}</span>{h(architecture_text)}</strong>"
        "</div>"
    )


def headline_speedup_architecture_only(label: str, values: list[object], note: str = "vs old") -> str:
    return (
        "<div class=\"headline-speedup\">"
        "<strong><span>Parity</span>Not measured</strong>"
        f"<strong><span>{h(label)}</span>{h(fmt_plain_ratio_range(values))} {h(note)}</strong>"
        "</div>"
    )


def headline_speedup_todo(parity_note: str = "Not measured", architecture_note: str = "Not measured") -> str:
    return (
        "<div class=\"headline-speedup\">"
        f"<strong><span>Parity</span>{h(parity_note)}</strong>"
        f"<strong><span>Architecture</span>{h(architecture_note)}</strong>"
        "</div>"
    )


def top_summary_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_rows: list[dict],
    resource_rows: list[dict],
    fixtures_evidence: dict,
    scalability_evidence: dict,
) -> str:
    native_rows = native_scheduler_rows(scalability_evidence)
    catalog_rows = catalog_interchange_rows(scalability_evidence)
    scheduler_parity = fmt_plain_ratio_range(
        [row.get("speedup") for row in scheduler_rows]
    )
    no_python_vs_old = fmt_plain_ratio_range(
        architecture_multiples(native_rows, scheduler_rows, "legacy")
    )
    catalog_speedup = fmt_plain_ratio_range([row.get("speedup") for row in catalog_rows])
    resource_rss = fmt_signed_percent(resource_summary.get("median_rss_reduction"))
    native_scheduler_rss = fmt_kb(
        median_path_value(native_rows, "/process_stats/max_rss_kb/p95")
    )
    fixture_passed = fixtures_evidence.get("passed")
    fixture_count = fixtures_evidence.get("fixture_count")
    cards = [
        (
            "Port evidence",
            f"{fmt_int(fixture_passed)}/{fmt_int(fixture_count)} scheduler fixtures",
            f"{fmt_int(len(resource_rows))} resource CLI rows also run through parity evidence.",
        ),
        (
            "Parity reality",
            f"Scheduler parity is {scheduler_parity}",
            "The legacy C++ scheduler extension is already well optimized around Python/Stackless interop and is more directly tied to CPython than the current Rust/PyO3 bridge. Some same-API gap may therefore be bridge integration overhead, not scheduler semantics alone.",
        ),
        (
            "No Python interop",
            f"{no_python_vs_old} architecture probe",
            "Pure Rust scheduler-core pressure rows show the upside hidden by bridge, greenlet, and refcount costs.",
        ),
        (
            "Arrow / Parquet",
            f"{catalog_speedup} vs YAML transport",
            "The biggest resource-catalog gains come from replacing text encode, compression, transmit, and parse with native columnar interchange.",
        ),
        (
            "Memory pressure",
            f"{resource_rss} resource RSS; {native_scheduler_rss} native scheduler RSS",
            "Rust reduces pressure most clearly when the hot path leaves legacy transport and Python-owned execution.",
        ),
    ]
    return "\n".join(
        "<article>"
        f"<span>{h(label)}</span>"
        f"<strong>{h(value)}</strong>"
        f"<p>{h(note)}</p>"
        "</article>"
        for label, value, note in cards
    )


def python_nogil_probe_note() -> str:
    title = "Python 3.14t no-GIL scratch probe"
    body = (
        "target/carbon/nogil-experiment/ showed the current legacy scheduler "
        "extension is not a ready Original no-GIL baseline. A 3.14t rebuild "
        "needed C API compatibility work, then _scheduler re-enabled the GIL on "
        "import by default; forced PYTHON_GIL=0 stayed no-GIL but only measured "
        "about 1.00x-1.04x on tiny same-API scheduler workloads."
    )
    return f"<div class=\"callout\"><strong>{h(title)}:</strong> {h(body)}</div>"


def render_reporting_guide(generated: str) -> str:
    sections = [
        (
            "Final Report Shape",
            [
                "The public report is target/carbon/report/carbon-to-rust-migration-test.html.",
                "Keep the public report executive-readable: top summary cards first, then one Key Stats table.",
                "Do not add visible report rules, methodology dumps, task lists, or repeated standalone benchmark tables; use scoped collapsible detail rows under grouped Key Stats rows when detail improves clarity.",
                "Use target/carbon/report/carbon-to-rust-reporting-guide.html as the handoff guide for agents completing evidence gaps.",
            ],
        ),
        (
            "Key Stats Standard",
            [
                "One row represents one function or workload, not one benchmark variant.",
                "The second column is Speedup, so the quick read is visible without horizontal scrolling.",
                "Keep the lane structure: Original, Rust Parity, Rust Architecture, then Scale / Readout.",
                "Each lane should carry throughput or scale, p99/tail latency, CPU, memory/RSS, and batch/scale where relevant.",
                "A summary row must be understandable without expansion; an optional collapsible detail row immediately below it may carry the supporting benchmark rows for that group.",
                "Leave visible evidence-gap cells where evidence is missing. Do not hide gaps in prose.",
            ],
        ),
        (
            "Evidence Rules",
            [
                "Only claim a speedup when old and new measure the same end-to-end function.",
                "For scheduler work, keep C++/Python API parity separate from pure Rust/no-Python architecture rows.",
                "Call out that the legacy C++ scheduler is more directly integrated with CPython than the Rust/PyO3 compatibility bridge; a tighter Rust/Python integration could reduce some of the observed same-API gap.",
                "For resources/catalog work, compare YAML/text encode, compression, transmit, decompress, and parse against Arrow IPC or Parquet write, transmit, and read for the same logical records.",
                "For IO, each socket/TLS row needs matched legacy Carbon IO throughput, p99/tail, CPU, and RSS before it can claim speedup.",
                "Do not invent percentages or headline claims that are not present in evidence JSON.",
            ],
        ),
        (
            "Top Summary Standard",
            [
                "Keep summary cards short and evidence-backed.",
                "Good themes: port evidence is real; same-interface scheduler parity is currently slower; Python interop is a major cost; pure Rust/no-Python architecture shows large upside; Arrow/Parquet/IPC removes YAML transport overhead; Rust can reduce memory pressure.",
                "If a top-card claim cannot be traced to Key Stats or evidence JSON, remove it or mark the matching table lane not measured.",
            ],
        ),
        (
            "Open TODOs / Evidence Gaps",
            [
                "Fill matched legacy Carbon IO baselines for every Network / IO row: throughput, p99/p99.9, CPU, RSS, memory, payload, concurrency, and speedup.",
                "Continue the scheduler optimization loop: improve same-Python-interface parity while keeping no-Python architecture rows separate.",
                "Add production or game-trace scheduler rows before claiming production scheduler performance.",
                "Extend resources/catalog evidence with CPU/RSS split for old YAML/text and new Arrow IPC/Parquet paths.",
                "Add batch, vectorized, Rayon, SIMD, Arrow IPC, and Parquet architecture rows only when they measure a clear replacement function and are labeled as architecture changes.",
                "Replace evidence-gap cells in the table instead of adding explanatory paragraphs below the report.",
            ],
        ),
        (
            "Do Not Add",
            [
                "No second metric table showing the same stats again.",
                "No floating expand/collapse sections outside the Key Stats table.",
                "No collapsible detail row that repeats the summary row without adding useful workload-level evidence.",
                "No standalone Arrow or Parquet microbenchmark presented as a legacy-vs-Rust speedup.",
                "No task-planning content in the public report.",
                "No broad speedup claim if the original and replacement paths do not measure the same end-to-end function.",
            ],
        ),
        (
            "Verification",
            [
                "python3 -m py_compile scripts/render-carbon-to-rust-migration-test.py",
                "python3 scripts/render-carbon-to-rust-migration-test.py --evidence-dir target/carbon/evidence",
                "git diff --check",
                "Scan generated HTML for unresolved placeholders, duplicate benchmark sections, detached expand/collapse sections, stale report-rule text, and unsupported headline claims.",
            ],
        ),
    ]
    section_html = "\n".join(
        "<section>"
        f"<h2>{h(title)}</h2>"
        "<ul>"
        + "".join(f"<li>{h(item)}</li>" for item in items)
        + "</ul>"
        "</section>"
        for title, items in sections
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Carbon to Rust Reporting Guide</title>
  <style>
    :root {{
      --ink: #17202a;
      --muted: #5d6b78;
      --paper: #ffffff;
      --wash: #f4f7f9;
      --line: #d8e0e7;
      --blue: #1f6f9f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--wash);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    header, main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 24px;
    }}
    header {{
      padding-top: 42px;
    }}
    .eyebrow {{
      margin: 0 0 8px;
      color: var(--blue);
      font-size: 0.75rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 4vw, 3.1rem);
      line-height: 1.05;
    }}
    .lead {{
      max-width: 780px;
      color: var(--muted);
      font-size: 1.05rem;
    }}
    section {{
      margin-bottom: 14px;
      padding: 18px;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }}
    h2 {{
      margin: 0 0 10px;
      font-size: 1.1rem;
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
    }}
    li + li {{
      margin-top: 6px;
    }}
    code {{
      background: #edf1f5;
      border: 1px solid #dce3ea;
      border-radius: 4px;
      padding: 1px 5px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.86em;
    }}
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">Carbon to Rust migration test</p>
    <h1>Reporting guide and evidence-gap standard.</h1>
    <p class="lead">Use this companion guide when filling evidence gaps or editing the report. It is intentionally separate from the public migration report.</p>
    <p class="lead">Generated {h(generated)}.</p>
  </header>
  <main>
    {section_html}
  </main>
</body>
</html>
"""


def architecture_tail_cell(architecture_rows: list[dict]) -> str:
    if not architecture_rows:
        return todo_cell("no architecture p99/tail row")
    p99 = median_path_value(architecture_rows, "/latency_us_extended/p99")
    p99_9 = median_path_value(architecture_rows, "/latency_us_extended/p99_9")
    p50 = median_path_value(architecture_rows, "/latency_us_extended/p50")
    return f"<strong>p99 {fmt_us(p99)}</strong><small>p50 {fmt_us(p50)}; p99.9 {fmt_us(p99_9)}</small>"


def architecture_process_cell(architecture_rows: list[dict]) -> str:
    if not architecture_rows:
        return todo_cell("no architecture CPU/RSS row")
    cpu = median_path_value(architecture_rows, "/process_stats/cpu_percent/p95")
    rss = median_path_value(architecture_rows, "/process_stats/max_rss_kb/p95")
    cv = median_path_value(architecture_rows, "/stability/coefficient_of_variation")
    return f"<strong>CPU {fmt_percent(cpu)}</strong><small>{fmt_kb(rss)} RSS p95; CV {fmt_cv(cv)}</small>"


PRESSURE_SCALE_LABELS = {
    "channel_pair_count": ("channel_pair_count", "channel pairs"),
    "domain_wakeup_count": ("wakeup_count", "domain wakeups"),
    "message_count": ("message_count", "messages"),
    "record_count": ("record_count", "records"),
    "tasklet_count": ("tasklet_count", "tasklets"),
    "zone_tick_count": ("tick_count", "zone ticks"),
}


def fmt_scale_values(values: list[object]) -> str:
    numeric = [float(value) for value in values if number(value) is not None]
    if not numeric:
        return "n/a"
    low = min(numeric)
    high = max(numeric)
    if low == high:
        return fmt_int(low)
    return f"{fmt_int(low)} to {fmt_int(high)}"


def architecture_scale_cell(architecture_rows: list[dict], fallback: str) -> str:
    if not architecture_rows:
        return todo_cell(fallback)
    scale_groups: dict[str, list[object]] = {}
    iterations = []
    for row in architecture_rows:
        pressure = row.get("pressure") or {}
        axis = str(pressure.get("axis") or "pressure")
        value_key, label = PRESSURE_SCALE_LABELS.get(axis, (axis, axis.replace("_", " ")))
        if number(pressure.get(value_key)) is not None:
            scale_groups.setdefault(label, []).append(pressure.get(value_key))
        if number(pressure.get("iterations_per_process")) is not None:
            iterations.append(pressure.get("iterations_per_process"))
    count_text = "; ".join(
        f"{fmt_scale_values(values)} {label}"
        for label, values in sorted(scale_groups.items())
    ) or "n/a"
    iter_text = fmt_int(min(iterations)) if iterations and min(iterations) == max(iterations) else (
        f"{fmt_int(min(iterations))} to {fmt_int(max(iterations))}" if iterations else "n/a"
    )
    return html_cell(
        count_text,
        f"{iter_text} iterations/process; no Python hot path",
    )


def dashboard_row(
    feature: str,
    headline_speedup: str,
    original_path: str,
    original_speed: str,
    original_tail: str,
    original_process: str,
    parity_path: str,
    parity_multiple: str,
    parity_tail: str,
    parity_process: str,
    architecture_path: str,
    architecture_speed: str,
    architecture_tail: str,
    architecture_process: str,
    scale_batch: str,
    readout: str,
) -> str:
    return (
        "<tr>"
        f"<td><strong>{h(feature)}</strong></td>"
        f"<td class=\"headline-cell\">{headline_speedup}</td>"
        f"<td>{original_path}</td>"
        f"<td>{original_speed}</td>"
        f"<td>{original_tail}</td>"
        f"<td>{original_process}</td>"
        f"<td>{parity_path}</td>"
        f"<td class=\"multiple-cell\">{parity_multiple}</td>"
        f"<td>{parity_tail}</td>"
        f"<td>{parity_process}</td>"
        f"<td>{architecture_path}</td>"
        f"<td class=\"multiple-cell\">{architecture_speed}</td>"
        f"<td>{architecture_tail}</td>"
        f"<td>{architecture_process}</td>"
        f"<td>{scale_batch}</td>"
        f"<td>{readout}</td>"
        "</tr>"
    )


def dashboard_detail_row(summary: str, body: str) -> str:
    return (
        "<tr class=\"dashboard-detail-row\">"
        "<td colspan=\"16\">"
        "<details class=\"dashboard-detail\">"
        f"<summary>{h(summary)}</summary>"
        f"{body}"
        "</details>"
        "</td>"
        "</tr>"
    )


def scheduler_group_detail_row(
    summary: str,
    scheduler_rows: list[dict],
    native_scheduler_rows_data: list[dict],
) -> str:
    body = f"""
<div class="dashboard-detail-content">
  <p class="detail-note">The same-API rows compare the legacy C++ scheduler extension with the Rust/PyO3 compatibility bridge. The C++ path is closer to CPython/Stackless internals than the current PyO3 bridge, so part of the measured gap may be bridge integration overhead. The native rows keep the target architecture separate.</p>
  <div class="dashboard-detail-panel">
    <h3>Same Python API Rows</h3>
    <div class="dashboard-detail-table-wrap">
      <table class="scheduler-comparison detail-table">
        <thead>
          <tr>
            <th>Workload</th>
            <th>Parity</th>
            <th>Speedup</th>
            <th>Throughput</th>
            <th>Latency p50</th>
            <th>Tail</th>
            <th>CPU</th>
            <th>Memory</th>
            <th>Stability / readout</th>
          </tr>
        </thead>
        <tbody>
          {scheduler_port_rows(scheduler_rows)}
        </tbody>
      </table>
    </div>
  </div>
  <div class="dashboard-detail-panel">
    <h3>Native No-Python Architecture Rows</h3>
    <div class="dashboard-detail-table-wrap">
      <table class="scheduler-comparison detail-table">
        <thead>
          <tr>
            <th>Feature</th>
            <th>Original</th>
            <th>Rust parity</th>
            <th>Architecture multiple</th>
            <th>Native no-Python</th>
            <th>CPU / memory</th>
            <th>Scale</th>
            <th>Claim boundary</th>
          </tr>
        </thead>
        <tbody>
          {scheduler_architecture_pressure_rows(scheduler_rows, native_scheduler_rows_data)}
        </tbody>
      </table>
    </div>
  </div>
</div>
"""
    return dashboard_detail_row(summary, body)


def resource_group_detail_row(summary: str, rows: list[dict]) -> str:
    body = f"""
<div class="dashboard-detail-content">
  <div class="dashboard-detail-panel">
    <h3>Same-Format Resource Rows</h3>
    <div class="dashboard-detail-table-wrap">
      <table class="resource-comparison detail-table">
        <thead>
          <tr>
            <th>Feature</th>
            <th>Surface</th>
            <th>Speedup</th>
            <th>Wall</th>
            <th>Tail</th>
            <th>CPU / memory</th>
            <th>Parity</th>
          </tr>
        </thead>
        <tbody>
          {resource_port_rows(rows)}
        </tbody>
      </table>
    </div>
  </div>
</div>
"""
    return dashboard_detail_row(summary, body)


def catalog_interchange_detail_row(summary: str, scalability_evidence: dict) -> str:
    body = f"""
<div class="dashboard-detail-content">
  <div class="dashboard-detail-panel">
    <h3>Catalog Interchange Rows</h3>
    <div class="dashboard-detail-table-wrap">
      <table class="resource-comparison detail-table">
        <thead>
          <tr>
            <th>Format</th>
            <th>Workload</th>
            <th>Records</th>
            <th>Multiple</th>
            <th>Original / text</th>
            <th>Native path</th>
            <th>Tail / memory</th>
            <th>Readout</th>
          </tr>
        </thead>
        <tbody>
          {catalog_serialization_detail_rows(scalability_evidence)}
        </tbody>
      </table>
    </div>
  </div>
</div>
"""
    return dashboard_detail_row(summary, body)


def feature_dashboard_rows(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_rows: list[dict],
    resource_rows: list[dict],
    fixtures_evidence: dict,
    scalability_evidence: dict,
) -> str:
    runnable_rows = workload_rows(scheduler_rows, ("runnable_tasklets",))
    channel_rows = workload_rows(scheduler_rows, ("channel_rendezvous",))
    game_rows = workload_rows(scheduler_rows, ("fanout_pipeline", "zone_tick_study"))
    manifest_rows = workload_rows(
        resource_rows,
        (
            "create_group",
            "merge_group",
            "diff_group",
            "remove_resources",
        ),
    )
    bundle_rows = workload_rows(
        resource_rows,
        (
            "create_bundle",
            "create_patch",
            "unpack_bundle",
            "apply_patch",
        ),
    )
    native_text = native_resource_architecture_text(scalability_evidence)
    catalog = catalog_interchange_dashboard(scalability_evidence, native_text)
    native_rows = native_scheduler_rows(scalability_evidence)
    native_runnable_rows = native_scheduler_rows_for(native_rows, ("runnable_tasklets",))
    native_channel_rows = native_scheduler_rows_for(native_rows, ("channel_rendezvous",))
    native_game_rows = native_scheduler_rows_for(
        native_rows, ("fanout_pipeline", "zone_tick_study")
    )
    native_domain_rows = [
        row
        for row in native_rows
        if str(row.get("workload") or "").startswith("native_scheduler_native-domain")
    ]
    catalog_rows = catalog_interchange_rows(scalability_evidence)
    io_rows_data = [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "io"
    ]

    rows_html = [
        dashboard_row(
            "Scheduler API semantics",
            headline_speedup_cell(scheduler_rows, native_rows, scheduler_rows),
            html_cell("C++ scheduler extension", "Python tasklet/channel API baseline"),
            lane_speed_cell(scheduler_rows, "legacy"),
            lane_tail_cell(scheduler_rows, "legacy"),
            lane_process_cell(scheduler_rows, "legacy"),
            f"<strong>Rust scheduler bridge</strong>{parity_text(scheduler_rows, fixture_count=fixtures_evidence.get('fixture_count'), fixture_passed=fixtures_evidence.get('passed'))}",
            parity_multiple_cell(scheduler_rows),
            lane_tail_cell(scheduler_rows, "rust"),
            lane_process_cell(scheduler_rows, "rust"),
            html_cell("Native scheduler-core", "no Python/PyO3/greenlet hot path rows"),
            architecture_speed_cell(native_rows, scheduler_rows),
            architecture_tail_cell(native_rows),
            architecture_process_cell(native_rows),
            architecture_scale_cell(native_rows, "production scale row missing"),
            html_cell(
                "Grouped scheduler evidence",
                f"{fmt_int(len(scheduler_rows))} same-API rows; {fmt_int(len(native_rows))} no-Python architecture rows",
            ),
        ),
        dashboard_row(
            "Runnable scheduling",
            headline_speedup_cell(runnable_rows, native_runnable_rows, runnable_rows),
            html_cell("Legacy run queue", "same Python API"),
            lane_speed_cell(runnable_rows, "legacy"),
            lane_tail_cell(runnable_rows, "legacy"),
            lane_process_cell(runnable_rows, "legacy"),
            f"<strong>Rust run queue</strong>{parity_text(runnable_rows)}",
            parity_multiple_cell(runnable_rows),
            lane_tail_cell(runnable_rows, "rust"),
            lane_process_cell(runnable_rows, "rust"),
            html_cell("Native runnable drain", "preallocated hot-loop architecture probe"),
            architecture_speed_cell(native_runnable_rows, runnable_rows),
            architecture_tail_cell(native_runnable_rows),
            architecture_process_cell(native_runnable_rows),
            architecture_scale_cell(native_runnable_rows, "batch runnable row missing"),
            "Architecture row shows target shape; parity row still needs optimization.",
        ),
        scheduler_group_detail_row(
            "Detail: runnable scheduling rows",
            runnable_rows,
            native_runnable_rows,
        ),
        dashboard_row(
            "Channel rendezvous",
            headline_speedup_cell(channel_rows, native_channel_rows, channel_rows),
            html_cell("Legacy channel queues", "same Python API"),
            lane_speed_cell(channel_rows, "legacy"),
            lane_tail_cell(channel_rows, "legacy"),
            lane_process_cell(channel_rows, "legacy"),
            f"<strong>Rust channel bridge</strong>{parity_text(channel_rows)}",
            parity_multiple_cell(channel_rows),
            lane_tail_cell(channel_rows, "rust"),
            lane_process_cell(channel_rows, "rust"),
            html_cell("Native channel rendezvous", "preallocated hot-loop architecture probe"),
            architecture_speed_cell(native_channel_rows, channel_rows),
            architecture_tail_cell(native_channel_rows),
            architecture_process_cell(native_channel_rows),
            architecture_scale_cell(native_channel_rows, "batch channel row missing"),
            "Use for wait-queue/handoff redesign decisions; not a same-API speedup claim.",
        ),
        scheduler_group_detail_row(
            "Detail: channel rendezvous rows",
            channel_rows,
            native_channel_rows,
        ),
        dashboard_row(
            "Fanout and zone tick",
            headline_speedup_cell(game_rows, native_game_rows, game_rows),
            html_cell("Legacy synthetic game loop", "fanout pipeline and zone tick rows"),
            lane_speed_cell(game_rows, "legacy"),
            lane_tail_cell(game_rows, "legacy"),
            lane_process_cell(game_rows, "legacy"),
            f"<strong>Rust bridge same workload</strong>{parity_text(game_rows)}",
            parity_multiple_cell(game_rows),
            lane_tail_cell(game_rows, "rust"),
            lane_process_cell(game_rows, "rust"),
            html_cell("Native fanout / zone tick", "Rust tasklet work with no Python hot path"),
            architecture_speed_cell(native_game_rows, game_rows),
            architecture_tail_cell(native_game_rows),
            architecture_process_cell(native_game_rows),
            architecture_scale_cell(native_game_rows, "batch/entity architecture row missing"),
            "Architecture row is measured; production game trace is still required before claiming production scheduler performance.",
        ),
        scheduler_group_detail_row(
            "Detail: fanout and zone-tick rows",
            game_rows,
            native_game_rows,
        ),
        dashboard_row(
            "Resource manifests",
            headline_speedup_cell(manifest_rows, [], manifest_rows, "Architecture"),
            html_cell("Legacy resources CLI", "YAML/CSV manifests"),
            lane_speed_cell(manifest_rows, "legacy"),
            lane_tail_cell(manifest_rows, "legacy"),
            lane_process_cell(manifest_rows, "legacy"),
            f"<strong>Rust resources CLI</strong>{parity_text(manifest_rows)}",
            parity_multiple_cell(manifest_rows),
            lane_tail_cell(manifest_rows, "rust"),
            lane_process_cell(manifest_rows, "rust"),
            html_cell("No architecture change", "same external format retained"),
            todo_cell("not an architecture-change row"),
            todo_cell("not applicable"),
            todo_cell("not applicable"),
            html_cell(f"{fmt_int(len(manifest_rows))} same-format rows", "YAML/CSV compatibility path"),
            "Same-format rewrite is already faster; keep this grouped as one resources-manifest row.",
        ),
        resource_group_detail_row("Detail: resource manifest rows", manifest_rows),
        dashboard_row(
            "Bundles and patches",
            headline_speedup_cell(bundle_rows, [], bundle_rows, "Architecture"),
            html_cell("Legacy local CDN tooling", "bundle and patch files"),
            lane_speed_cell(bundle_rows, "legacy"),
            lane_tail_cell(bundle_rows, "legacy"),
            lane_process_cell(bundle_rows, "legacy"),
            f"<strong>Rust local CDN tooling</strong>{parity_text(bundle_rows)}",
            parity_multiple_cell(bundle_rows),
            lane_tail_cell(bundle_rows, "rust"),
            lane_process_cell(bundle_rows, "rust"),
            html_cell("Batch file operations", "future Rayon/chunked path"),
            todo_cell("architecture benchmark not measured"),
            todo_cell("architecture p99 not measured"),
            todo_cell("architecture CPU/RSS not measured"),
            html_cell(f"{fmt_int(len(bundle_rows))} same-format rows", "bundle/patch workflow group"),
            "Do not split every bundle/patch operation into separate summary rows unless one regresses.",
        ),
        resource_group_detail_row("Detail: bundle and patch rows", bundle_rows),
    ]

    if catalog_rows:
        rows_html.append(
            dashboard_row(
                "Resource catalog interchange",
                headline_speedup_architecture_only(
                    "Arrow / Parquet",
                    [row.get("speedup") for row in catalog_rows],
                ),
                html_cell(catalog["original_path"], catalog["original_metric"]),
                html_cell(
                    fmt_rate_range([row.get("legacy_throughput_rows_per_sec") for row in catalog_rows], "rows"),
                    f"wire {fmt_bytes_range([row.get('legacy_bytes_over_wire') for row in catalog_rows])}",
                ),
                html_cell(
                    f"p99 {fmt_us(median_path_value(catalog_rows, '/legacy_latency_us_extended/p99'))}",
                    f"p50 {fmt_us(median_path_value(catalog_rows, '/legacy_latency_us_extended/p50'))}",
                ),
                todo_cell("CPU/RSS split by old path not measured"),
                html_cell("No same-format parity lane", "architecture replacement workload"),
                todo_cell("parity lane not applicable"),
                todo_cell("parity lane not applicable"),
                todo_cell("parity lane not applicable"),
                html_cell(catalog["architecture_change"], "Arrow IPC and Parquet/Zstd grouped"),
                html_cell(
                    "; ".join(catalog_interchange_format_summaries(catalog_rows)),
                    "speed and wire result by format",
                ),
                html_cell(
                    f"p99 {fmt_us(median_path_value(catalog_rows, '/rust_latency_us_extended/p99'))}",
                    f"p50 {fmt_us(median_path_value(catalog_rows, '/rust_latency_us_extended/p50'))}",
                ),
                html_cell(
                    f"CPU {fmt_percent(median_path_value(catalog_rows, '/process_stats/cpu_percent/p95'))}",
                    f"{fmt_kb(median_path_value(catalog_rows, '/process_stats/max_rss_kb/p95'))} RSS p95; worker-level",
                ),
                html_cell(
                    f"{fmt_int(min(path_value(row, '/pressure/record_count') for row in catalog_rows))} to {fmt_int(max(path_value(row, '/pressure/record_count') for row in catalog_rows))} records",
                    "logical rows in/out",
                ),
                "Only this grouped row should appear for catalog serialization in the summary.",
            )
        )
        rows_html.append(
            catalog_interchange_detail_row(
                "Detail: catalog interchange rows",
                scalability_evidence,
            )
        )
    else:
        rows_html.append(
            dashboard_row(
                "Resource catalog interchange",
                headline_speedup_todo("n/a", "Not measured"),
                html_cell("YAML/text resource catalog", "old path"),
                todo_cell("end-to-end old path missing"),
                todo_cell("old path p99 missing"),
                todo_cell("old path CPU/RSS missing"),
                html_cell("No same-format parity lane", "architecture replacement workload"),
                todo_cell("parity lane not applicable"),
                todo_cell("parity lane not applicable"),
                todo_cell("parity lane not applicable"),
                html_cell("Arrow IPC / Parquet catalog path", "native replacement"),
                todo_cell("end-to-end architecture benchmark missing"),
                todo_cell("architecture p99 missing"),
                todo_cell("architecture CPU/RSS missing"),
                todo_cell("record-count pressure missing"),
                "Do not publish a speedup claim until the grouped catalog interchange row exists.",
            )
        )

    for row in sorted(io_rows_data, key=lambda item: str(item.get("workload") or "")):
        pressure = row.get("pressure") or {}
        kind = str(row.get("kind") or "io").upper()
        payload = fmt_bytes(pressure.get("payload_bytes"))
        concurrency = fmt_int(pressure.get("concurrency"))
        rows_html.append(
            dashboard_row(
                f"Network / IO {kind}",
                headline_speedup_todo("Not measured", "pressure only"),
                html_cell("Carbon IO baseline", f"{payload}; concurrency {concurrency}"),
                todo_cell("matched legacy Carbon IO throughput missing"),
                todo_cell("matched legacy Carbon IO p99 missing"),
                todo_cell("matched legacy Carbon IO CPU/RSS missing"),
                html_cell("Rust parity lane", "not measured against legacy Carbon IO"),
                todo_cell("no old-vs-Rust IO multiple"),
                todo_cell("Rust parity p99 missing"),
                todo_cell("Rust parity CPU/RSS missing"),
                html_cell("Local pressure path", "socket/TLS loopback capacity row"),
                html_cell(
                    fmt_rate(row.get("throughput_requests_per_sec"), "requests"),
                    f"{fmt_rate(row.get('throughput_network_bytes_per_sec'), 'bytes')} network",
                ),
                html_cell(
                    f"p99 {fmt_us(path_value(row, '/latency_us_extended/p99'))}",
                    f"p99.9 {fmt_us(path_value(row, '/latency_us_extended/p99_9'))}",
                ),
                html_cell(
                    f"CPU {fmt_percent(path_value(row, '/process_stats/cpu_percent/p95'))}",
                    f"{fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95'))} RSS p95; CV {fmt_cv(path_value(row, '/stability/coefficient_of_variation'))}",
                ),
                html_cell(
                    f"{payload}; concurrency {concurrency}",
                    f"{fmt_int(pressure.get('requests_per_connection'))} requests/connection",
                ),
                "Keep as an evidence gap until each IO row has a matched legacy Carbon IO baseline.",
            )
        )

    return "\n".join(rows_html)


def executive_readout_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_rows: list[dict],
    fixtures_evidence: dict,
    scalability_evidence: dict,
) -> str:
    mismatch_count = semantic_mismatch_count(scheduler_rows)
    reconciliation_count = reconciliation_mismatch_count(scheduler_rows)
    native_rows = native_resource_rows_data(scalability_evidence)
    items = [
        (
            "Scheduler parity",
            f"The covered scheduler semantics run through Rust-owned state and pass {fmt_int(fixtures_evidence.get('passed'))}/{fmt_int(fixtures_evidence.get('fixture_count'))} deterministic fixtures. The matched Python API performance rows also reconcile the same workload specs, counts, and checksums with {fmt_int(mismatch_count)} semantic mismatches and {fmt_int(reconciliation_count)} reconciliation mismatches.",
        ),
        (
            "Scheduler performance",
            f"The scheduler bridge is slower today: {fmt_directional_ratio(scheduler_summary['median_speedup'])} median across {fmt_int(scheduler_summary['rows'])} matched legacy-vs-Rust workloads. That turns the next phase into a quantified optimization loop, not a broad speedup claim.",
        ),
        (
            "Resource results",
            f"The resources port is reported separately because it is a different repo and workload class. On same-format YAML/CSV and local bundle workflows, Rust is {fmt_directional_ratio(resource_summary['median_speedup'])} median while preserving parity.",
        ),
        (
            "Production gate",
            f"The report is still lab evidence. The next scheduler claim needs a real game-environment tasklet workload and a rerun after optimizing the measured bridge costs. Native Arrow IPC and Parquet resource rows ({fmt_int(len(native_rows))} sampled) remain upgraded-interface evidence only.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in items
    )


def repo_conversion_cards() -> str:
    items = [
        (
            "Scheduler repo",
            "Covered tasklet, run-queue, channel, switch-trap, and invalid direct run/switch behavior is exercised through the same Python scheduler surface against a Rust-owned core. The remaining challenge is performance and production workload coverage.",
        ),
        (
            "Resources repo",
            "The CLI-compatible Rust path beats the optimized legacy baseline on the measured YAML/CSV and local bundle operations. The native path adds Arrow IPC and Parquet catalog storage for non-YAML hot interchange.",
        ),
        (
            "Evidence stance",
            "Same-interface comparisons and upgraded-interface measurements are reported separately. That keeps the scheduler claim clean while still showing where modern storage and batch execution can replace dated interchange formats.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in items
    )


def scheduler_architecture_signal(workload: str) -> str:
    if workload.startswith("channel_rendezvous"):
        return "Channel ordering matches; the cost to reduce next is scheduler handoff overhead."
    if workload.startswith("runnable_tasklets"):
        return "Run-queue behavior matches; the cost to reduce next is dispatch overhead per tasklet."
    if workload.startswith("fanout_pipeline"):
        return "Message fanout is the next place to validate batching once basic handoff is faster."
    if workload.startswith("zone_tick_study"):
        return "The game-loop study should separate entity work from scheduling overhead."
    return "Keep semantics green, profile the row, and only promote measured wins."


SCHEDULER_WORKLOAD_ORDER = {
    "runnable_tasklets_128": 10,
    "runnable_tasklets_1024": 20,
    "runnable_tasklets_4096": 30,
    "channel_rendezvous_32": 40,
    "channel_rendezvous_256": 50,
    "channel_rendezvous_1024": 60,
    "fanout_pipeline_256b": 70,
    "fanout_pipeline_4096b": 80,
    "zone_tick_study_small": 90,
    "zone_tick_study_large": 100,
}


def scheduler_row_sort_key(row: dict) -> tuple[int, str]:
    workload = str(row.get("workload") or "")
    return SCHEDULER_WORKLOAD_ORDER.get(workload, 999), workload


def scheduler_port_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="9">No matched scheduler rows available.</td></tr>'
    rendered = []
    max_speedup = max(float(row.get("speedup") or 0) for row in rows) or 1.0
    for row in sorted(rows, key=scheduler_row_sort_key):
        workload = str(row.get("workload") or "")
        title, description = workload_label(workload)
        speedup = number(row.get("speedup"))
        parity = row.get("parity_status") or "n/a"
        mismatch_count = path_value(row, "/semantic/mismatch_count")
        if mismatch_count is not None:
            parity = f"{parity}; {fmt_int(mismatch_count)} mismatches"
        throughput_unit, old_throughput, rust_throughput = throughput_pair(row)
        throughput_ratio = higher_is_better_ratio(old_throughput, rust_throughput)
        p50_old = path_value(row, "/legacy_sample_stats_us/p50")
        p50_rust = path_value(row, "/rust_sample_stats_us/p50")
        p99_old = path_value(row, "/legacy_sample_stats_us/p99")
        p99_rust = path_value(row, "/rust_sample_stats_us/p99")
        p999_old = path_value(row, "/legacy_sample_stats_us/p99_9")
        p999_rust = path_value(row, "/rust_sample_stats_us/p99_9")
        p99_reduction = reduction_percent(
            p99_old,
            p99_rust,
        )
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        bar_class = "faster" if (speedup is not None and speedup >= 1.0) else "slower"
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(description)}</small></td>"
            f"<td><strong>{h(parity)}</strong><small>{h(reconciliation_text(row))}</small></td>"
            f"<td><strong>{fmt_directional_ratio(speedup)}</strong><div class=\"bar {bar_class}\"><span style=\"width:{bar_width(speedup, max_speedup):.1f}%\"></span></div><small>{fmt_ms_from_us(row.get('legacy_duration_us'))} legacy vs {fmt_ms_from_us(row.get('rust_duration_us'))} Rust</small></td>"
            f"<td><strong>{h(fmt_rate(old_throughput, throughput_unit))} -> {h(fmt_rate(rust_throughput, throughput_unit))}</strong><small>{h(throughput_unit)}; Rust throughput {fmt_directional_ratio(throughput_ratio)}</small></td>"
            f"<td><strong>{fmt_us(p50_old)} -> {fmt_us(p50_rust)}</strong><small>p50 per sampled iteration</small></td>"
            f"<td><strong>{fmt_signed_percent(p99_reduction)}</strong><small>p99 {fmt_us(p99_old)} -> {fmt_us(p99_rust)}; p99.9 {fmt_us(p999_old)} -> {fmt_us(p999_rust)}</small></td>"
            f"<td><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>{fmt_ms(path_value(row, '/legacy_process_stats/cpu_burn_effective_ms/mean'))} -> {fmt_ms(path_value(row, '/rust_process_stats/cpu_burn_effective_ms/mean'))} CPU burn; p95 CPU {fmt_percent(path_value(row, '/legacy_process_stats/cpu_percent/p95'))} -> {fmt_percent(path_value(row, '/rust_process_stats/cpu_percent/p95'))}</small></td>"
            f"<td><strong>{fmt_signed_percent(rss_reduction)}</strong><small>{fmt_kb(path_value(row, '/legacy_process_stats/max_rss_kb/p95'))} -> {fmt_kb(path_value(row, '/rust_process_stats/max_rss_kb/p95'))} peak RSS p95</small></td>"
            f"<td><strong>{fmt_cv(path_value(row, '/legacy_throughput_stability/coefficient_of_variation'))} -> {fmt_cv(path_value(row, '/rust_throughput_stability/coefficient_of_variation'))}</strong><small>throughput CV; lower is steadier</small><small>{h(scheduler_architecture_signal(workload))}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def scheduler_workload_cards(rows: list[dict]) -> str:
    if not rows:
        return '<div class="workload-card"><h3>No matched scheduler rows available.</h3></div>'
    rendered = []
    for row in sorted(rows, key=scheduler_row_sort_key):
        workload = str(row.get("workload") or "")
        title, description = workload_label(workload)
        speedup = number(row.get("speedup"))
        throughput_unit, old_throughput, rust_throughput = throughput_pair(row)
        throughput_ratio = higher_is_better_ratio(old_throughput, rust_throughput)
        p50_old = path_value(row, "/legacy_sample_stats_us/p50")
        p50_rust = path_value(row, "/rust_sample_stats_us/p50")
        p99_old = path_value(row, "/legacy_sample_stats_us/p99")
        p99_rust = path_value(row, "/rust_sample_stats_us/p99")
        p999_old = path_value(row, "/legacy_sample_stats_us/p99_9")
        p999_rust = path_value(row, "/rust_sample_stats_us/p99_9")
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        p99_reduction = reduction_percent(p99_old, p99_rust)
        mismatch_count = path_value(row, "/semantic/mismatch_count")
        parity = row.get("parity_status") or "n/a"
        if mismatch_count is not None:
            parity = f"{parity}; {fmt_int(mismatch_count)} mismatches"
        bar_class = "faster" if (speedup is not None and speedup >= 1.0) else "slower"
        rendered.append(
            f"""<article class="workload-card">
  <div class="workload-head">
    <div>
      <h3>{h(title)}</h3>
      <p>{h(description)}</p>
    </div>
    <span class="status-pill">{h(parity)}</span>
  </div>
  <div class="result-strip {bar_class}">
    <span>Same API old-vs-Rust result</span>
    <strong>{fmt_directional_ratio(speedup)}</strong>
    <small>{fmt_ms_from_us(row.get('legacy_duration_us'))} legacy vs {fmt_ms_from_us(row.get('rust_duration_us'))} Rust</small>
  </div>
  <p class="workload-note">{h(reconciliation_text(row))}</p>
  <div class="stat-grid">
    <div class="stat"><span>Throughput</span><strong>{h(fmt_rate(old_throughput, throughput_unit))} -> {h(fmt_rate(rust_throughput, throughput_unit))}</strong><small>{h(throughput_unit)}; Rust {fmt_directional_ratio(throughput_ratio)}</small></div>
    <div class="stat"><span>Latency</span><strong>{fmt_us(p50_old)} -> {fmt_us(p50_rust)}</strong><small>p50 sampled iteration</small></div>
    <div class="stat"><span>Tail</span><strong>{fmt_signed_percent(p99_reduction)}</strong><small>p99 {fmt_us(p99_old)} -> {fmt_us(p99_rust)}; p99.9 {fmt_us(p999_old)} -> {fmt_us(p999_rust)}</small></div>
    <div class="stat"><span>CPU</span><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>{fmt_ms(path_value(row, '/legacy_process_stats/cpu_burn_effective_ms/mean'))} -> {fmt_ms(path_value(row, '/rust_process_stats/cpu_burn_effective_ms/mean'))} effective burn</small></div>
    <div class="stat"><span>Memory</span><strong>{fmt_signed_percent(rss_reduction)}</strong><small>{fmt_kb(path_value(row, '/legacy_process_stats/max_rss_kb/p95'))} -> {fmt_kb(path_value(row, '/rust_process_stats/max_rss_kb/p95'))} peak RSS p95</small></div>
    <div class="stat"><span>Stability</span><strong>{fmt_cv(path_value(row, '/legacy_throughput_stability/coefficient_of_variation'))} -> {fmt_cv(path_value(row, '/rust_throughput_stability/coefficient_of_variation'))}</strong><small>throughput CV; lower is steadier</small></div>
  </div>
  <p class="row-read">{h(scheduler_architecture_signal(workload))}</p>
</article>"""
        )
    return "\n".join(rendered)


def resource_surface(workload: str) -> str:
    if "bundle" in workload or "patch" in workload:
        return "Local bundle/patch files"
    if "csv" in workload:
        return "CSV manifest"
    return "YAML manifest"


def resource_port_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="7">No comparable resource rows available.</td></tr>'
    rendered = []
    for row in sorted(rows, key=lambda item: float(item.get("speedup") or 0), reverse=True):
        workload = str(row.get("workload") or "")
        title, description = workload_label(workload)
        p99_reduction = reduction_percent(
            path_value(row, "/legacy_sample_stats_us/p99"),
            path_value(row, "/rust_sample_stats_us/p99"),
        )
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        parity = row.get("parity_status") or row.get("claim_eligibility") or "n/a"
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(description)}</small></td>"
            f"<td>{h(resource_surface(workload))}</td>"
            f"<td><strong>{fmt_directional_ratio(row.get('speedup'))}</strong><small>same legacy-compatible operation</small></td>"
            f"<td><strong>{fmt_ms_from_us(row.get('legacy_duration_us'))} -> {fmt_ms_from_us(row.get('rust_duration_us'))}</strong><small>legacy wall vs Rust wall</small></td>"
            f"<td><strong>{fmt_signed_percent(p99_reduction)}</strong><small>p99 tail latency</small></td>"
            f"<td><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>CPU burn; memory {fmt_signed_percent(rss_reduction)}</small></td>"
            f"<td><small>{h(parity)}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


RESOURCE_WORKLOAD_ORDER = {
    "create_group_directory_yaml": 10,
    "create_group_from_filter_yaml": 20,
    "merge_group_yaml_additive": 30,
    "diff_group_csv_additions": 40,
    "remove_resources_yaml": 50,
    "create_bundle_local_cdn": 60,
    "create_patch_local_cdn": 70,
    "unpack_bundle_local_cdn": 80,
    "apply_patch_local_cdn": 90,
}


def resource_row_sort_key(row: dict) -> tuple[int, str]:
    workload = str(row.get("workload") or "")
    return RESOURCE_WORKLOAD_ORDER.get(workload, 999), workload


def resource_workload_cards(rows: list[dict]) -> str:
    if not rows:
        return '<div class="workload-card"><h3>No comparable resource rows available.</h3></div>'
    rendered = []
    for row in sorted(rows, key=resource_row_sort_key):
        workload = str(row.get("workload") or "")
        title, description = workload_label(workload)
        p99_reduction = reduction_percent(
            path_value(row, "/legacy_sample_stats_us/p99"),
            path_value(row, "/rust_sample_stats_us/p99"),
        )
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rss_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/max_rss_kb/p95"),
            path_value(row, "/rust_process_stats/max_rss_kb/p95"),
        )
        parity = row.get("parity_status") or row.get("claim_eligibility") or "n/a"
        speedup = number(row.get("speedup"))
        bar_class = "faster" if (speedup is not None and speedup >= 1.0) else "slower"
        rendered.append(
            f"""<article class="workload-card resource">
  <div class="workload-head">
    <div>
      <h3>{h(title)}</h3>
      <p>{h(description)}</p>
    </div>
    <span class="status-pill">{h(resource_surface(workload))}</span>
  </div>
  <div class="result-strip {bar_class}">
    <span>Same-format old-vs-Rust result</span>
    <strong>{fmt_directional_ratio(speedup)}</strong>
    <small>{fmt_ms_from_us(row.get('legacy_duration_us'))} legacy vs {fmt_ms_from_us(row.get('rust_duration_us'))} Rust</small>
  </div>
  <div class="stat-grid resource-stats">
    <div class="stat"><span>p99 tail</span><strong>{fmt_signed_percent(p99_reduction)}</strong><small>{fmt_us(path_value(row, '/legacy_sample_stats_us/p99'))} -> {fmt_us(path_value(row, '/rust_sample_stats_us/p99'))}</small></div>
    <div class="stat"><span>CPU burn</span><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>{fmt_ms(path_value(row, '/legacy_process_stats/cpu_burn_effective_ms/mean'))} -> {fmt_ms(path_value(row, '/rust_process_stats/cpu_burn_effective_ms/mean'))}</small></div>
    <div class="stat"><span>Peak memory</span><strong>{fmt_signed_percent(rss_reduction)}</strong><small>{fmt_kb(path_value(row, '/legacy_process_stats/max_rss_kb/p95'))} -> {fmt_kb(path_value(row, '/rust_process_stats/max_rss_kb/p95'))}</small></div>
    <div class="stat"><span>Parity</span><strong>{h(short_text(parity, 42))}</strong><small>same legacy-compatible operation</small></div>
  </div>
</article>"""
        )
    return "\n".join(rendered)


def native_resource_cards(scalability_evidence: dict) -> str:
    rows = native_resource_rows_data(scalability_evidence)
    formats = sorted({str(row.get("serialization_format") or "") for row in rows})
    arrow_peak = max(
        (
            float(row.get("throughput_rows_per_sec"))
            for row in rows
            if row.get("serialization_format") == "arrow_ipc"
            and number(row.get("throughput_rows_per_sec")) is not None
        ),
        default=None,
    )
    parquet_peak = max(
        (
            float(row.get("throughput_rows_per_sec"))
            for row in rows
            if row.get("serialization_format") == "parquet_zstd"
            and number(row.get("throughput_rows_per_sec")) is not None
        ),
        default=None,
    )
    return "\n".join(
        [
            metric_card(
                "Columnar formats",
                fmt_int(len(rows)),
                ", ".join(format_name.replace("_", " ") for format_name in formats) or "no native rows",
            ),
            metric_card(
                "Arrow IPC peak",
                fmt_rate(arrow_peak, "rows"),
                "native in-memory or transport catalog round-trip",
            ),
            metric_card(
                "Parquet/Zstd peak",
                fmt_rate(parquet_peak, "rows"),
                "compressed persisted catalog snapshot round-trip",
            ),
            metric_card(
                "YAML/JSON role",
                "edge only",
                "compatibility import/export; not the target hot interchange path",
            ),
        ]
    )


def architecture_takeaway_cards() -> str:
    items = [
        (
            "Same public scheduler",
            "The legacy C++ scheduler extension is tuned close to Python/Stackless internals, while the Rust bridge uses PyO3 as a compatibility layer. The same-API comparison preserves behavior first and exposes integration overhead separately from the no-Python target architecture.",
        ),
        (
            "Rust-owned scheduling state",
            "Covered tasklet, channel, run-queue, switch-trap, and invalid-control-flow state now runs through Rust-owned scheduler data while Python remains the compatibility surface.",
        ),
        (
            "Deterministic parity first",
            "The fixture gate and matched rows must stay green before any performance work is promoted. That keeps the rewrite from trading correctness for a headline number.",
        ),
        (
            "Measured optimization loop",
            "The current same-API scheduler rows show the Rust bridge is slower today. That gives the team a precise optimization target: reduce per-tasklet dispatch and channel handoff cost.",
        ),
        (
            "Resource results separated",
            "Resource CLI and native catalog numbers are included because they are useful, but they are not used to imply scheduler speedup.",
        ),
        (
            "Production proof still open",
            "The next scheduler milestone is the same report shape against a real game-environment workload, after the bridge costs visible here are reduced.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in items
    )


def scope_stat(label: str, value: str) -> str:
    return (
        '<div class="scope-stat">'
        f"<span>{h(label)}</span>"
        f"<strong>{h(value)}</strong>"
        "</div>"
    )


def scope_card(
    title: str,
    kind: str,
    boundary: str,
    stats: list[tuple[str, str]],
    note: str,
) -> str:
    stats_html = "\n".join(scope_stat(label, value) for label, value in stats)
    return f"""<article class="scope-card">
  <div class="scope-head">
    <h3>{h(title)}</h3>
    <span class="scope-kind">{h(kind)}</span>
  </div>
  <p class="scope-boundary">{h(boundary)}</p>
  <div class="scope-metrics">
    {stats_html}
  </div>
  <p class="scope-note">{h(note)}</p>
</article>"""


def performance_breakdown_cards(
    scheduler_summary: dict,
    resource_summary: dict,
    scheduler_pressure_rows_data: list[dict],
    io_capacity_rows_data: list[dict],
    data_rows_data: list[dict],
) -> str:
    pressure_rows_count = len(scheduler_pressure_rows_data)
    stable_rows = sum(
        1
        for row in scheduler_pressure_rows_data
        if number(path_value(row, "/stability/coefficient_of_variation")) is not None
        and float(path_value(row, "/stability/coefficient_of_variation")) <= 0.10
    )
    peak_operations_per_sec = max(
        (
            float(row.get("throughput_operations_per_sec"))
            for row in scheduler_pressure_rows_data
            if number(row.get("throughput_operations_per_sec")) is not None
        ),
        default=None,
    )
    worst_latency_p99_us = max(
        (
            float(path_value(row, "/latency_us_extended/p99"))
            for row in scheduler_pressure_rows_data
            if number(path_value(row, "/latency_us_extended/p99")) is not None
        ),
        default=None,
    )
    worst_latency_p99_9_us = max(
        (
            float(path_value(row, "/latency_us_extended/p99_9"))
            for row in scheduler_pressure_rows_data
            if number(path_value(row, "/latency_us_extended/p99_9")) is not None
        ),
        default=None,
    )
    highest_peak_rss_kb_p95 = max(
        (
            float(path_value(row, "/process_stats/max_rss_kb/p95"))
            for row in scheduler_pressure_rows_data
            if number(path_value(row, "/process_stats/max_rss_kb/p95")) is not None
        ),
        default=None,
    )
    peak_network_bytes_per_sec = max(
        (
            float(row.get("throughput_network_bytes_per_sec"))
            for row in io_capacity_rows_data
            if number(row.get("throughput_network_bytes_per_sec")) is not None
        ),
        default=None,
    )
    peak_requests_per_sec = max(
        (
            float(row.get("throughput_requests_per_sec"))
            for row in io_capacity_rows_data
            if number(row.get("throughput_requests_per_sec")) is not None
        ),
        default=None,
    )
    worst_io_p99_us = max(
        (
            float(path_value(row, "/latency_us_extended/p99"))
            for row in io_capacity_rows_data
            if number(path_value(row, "/latency_us_extended/p99")) is not None
        ),
        default=None,
    )
    peak_data_bytes_per_sec = max(
        (
            float(row.get("throughput_data_bytes_per_sec"))
            for row in data_rows_data
            if number(row.get("throughput_data_bytes_per_sec")) is not None
        ),
        default=None,
    )
    peak_rows_per_sec = max(
        (
            float(row.get("throughput_rows_per_sec"))
            for row in data_rows_data
            if number(row.get("throughput_rows_per_sec")) is not None
        ),
        default=None,
    )
    native_format_rows = [
        row for row in data_rows_data if row.get("serialization_format") is not None
    ]
    return "\n".join(
        [
            scope_card(
                "Scheduler same-API comparison",
                "old vs Rust",
                "Legacy C++ scheduler extension and Rust/PyO3 scheduler bridge run the same Python tasklet/channel workloads. The C++ path is more directly integrated with CPython, so this row is the compatibility comparison, not the final architecture cost model.",
                [
                    ("Rows", fmt_int(scheduler_summary["rows"])),
                    ("Throughput", fmt_directional_ratio(scheduler_summary["median_speedup"])),
                    ("p99 tail", fmt_signed_percent(scheduler_summary["median_p99_reduction"])),
                    ("CPU burn", fmt_signed_percent(scheduler_summary["median_cpu_reduction"])),
                    ("Peak memory", fmt_signed_percent(scheduler_summary["median_rss_reduction"])),
                    ("Equal/faster rows", f"{fmt_int(scheduler_summary['equal_or_faster'])}/{fmt_int(scheduler_summary['rows'])}"),
                ],
                "Current read: parity is measurable, but the Rust bridge is slower on these lab rows.",
            ),
            scope_card(
                "Scheduler pressure shape",
                "Rust-only",
                "Core scheduler pressure rows increase tasklet and channel-pair load to show scaling shape, tail behavior, CPU, memory, and stability without claiming old-vs-Rust speedup.",
                [
                    ("Rows", fmt_int(pressure_rows_count)),
                    ("Peak throughput", fmt_rate(peak_operations_per_sec, "operations")),
                    ("Worst p99", fmt_us(worst_latency_p99_us)),
                    ("Worst p99.9", fmt_us(worst_latency_p99_9_us)),
                    ("Stable rows", f"{fmt_int(stable_rows)}/{fmt_int(pressure_rows_count)}"),
                    ("Peak RSS p95", fmt_kb(highest_peak_rss_kb_p95)),
                ],
                "Use this to reason about scheduler scale pressure before a real game-environment run is available.",
            ),
            scope_card(
                "Network and IO pressure",
                "local loopback",
                "Socket and TLS loopback rows measure request rate, data rate, tail latency, CPU, memory, and stability. They are not legacy Carbon IO parity rows.",
                [
                    ("Rows", fmt_int(len(io_capacity_rows_data))),
                    ("Peak network", fmt_rate(peak_network_bytes_per_sec, "bytes")),
                    ("Peak requests", fmt_rate(peak_requests_per_sec, "requests")),
                    ("Worst p99", fmt_us(worst_io_p99_us)),
                ],
                "Use this as local transport capacity context while legacy Carbon IO traces remain a separate gate.",
            ),
            scope_card(
                "Resources same-format comparison",
                "separate repo",
                "Legacy resources CLI and Rust release-native resource commands run the same YAML/CSV and local bundle or patch operations. These rows support the resources port, not scheduler speed.",
                [
                    ("Rows", fmt_int(resource_summary["rows"])),
                    ("Throughput", fmt_directional_ratio(resource_summary["median_speedup"])),
                    ("p99 tail", fmt_signed_percent(resource_summary["median_p99_reduction"])),
                    ("CPU burn", fmt_signed_percent(resource_summary["median_cpu_reduction"])),
                    ("Peak memory", fmt_signed_percent(resource_summary["median_rss_reduction"])),
                    ("Equal/faster rows", f"{fmt_int(resource_summary['equal_or_faster'])}/{fmt_int(resource_summary['rows'])}"),
                ],
                "This is the current positive performance story, but it is clearly outside the scheduler claim.",
            ),
            scope_card(
                "Native resource/data formats",
                "upgraded path",
                "Rust-only pressure rows cover checksum/compression, YAML/JSON catalog round-trips, and native Arrow IPC or Parquet/Zstd catalog round-trips.",
                [
                    ("Rows", fmt_int(len(data_rows_data))),
                    ("Native rows", fmt_int(len(native_format_rows))),
                    ("Peak data", fmt_rate(peak_data_bytes_per_sec, "bytes")),
                    ("Peak rows", fmt_rate(peak_rows_per_sec, "rows")),
                ],
                "These show where Arrow IPC and Parquet help resource/data interchange; they are not scheduler dispatch wins.",
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


def scheduler_regression_target(workload: str) -> str:
    if workload.startswith("fanout_pipeline"):
        return "Batch channel wakeups, reduce Python bridge touches, and profile message fanout allocations first."
    if workload.startswith("zone_tick_study"):
        return "Separate dense entity work from scheduler dispatch; test scalar Rust snapshot, then Rayon/SIMD only after profiling."
    if workload.startswith("channel_rendezvous"):
        return "Move channel wait queues to ID-linked O(1) queues and remove bridge-global ordering work from the handoff path."
    if workload.startswith("runnable_tasklets"):
        return "Replace BTreeMap/VecDeque scans with dense tasklet storage and known-tasklet O(1) queue removal."
    return "Keep parity green, profile row-level costs, and land only measured wins."


def scheduler_regression_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="5">No scheduler regression rows available.</td></tr>'
    rendered = []
    for row in sorted(rows, key=lambda item: float(item.get("speedup") or 0)):
        workload = str(row.get("workload") or "")
        title, description = workload_label(workload)
        speedup = number(row.get("speedup"))
        p99_reduction = reduction_percent(
            path_value(row, "/legacy_sample_stats_us/p99"),
            path_value(row, "/rust_sample_stats_us/p99"),
        )
        cpu_reduction = reduction_percent(
            path_value(row, "/legacy_process_stats/cpu_burn_effective_ms/mean"),
            path_value(row, "/rust_process_stats/cpu_burn_effective_ms/mean"),
        )
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(description)}</small></td>"
            f"<td><strong>{fmt_directional_ratio(speedup)}</strong><small>target gate: row must be at least 1.0x before robust-win promotion</small></td>"
            f"<td><strong>{fmt_signed_percent(p99_reduction)}</strong><small>{fmt_us(path_value(row, '/legacy_sample_stats_us/p99'))} legacy vs {fmt_us(path_value(row, '/rust_sample_stats_us/p99'))} Rust</small></td>"
            f"<td><strong>{fmt_signed_percent(cpu_reduction)}</strong><small>effective CPU burn</small></td>"
            f"<td>{h(scheduler_regression_target(workload))}</td>"
            "</tr>"
        )
    return "\n".join(rendered)


def optimization_loop_section() -> str:
    cards = [
        (
            "Gate",
            "Every iteration starts from passing semantic parity and the current matched benchmark row. No speedup claim is promoted from a failing or unmatched row.",
        ),
        (
            "Hypothesis",
            "Each change must name the likely cost source: queue scan, Python bridge touch, allocation, lifecycle branch, data conversion, or dense data kernel.",
        ),
        (
            "Decision",
            "Land only if the row improves and no quick scheduler row regresses. Delete or isolate experiments that do not beat the scalar/simple baseline.",
        ),
        (
            "Robust win",
            "Promotion requires at least 1.20x median scheduler throughput, no quick row below 1.0x, zero semantic mismatches, and Rust p99 no worse than legacy.",
        ),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in cards
    )


def resource_format_rows(rows: list[dict]) -> str:
    format_rows = [
        row
        for row in rows
        if row.get("component") == "resources"
        and row.get("serialization_format")
        and not str(row.get("workload") or "").startswith("data_catalog_interchange_")
    ]
    if not format_rows:
        return '<tr><td colspan="9">No native Arrow IPC or Parquet resource rows were sampled in this evidence file.</td></tr>'
    rendered = []
    for row in sorted(format_rows, key=lambda item: str(item.get("workload") or "")):
        title = catalog_format_label(str(row.get("serialization_format") or "native"))
        pressure = row.get("pressure") or {}
        rendered.append(
            "<tr>"
            f"<td><strong>{h(title)}</strong><small>{h(row.get('workload'))}</small></td>"
            f"<td>{h(fmt_int(pressure.get('record_count')))}</td>"
            "<td>Required old path: same logical records through YAML/JSON text encode, compression/transmit, decompress, and parse.</td>"
            "<td><strong>Not measured</strong><small>This row is Rust native-format only; it is not byte-to-byte transmit evidence and has no old-resources interchange baseline.</small></td>"
            f"<td>{h(fmt_rate(row.get('throughput_rows_per_sec'), 'rows'))}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_data_bytes_per_sec'), 'bytes'))}</td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99')))}</td>"
            f"<td>{h(fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95')))}</td>"
            f"<td><small>{h(row.get('claim_scope') or row.get('comparability'))}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def catalog_serialization_summary_rows(scalability_evidence: dict) -> str:
    interchange_rows = catalog_interchange_rows(scalability_evidence)
    native_rows = native_resource_rows_data(scalability_evidence)
    text_rows = catalog_text_baseline_rows(scalability_evidence)
    if not interchange_rows and not native_rows and not text_rows:
        return (
            "<tr>"
            "<td colspan=\"7\"><strong>Not measured</strong><small>No catalog serialization rows found in scalability evidence.</small></td>"
            "</tr>"
        )

    interchange_by_format: dict[str, list[dict]] = {}
    native_by_format: dict[str, list[dict]] = {}
    for row in interchange_rows:
        interchange_by_format.setdefault(str(row.get("serialization_format") or "native"), []).append(row)
    for row in native_rows:
        native_by_format.setdefault(str(row.get("serialization_format") or "native"), []).append(row)

    rendered = []
    if interchange_rows or text_rows:
        rendered.append(
            "<tr>"
            "<td><strong>YAML/text + gzip</strong><small>Old-path baseline plus standalone text rows.</small></td>"
            "<td class=\"multiple-cell\"><strong>Baseline</strong><small>comparison anchor</small></td>"
            f"<td>{h(fmt_rate_range([row.get('legacy_throughput_rows_per_sec') for row in interchange_rows], 'rows'))}<small>end-to-end old path</small></td>"
            f"<td>{h(fmt_bytes_range([row.get('legacy_bytes_over_wire') for row in interchange_rows]))}<small>wire payload after gzip</small></td>"
            f"<td>{h(fmt_rate_range([row.get('throughput_rows_per_sec') for row in text_rows], 'rows'))}<small>standalone text export/parse pressure</small></td>"
            f"<td>{fmt_int(len(interchange_rows))} baseline paths; {fmt_int(len(text_rows))} standalone rows</td>"
            "<td>Measured old resource-catalog path for the architecture comparison.</td>"
            "</tr>"
        )

    for format_name in ["arrow_ipc", "parquet_zstd"]:
        interchange_group = interchange_by_format.get(format_name, [])
        native_group = native_by_format.get(format_name, [])
        if not interchange_group and not native_group:
            continue
        wire_ratios = [
            (number(row.get("legacy_bytes_over_wire")) or 0.0)
            / (number(row.get("rust_bytes_over_wire")) or 1.0)
            for row in interchange_group
            if number(row.get("legacy_bytes_over_wire")) not in (None, 0)
            and number(row.get("rust_bytes_over_wire")) not in (None, 0)
        ]
        best_speedup = max(
            (number(row.get("speedup")) or 0.0 for row in interchange_group),
            default=0.0,
        )
        row_class = ' class="highlight-row"' if best_speedup >= 10 else ""
        rendered.append(
            f"<tr{row_class}>"
            f"<td><strong>{h(catalog_format_label(format_name))}</strong><small>Grouped catalog serialization tests.</small></td>"
            f"<td class=\"multiple-cell\"><strong>{h(fmt_ratio_range([row.get('speedup') for row in interchange_group]))}</strong><small>vs YAML/text + gzip end-to-end</small></td>"
            f"<td>{h(fmt_rate_range([row.get('rust_throughput_rows_per_sec') for row in interchange_group], 'rows'))}<small>end-to-end native path</small></td>"
            f"<td>{h(fmt_ratio_range(wire_ratios, faster_label='fewer wire bytes', slower_label='more wire bytes'))}<small>wire payload vs YAML+gzip</small></td>"
            f"<td>{h(fmt_rate_range([row.get('throughput_rows_per_sec') for row in native_group], 'rows'))}<small>standalone native round-trip pressure</small></td>"
            f"<td>{fmt_int(len(interchange_group))} interchange rows; {fmt_int(len(native_group))} standalone rows</td>"
            "<td>Use interchange rows for speedup claims; standalone rows show capacity shape only.</td>"
            "</tr>"
        )

    return "\n".join(rendered)


def catalog_serialization_detail_rows(scalability_evidence: dict) -> str:
    interchange_rows = catalog_interchange_rows(scalability_evidence)
    native_rows = native_resource_rows_data(scalability_evidence)
    text_rows = catalog_text_baseline_rows(scalability_evidence)
    if not interchange_rows and not native_rows and not text_rows:
        return '<tr><td colspan="8"><strong>Not measured</strong><small>No catalog serialization detail rows available.</small></td></tr>'

    rendered = []
    for row in sorted(
        text_rows,
        key=lambda item: int(number(path_value(item, "/pressure/record_count")) or 0),
    ):
        record_count = fmt_int(path_value(row, "/pressure/record_count"))
        rendered.append(
            "<tr>"
            "<td><strong>YAML/CSV text</strong></td>"
            f"<td><strong>Standalone text round-trip</strong><small>{h(row.get('workload'))}</small></td>"
            f"<td>{h(record_count)}</td>"
            "<td class=\"multiple-cell\"><strong>Baseline</strong><small>capacity row only</small></td>"
            f"<td>{h(fmt_rate(row.get('throughput_rows_per_sec'), 'rows'))}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_data_bytes_per_sec'), 'bytes'))}<small>processed bytes/sec</small></td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99')))}<small>{h(fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95')))} peak RSS p95</small></td>"
            f"<td>{h(row.get('claim_scope') or row.get('comparability'))}</td>"
            "</tr>"
        )

    for row in sorted(
        interchange_rows,
        key=lambda item: (
            str(item.get("serialization_format") or ""),
            int(number(path_value(item, "/pressure/record_count")) or 0),
        ),
    ):
        format_name = catalog_format_label(str(row.get("serialization_format") or "native"))
        record_count = fmt_int(path_value(row, "/pressure/record_count"))
        legacy_metric = (
            f"<strong>{fmt_rate(row.get('legacy_throughput_rows_per_sec'), 'rows')}</strong>"
            f"<small>{fmt_ms_from_us(row.get('legacy_duration_us'))}; "
            f"wire {fmt_bytes(row.get('legacy_bytes_over_wire'))}; "
            f"uncompressed {fmt_bytes(row.get('legacy_uncompressed_bytes'))}</small>"
        )
        rust_metric = (
            f"<strong>{fmt_rate(row.get('rust_throughput_rows_per_sec'), 'rows')}</strong>"
            f"<small>{fmt_ms_from_us(row.get('rust_duration_us'))}; "
            f"wire {fmt_bytes(row.get('rust_bytes_over_wire'))}; "
            f"native bytes {fmt_bytes(row.get('rust_uncompressed_bytes'))}</small>"
        )
        status = (
            f"<strong>{fmt_directional_ratio(row.get('speedup'))}</strong>"
            f"<small>{bytes_ratio_text(row.get('legacy_bytes_over_wire'), row.get('rust_bytes_over_wire'))}; "
            f"{h(row.get('claim_scope'))}</small>"
        )
        row_class = ' class="highlight-row"' if (number(row.get("speedup")) or 0.0) >= 10 else ""
        rendered.append(
            f"<tr{row_class}>"
            f"<td><strong>{h(format_name)}</strong></td>"
            f"<td><strong>End-to-end catalog interchange</strong><small>{h(row.get('workload'))}</small></td>"
            f"<td>{h(record_count)}</td>"
            f"<td class=\"multiple-cell\">{status}</td>"
            f"<td>{legacy_metric}</td>"
            f"<td>{rust_metric}</td>"
            f"<td>p99 {fmt_us(path_value(row, '/rust_latency_us_extended/p99'))}<small>{fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95'))} peak RSS p95</small></td>"
            "<td>Speedup claim row: same logical records through old and native paths.</td>"
            "</tr>"
        )

    for row in sorted(
        native_rows,
        key=lambda item: (
            str(item.get("serialization_format") or ""),
            int(number(path_value(item, "/pressure/record_count")) or 0),
        ),
    ):
        format_name = catalog_format_label(str(row.get("serialization_format") or "native"))
        record_count = fmt_int(path_value(row, "/pressure/record_count"))
        rendered.append(
            "<tr>"
            f"<td><strong>{h(format_name)}</strong></td>"
            f"<td><strong>Standalone native round-trip</strong><small>{h(row.get('workload'))}</small></td>"
            f"<td>{h(record_count)}</td>"
            "<td class=\"multiple-cell\"><strong>Capacity only</strong><small>not old-vs-new</small></td>"
            f"<td>{h(fmt_rate(row.get('throughput_rows_per_sec'), 'rows'))}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_data_bytes_per_sec'), 'bytes'))}<small>processed bytes/sec</small></td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99')))}<small>{h(fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95')))} peak RSS p95</small></td>"
            f"<td>{h(row.get('claim_scope') or row.get('comparability'))}</td>"
            "</tr>"
        )
    return "\n".join(rendered)


def scalability_io_rows(rows: list[dict]) -> str:
    io_rows_data = [row for row in rows if row.get("component") == "io"]
    if not io_rows_data:
        return '<tr><td colspan="8">No network pressure rows were sampled in this evidence file.</td></tr>'
    rendered = []
    for row in sorted(io_rows_data, key=lambda item: str(item.get("workload") or "")):
        pressure = row.get("pressure") or {}
        label = str(row.get("kind") or "io").upper()
        payload = fmt_int(pressure.get("payload_bytes"))
        concurrency = fmt_int(pressure.get("concurrency"))
        rendered.append(
            "<tr>"
            f"<td><strong>{h(label)}</strong><small>{h(payload)} B payload; concurrency {h(concurrency)}</small></td>"
            "<td class=\"todo-cell\"><strong>Needs evidence</strong><small>capture matched legacy Carbon IO baseline for this payload and concurrency.</small></td>"
            f"<td><strong>{h(fmt_rate(row.get('throughput_requests_per_sec'), 'requests'))}</strong><small>{h(fmt_rate(row.get('throughput_network_bytes_per_sec'), 'bytes'))} network throughput</small></td>"
            "<td class=\"multiple-cell todo-cell\"><strong>Not measured</strong><small>needs original Carbon IO row</small></td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99')))}</td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99_9')))}</td>"
            f"<td>{h(fmt_percent(path_value(row, '/process_stats/cpu_percent/p95')))}</td>"
            f"<td>{h(fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95')))}<small>CV {h(fmt_cv(path_value(row, '/stability/coefficient_of_variation')))}</small></td>"
            f"<td class=\"todo-cell\"><strong>Needs evidence</strong><small>make this a legacy-vs-Rust IO comparison; current scope: {h(row.get('claim_scope'))}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def resource_pressure_title(row: dict) -> str:
    workload = str(row.get("workload") or "")
    if workload.startswith("data_md5_gzip_"):
        return f"MD5 + gzip, {fmt_bytes(workload.rsplit('_', 1)[-1])}"
    if workload.startswith("data_catalog_roundtrip_"):
        return "YAML/JSON catalog round-trip"
    if workload.startswith("data_catalog-arrow-ipc-roundtrip_"):
        return "Arrow IPC catalog round-trip"
    if workload.startswith("data_catalog-parquet-roundtrip_"):
        return "Parquet/Zstd catalog round-trip"
    if workload.startswith("data_catalog_interchange_arrow_ipc_"):
        return "Catalog interchange, Arrow IPC"
    if workload.startswith("data_catalog_interchange_parquet_zstd_"):
        return "Catalog interchange, Parquet/Zstd"
    return workload.replace("_", " ").title()


def resource_pressure_rows(rows: list[dict]) -> str:
    data_rows = [
        row
        for row in rows
        if row.get("component") == "resources"
        and not str(row.get("workload") or "").startswith("data_catalog")
    ]
    if not data_rows:
        return '<tr><td colspan="8">No resource/data pressure rows were sampled in this evidence file.</td></tr>'
    rendered = []
    for row in sorted(data_rows, key=lambda item: str(item.get("workload") or "")):
        pressure = row.get("pressure") or {}
        if pressure.get("record_count") is not None:
            pressure_label = f"{fmt_int(pressure.get('record_count'))} records"
        elif pressure.get("payload_bytes") is not None:
            pressure_label = f"{fmt_bytes(pressure.get('payload_bytes'))} payload"
        else:
            pressure_label = str(row.get("primary_throughput_metric") or "pressure row")
        rendered.append(
            "<tr>"
            f"<td><strong>{h(resource_pressure_title(row))}</strong><small>{h(row.get('workload'))}</small></td>"
            f"<td>{h(pressure_label)}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_operations_per_sec'), 'operations'))}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_data_bytes_per_sec'), 'bytes'))}</td>"
            f"<td>{h(fmt_rate(row.get('throughput_rows_per_sec'), 'rows'))}</td>"
            f"<td>{h(fmt_us(path_value(row, '/latency_us_extended/p99')))}</td>"
            f"<td>{h(fmt_percent(path_value(row, '/process_stats/cpu_percent/p95')))}<small>{h(fmt_kb(path_value(row, '/process_stats/max_rss_kb/p95')))} peak RSS p95</small></td>"
            f"<td>{h(fmt_cv(path_value(row, '/stability/coefficient_of_variation')))}<small>{h(row.get('claim_scope'))}</small></td>"
            "</tr>"
        )
    return "\n".join(rendered)


def technology_fit_section() -> str:
    items = [
        ("Arrow IPC", "Use now for native resource catalog transport, parity batches, and offline trace batches; keep it off scheduler dispatch."),
        ("Parquet/Zstd", "Use now for persisted resource catalog snapshots and long-lived benchmark/trace batches."),
        ("Rayon", "Use after scalar Rust dense-data baselines prove the work is pure Rust and scheduler wakeup cost will not erase the win."),
        ("SIMD", "Use only for profiled dense kernels such as filters, masks, checksums, serialization, or diagnostics; do not SIMD-optimize FIFO/channel control flow."),
        ("Tokio", "Keep out of tasklet scheduling. Consider only behind a future local reactor abstraction for IO evidence."),
        ("Proto", "Consider for compact control frames only if Arrow IPC is a poor fit; do not replace resource columnar data with ad hoc JSON/Proto envelopes."),
    ]
    return "\n".join(
        f"<article><h3>{h(title)}</h3><p>{h(body)}</p></article>" for title, body in items
    )


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
    if pressure.get("axis") == "domain_wakeup_count":
        return (
            f"Domain wakeups, {fmt_int(pressure.get('wakeup_count'))}",
            f"{fmt_int(pressure.get('domain_count'))} domains; {fmt_int(pressure.get('iterations_per_process'))} iterations per process",
        )
    if pressure.get("axis") == "message_count":
        return (
            f"Fanout messages, {fmt_int(pressure.get('message_count'))}",
            f"{fmt_int(pressure.get('worker_count'))} workers; {fmt_bytes(pressure.get('payload_bytes'))} payload; {fmt_int(pressure.get('iterations_per_process'))} iterations per process",
        )
    if pressure.get("axis") == "zone_tick_count":
        return (
            f"Zone ticks, {fmt_int(pressure.get('tick_count'))}",
            f"{fmt_int(pressure.get('zone_count'))} zones; {fmt_int(pressure.get('entities_per_zone'))} entities/zone; stride {fmt_int(pressure.get('message_stride'))}",
        )
    return workload.replace("_", " ").title(), "Rust scheduler pressure row"


def pressure_workload_key(row: dict) -> str | None:
    explicit_key = row.get("architecture_comparison_key")
    if explicit_key:
        return str(explicit_key)
    pressure = row.get("pressure") or {}
    axis = pressure.get("axis")
    if axis == "tasklet_count":
        return f"runnable_tasklets_{pressure.get('tasklet_count')}"
    if axis == "channel_pair_count":
        return f"channel_rendezvous_{pressure.get('channel_pair_count')}"
    return None


def scheduler_architecture_pressure_rows(
    scheduler_rows: list[dict],
    scheduler_pressure_rows: list[dict],
) -> str:
    if not scheduler_pressure_rows:
        return '<tr><td colspan="8">No scheduler architecture pressure rows available.</td></tr>'
    comparable_by_workload = {
        str(row.get("workload") or ""): row for row in scheduler_rows
    }
    rendered = []
    for pressure_row in scheduler_pressure_rows:
        workload_key = pressure_workload_key(pressure_row)
        comparison = comparable_by_workload.get(workload_key or "")
        pressure_title, pressure_detail = pressure_label(pressure_row)
        architecture_throughput = pressure_row.get("throughput_operations_per_sec")
        native_rate_unit = "operations"
        if comparison:
            title, description = workload_label(str(comparison.get("workload") or ""))
            throughput_unit, legacy_throughput, rust_throughput = throughput_pair(comparison)
            joined = pressure_row.get("_architecture_comparison") or {}
            metric = str(joined.get("native_comparable_metric") or "")
            architecture_throughput = joined.get("native_comparable_throughput_per_sec")
            if number(architecture_throughput) is None and metric == "native_messages_per_sec":
                architecture_throughput = pressure_row.get("throughput_messages_per_sec")
            if number(architecture_throughput) is None and metric in ("native_rendezvous_per_sec", "native_tasklets_per_sec"):
                architecture_throughput = pressure_row.get("throughput_completed_units_per_sec")
            if number(architecture_throughput) is None:
                architecture_throughput = pressure_row.get("throughput_operations_per_sec")
            legacy_baseline = architecture_comparable_baseline(comparison, metric, "legacy")
            rust_baseline = architecture_comparable_baseline(comparison, metric, "rust")
            native_rate_unit = (
                "messages"
                if metric in ("native_messages_per_sec", "native_rendezvous_per_sec")
                else throughput_unit
            )
            architecture_vs_legacy = (
                (number(architecture_throughput) or 0.0) / number(legacy_baseline)
                if number(legacy_baseline) not in (None, 0)
                else None
            )
            architecture_vs_parity = (
                joined.get("native_over_bridge_comparable_throughput_ratio")
                if number(joined.get("native_over_bridge_comparable_throughput_ratio")) is not None
                else (
                    (number(architecture_throughput) or 0.0) / number(rust_baseline)
                    if number(rust_baseline) not in (None, 0)
                    else None
                )
            )
            legacy_cell = (
                f"<strong>{h(fmt_rate(legacy_throughput, throughput_unit))}</strong>"
                f"<small>{fmt_ms_from_us(comparison.get('legacy_duration_us'))} wall; "
                f"p99 {fmt_us(path_value(comparison, '/legacy_sample_stats_us/p99'))}</small>"
            )
            parity_cell = (
                f"<strong>{h(fmt_rate(rust_throughput, throughput_unit))}</strong>"
                f"<small>{fmt_directional_ratio(comparison.get('speedup'))}; "
                f"p99 {fmt_us(path_value(comparison, '/rust_sample_stats_us/p99'))}; "
                f"{h(comparison.get('parity_status') or 'n/a')}</small>"
            )
            feature = f"<strong>{h(title)}</strong><small>{h(description)}</small>"
            multiple_cell = (
                f"<strong>{fmt_multiplier(architecture_vs_legacy)} vs original</strong>"
                f"<small>{fmt_multiplier(architecture_vs_parity)} vs parity rewrite</small>"
            )
            row_class = ' class="highlight-row"' if number(architecture_vs_legacy) and architecture_vs_legacy >= 10 else ""
        else:
            legacy_cell = "<strong>Not sampled</strong><small>No old same-shape row in scheduler comparison evidence.</small>"
            parity_cell = "<strong>Not sampled</strong><small>No Rust bridge same-shape row in scheduler comparison evidence.</small>"
            feature = f"<strong>{h(pressure_title)}</strong><small>{h(pressure_detail)}</small>"
            multiple_cell = "<strong>n/a</strong><small>needs original and parity rows</small>"
            row_class = ""
        architecture_cell = (
            f"<strong>{h(fmt_rate(architecture_throughput, native_rate_unit))}</strong>"
            f"<small>p99 {fmt_us(path_value(pressure_row, '/latency_us_extended/p99'))}; "
            f"p99.9 {fmt_us(path_value(pressure_row, '/latency_us_extended/p99_9'))}</small>"
        )
        resource_cell = (
            f"<strong>{fmt_percent(path_value(pressure_row, '/process_stats/cpu_percent/p95'))} CPU p95</strong>"
            f"<small>{fmt_kb(path_value(pressure_row, '/process_stats/max_rss_kb/p95'))} peak RSS p95; "
            f"CV {fmt_cv(path_value(pressure_row, '/stability/coefficient_of_variation'))}</small>"
        )
        comparability = (
            "Architecture pressure row: same pressure shape where a matching workload exists, but it is Rust scheduler-core only. "
            "It bypasses the Python bridge and process-to-process old-vs-Rust harness, so use it for architecture/capacity direction, not a parity speedup claim."
        )
        rendered.append(
            f"<tr{row_class}>"
            f"<td>{feature}</td>"
            f"<td>{legacy_cell}</td>"
            f"<td>{parity_cell}</td>"
            f"<td class=\"multiple-cell\">{multiple_cell}</td>"
            f"<td>{architecture_cell}</td>"
            f"<td>{resource_cell}</td>"
            f"<td>{h(pressure_detail)}</td>"
            f"<td>{h(comparability)}</td>"
            "</tr>"
        )
    return "\n".join(rendered)


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
        ("Synthetic game-loop study", [
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
      <title>Carbon to Rust Migration Test: Report Blocked</title>
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
      <h1>Carbon to Rust migration test is blocked</h1>
      <p>This artifact is intentionally incomplete because the main report requires matched legacy-vs-Rust scheduler comparison rows.</p>
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
    if scheduler_report_is_publishable(scheduler_evidence, all_scheduler_rows):
        bench = scheduler_evidence
        rows = all_scheduler_rows
    else:
        bench = scheduler_evidence
        rows = all_scheduler_rows
        return blocked_report(evidence_dir, bench, rows)

    resources_evidence = optional_json(evidence_dir / "bench-tier-local.json")
    resource_rows = comparable_rows(resources_evidence)
    scalability_evidence = optional_json(evidence_dir / "scalability-matrix.json")
    scheduler_architecture_evidence = optional_json(evidence_dir / "scheduler-architecture.json")
    scalability_evidence = merge_scheduler_architecture_rows(
        scalability_evidence, scheduler_architecture_evidence
    )
    scheduler_pressure_rows = [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "scheduler"
        and row.get("family") == "native-scheduler"
    ]
    scalability_io_capacity_rows = [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "io"
    ]
    scalability_resource_pressure_rows = [
        row
        for row in scalability_evidence.get("rows", []) or []
        if row.get("component") == "resources"
    ]
    io_evidence = optional_json(evidence_dir / "io-workloads.json")
    io_comparison_rows = io_evidence.get("comparisons", []) or []
    fixtures_evidence = optional_json(evidence_dir / "scheduler-fixtures.json")
    scheduler_summary = build_summary(rows)
    resource_summary = build_summary(resource_rows)
    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    page = Template(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Carbon to Rust Migration Test</title>
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
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
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
    .scope-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 12px;
    }
    .metric, .panel, .arch-grid article, .tested-grid article, .story-grid article, .callout {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }
    .scope-card {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
      padding: 16px;
    }
    .scope-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 8px;
    }
    .scope-head h3 { margin-bottom: 0; }
    .scope-kind {
      flex: 0 0 auto;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 3px 8px;
      color: var(--muted);
      font-size: 0.70rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      white-space: nowrap;
    }
    .scope-boundary {
      min-height: 3.9em;
      color: var(--muted);
      margin: 0 0 12px;
    }
    .scope-metrics {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      border-top: 1px solid var(--line);
      padding-top: 12px;
    }
    .scope-stat { min-width: 0; }
    .scope-stat span {
      display: block;
      color: var(--muted);
      font-size: 0.70rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .scope-stat strong {
      display: block;
      margin-top: 3px;
      font-size: 1rem;
      overflow-wrap: anywhere;
    }
    .scope-note {
      color: var(--muted);
      font-size: 0.86rem;
      margin: 12px 0 0;
    }
    .metric { padding: 15px; }
    .metric, .panel, .arch-grid article, .tested-grid article, .story-grid article, .callout, .table-wrap {
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
      margin-top: 6px;
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
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 12px;
      margin-bottom: 22px;
    }
    .summary-grid article {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }
    .summary-grid span {
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .summary-grid strong {
      display: block;
      margin-top: 5px;
      color: var(--ink);
      font-size: 1.24rem;
      line-height: 1.1;
      overflow-wrap: anywhere;
    }
    .summary-grid p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .arch-grid, .tested-grid, .story-grid, .workload-grid, .resource-workload-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .workload-grid, .resource-workload-grid {
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      margin-top: 14px;
    }
    .arch-grid article, .tested-grid article, .story-grid article, .workload-card { padding: 16px; }
    .story-grid article p,
    .arch-grid article p { color: var(--muted); }
    .workload-card {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }
    .workload-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 12px;
    }
    .workload-head p {
      color: var(--muted);
      margin: 0;
    }
    .workload-note {
      color: var(--muted);
      font-size: 0.86rem;
      margin: -2px 0 12px;
    }
    .status-pill {
      flex: 0 0 auto;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 8px;
      color: var(--green);
      font-size: 0.74rem;
      font-weight: 800;
      white-space: nowrap;
    }
    .result-strip {
      border-left: 5px solid var(--green);
      background: #f4faf6;
      padding: 10px 12px;
      margin-bottom: 12px;
    }
    .result-strip.slower {
      border-left-color: var(--amber);
      background: #fff8ec;
    }
    .result-strip.faster {
      border-left-color: var(--green);
      background: #f4faf6;
    }
    .result-strip span,
    .stat span {
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .result-strip strong {
      display: block;
      margin-top: 2px;
      font-size: 1.35rem;
      line-height: 1.05;
      overflow-wrap: anywhere;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      min-width: 0;
      border-top: 1px solid var(--line);
      padding-top: 9px;
    }
    .stat strong {
      display: block;
      margin-top: 3px;
      overflow-wrap: anywhere;
    }
    .stat small,
    .result-strip small {
      overflow-wrap: anywhere;
    }
    .row-read {
      margin: 12px 0 0;
      color: var(--muted);
      font-size: 0.93rem;
    }
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
    table.dashboard-table { min-width: 2800px; }
    table.scheduler-comparison { min-width: 1320px; }
    table.resource-comparison { min-width: 1080px; }
    .dashboard-table th.group {
      text-align: center;
      background: #dfe7ed;
      color: var(--ink);
    }
    .dashboard-table td:first-child {
      width: 160px;
      background: #fbfcfd;
    }
    .dashboard-table td.headline-cell {
      min-width: 210px;
      background: #edf7f2;
      border-left: 1px solid #cfe7da;
      border-right: 1px solid #cfe7da;
    }
    .dashboard-table tr.dashboard-detail-row > td {
      padding: 0;
      background: #fbfcfd;
      border-bottom: 1px solid var(--line);
    }
    .dashboard-table tr.dashboard-detail-row > td:first-child {
      width: auto;
      background: #fbfcfd;
    }
    .dashboard-detail {
      padding: 12px 14px 16px;
    }
    .dashboard-detail summary {
      cursor: pointer;
      color: var(--blue);
      font-weight: 800;
      min-height: 30px;
    }
    .dashboard-detail-content {
      display: grid;
      gap: 16px;
      margin-top: 12px;
    }
    .dashboard-detail-content .detail-note {
      margin: 0;
      padding: 10px 12px;
      border: 1px solid var(--line);
      background: var(--wash);
      color: var(--muted);
    }
    .dashboard-detail-panel {
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--paper);
    }
    .dashboard-detail-panel h3 {
      margin: 0;
      padding: 10px 12px;
      background: var(--wash-strong);
      border-bottom: 1px solid var(--line);
    }
    .dashboard-detail-table-wrap {
      overflow-x: auto;
    }
    .dashboard-detail .detail-table {
      min-width: 1320px;
      font-size: 0.86rem;
    }
    .headline-speedup {
      display: grid;
      gap: 8px;
    }
    .headline-speedup strong {
      color: #133d30;
      font-size: 1.08rem;
      line-height: 1.15;
    }
    .headline-speedup span {
      display: block;
      margin-bottom: 2px;
      color: #61706b;
      font-size: 0.68rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
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
    tr.highlight-row td {
      background: #fff7e5;
      border-bottom-color: #ead4a6;
    }
    tr.highlight-row td:first-child {
      background: #fff1d2;
    }
    .multiple-cell strong {
      color: var(--blue);
      font-size: 1.12rem;
      line-height: 1.1;
    }
    .todo-cell strong {
      color: var(--amber);
    }
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
    .bar.slower span { background: var(--amber); }
    .bar.faster span { background: var(--green); }
    .bar span[style*="width:2.0"] { background: var(--amber); }
    code {
      background: #edf1f5;
      border: 1px solid #dce3ea;
      border-radius: 4px;
      padding: 1px 5px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.86em;
    }
    .evidence-group { margin-top: 30px; }
    .evidence-group > h2 { margin-bottom: 8px; }
    @media (max-width: 820px) {
      .hero { padding: 32px 18px 24px; }
      main { padding: 22px 18px 44px; }
      .takeaways { grid-template-columns: 1fr; }
      .section-head { display: block; }
      .section-head .tag { margin-top: 8px; }
      h1 { font-size: 2.05rem; }
      .hero .metric-grid { grid-template-columns: 1fr 1fr; }
      .metric-grid { grid-template-columns: 1fr 1fr; }
      .workload-grid, .resource-workload-grid { grid-template-columns: 1fr; }
      .scope-grid { grid-template-columns: 1fr; }
      .stat-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      table { min-width: 760px; font-size: 0.84rem; }
      th, td { padding: 8px; }
    }
    @media (max-width: 520px) {
      .hero { padding: 28px 18px 22px; }
      h1 { font-size: 1.95rem; }
      .lead { font-size: 1rem; }
      .hero .metric-grid,
      .metric-grid { grid-template-columns: 1fr; }
      .scope-metrics { grid-template-columns: 1fr; }
      .workload-head { display: block; }
      .status-pill { display: inline-flex; margin-top: 8px; }
      .stat-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class="hero">
      <p class="eyebrow">Carbon to Rust migration test</p>
      <h1>Function-by-function migration evidence.</h1>
      <p class="lead">The key stats table names each workload, shows the original path, the same-interface Rust result where it exists, and the architecture-change path where replacing older data movement is the intended product improvement.</p>
    </div>
  </header>
  <main>
    <section>
      <div class="summary-grid">
        $top_summary_cards
      </div>
      $python_nogil_probe_note
    </section>

    <section>
      <div class="section-head">
        <div>
          <h2>Key Stats</h2>
          <p>Each row is one function: old Carbon, Rust through the same interface, and the faster Rust path where we remove legacy overhead such as Python interop or YAML transport.</p>
        </div>
        <span class="tag">Generated $generated</span>
      </div>
      <div class="table-wrap">
        <table class="dashboard-table">
          <thead>
            <tr>
              <th rowspan="2">Feature</th>
              <th rowspan="2">Speedup</th>
              <th class="group" colspan="4">Original</th>
              <th class="group" colspan="4">Rust Parity</th>
              <th class="group" colspan="4">Rust Architecture</th>
              <th class="group" colspan="2">Scale / Readout</th>
            </tr>
            <tr>
              <th>Path</th>
              <th>Speed / scale</th>
              <th>Latency / tail</th>
              <th>CPU / memory</th>
              <th>Path</th>
              <th>Multiple / speed</th>
              <th>Latency / tail</th>
              <th>CPU / memory</th>
              <th>Path</th>
              <th>Multiple / speed</th>
              <th>Latency / tail</th>
              <th>CPU / memory</th>
              <th>Batch / scale</th>
              <th>Readout / Evidence gap</th>
            </tr>
          </thead>
          <tbody>
            $feature_dashboard_rows
          </tbody>
        </table>
      </div>
    </section>

  </main>
</body>
</html>
"""
    )
    return page.safe_substitute(
        feature_dashboard_rows=feature_dashboard_rows(
            scheduler_summary,
            resource_summary,
            rows,
            resource_rows,
            fixtures_evidence,
            scalability_evidence,
        ),
        top_summary_cards=top_summary_cards(
            scheduler_summary,
            resource_summary,
            rows,
            resource_rows,
            fixtures_evidence,
            scalability_evidence,
        ),
        python_nogil_probe_note=python_nogil_probe_note(),
        scheduler_rows=scheduler_port_rows(rows),
        scheduler_architecture_pressure_rows=scheduler_architecture_pressure_rows(
            rows,
            scheduler_pressure_rows,
        ),
        resource_rows=resource_port_rows(resource_rows),
        catalog_serialization_summary_rows=catalog_serialization_summary_rows(scalability_evidence),
        catalog_serialization_detail_rows=catalog_serialization_detail_rows(scalability_evidence),
        io_capacity_rows=scalability_io_rows(
            scalability_evidence.get("rows", []) or []
        ),
        io_rows=io_rows(io_comparison_rows),
        resource_pressure_rows=resource_pressure_rows(
            scalability_evidence.get("rows", []) or []
        ),
        generated=h(generated),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--guide-output", type=Path, default=DEFAULT_GUIDE_OUTPUT)
    args = parser.parse_args()

    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    html_text = render(args.evidence_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html_text, encoding="utf-8")
    guide_text = render_reporting_guide(generated)
    args.guide_output.parent.mkdir(parents=True, exist_ok=True)
    args.guide_output.write_text(guide_text, encoding="utf-8")
    print(f"wrote {args.output}")
    print(f"wrote {args.guide_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
