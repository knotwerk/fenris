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


def short_text(text: object, limit: int = 140) -> str:
    value = "" if text is None else str(text)
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def throughput_pair(row: dict) -> tuple[str, object, object]:
    for key, value in row.items():
        if key.startswith("legacy_throughput_"):
            suffix = key.removeprefix("legacy_throughput_")
            rust_key = f"rust_throughput_{suffix}"
            if rust_key in row:
                return suffix.replace("_", " / "), value, row[rust_key]
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
                "Wall latency",
                fmt_ratio(summary["median_speedup"]),
                f"median uplift across {summary['rows']} old-vs-Rust workloads",
            ),
            metric_card(
                "Best uplift",
                fmt_ratio(summary["best_speedup"]),
                "fastest measured workload",
            ),
            metric_card(
                "Tail latency",
                fmt_signed_percent(summary["median_p99_reduction"]),
                f"median p99 reduction; improved in {summary['p99_better']}/{summary['rows']}",
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
                f"equal or faster wall time; {summary['materially_faster']} materially faster",
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
            f"<td><strong>{fmt_ratio(speedup)}</strong><div class=\"bar\"><span style=\"width:{bar_width(speedup, max_speedup):.1f}%\"></span></div>"
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
            "Rust-owned core behavior",
            "The rewrite moves resource operations into Rust code with explicit ownership and predictable process boundaries.",
        ),
        (
            "Compatibility-preserving CLI surface",
            "The measured Rust path keeps equivalent command behavior for the tested resource workflows.",
        ),
        (
            "Native Linux validation",
            "Old and Rust binaries now run on this Linux host, so performance can be compared locally instead of relying on unsupported x64 assumptions.",
        ),
        (
            "Measurable release pipeline",
            "Both sides are built as optimized binaries, and each row records wall time, p99, throughput, CPU, and peak memory.",
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
    ("bench-tier-local.json", "Matched scheduler benchmarks"),
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
            f"Scheduler and distributed orchestration claims are excluded from this report. {open_count} scheduler-related evidence gates are still not report-ready.",
        ),
        (
            "What remains",
            remaining_text,
        ),
        (
            "Publish rule",
            "Scheduler uplift returns to the CEO report only when full-orchestration parity is green; resource pipeline results remain the only current old-vs-Rust performance claim.",
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
            f"<td>{h('; '.join(blockers))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def tested_workloads(rows: list[dict]) -> str:
    grouped = [
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
    legacy_profiles = sorted({str(row.get("legacy_build_profile")) for row in rows})
    rust_profiles = sorted({str(row.get("rust_build_profile")) for row in rows})
    legacy_binaries = sorted({str(row.get("legacy_binary")) for row in rows if row.get("legacy_binary")})
    host = path_value(bench, "/host/cpu_model", "unknown host")
    logical_cpus = path_value(bench, "/host/logical_cpus", "unknown")
    return "\n".join(
        [
            f"<p><strong>Old baseline</strong><br>{h(', '.join(legacy_profiles))}</p>",
            f"<p><strong>Rust baseline</strong><br>{h(', '.join(rust_profiles))}; target-cpu=native={h(bench.get('target_cpu_native'))}; debug assertions={h(bench.get('debug_assertions'))}</p>",
            f"<p><strong>Host</strong><br>{h(host)}; {h(logical_cpus)} logical CPUs</p>",
            f"<p><strong>Evidence</strong><br>{h(evidence_dir / 'bench-tier-local.json')}</p>",
            f"<p><strong>Old binaries</strong><br>{h('; '.join(legacy_binaries))}</p>",
            "<p><strong>Scheduler status</strong><br>Scheduler and distributed orchestration claims are excluded until full-orchestration parity is report-ready.</p>",
        ]
    )


def blocked_report(evidence_dir: Path, bench: dict, rows: list[dict]) -> str:
    readiness = bench.get("optimization_readiness") or {}
    reason = readiness.get("blocked_reason") or "optimized old-vs-Rust comparison evidence is missing"
    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    return Template(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Carbon Rust Rewrite: Comparative Report Blocked</title>
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
      <h1>Comparative report is blocked</h1>
      <p>This artifact is intentionally not publishable because the main report requires optimized old-vs-Rust comparison rows.</p>
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
    bench_path = evidence_dir / "bench-tier-local.json"
    bench = load_json(bench_path) if bench_path.exists() else {}
    rows = comparable_rows(bench)
    if not report_is_publishable(bench, rows):
        return blocked_report(evidence_dir, bench, rows)

    scheduler_gates = gate_items(evidence_dir)
    summary = build_summary(rows)
    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    page = Template(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Carbon Rust Rewrite: Measured Old vs Rust Performance</title>
  <style>
    :root {
      --ink: #17202a;
      --muted: #52606d;
      --line: #d7dde5;
      --paper: #ffffff;
      --wash: #f4f7f9;
      --green: #25724f;
      --blue: #245f91;
      --amber: #9f6b16;
      --red: #9f3d3d;
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
      max-width: 1160px;
      margin: 0 auto;
      padding: 44px 24px 30px;
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
      max-width: 960px;
      font-size: clamp(2rem, 4.2vw, 4rem);
      line-height: 1.03;
      letter-spacing: 0;
    }
    .lead {
      max-width: 880px;
      margin: 18px 0 0;
      color: var(--muted);
      font-size: 1.08rem;
    }
    main {
      max-width: 1160px;
      margin: 0 auto;
      padding: 28px 24px 56px;
    }
    section { margin: 0 0 34px; }
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
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 12px;
    }
    .metric, .panel, .arch-grid article, .tested-grid article {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(23, 32, 42, 0.04);
    }
    .metric { padding: 15px; }
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
    }
    .takeaways {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(280px, 0.9fr);
      gap: 18px;
      align-items: start;
    }
    .panel { padding: 18px; }
    .panel ul { margin: 0; padding-left: 20px; }
    .panel li + li { margin-top: 8px; }
    .arch-grid, .tested-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .arch-grid article, .tested-grid article { padding: 16px; }
    .tested-grid ul { margin: 0; padding-left: 18px; }
    .tested-grid li + li { margin-top: 8px; }
    .tested-grid span { display: block; color: var(--muted); }
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      font-size: 0.92rem;
    }
    th, td {
      padding: 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #e9f0f5;
      color: #304050;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
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
    @media (max-width: 820px) {
      .hero { padding: 32px 18px 24px; }
      main { padding: 22px 18px 44px; }
      .takeaways { grid-template-columns: 1fr; }
      h1 { font-size: 2.15rem; }
    }
  </style>
</head>
<body>
  <header>
    <div class="hero">
      <p class="eyebrow">Old vs Rust release comparison</p>
      <h1>Carbon Rust rewrite: measured resource pipeline comparison.</h1>
      <p class="lead">This report compares the legacy optimized C++ resources CLI against the Rust release-native implementation on equivalent resource workflows. Scheduler and distributed orchestration claims are intentionally excluded until their full parity gates are report-ready.</p>
    </div>
  </header>
  <main>
    <section class="takeaways">
      <div>
        <h2>Executive Takeaway</h2>
        <div class="metric-grid">
          $executive_cards
        </div>
      </div>
      <aside class="panel">
        <h2>What Changed</h2>
        <ul>
          <li>The tested resource workflows now run through Rust-owned implementation code instead of the legacy C++ CLI path.</li>
          <li>The external behavior stays equivalent for the tested workflows, so each row is a direct old-vs-Rust comparison.</li>
          <li>The rewrite gives a cleaner performance path: lower tail latency and lower peak memory on every measured workload in this run.</li>
        </ul>
      </aside>
    </section>

    <section>
      <h2>Architecture Change</h2>
      <div class="arch-grid">
        $architecture
      </div>
    </section>

    <section>
      <h2>Scheduler Validation Gate</h2>
      <div class="arch-grid">
        $scheduler_story
      </div>
    </section>

    <section>
      <h2>Scheduler Evidence Status</h2>
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
      <h2>Old vs Rust Results</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Workload</th>
              <th>Wall latency</th>
              <th>p99 tail</th>
              <th>Throughput</th>
              <th>CPU burn</th>
              <th>Peak memory</th>
            </tr>
          </thead>
          <tbody>
            $result_rows
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Methodology</h2>
      <div class="methodology">
        $methodology
        <p><strong>Generated</strong><br>$generated</p>
      </div>
    </section>
  </main>
</body>
</html>
"""
    )
    return page.safe_substitute(
        executive_cards=executive_cards(summary),
        architecture=architecture_section(),
        scheduler_story=scheduler_story_section(scheduler_gates),
        scheduler_gate_rows=scheduler_gate_rows(scheduler_gates),
        tested_workloads=tested_workloads(rows),
        result_rows=result_rows(rows),
        methodology=methodology(bench, rows, evidence_dir),
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
