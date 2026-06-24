# Review Notes

This directory contains the review material that drives the migration backlog.
It is intentionally separate from the top-level README: the README is for
orientation and reproducibility, while these files preserve review decisions and
open proof obligations.

## Current Files

| File | Purpose |
| --- | --- |
| [tasks.md](tasks.md) | Consolidated migration backlog. |
| [report-readiness.md](report-readiness.md) | Current gate status for shareable reports. |
| [coverage-map.md](coverage-map.md) | Feature and fixture coverage map. |
| [optimization-map.md](optimization-map.md) | Optimization opportunities and evidence requirements. |
| [performance-map.md](performance-map.md) | Benchmark evidence and allowed performance claims. |
| [scheduler-cpp-baseline-note.md](scheduler-cpp-baseline-note.md) | Positioning note for the optimized legacy C++ scheduler baseline. |
| [queue.md](queue.md) | Ordered pair-review queue. |
| [findings.jsonl](findings.jsonl) | Machine-readable consolidated findings. |
| [pairs/](pairs/) | Pairwise legacy-to-Rust review notes by subsystem. |

The generated report should cite evidence JSON and these review maps, not the
archived planning docs directly.
