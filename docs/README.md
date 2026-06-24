# Fenris Documentation

This directory contains the current handoff documentation for the Fenris
workspace. Historical planning notes are kept under `docs/archive/` so the root
of the repository stays focused on reproducibility and review.

## Current Docs

| Document | Purpose |
| --- | --- |
| [repo-organization.md](repo-organization.md) | Repo boundaries, Knotwerk mirror policy, and CarbonEngine patch ownership. |
| [functionality-matrix.md](functionality-matrix.md) | Current parity and functionality coverage across scheduler, resources, IO, and reporting gates. |

## Review Material

Detailed review notes live in [../reviews](../reviews):

- `reviews/tasks.md`: current task queue.
- `reviews/report-readiness.md`: evidence gate status for shareable reports.
- `reviews/coverage-map.md`: source coverage map.
- `reviews/optimization-map.md`: optimization targets and evidence links.
- `reviews/performance-map.md`: benchmark and performance evidence map.
- `reviews/pairs/`: source-pair analysis by subsystem.

## Archived Planning Notes

Archived docs are useful for audit history, but they are not the current
execution plan:

- [archive/planning/carbon-rust-migration-plan.md](archive/planning/carbon-rust-migration-plan.md)
- [archive/planning/performance-dashboard-plan.md](archive/planning/performance-dashboard-plan.md)
- [archive/planning/scheduler-v2-rust-rewrite-plan.md](archive/planning/scheduler-v2-rust-rewrite-plan.md)
- [archive/baseline/carbonengine-baseline.md](archive/baseline/carbonengine-baseline.md)
- [archive/baseline/test-harness-status.md](archive/baseline/test-harness-status.md)

Use the generated evidence under `target/carbon/evidence/` for current measured
status.
