# Fenris Documentation

This directory contains the current handoff documentation for the Fenris
workspace. Old planning notes were removed so the repository stays focused on
reproducibility, current evidence, and review.

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
- `reviews/pairs/`: source-pair analysis by subsystem.

Use the generated evidence under `target/carbon/evidence/` for current measured
status.
