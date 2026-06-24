# Repo Organization

Fenris is the integration and evidence workbench. Component implementation code
lives in standalone Rust repos, while Fenris keeps the legacy CarbonEngine
submodules, cross-repo gates, benchmark orchestration, generated evidence, and
report rendering.

## Ownership Boundaries

| Repo or path | Role | Public-share stance |
| --- | --- | --- |
| `fenris` | Evidence workbench, `xtask`, report scripts, docs, review notes, submodule pins | Share with sanitized evidence and clear gate status. |
| `carbon-scheduler-rs` | Rust scheduler implementation, trace fixtures, FFI shell, PyO3 bridge | Standalone Rust migration repo. |
| `carbon-resources-rs` | Rust resources implementation, compatibility adapters, native catalog experiments | Private standalone Rust migration repo under `knotwerk`; Fenris records it as a submodule. |
| `carbonengine/*` | Upstream CarbonEngine source-of-truth repos used for legacy baselines | Keep as Fenris submodules; do not fork publicly by default. |

The Knotwerk GitHub organization is operated by ReLU ehf. Use `ReLU ehf` for new
Rust migration copyright notices and keep upstream CCP copyright notices where
code or fixtures derive from CarbonEngine behavior.

## CarbonEngine Fork Policy

Keep the CarbonEngine repos as submodules unless one of these is true:

- CCP asks for a public mirror or client-visible fork.
- A host-enablement patch must be reproducible for people without write access
  to the upstream private repo.
- CI for the Rust migration needs a stable branch containing Linux/macOS/Windows
  build fixes that cannot land upstream immediately.

When a fork or branch is needed, prefer an upstreamable patch branch over a
divergent fork. Fenris should pin the exact commit SHA and record why that SHA is
not an upstream tag.

## Current CarbonEngine Patch State

These local CarbonEngine deltas are required evidence context and should be
upstreamed, converted into patch files, or pinned on explicit branches before
client/public handoff:

- `carbonengine/resources`: two commits ahead of `origin/main`.
  - `resources: support Linux case-sensitive test paths`
  - `resources: ignore local CMake build trees`
- `carbonengine/scheduler`: one commit ahead of `origin/main`, plus dirty
  edits in `src/PyTasklet.cpp` and `src/stdafx.h`.
  - `scheduler: drain queue channel receives from main tasklet`
- `carbonengine/core`: dirty Linux/build-support edits in CMake, telemetry,
  thread, time, string, memory, and atomic files.

Do not bury these deltas inside Fenris evidence. They need a visible ownership
decision: upstream PR, private branch, patch queue, or deliberate fork.

## Cleanup Rules

- Fenris should not contain duplicate copies of Rust implementation crates.
  `xtask` depends on component repos by path.
- Generated artifacts under `target/carbon` stay out of git. Share generated
  HTML/evidence as release artifacts or client handoff bundles.
- Public fixtures and docs should avoid local absolute paths such as
  `/data/repos/fenris`; use relative source references instead.
- Final report claims must continue to come from the existing readiness gates,
  not hand-edited prose.
