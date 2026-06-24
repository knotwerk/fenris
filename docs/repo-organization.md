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
| `carbonengine/core`, `carbonengine/resources`, `carbonengine/scheduler` | CarbonEngine source repos with migration host-enablement patches | Pinned to private Knotwerk mirrors so Fenris does not depend on machine-local commits. Upstream remains the source to reconcile with later. |
| `carbonengine/io`, `carbonengine/vcpkg-registry` | Upstream CarbonEngine repos used without local Fenris patches | Keep on upstream CarbonEngine URLs. |

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
not an upstream tag. The current patched CarbonEngine submodules are private
Knotwerk mirrors, not permanent forks.

## Current CarbonEngine Patch State

These CarbonEngine deltas are required evidence context. They are saved in
private Knotwerk mirrors so the Fenris submodule pins are fetchable by invited
client reviewers, but they should still be upstreamed or otherwise reconciled
before a fully public handoff:

- `carbonengine/core`: one host-compatibility commit on
  `https://github.com/knotwerk/carbonengine-core.git`.
  - `core: add Linux host compatibility fixes`
- `carbonengine/resources`: two commits on
  `https://github.com/knotwerk/carbonengine-resources.git`.
  - `resources: support Linux case-sensitive test paths`
  - `resources: ignore local CMake build trees`
- `carbonengine/scheduler`: two commits on
  `https://github.com/knotwerk/carbonengine-scheduler.git`.
  - `scheduler: drain queue channel receives from main tasklet`
  - `scheduler: fix Python boolean argument parsing`

Do not bury these deltas inside Fenris evidence. They need a visible ownership
decision after client review: upstream PR, long-lived private branch, patch
queue, or deliberate public fork.

## Cleanup Rules

- Fenris should not contain duplicate copies of Rust implementation crates.
  `xtask` depends on component repos by path.
- Generated artifacts under `target/carbon` stay out of git. Share generated
  HTML/evidence as release artifacts or client handoff bundles.
- Public fixtures and docs should avoid local absolute paths such as
  `/data/repos/fenris`; use relative source references instead.
- Final report claims must continue to come from the existing readiness gates,
  not hand-edited prose.
