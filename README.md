# Fenris

Fenris is the integration and evidence workbench for the CarbonEngine-to-Rust
migration. It keeps the cross-repo harness, benchmark orchestration, report
generation, review notes, and pinned legacy CarbonEngine baselines in one place.

Implementation code lives in standalone component repositories and is consumed
here as submodules. Fenris should therefore be treated as the client-facing
project dashboard and reproducibility workspace, not as the source repository
for every migrated component.

## Current Status

- `carbon-scheduler-rs` contains the Rust scheduler core, trace fixtures, C ABI
  shell, and PyO3 compatibility bridge.
- `carbon-resources-rs` contains the Rust resources model, compatibility
  adapters, bundle/patch helpers, and native catalog format experiments.
- Patched CarbonEngine baselines for `core`, `resources`, and `scheduler` are
  pinned to private Knotwerk mirrors so the recorded submodule SHAs are
  fetchable by invited reviewers.
- `carbonengine/io` remains an upstream submodule for dependency and trace
  classification; it has not been forked into Knotwerk.
- Generated evidence and HTML reports are written under `target/carbon/` and
  are not tracked in git.

The repository is suitable for invited client review once the reviewer has
access to the private Knotwerk submodules. Making everything public is a
separate release decision because some submodules are still private mirrors.

## Repository Map

| Path | Repository | Role |
| --- | --- | --- |
| `carbon-scheduler-rs` | `https://github.com/knotwerk/carbon-scheduler-rs.git` | Rust scheduler migration repo. |
| `carbon-resources-rs` | `https://github.com/knotwerk/carbon-resources-rs.git` | Rust resources migration repo. |
| `carbonengine/core` | `https://github.com/knotwerk/carbonengine-core.git` | CarbonEngine core mirror with Linux host-enablement patches. |
| `carbonengine/resources` | `https://github.com/knotwerk/carbonengine-resources.git` | CarbonEngine resources mirror with test and host-enablement patches. |
| `carbonengine/scheduler` | `https://github.com/knotwerk/carbonengine-scheduler.git` | CarbonEngine scheduler mirror with migration host patches. |
| `carbonengine/io` | `https://github.com/carbonengine/io.git` | Upstream CarbonEngine IO dependency and trace classification source. |
| `carbonengine/vcpkg-registry` | `https://github.com/carbonengine/vcpkg-registry.git` | Upstream CarbonEngine vcpkg registry. |

See [docs/repo-organization.md](docs/repo-organization.md) for the ownership
boundaries and CarbonEngine mirror policy.

## Clone

```sh
git clone --recurse-submodules https://github.com/knotwerk/fenris.git
cd fenris
```

For an existing checkout:

```sh
git submodule update --init --recursive
```

If a submodule cannot be fetched, confirm that the GitHub account has access to
the relevant private Knotwerk repository.

## First Checks

These commands validate the workspace shape without rebuilding every legacy
dependency:

```sh
cargo metadata --no-deps
cargo metadata --manifest-path carbon-scheduler-rs/Cargo.toml --no-deps
cargo metadata --manifest-path carbon-resources-rs/Cargo.toml --no-deps
cargo run -p xtask -- scheduler-fixtures
cargo run -p xtask -- rust-resources
```

The scheduler fixture gate currently reports `67/67` semantic fixtures passing.

## Evidence And Reports

Fenris writes machine-readable evidence to `target/carbon/evidence/` and HTML
reports to `target/carbon/report/`.

Useful entry points:

```sh
cargo run -p xtask -- scheduler-fixtures
cargo run -p xtask -- rust-resources
cargo run -p xtask -- bench
python3 scripts/render-carbon-to-rust-migration-test.py
```

The main generated report is:

```text
target/carbon/report/carbon-to-rust-migration-test.html
```

The companion reporting guide is:

```text
target/carbon/report/carbon-to-rust-reporting-guide.html
```

Report claims are gated by the evidence JSON. Do not hand-edit broad speedup or
readiness claims into generated reports.

## Documentation

- [docs/README.md](docs/README.md) is the documentation index.
- [docs/functionality-matrix.md](docs/functionality-matrix.md) tracks parity
  and functionality coverage.
- [reviews/](reviews/) contains review maps, task queues, and source-pair
  analysis used to drive the migration.

## Licensing

Fenris is MIT licensed. Submodules keep their own licenses and notices; the root
license does not override submodule licensing.

Read [LICENSES.md](LICENSES.md) before sharing source archives, generated
evidence bundles, or public mirrors.
