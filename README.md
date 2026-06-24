# Fenris

Fenris is the Carbon Rust rewrite evidence workbench. It owns the integration
harness, report generation, benchmark orchestration, review notes, and pinned
legacy CarbonEngine checkouts. Rust implementation code lives in standalone
component repos and is consumed by Fenris through path dependencies.

See [docs/repo-organization.md](docs/repo-organization.md) for the repo boundary
and CarbonEngine fork policy.

## Rust Migration Repos

| Path | Role |
| --- | --- |
| `carbon-scheduler-rs` | Rust scheduler implementation, trace fixtures, FFI shell, PyO3 compatibility bridge |
| `carbon-resources-rs` | Rust resources implementation, compatibility adapters, native catalog experiments |

## Submodules

| Path | Repository |
| --- | --- |
| `carbonengine/core` | `https://github.com/carbonengine/core.git` |
| `carbonengine/io` | `https://github.com/carbonengine/io.git` |
| `carbonengine/resources` | `https://github.com/carbonengine/resources.git` |
| `carbonengine/scheduler` | `https://github.com/carbonengine/scheduler.git` |
| `carbonengine/vcpkg-registry` | `https://github.com/carbonengine/vcpkg-registry.git` |
| `carbon-scheduler-rs` | `https://github.com/knotwerk/carbon-scheduler-rs.git` |
| `carbon-resources-rs` | `https://github.com/knotwerk/carbon-resources-rs.git` |

Initialize after cloning with:

```sh
git submodule update --init --recursive
```

`carbon-resources-rs` is private under the Knotwerk organization for now. The
public/private decision for Fenris and its submodules can be made later as a
separate release step.

## Evidence Commands

Generate the comparative report from existing evidence:

```sh
python3 scripts/render-carbon-to-rust-migration-test.py
```

Run the native resource comparison after building optimized legacy resources
binaries:

```sh
RUSTFLAGS="-C target-cpu=native" cargo run -p xtask --profile release-native -- bench
```

The shareable HTML report is written to `target/carbon/report/carbon-to-rust-migration-test.html`.
The companion reporting/TODO guide is written to `target/carbon/report/carbon-to-rust-reporting-guide.html`.

## Licensing

Fenris and the new Rust migration repos use the MIT License. The Knotwerk GitHub
organization is operated by ReLU ehf; new Rust migration copyright notices use
`ReLU ehf` while upstream-derived CarbonEngine behavior retains CCP attribution.
