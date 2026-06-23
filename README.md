# Fenris

Fenris is the superproject for the Carbon Rust rewrite evidence workspace. It
keeps the active Rust harness, reports, fixtures, and planning documents at the
root, with the existing Carbon repositories and the standalone Rust scheduler
port attached as submodules.

## Submodules

| Path | Repository |
| --- | --- |
| `carbonengine/core` | `https://github.com/carbonengine/core.git` |
| `carbonengine/io` | `https://github.com/carbonengine/io.git` |
| `carbonengine/resources` | `https://github.com/carbonengine/resources.git` |
| `carbonengine/scheduler` | `https://github.com/carbonengine/scheduler.git` |
| `carbonengine/vcpkg-registry` | `https://github.com/carbonengine/vcpkg-registry.git` |
| `carbon-scheduler-rs` | `https://github.com/knotwerk/carbon-scheduler-rs.git` |

Initialize after cloning with:

```sh
git submodule update --init --recursive
```

## Evidence Commands

Generate the comparative report from existing evidence:

```sh
python3 scripts/render-blog-report.py
```

Run the native resource comparison after building optimized legacy resources
binaries:

```sh
RUSTFLAGS="-C target-cpu=native" cargo run -p xtask --profile release-native -- bench
```

The shareable HTML report is written to `target/carbon/report/blog.html`.
