# Licensing

Fenris uses submodules, so there is no single license file that covers every
file visible in a recursive checkout. The root [LICENSE](LICENSE) covers the
Fenris parent repository only. Each submodule keeps its own license and notice
files.

The Knotwerk GitHub organization is operated by ReLU ehf. New Rust migration
work uses ReLU ehf copyright notices while upstream-derived CarbonEngine code
and behavior retain the upstream CCP Games notices.

## License Matrix

| Path | License | License file | Notes |
| --- | --- | --- | --- |
| `.` | MIT | [LICENSE](LICENSE) | Fenris integration workspace, docs, scripts, and `xtask`. |
| `carbon-scheduler-rs` | MIT | [carbon-scheduler-rs/LICENSE](carbon-scheduler-rs/LICENSE) | Standalone Rust scheduler migration repo. |
| `carbon-resources-rs` | MIT | [carbon-resources-rs/LICENSE](carbon-resources-rs/LICENSE) | Standalone Rust resources migration repo. |
| `carbonengine/core` | MIT | [carbonengine/core/LICENSE.txt](carbonengine/core/LICENSE.txt) | Knotwerk mirror of upstream CarbonEngine core with host-enablement patches. |
| `carbonengine/resources` | MIT | [carbonengine/resources/LICENSE.txt](carbonengine/resources/LICENSE.txt) | Knotwerk mirror of upstream CarbonEngine resources. Preserve [carbonengine/resources/NOTICE.md](carbonengine/resources/NOTICE.md) when redistributing. |
| `carbonengine/scheduler` | MIT | [carbonengine/scheduler/LICENSE.txt](carbonengine/scheduler/LICENSE.txt) | Knotwerk mirror of upstream CarbonEngine scheduler. Preserve [carbonengine/scheduler/NOTICE.md](carbonengine/scheduler/NOTICE.md) when redistributing. |
| `carbonengine/io` | PSF License Version 2 with Carbon IO notice | [carbonengine/io/LICENSE.txt](carbonengine/io/LICENSE.txt) | Upstream CarbonEngine IO derivative of Python socket, SSL, and select sources. Preserve [carbonengine/io/NOTICE.md](carbonengine/io/NOTICE.md). |
| `carbonengine/vcpkg-registry` | MIT | [carbonengine/vcpkg-registry/LICENSE.txt](carbonengine/vcpkg-registry/LICENSE.txt) | Upstream CarbonEngine vcpkg registry. Preserve [carbonengine/vcpkg-registry/NOTICE.md](carbonengine/vcpkg-registry/NOTICE.md) when redistributing. |

Nested vendor submodules, including Microsoft `vcpkg`, retain their own
upstream licenses. Inspect them after `git submodule update --init --recursive`
before preparing a source distribution.

## Sharing Rules

- Do not assume the Fenris root MIT license overrides submodule licenses.
- Preserve every `LICENSE*` and `NOTICE*` file when sharing recursive source
  archives or client review bundles.
- Generated evidence and HTML reports may reference submodule behavior, but they
  should not embed third-party source code unless the corresponding license and
  notice text is included.
- The public Fenris checkout uses public Knotwerk mirrors for patched
  CarbonEngine baselines. Any source archive still needs to preserve every
  submodule license and notice file.
