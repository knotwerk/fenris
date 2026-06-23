# Wave 4 Pair: ThreadPool/pipeline code -> Rayon/Tokio split

Status: queued.

Legacy source:

- `carbonengine/resources/tools/include/ThreadPool.h`
- resource pipeline call sites

Rust target:

- `carbon-resources-pipeline`
- `carbon-resources-remote`

## Review Scope

CPU-bound Rayon stages, async remote IO stages, cancellation boundaries, deterministic output ordering, and benchmark scaling.

