# Wave 4 Pair: GzipCompressionStream.cpp -> gzip compatibility

Status: queued.

Legacy source:

- `carbonengine/resources/tools/src/GzipCompressionStream.cpp`

Rust target:

- `carbon-resources-pipeline`

Seed finding: `RES-TOOLS-002`.

## Review Scope

Gzip stream compatibility, chunk boundaries, finalization, externally visible compressed bytes, and benchmark cases.

