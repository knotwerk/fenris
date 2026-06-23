# Wave 4 Pair: ResourceTools.cpp -> Rust IO/hash/compression toolkit

Status: partial.

Legacy source:

- `carbonengine/resources/tools/src/ResourceTools.cpp`

Rust target:

- `carbon-resources-pipeline`

Seed findings:

- `RES-TOOLS-001`
- `RES-TOOLS-002`

## Notes

Digest, compression, rolling checksum, chunk matching, file stream, and path behavior must pass identity tests before bundle/patch benchmarks are comparable.

