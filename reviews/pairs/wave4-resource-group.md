# Wave 4 Pair: ResourceGroup*.cpp/.h -> Rust resource catalog/model

Status: partial.

Legacy source:

- `carbonengine/resources/src/ResourceGroupImpl.cpp`
- `carbonengine/resources/include/ResourceGroup.h`

Rust target:

- `carbon-resources-core`
- `carbon-resources-compat`

Seed finding: `RES-CORE-001`.

## Notes

Legacy `resources` CTest is green on this Linux host. Rust parity must preserve YAML/CSV import/export, result codes, directory discovery, checksums, compression metadata, path casing behavior, and platform-specific binary-operation metadata.

