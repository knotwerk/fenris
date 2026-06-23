# Wave 4 Pair: PatchResourceGroup*.cpp/.h -> Rust patch pipeline

Status: partial.

Legacy source:

- `carbonengine/resources/src/PatchResourceGroupImpl.cpp`
- `carbonengine/resources/include/PatchResourceGroup.h`

Rust target:

- `carbon-resources-pipeline`

Seed finding: `RES-PATCH-001`.

## Notes

Patch parity needs deterministic generation/application checks, temp-file cleanup behavior, removed-resource metadata, and compatibility rules for generated patch bytes.

