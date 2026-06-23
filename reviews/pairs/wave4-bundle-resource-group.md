# Wave 4 Pair: BundleResourceGroup*.cpp/.h -> Rust bundle pipeline

Status: partial.

Legacy source:

- `carbonengine/resources/src/BundleResourceGroupImpl.cpp`
- `carbonengine/resources/include/BundleResourceGroup.h`

Rust target:

- `carbon-resources-pipeline`

Seed finding: `RES-BUNDLE-001`.

## Notes

Bundle parity needs manifest compatibility, chunk ordering, checksum validation, and externally visible byte checks for create/unpack flows.

