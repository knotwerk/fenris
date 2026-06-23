# Wave 4 Pair: resources GTest/CLI tests -> Rust parity fixtures

Status: partial.

Legacy source:

- `carbonengine/resources/tests/src/*.cpp`
- `carbonengine/resources/tests/testData`

Rust target:

- Rust resources parity fixtures

## Notes

Legacy `resources` is green locally with `121/121` CTest tests passing. Rust parity must reuse the fixture corpus for YAML/CSV, bundle, patch, compression, checksums, filters, CLI behavior, and failure cases.

