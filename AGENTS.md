# Agent Workspace Instructions

## Architecture & Subsystem Contracts
- **System Blueprint**: Read [README.md](./README.md) before designing features, changing cross-crate interfaces, or modifying build/verification workflows.
- **Cloud Firestore Conformance Suite**: Read [test-suite/README.md](./test-suite/README.md) and [test-suite/tests/README.md](./test-suite/tests/README.md) whenever adding features, changing Firestore semantics, or fixing bugs in `crates/emulator-database`, `crates/emulator-grpc`, `crates/emulator-http`, `crates/googleapis`, or `crates/fifo-rwlock`.

## Conformance-First Development Rule
`firestore-emulator` conforms strictly to **production Google Cloud Firestore**, verified by `test-suite/`. Every new feature (including lifting `notImplementedInRust` guards) and every behavioral bugfix **must** include or update conformance tests in `test-suite/tests/`:

1. **Ground-Truth Cloud Verification (`cd test-suite && npm test`)**: Runs Jest directly against live Google Cloud Firestore (requires GCP authentication and unset `FIRESTORE_EMULATOR_HOST`). Assertions in `test-suite/tests/` must always match real Cloud Firestore behavior rather than emulator quirks.
2. **Rust Emulator Verification (`just sdk-test`)**: Compiles `cargo build --no-default-features` and executes `npm run test:rust-emulator`.
   - **Gotcha**: Running `npm run test:rust-emulator` directly inside `test-suite/` starts `../target/debug/firestore-emulator` **without a build step**; always rebuild first (`just sdk-test`, `cargo build`, or `npm run build:rust`) so tests do not run against a stale binary.
3. **Java Emulator Comparison (`cd test-suite && npm run test:java-emulator`)**: Runs the suite against Google's official Java-based Firebase Firestore emulator (`firebase emulators:exec --only firestore --maxConcurrency 1`) for behavioral comparison.
4. **Full Verification Gate (`just`)**: Runs Rust unit/doc tests, `cargo clippy` + Web UI JSDoc type-checking (`just ui-check`), and `just sdk-test`. Design verification plans and PR checks must include conformance testing alongside crate unit tests.
