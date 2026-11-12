# emulator-tests

## Overview
Workspace-only black-box verification crate (`[Cargo.toml:emulator-tests]`, `publish = false`) compiling a single `#![cfg(test)]` library target (`[lib] doctest = false`). It exercises the Firestore emulator's public gRPC (`emulator-grpc`), HTTP REST/Admin/SSE (`emulator-http`), dynamic tracing (`emulator-tracing`), and TCP multiplexing (`firestore-emulator`) surfaces end-to-end without coupling to `emulator-database` internal storage types.

## Subdirectories & Boundaries
- `src`: [./src/README.md](./src/README.md) - In-process Tonic/Axum black-box test harness (`harness/`) and wire-level scenario, virtual-time contention, `proptest`, and loopback multiplexer suites (`suites/`).

## Invariants
- `[Cargo.toml:dependencies]`: Kept intentionally empty (all dependencies live under `[dev-dependencies]`) so non-test builds (`cargo build --release`) compile zero `emulator-tests` code and never enable `googleapis/client` on production crates.
- `[Cargo.toml:dev-dependencies]`: Depends on `firestore-emulator` and `emulator-http` with `default-features = false` so `cargo nextest run --workspace --exclude emulator-tests --no-default-features` never activates `emulator-http/ui`.

## Verification
- Run: `cargo nextest run -p emulator-tests --profile ci`
