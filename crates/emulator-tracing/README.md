# emulator-tracing

## Overview
Crate root for `emulator-tracing` (`[Cargo.toml:emulator-tracing]`), providing global `tracing` subscriber setup, optional `console-subscriber` integration (`[Cargo.toml:console]`), and dynamic per-subscriber log filtering and buffering for the Firestore emulator workspace.

## Subdirectories & Boundaries
- `src`: [`./src/README.md`](./src/README.md) - Core implementation of `DefaultTracing`, the `Tracing` and `DynamicSubscriber` traits, and `metadata_filter` directive parsing and matching.
