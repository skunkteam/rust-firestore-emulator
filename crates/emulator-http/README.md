# emulator-http

## Overview
Crate directory for `emulator-http`, providing the Axum-based HTTP interface: Firestore v1 ProtoJSON REST CRUD & query endpoints (`/v1/projects`), emulator management, SSE live-invalidation (`/emulator/v1/events`), lock contention snapshot & SSE invalidation endpoints (`/emulator/v1/contention`, `/emulator/v1/contention/events`), recursive prefix deletion (`/emulator/v1/{*ref}`) routes, dynamic log capture endpoints (`/emulator/v1/logging`), and optional web UI serving (`ui` Cargo feature, reading disk assets live in debug builds and embedding them via `build.rs` in release builds).

## Subdirectories & Boundaries
- `[build.rs]`: Walks `../../ui` at build time only when the `ui` Cargo feature (`CARGO_FEATURE_UI`) is enabled—emitting `build.rs` / `CARGO_FEATURE_UI` rerun directives and returning early otherwise—and generates `$OUT_DIR/ui_assets.rs` (`UI_ASSETS`, with an empty `naersk`-safe fallback when `../../ui` is absent).
- [src](./src/README.md) - Core HTTP router builder (`RouterBuilder`), error mapping (`RestError`), and nested route handlers (`routes/`).
- `tests`: In-memory Axum REST/Admin/SSE/UI integration tests (`tests/admin_api.rs`), fallback/UI route regression tests (`tests/http_routes.rs`), and `#![cfg(feature = "ui")]` pure-Rust Web UI CSS/WCAG regression tests (`tests/ui_regressions.rs`; JavaScript Web UI unit and regression tests live in `ui/tests/*.test.mjs` and run via `just ui-test`).
