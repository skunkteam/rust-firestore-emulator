# emulator-http

## Overview
Crate directory for `emulator-http`, providing the Axum-based HTTP interface: Firestore v1 ProtoJSON REST CRUD & query endpoints (`/v1/projects`), emulator management, SSE live-invalidation (`/emulator/v1/events`), and recursive prefix deletion (`/emulator/v1/{*ref}`) routes, dynamic log capture endpoints (`/emulator/v1/logging`), and optional web UI serving (`ui` Cargo feature, reading disk assets live in debug builds and embedding them via `build.rs` in release builds).

## Subdirectories & Boundaries
- `[build.rs]`: Walks `../../ui` at build time and emits `$OUT_DIR/ui_assets.rs` (`UI_ASSETS`, with an empty `naersk`-safe fallback when `../../ui` is absent).
- [src](./src/README.md) - Core HTTP router builder (`RouterBuilder`), error mapping (`RestError`), and nested route handlers (`routes/`).
- `tests`: In-memory Axum REST/Admin/SSE/UI integration tests (`tests/admin_api.rs`) and Node.js Web UI regression tests (`tests/ui_regressions.rs`).
