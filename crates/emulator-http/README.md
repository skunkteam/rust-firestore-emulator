# emulator-http

## Overview
Crate directory for `emulator-http`, providing the Axum-based HTTP interface, REST management endpoints, dynamic log capture endpoints, and optional embedded web UI (`ui` Cargo feature) for the Firestore emulator.

## Subdirectories & Boundaries
- [src](./src/README.md) - Core HTTP router builder (`RouterBuilder`), error mapping (`RestError`), and nested route handlers (`routes/`).
