# src

## Overview
Core source package for `emulator-http`. Exposes `RouterBuilder` to construct the Axum HTTP `Router` with nested emulator, project, and dynamic logging routes, optional embedded UI fallback routes, response header layers, and crate-internal HTTP error conversions (`RestError`).

## Interface
- `pub struct RouterBuilder(Router)` (`[lib.rs:RouterBuilder]`, `[routes.rs:RouterBuilder]`): Builder for assembling the emulator's Axum `Router`.
- `pub fn RouterBuilder::new(project: &'static FirestoreProject) -> Self` (`[routes.rs:RouterBuilder::new]`): Creates a `RouterBuilder` nesting `/emulator/v1` and `/v1/projects` bound to `project`.
- `pub fn RouterBuilder::add_dynamic_tracing(self, tracing: &'static impl Tracing) -> Self` (`[routes.rs:RouterBuilder::add_dynamic_tracing]`): Nests the `/emulator/v1/logging` router bound to `tracing`.
- `pub fn RouterBuilder::build(self) -> Router` (`[routes.rs:RouterBuilder::build]`): Finalizes the `Router` with fallback routes, `Cache-Control: no-cache` header overriding, and HTTP tracing middleware.
- `pub(crate) struct RestError` and `pub(crate) type Result<T, E = RestError>` (`[error.rs:RestError]`, `[error.rs:Result]`): Internal error type converting `GenericDatabaseError` and `SetLogLevelsError` into Axum HTTP responses (`IntoResponse`).
- Subdirectories:
  - [routes](./routes/README.md): Axum route handlers for `/emulator/v1`, `/emulator/v1/logging`, and `/v1/projects`.

## Invariants
- `RouterBuilder::new` and `RouterBuilder::add_dynamic_tracing` require `'static` references to `FirestoreProject` and `impl Tracing` (`[routes.rs:RouterBuilder::new]`, `[routes.rs:RouterBuilder::add_dynamic_tracing]`).
- Every response from the built router overrides `Cache-Control` to `"no-cache"` via `SetResponseHeaderLayer::overriding` (`[routes.rs:NO_CACHE]`, `[routes.rs:RouterBuilder::build]`).
- When Cargo feature `ui` is enabled (`#[cfg(feature = "ui")]`), `RouterBuilder::build` serves `/lit-html.js` (`text/javascript`) and falls back to embedded HTML (`../../../ui/index.html` with `CARGO_PKG_VERSION` injected); when `ui` is disabled, unmatched routes fall back to `GET -> "OK"` (`[Cargo.toml:ui]`, `[routes.rs:HTML]`, `[routes.rs:RouterBuilder::build]`).
- `RestError` maps `GenericDatabaseError` variants to HTTP status codes: `InvalidArgument` / `InvalidReference` -> `400 Bad Request`, `AlreadyExists` / `FailedPrecondition` -> `412 Precondition Failed`, `NotImplemented` -> `501 Not Implemented`, and `Aborted` / `Cancelled` / `Internal` -> `500 Internal Server Error` (`[error.rs:RestError]`).
- `RestError` maps `SetLogLevelsError::InvalidDirectives` to `400 Bad Request` and `SetLogLevelsError::ReloadError` to `500 Internal Server Error` (`[error.rs:RestError]`).

## Side Effects
- Routes incoming HTTP requests to handlers in [routes](./routes/README.md) that read and mutate `FirestoreProject` and `Tracing` state (`[routes.rs:RouterBuilder]`).
- Emits HTTP trace events via `tower_http::trace::TraceLayer::new_for_http()` (`[routes.rs:RouterBuilder::build]`).

## Verification
- Run: `cargo test -p emulator-http`
