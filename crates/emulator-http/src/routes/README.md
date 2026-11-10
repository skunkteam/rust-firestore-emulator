# routes

## Overview
Axum HTTP route modules implementing the Firestore emulator's REST endpoints for database inspection and clearing (`/emulator/v1`), dynamic log capture and polling (`/emulator/v1/logging`), and database edition/concurrency configuration (`/v1/projects`).

## Interface
- `pub(crate) fn emulator::router() -> Router<&'static FirestoreProject>` (`[emulator.rs:router]`): Mounts `GET /` (`[emulator.rs:list_databases]`) and `GET` / `DELETE /{*ref}` (`[emulator.rs:get_by_ref]`, `[emulator.rs:delete_by_ref]`).
- `pub(crate) fn logging::router(tracing: &'static impl Tracing) -> Router` (`[logging.rs:router]`): Mounts `POST /{name}` (`[logging.rs:start_capture_or_poll_logs]`, `[logging.rs:poll_logs]`) and `DELETE /{name}` (`[logging.rs:stop_capture]`) backed by `TracingState` (`[logging.rs:TracingState]`).
- `pub(crate) fn projects::router() -> Router<&'static FirestoreProject>` (`[projects.rs:router]`): Mounts `GET` and `PATCH /{project_id}/databases/{database_id}` (`[projects.rs:get_database]`, `[projects.rs:patch_database]`) using JSON models `DatabaseMetadata` (`[projects.rs:DatabaseMetadata]`), `PatchDatabase` (`[projects.rs:PatchDatabase]`), `DatabaseEdition` (`[projects.rs:DatabaseEdition]`), and `ConcurrencyMode` (`[projects.rs:ConcurrencyMode]`).

## Invariants
- `DELETE /{*ref}` only supports deleting root database references (`Ref::Root`); attempting to delete a collection (`Ref::Collection`) or document (`Ref::Document`) returns `StatusCode::NOT_IMPLEMENTED` (`501 Not Implemented`) (`[emulator.rs:delete_by_ref]`).
- `GET /{*ref}` reads documents using `ReadConsistency::Default` (`[emulator.rs:get_by_ref]`).
- `POST /{name}` on the logging router treats a `:poll` suffix (`name.strip_suffix(":poll")`) as a poll operation (`poll_logs`) rather than subscriber creation; polling or deleting an unknown handle returns `404 Not Found` via `RestError::not_found` (`[logging.rs:start_capture_or_poll_logs]`, `[logging.rs:poll_logs]`, `[logging.rs:stop_capture]`).
- Starting log capture on an already-registered handle `name` replaces the existing `DynamicSubscriber` and returns any buffered logs drained from the replaced subscriber (`[logging.rs:start_capture_or_poll_logs]`).
- `DatabaseMetadata::new` always sets `uid` to `"emulator-uid"` and `type` to `"FIRESTORE_NATIVE"` (`[projects.rs:DatabaseMetadata]`).
- In `PATCH /{project_id}/databases/{database_id}`, updating `database_edition` automatically sets `optimistic_concurrency` to match `is_enterprise` (`true` for `Enterprise`, `false` for `Standard`), unless `concurrency_mode` is also explicitly supplied in `PatchDatabase` (`[projects.rs:patch_database]`).

## Side Effects
- Accesses and mutates in-memory `FirestoreProject` state, including instantiating/retrieving databases (`project.database`), clearing database contents (`project.clear_database`), and updating database edition and concurrency mode settings (`db.set_enterprise_edition`, `db.set_optimistic_concurrency`) (`[emulator.rs:get_by_ref]`, `[emulator.rs:delete_by_ref]`, `[projects.rs:get_database]`, `[projects.rs:patch_database]`).
- Acquires a synchronous mutex (`Arc<Mutex<HashMap<String, Box<dyn DynamicSubscriber>>>>`), registers and removes dynamic log subscribers via `Tracing::subscribe`, and drains captured log buffers (`subscriber.drain()`) (`[logging.rs:TracingState]`, `[logging.rs:start_capture_or_poll_logs]`, `[logging.rs:poll_logs]`, `[logging.rs:stop_capture]`).

## Verification
- Run: `cargo test -p emulator-http`
