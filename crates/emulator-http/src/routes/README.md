# routes

## Overview
Axum HTTP route modules implementing the Firestore emulator's control plane and SSE live-invalidation stream (`/emulator/v1`), dynamic log capture and polling (`/emulator/v1/logging`), and official Google Cloud Firestore v1 ProtoJSON REST CRUD, query, and database metadata endpoints (`/v1/projects`).

## Interface
- `pub(crate) fn emulator::router() -> Router<&'static FirestoreProject>` (`[emulator.rs:router]`): Mounts `GET /` (`[emulator.rs:list_databases]`), `GET /events` (`[emulator.rs:sse_events]`), and `DELETE /{*ref}` (`[emulator.rs:delete_by_ref]`).
- `pub(crate) fn logging::router(tracing: &'static impl Tracing) -> Router` (`[logging.rs:router]`): Mounts `POST /{name}` (`[logging.rs:start_capture_or_poll_logs]`, `[logging.rs:poll_logs]`) and `DELETE /{name}` (`[logging.rs:stop_capture]`) backed by `TracingState` (`[logging.rs:TracingState]`).
- `pub(crate) fn projects::router() -> Router<&'static FirestoreProject>` (`[projects.rs:router]`): Mounts `GET` and `PATCH /{project_id}/databases/{database_id}` (`[projects.rs:get_database]`, `[projects.rs:patch_database]`) and the unified Firestore v1 document/query dispatcher on `/{project_id}/databases/{database_id}/documents`, `:listCollectionIds`, `:runQuery`, and `/{*tail}` (`[projects.rs:dispatch_documents]`).

## Invariants
- `GET /emulator/v1` returns `Json(Vec<RootRef>)` (`["projects/{p}/databases/{d}/documents"]`) without creating databases (`[emulator.rs:list_databases]`).
- `DELETE /emulator/v1/{*ref}` clears an entire database when `{*ref}` is `Ref::Root` (`project.clear_database`) and recursively deletes matching documents and subcollections via `FirestoreDatabase::delete_prefix` when `{*ref}` is `Ref::Collection` or `Ref::Document` (`[emulator.rs:delete_by_ref]`).
- `GET /emulator/v1/events` streams `ProjectEvent` notifications as SSE `change` events (and emits `event: resync` if the subscriber lags behind the 1024-event broadcast buffer) with a 15-second keep-alive heartbeat (`[emulator.rs:sse_events]`).
- `POST` and `PATCH` endpoints in `projects.rs` require `Content-Type: application/json` via `parse_json_body` / `ApiJson<T>` (`[projects.rs:dispatch_documents]`, `[projects.rs:patch_database]`).
- `POST /v1/projects/{p}/databases/{d}/documents/{col}` (`CreateDocument`) requires a non-empty `documentId` query parameter and an empty `document.name` field (`[projects.rs:handle_create_document]`).
- `POST .../documents:runQuery` returns a JSON array of `RunQueryResponse` objects, padded with a single `{ "readTime": ... }` element when zero documents match (`[projects.rs:handle_run_query]`).

## Side Effects
- Accesses and mutates in-memory `FirestoreProject` and `FirestoreDatabase` state across database configuration, document CRUD, recursive deletion, and query execution (`[emulator.rs:delete_by_ref]`, `[projects.rs:dispatch_documents]`).
- Subscribes to `FirestoreProject::subscribe_project_events` for long-lived SSE connections (`[emulator.rs:sse_events]`).
- Acquires a synchronous mutex (`Arc<Mutex<HashMap<String, Box<dyn DynamicSubscriber>>>>`) to register, poll, and remove dynamic log subscribers (`[logging.rs:TracingState]`).

## Verification
- Run: `cargo nextest run -p emulator-http`
