# src

## Overview
Root library module for `emulator-database`, implementing `FirestoreProject` multi-database management, `FirestoreDatabase` CRUD/query/aggregation/transaction operations, real-time `Listener` target streams, configuration (`FirestoreConfig`), error mapping (`GenericDatabaseError`), and internal concurrency/validation utilities.

## Interface
- `[lib.rs]`: Re-exports `pub use config::*;`, `pub use database::*;`, `pub use error::GenericDatabaseError;`, `pub use project::FirestoreProject;`, and `pub mod config;`.
- `[config.rs:FirestoreConfig]`: `pub struct FirestoreConfig { pub long_contention_timeout: bool, pub default_enterprise: bool }`
- `[project.rs:FirestoreProject]`: `pub struct FirestoreProject` (`new`, `send_project_event`, `subscribe_project_events`, `clear_database`, `database`, `database_names`, `listen`)
- `[database.rs:FirestoreDatabase]`: `pub struct FirestoreDatabase` (`new`, `get_doc`, `get_collection_ids`, `get_document_ids`, `run_query`, `run_aggregation_query`, `write`, `create_doc`, `update_doc`, `delete_doc`, `delete_prefix`, `list_docs`, `commit`, `get_read_time`, `new_txn`, `rollback`, `send_event`, `subscribe`, `enterprise_edition`, `set_enterprise_edition`, `optimistic_concurrency`, `set_optimistic_concurrency`) and public modules `event`, `field_path`, `pipeline`, `projection`, `query`, `read_consistency`, `reference`.
- `[error.rs:GenericDatabaseError]`: `pub enum GenericDatabaseError { Aborted, AlreadyExists, Cancelled, Internal, InvalidArgument, InvalidReference, FailedPrecondition, NotImplemented }` (`grpc_code`, `From<GenericDatabaseError> for tonic::Status`, `From<GenericDatabaseError> for googleapis::google::rpc::Status`)
- `[listener.rs:Listener]`: `pub(crate) struct Listener` (`start`)
- `[utils.rs]`: `pub(crate) trait RwLockHashMapExt` (`get_or_insert`) and macros `unimplemented!`, `required_option!`, `unimplemented_option!`, `unimplemented_collection!`, `unimplemented_bool!`.
- Subdirectories:
  - [database](./database/README.md): Core document, collection, reference, field path, projection, query, pipeline, read consistency, and transaction internals.

## Invariants
- `[database.rs:FirestoreDatabase::commit]`: Read-only transactions cannot modify documents; read-write commits with non-empty writes must pass `check_concurrency_violations()`, acquire write guards on all target documents in phase one before mutating any document in phase two, and remove the `TransactionId` from `RunningTransactions`.
- `[database.rs:FirestoreDatabase::run_aggregation_query]`: `StructuredQuery.select` must be `None`, and every `Aggregation` must define an `Operator` (`Count`, `Sum`, or `Avg`).
- `[listener.rs:Listener::process_request]`: Listen streams only support `AddTarget` with `target_id == 1` (`TARGET_ID`), a single active target per stream, and a single document in `DocumentsTarget`; `RemoveTarget`, `once`, `expected_count`, and `labels` are rejected as unimplemented.

## Side Effects
- `[project.rs:FirestoreProject]`: Lazily allocates and caches `Arc<FirestoreDatabase>` instances in an async `RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>`, drops them via `clear_database`, and broadcasts lifecycle and mutation `ProjectEvent` notifications over a bounded `tokio::sync::broadcast::Sender<ProjectEvent>` channel (`MAX_PROJECT_EVENT_BACKLOG = 1024`).
- `[database.rs:FirestoreDatabase]`: Validates writable `Value` variants in `perform_write`, mutates in-memory collection and document state during `write`, `create_doc`, `update_doc`, `delete_doc`, `delete_prefix`, and `commit`, and broadcasts `Arc<DatabaseEvent>` updates (as well as `ProjectEvent::DocumentsChanged` and `ProjectEvent::DatabaseConfigChanged`) over `tokio::sync::broadcast::Sender` channels (`MAX_EVENT_BACKLOG = 1024`).
- `[listener.rs:Listener::start]`: Spawns a background `tokio::spawn` task that subscribes to database broadcast streams, emits `ListenResponse` messages over a bounded `mpsc::channel(16)`, and automatically reconnects and resets targets when a database is dropped.
- `[utils.rs:unimplemented]`: Logs unsupported feature messages to `stderr` via `eprintln!`.

## Verification
- Run: `cargo test -p emulator-database`
- Run (Cloud Firestore conformance suite against built binary): `just sdk-test`
