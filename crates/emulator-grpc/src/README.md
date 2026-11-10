# emulator-grpc Source (`src`)

## Overview
Implements the Tonic gRPC server (`FirestoreServer`) for the Google Cloud Firestore v1 API (`google.firestore.v1.Firestore`), bridging incoming gRPC requests and streaming RPCs to the in-memory `emulator-database` crate.

## Interface
- `[lib.rs:service]`: `pub fn service(project: &'static FirestoreProject) -> FirestoreServer<impl Firestore>` — constructs the Tonic `FirestoreServer` configured with Gzip compression (`CompressionEncoding::Gzip`) and a 50 MiB max decoding message size (`[lib.rs:MAX_MESSAGE_SIZE]`).
- `[lib.rs:FirestoreEmulator]`: Internal service struct implementing `googleapis::google::firestore::v1::firestore_server::Firestore`:
  - Implemented RPCs: `[lib.rs:FirestoreEmulator::get_document]`, `[lib.rs:FirestoreEmulator::batch_get_documents]`, `[lib.rs:FirestoreEmulator::commit]`, `[lib.rs:FirestoreEmulator::list_documents]`, `[lib.rs:FirestoreEmulator::begin_transaction]`, `[lib.rs:FirestoreEmulator::rollback]`, `[lib.rs:FirestoreEmulator::run_query]`, `[lib.rs:FirestoreEmulator::execute_pipeline]`, `[lib.rs:FirestoreEmulator::run_aggregation_query]`, `[lib.rs:FirestoreEmulator::listen]`, `[lib.rs:FirestoreEmulator::list_collection_ids]`, `[lib.rs:FirestoreEmulator::batch_write]`.
  - Unimplemented RPC stubs (`Status::unimplemented`): `[lib.rs:FirestoreEmulator::create_document]`, `[lib.rs:FirestoreEmulator::update_document]`, `[lib.rs:FirestoreEmulator::delete_document]`, `[lib.rs:FirestoreEmulator::partition_query]`, `[lib.rs:FirestoreEmulator::write]`.
- `[utils.rs:error_in_stream]`: `pub(crate) async fn error_in_stream<T, F, S>(stream: F) -> Result<Response<BoxStream<'static, Result<T>>>>` — wraps stream-producing futures so initialization errors are yielded as the first element of the gRPC stream rather than failing the outer RPC response.
- `[utils.rs:unimplemented]`, `[utils.rs:unimplemented_option]`, `[utils.rs:unimplemented_collection]`, `[utils.rs:unimplemented_bool]`, `[utils.rs:mandatory]`: Internal macros for validating request fields and reporting unsupported options.

## Invariants
- `[lib.rs:service]` and `[lib.rs:FirestoreEmulator]` require a `'static` reference to `FirestoreProject` (`&'static FirestoreProject`).
- `[lib.rs:MAX_MESSAGE_SIZE]` restricts decoded gRPC request payloads to `50 * 1024 * 1024` bytes (50 MiB).
- `[utils.rs:error_in_stream]` never returns `Err` at the outer `Result<Response<_>>` level; setup and validation errors in `batch_get_documents`, `run_query`, `execute_pipeline`, and `run_aggregation_query` are emitted as the first item inside the stream (`once(Err(err))`) to prevent retry delays in the Node.js SDK.
- When `ConsistencySelector::NewTransaction` is used in `[lib.rs:FirestoreEmulator::batch_get_documents]`, `[lib.rs:FirestoreEmulator::run_query]`, or `[lib.rs:FirestoreEmulator::execute_pipeline]`, `mem::take(&mut new_transaction)` ensures the new transaction ID is sent only in the first streamed response message.
- `[lib.rs:FirestoreEmulator::run_query]` guarantees at least one `RunQueryResponse` is sent when a query matches zero documents (`pad_using(1, |_| None)`), emitting `document: None` with `read_time: Some(read_time)`.
- `[lib.rs:FirestoreEmulator::commit]` automatically allocates a new `ReadWrite` transaction when `CommitRequest.transaction` is empty.
- `[lib.rs:FirestoreEmulator::list_documents]` returns `Status::unimplemented` if `order_by` is non-empty, `page_token` is non-empty, `show_missing` is set with an empty `collection_id`, or the result count exceeds a non-zero `page_size`.
- `[lib.rs:FirestoreEmulator::list_collection_ids]` returns `Status::unimplemented` if `page_size != 0 && page_size < collection_ids.len() as i32`, `page_token` is non-empty, or `consistency_selector` is `Some`.
- `[lib.rs:FirestoreEmulator::batch_write]` returns `Status::unimplemented` if `labels` is non-empty, and applies writes concurrently and non-atomically via `join_all`, recording per-write errors in `BatchWriteResponse.status`.

## Side Effects
- Reads and mutates in-memory database state and transactions in `FirestoreProject` (`[lib.rs:FirestoreEmulator]`).
- `[lib.rs:FirestoreEmulator::batch_get_documents]` spawns a background Tokio task (`tokio::spawn`) that streams document lookups over a bounded `tokio::sync::mpsc::channel(16)`.
- `[utils.rs:unimplemented]` prints `<name> is not supported yet (<file>:<line>)` to `stderr` via `eprintln!` whenever an unsupported RPC or request field is encountered.
- Records structured `tracing` spans and debug events (`#[instrument(level = Level::DEBUG, ...)]`, `debug!`) across all gRPC handlers.

## Verification
- Run: `cargo test -p emulator-grpc`
