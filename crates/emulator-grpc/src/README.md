# emulator-grpc Source (`src`)

## Overview
Implements the Tonic gRPC server (`FirestoreServer`) for the Google Cloud Firestore v1 API (`google.firestore.v1.Firestore`), bridging incoming gRPC requests and streaming RPCs to the in-memory `emulator-database` crate.

## Interface
- `[lib.rs:service]`: `pub fn service(project: &'static FirestoreProject) -> FirestoreServer<impl Firestore>` — constructs the Tonic `FirestoreServer` configured with Gzip compression (`CompressionEncoding::Gzip`) and a 50 MiB max decoding message size (`[lib.rs:MAX_MESSAGE_SIZE]`).
- `[lib.rs:FirestoreEmulator]`: Internal service struct implementing `googleapis::google::firestore::v1::firestore_server::Firestore`:
  - Implemented RPCs: `[lib.rs:FirestoreEmulator::get_document]`, `[lib.rs:FirestoreEmulator::batch_get_documents]`, `[lib.rs:FirestoreEmulator::commit]`, `[lib.rs:FirestoreEmulator::create_document]`, `[lib.rs:FirestoreEmulator::list_documents]`, `[lib.rs:FirestoreEmulator::update_document]`, `[lib.rs:FirestoreEmulator::delete_document]`, `[lib.rs:FirestoreEmulator::begin_transaction]`, `[lib.rs:FirestoreEmulator::rollback]`, `[lib.rs:FirestoreEmulator::run_query]`, `[lib.rs:FirestoreEmulator::execute_pipeline]`, `[lib.rs:FirestoreEmulator::run_aggregation_query]`, `[lib.rs:FirestoreEmulator::listen]`, `[lib.rs:FirestoreEmulator::list_collection_ids]`, `[lib.rs:FirestoreEmulator::batch_write]`.
  - Unimplemented RPC stubs (`Status::unimplemented`): `[lib.rs:FirestoreEmulator::partition_query]`, `[lib.rs:FirestoreEmulator::write]`.
- `[utils.rs:error_in_stream]`: `pub(crate) async fn error_in_stream<T, F, S>(stream: F) -> Result<Response<BoxStream<'static, Result<T>>>>` — wraps stream-producing futures so initialization errors are yielded as the first element of the gRPC stream rather than failing the outer RPC response.
- `[utils.rs:unimplemented]`, `[utils.rs:unimplemented_option]`, `[utils.rs:unimplemented_collection]`, `[utils.rs:unimplemented_bool]`, `[utils.rs:mandatory]`: Internal macros for validating request fields and reporting unsupported options.

## Invariants
- `[lib.rs:service]` and `[lib.rs:FirestoreEmulator]` require a `'static` reference to `FirestoreProject` (`&'static FirestoreProject`).
- `[lib.rs:MAX_MESSAGE_SIZE]` restricts decoded gRPC request payloads to `50 * 1024 * 1024` bytes (50 MiB), and `[lib.rs:MAX_PIPELINE_RESPONSE_BYTES]` bounds multi-document `ExecutePipelineResponse` frames to `1024 * 1024` bytes (1 MiB) via lazy `[lib.rs:chunk_pipeline_responses]` (`std::iter::from_fn`), attaching `transaction` only on frame `0`, emitting one empty frame when `results` is empty, and always sending at least one document per frame even if a single `Document` exceeds 1 MiB.
- `[utils.rs:error_in_stream]` never returns `Err` at the outer `Result<Response<_>>` level; setup and validation errors in `batch_get_documents`, `run_query`, `execute_pipeline`, and `run_aggregation_query` are emitted as the first item inside the stream (`once(Err(err))`) to prevent retry delays in the Node.js SDK.
- When `ConsistencySelector::NewTransaction` is used in `[lib.rs:FirestoreEmulator::batch_get_documents]`, `[lib.rs:FirestoreEmulator::run_query]`, `[lib.rs:FirestoreEmulator::execute_pipeline]`, or `[lib.rs:FirestoreEmulator::run_aggregation_query]`, the new transaction ID is sent only in the first streamed response (`mem::take(&mut new_transaction)` in `batch_get_documents` — emitting a metadata frame with `result: None` when `documents` is empty — and `NewTxnGuard::take_bytes` in the other three RPCs); `[lib.rs:NewTxnGuard]` rolls back newly allocated transactions synchronously via `database.rollback_sync(id)` on error or stream drop across those four RPCs (`[lib.rs:BatchGetStream]` rolls back on drop even after the first item unless `Poll::Ready(None)` with `completed == true` is reached, and the background worker cancels in-flight lookups when `tx.closed()`).
- `[lib.rs:FirestoreEmulator::run_query]` guarantees at least one `RunQueryResponse` is sent when a query matches zero documents (`pad_using(1, |_| None)`), emitting `document: None` with `read_time: Some(read_time)`.
- `[lib.rs:FirestoreEmulator::commit]` routes empty `CommitRequest.transaction` directly to `FirestoreDatabase::commit(writes, None)` without registering a synthetic transaction in `RunningTransactions`.
- `[lib.rs:FirestoreEmulator::create_document]` requires a non-empty `collection_id` (no `/`), a non-empty `document_id` (no `/`), `Some(document)` with empty `document.name`, and rejects `Ref::Collection` as `parent`.
- `[lib.rs:FirestoreEmulator::list_documents]` delegates pagination (`page_size`, `page_token`), `order_by`, `mask`, and `show_missing` discovery to `FirestoreDatabase::list_docs` (returning `Status::unimplemented` when `show_missing` is set with an empty `collection_id` or when `request_options` is `Some`).
- `[lib.rs:FirestoreEmulator::list_collection_ids]` returns `Status::invalid_argument` for `page_size < 0` and `Status::unimplemented` if `request_options` is `Some`, `page_size != 0 && (page_size as usize) < collection_ids.len()`, `page_token` is non-empty, or `consistency_selector` is `Some`.
- `[lib.rs:FirestoreEmulator::batch_write]` returns `Status::unimplemented` if `request_options` is `Some` or `labels` is non-empty, and applies writes concurrently and non-atomically via `join_all`, recording per-write errors in `BatchWriteResponse.status`.
- `[lib.rs:FirestoreEmulator]` enforces `unimplemented_option!(request_options)` across all 14 non-`listen` implemented RPCs, `unimplemented_option!(explain_options)` in `run_query` and `run_aggregation_query`, `unimplemented_bool!(auto_commit_transaction)` in `execute_pipeline`, and same-database ownership via `[lib.rs:check_doc_belongs_to_db]` and `[lib.rs:check_write_database]` in `batch_get_documents`, `commit`, and per-write in `batch_write`; `[lib.rs:forward_listen_requests]` / `[lib.rs:FirestoreEmulator::listen]` stop forwarding on incoming client-stream `Err(status)` and chain that error onto the response stream via a `oneshot` channel.

## Side Effects
- Reads and mutates in-memory database state and transactions in `FirestoreProject` (`[lib.rs:FirestoreEmulator]`).
- `[lib.rs:FirestoreEmulator::batch_get_documents]` spawns a background Tokio task (`tokio::spawn`) that streams document lookups over a bounded `tokio::sync::mpsc::channel(16)`.
- `[utils.rs:unimplemented]` prints `<name> is not supported yet (<file>:<line>)` to `stderr` via `eprintln!` whenever an unsupported RPC or request field is encountered.
- Records structured `tracing` spans and debug events (`#[instrument(level = Level::DEBUG, ...)]`, `debug!`) across all gRPC handlers.

## Verification
- Run: `cargo test -p emulator-grpc`
- Run (Cloud Firestore conformance suite against built binary): `just sdk-test`
