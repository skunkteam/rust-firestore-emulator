# emulator-tests Source (`src`)

## Overview
Implements the centralized in-process black-box test harness (`harness/`) and hot-path / perimeter verification suites (`suites/`) for the Firestore emulator. Regular tests run entirely in-memory over `FirestoreClient::with_origin(emulator_grpc::service(project), ...)` and `RouterBuilder::new(project).build().oneshot(...)` with zero OS socket binds, except for a single loopback TCP multiplexer smoke test in `suites/http_and_multiplex.rs`.

## Interface
- `[lib.rs]`: `#![cfg(test)]` crate root registering private `mod harness;` and `mod suites;`.
- `[harness/env.rs:TestEnv]`: Configures `FirestoreConfig` (`long_contention_timeout` for `Timeouts::FAST` vs. `Timeouts::CLOUD`, `default_enterprise` for `STANDARD` vs. `ENTERPRISE`) and leaks one isolated `&'static FirestoreProject` per test.
- `[harness/client.rs:TestDb]`: Dual-protocol in-memory client wrapping Gzip-enabled `FirestoreClient` (`50 MiB` decoding limit) and Axum `Router` (`http_oneshot`, `http_raw_body`, `http_stream`, `set_admin_mode`) with scenario helpers (`put`, `create`, `get`, `update`, `delete`, `list_docs`, `list_collection_ids`, `batch_get`, `batch_write`, `commit_writes`, `query`, `aggregate`, `pipeline`, `begin_rw_txn`, `begin_ro_txn`, `listen_doc`, `listen_query`).
- `[harness/builders.rs]`: Concise wire builders (`QuerySpec`, `AggregationSpec`, `PipelineSpec`, `WriteBatchSpec`, `TxnClient`, transform and filter constructors) lowering relative document/collection paths into `google.firestore.v1` Protobuf messages.
- `[harness/stream.rs]`: Stream collectors (`collect_run_query`, `collect_batch_get`, `collect_aggregation`, `collect_pipeline`, `ListenHandle`) unpacking `error_in_stream` first-frame errors, stripping empty `RunQueryResponse` sentinel frames, and draining bounded server `mpsc::channel(16)` streams on background tasks.
- `[harness/contention.rs:ContentionProbe]`: Two-actor deterministic concurrency harness driving `tokio_test::task::spawn` handles across `tokio::time::advance` and intermediate `1s` slow-lock warning wakeups under `#[tokio::test(start_paused = true)]`.
- `[harness/matchers.rs]`: Timestamp-ignoring document matchers defaulting to wire-exact `Value::fields_storage_eq` (`assert_doc_wire_eq`) with opt-in query-semantic `PartialEq` (`assert_doc_semantic_eq`), `assert_monotonic_timestamps`, and `assert_status_code`.
- `[harness/proptest_support.rs]`: Bounded 16-ID vocabulary strategies, non-nested-array `Value`/`Document`/`QuerySpec` generators, and `run_async_proptest` reusing a single Tokio runtime and `FirestoreProject` across iterations via `clear_database`.
- `[suites/]`: Black-box test modules covering `crud`, `transforms_preconditions`, `list_and_collections`, `structured_query`, `aggregation`, `pipelines`, `transactions_mvcc`, `contention_timeouts`, `listeners`, `properties`, and `http_and_multiplex`.

## Invariants
- All items inside `harness/` and `suites/` use `pub(crate)` visibility to satisfy workspace `unreachable_pub = "warn"` under `#![cfg(test)]`.
- `assert_doc_wire_eq` compares stored fields with `Value::fields_storage_eq` (distinguishing `1` vs. `1.0` and `-0.0` vs. `+0.0`) and verifies Prost binary encode/decode roundtrip fidelity.
- `run_async_proptest` clears `(default)` at the start of every generated case and bounds generated identifiers to a fixed 16-ID vocabulary so `string_cache::DefaultAtom` global interning remains bounded.
- Any status-code edge case whose live Cloud Firestore behavior is ambiguous and unverified by `test-suite/tests/` is marked `#[ignore = "needs-cloud-oracle: <detail>"]` rather than pinned to speculative emulator behavior.

## Side Effects
- Leaks isolated `FirestoreProject` allocations per test (`Box::leak`) and initializes `DefaultTracing::start()` idempotently inside the test process.
- `[suites/http_and_multiplex.rs:loopback_tcp_multiplexer_serves_grpc_and_http_on_same_port_and_shuts_down_cleanly]` binds an ephemeral `127.0.0.1:0` TCP socket to verify `firestore_emulator::run` HTTP/2 gRPC + HTTP/1.1 REST multiplexing and graceful shutdown.

## Verification
- Run: `cargo nextest run -p emulator-tests --profile ci`
