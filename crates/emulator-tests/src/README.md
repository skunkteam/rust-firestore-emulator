# emulator-tests Source (`src`)

## Overview
Implements the centralized in-process black-box test harness (`harness/`) and hot-path / perimeter verification suites (`suites/`) for the Firestore emulator. Regular tests run entirely in-memory over `FirestoreClient::with_origin(emulator_grpc::service(project), ...)` and `RouterBuilder::new(project).add_dynamic_tracing(DefaultTracing::start()).build().oneshot(...)` with zero OS socket binds, except for two loopback TCP multiplexer tests in `suites/http_and_multiplex.rs`.

## Interface
- `[lib.rs]`: `#![cfg(test)]` crate root registering private `mod harness;` and `mod suites;`.
- `[harness/env.rs:TestEnv]`: Configures `FirestoreConfig` (`long_contention_timeout` for `Timeouts::FAST` vs. `Timeouts::CLOUD`, `default_enterprise` for `STANDARD` vs. `ENTERPRISE`) and leaks one isolated `&'static FirestoreProject` per test.
- `[harness/client.rs:TestDb]`: Dual-protocol in-memory client wrapping Gzip-enabled `FirestoreClient` (`50 MiB` decoding limit) and Axum `Router` (`project`, `raw_grpc`, `clear_default_db`, `http_oneshot`, `http_raw_body`, `http_stream`, `set_admin_mode`) with scenario and consistency/mask helpers (`put`, `create`, `get`, `get_masked`, `get_at_time`, `get_in_txn`, `update`, `delete`, `list_docs`, `list_collection_ids`, `batch_get`, `batch_get_in_txn`, `batch_get_new_txn`, `batch_write`, `commit_writes`, `commit_raw`, `rollback_raw`, `query`, `query_at_time`, `query_in_txn`, `query_new_txn`, `aggregate`, `aggregate_at_time`, `aggregate_in_txn`, `pipeline`, `pipeline_in_txn`, `pipeline_new_txn`, `begin_rw_txn`, `begin_retry_rw_txn`, `begin_ro_txn`, `open_raw_listen`, `listen_doc`, `listen_query`).
- `[harness/builders.rs]`: Concise wire builders (`vec_val`, `QuerySpec`, `QuerySpec::find_nearest`, `AggregationSpec`, `PipelineSpec`, `PipelineSpec::find_nearest`, `WriteBatchSpec`, `TxnClient`, transform and filter constructors) lowering relative document/collection paths into `google.firestore.v1` Protobuf messages.
- `[harness/stream.rs]`: Stream collectors (`collect_run_query`, `collect_batch_get`, `collect_aggregation`, `collect_pipeline`, `ListenHandle`) unpacking `error_in_stream` first-frame errors, stripping empty `RunQueryResponse` sentinel frames, and draining bounded server `mpsc::channel(16)` streams on background tasks.
- `[harness/contention.rs:ContentionProbe]` & `[harness/contention.rs:SpawnedOp]`: Two-actor deterministic concurrency harness driving `tokio_test::task::spawn` handles (`drive_settled`, `assert_pending`, `expect_ready`, `expect_aborted`) across `tokio::time::advance` and intermediate `1s` slow-lock warning wakeups under `#[tokio::test(start_paused = true)]`.
- `[harness/matchers.rs]`: Timestamp-ignoring document matchers defaulting to wire-exact `Value::fields_storage_eq` (`assert_doc_wire_eq`) with opt-in query-semantic `PartialEq` (`assert_doc_semantic_eq`), `assert_monotonic_timestamps`, `assert_status_code`, `doc_names`, and `short_ids`.
- `[harness/proptest_support.rs:AsyncProptestEnv]`: Bounded 16-ID vocabulary strategies (`DOC_ID_VOCAB`, `arb_doc_id`, `FIELD_VOCAB`, `arb_field_name`, `proptest_config`), non-nested-array `Value` (`arb_value`), `HashMap<String, Value>` (`arb_doc_fields`), and `GeneratedQueryKind` (`arb_query_kind`) generators, and `AsyncProptestEnv::{new, run_case}` reusing a single Tokio runtime and `FirestoreProject` across iterations via `clear_default_db`.
- `[suites/]`: Black-box test modules covering `crud`, `transforms_preconditions`, `list_and_collections`, `structured_query`, `aggregation`, `pipelines`, `transactions_mvcc`, `contention_timeouts`, `listeners`, `properties`, `http_and_multiplex`, and `vector_search`.

## Invariants
- Shared harness APIs inside `harness/` use `pub(crate)` visibility (never `pub`), while internal harness helpers and all modules/tests in `suites/` remain private to satisfy workspace `unreachable_pub = "warn"` under `#![cfg(test)]`.
- `assert_doc_wire_eq` compares stored fields with `Value::fields_storage_eq` (distinguishing `1` vs. `1.0` and `-0.0` vs. `+0.0`) and verifies Prost binary encode/decode roundtrip fidelity.
- `AsyncProptestEnv::run_case` clears `(default)` at the start of every generated case and bounds generated identifiers to a fixed 16-ID vocabulary so `string_cache::DefaultAtom` global interning remains bounded.
- Any status-code edge case whose live Cloud Firestore behavior is ambiguous and unverified by `test-suite/tests/` is marked `#[ignore = "needs-cloud-oracle: <detail>"]` rather than pinned to speculative emulator behavior.

## Side Effects
- Leaks isolated `FirestoreProject` allocations per test (`Box::leak`) and initializes `DefaultTracing::start()` idempotently inside the test process.
- `[suites/http_and_multiplex.rs:loopback_tcp_multiplexer_serves_grpc_and_http_on_same_port_and_shuts_down_cleanly]` and `[suites/http_and_multiplex.rs:loopback_tcp_multiplexer_routes_case_insensitive_grpc_content_type_and_rejects_grpc_web]` bind ephemeral `127.0.0.1:0` TCP sockets to verify `firestore_emulator::run` HTTP/2 gRPC + HTTP/1.1 REST multiplexing, case-insensitive `content-type` routing, `application/grpc-web` rejection, and graceful shutdown.

## Verification
- Run: `cargo nextest run -p emulator-tests --profile ci`
