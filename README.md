# Firestore Emulator Blueprint

A local, Rust-based Google Cloud Firestore emulator built with a focus on stability, performance, and exact cloud-compliance for extensive unit testing and CI environments.

## System Overview

This repository provides a Rust alternative to the official Java-based Firebase emulator for Firestore. It acts as an in-memory backend for running large, concurrent test suites while aligning strictly with Cloud Firestore's concurrency, ordering, query, and transaction semantics. At runtime, the root binary (`src/`) binds a single TCP listener that multiplexes incoming HTTP/2 `application/grpc` requests to the Tonic gRPC service (`crates/emulator-grpc`) and all other HTTP/1.1 or HTTP/2 requests to the Axum REST and Web UI router (`crates/emulator-http` and `ui/`), both backed by shared `'static` `FirestoreProject` state (`crates/emulator-database`) and dynamic tracing (`crates/emulator-tracing`).

### Key Characteristics

- **Strict Concurrency Simulation**: Utilizes a custom `fifo-rwlock` (`crates/fifo-rwlock`) underneath the hood because standard async read-write locks do not provide the strict FIFO queuing and priority-preserving read-to-write lock upgrades required to emulate Cloud Firestore's pessimistic locking and contention behavior.
- **Driven by Need**: Exceptionally complete for core Firestore workflows (collections, documents, structured queries, aggregations, Enterprise Edition pipelines, real-time listeners, and pessimistic/optimistic transactions).
- **Cloud-Compliant Behavior**: Aligns with actual Google Cloud Firestore behavior rather than Java emulator edge cases.

### Missing Features

Because the implementation is driven by consumed features, some advanced or edge-case features remain unimplemented:

- Some updates with Transforms (such as `Maximum` and `Minimum`)
- Multiplexing listeners in a single stream (used by frontend Firestore SDKs; `Listener` enforces a single active target with `target_id == 1`)
- Nearest neighbors search
- Some field filters (such as `ArrayContainsAny` and `NotIn`)
- Explain options
- Additional unimplemented gRPC endpoints (`partition_query`, `write`) or request options guarded by `unimplemented!` macros across the codebase.

### Usage & Development Workflow

#### Prerequisites

```shell
brew install protobuf
```

For executing the test-suite and development tools, `just` and `node` are recommended:

```shell
brew install just node
```

#### Building and Testing

The full suite of tests (both Rust and Node.js SDK tests), including linting, can be executed using:

```shell
just
```

To run `just` in watch-mode, automatically re-running on file modifications:

```shell
just watch
```

#### Ground-Truth Testing Against the Cloud

The verification suite in `test-suite/` runs against the local emulator by default, and can also run directly against Google Cloud Firestore (requires GCP CLI authentication):

```shell
cd test-suite/
npm test
```

#### Standalone Server Starts

Run locally in debug-mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run
```

Run locally in release-mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run --release
```

Install directly via Cargo (includes the default `ui` feature for local use; prebuilt GitHub Release tarballs omit `ui` via `--no-default-features` for headless CI environments):

```shell
cargo install --path .
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 firestore-emulator
```

## Subdirectories & System Boundaries
- `crates`: [./crates/README.md](./crates/README.md) - Workspace library crates providing the database engine (`emulator-database`), gRPC server (`emulator-grpc`), HTTP/REST server (`emulator-http`), dynamic tracing (`emulator-tracing`), FIFO reader-writer lock (`fifo-rwlock`), and compiled Firestore v1 protobufs (`googleapis`).
- `src`: [./src/README.md](./src/README.md) - Root binary (`main.rs`) and server entrypoint (`lib.rs`, `multiplex.rs`) that initialize CLI options, Jemalloc, `DefaultTracing`, and `'static FirestoreProject` state, and multiplex gRPC and HTTP traffic over a single TCP listener.
- `ui`: [./ui/README.md](./ui/README.md) - Zero-build multi-file ES-module web UI (`index.html`, `styles.css`, `lit-html.js`, `app.js`, `api.js`, `values.js`, and `components/`) served by `emulator-http` (under the default `ui` feature) for browsing, creating, editing, deleting, and querying emulated databases via `/v1/projects/...` and `/emulator/v1`.

## Global Invariants
- **Static Server State (`[src/lib.rs:run]`, `[src/main.rs:main]`)**: Both `FirestoreProject` and `DefaultTracing` are leaked into `'static` memory (`Box::leak`) once at startup and shared across `emulator-grpc` and `emulator-http` services.
- **Protocol Multiplexing (`[src/lib.rs:is_not_a_grpc_request]`)**: Incoming requests on the shared TCP listener are routed to the Tonic `FirestoreServer` if and only if the `content-type` header starts with `application/grpc`; all other requests route to the Axum HTTP `Router`.
- **Monotonic Timestamps & MVCC (`[crates/googleapis/src/timestamp_ext.rs:Timestamp::now]`, `[crates/emulator-database/src/database/document.rs:OwnedDocumentContentsWriteGuard::maybe_add_version]`)**: `Timestamp::now()` is strictly monotonic across the process, and document version `update_time` values must be monotonically non-decreasing and `>= acquire_time`.
- **FIFO Lock Ordering & Contention (`[crates/fifo-rwlock/src/queue.rs:FifoRwLockQueue]`, `[crates/emulator-database/src/database/document.rs:OwnedDocumentContentsReadGuard::upgrade]`)**: Per-document read, write, and read-to-write upgrade requests are scheduled in strict FIFO order; upgrading a read guard aborts with `GenericDatabaseError::Aborted("contention")` if another writer updated the document while the upgrade waited.
- **Edition & Concurrency Coupling (`[crates/emulator-database/src/database/pipeline/mod.rs:execute]`, `[crates/emulator-http/src/routes/projects.rs:patch_database]`)**: Pipeline execution (`execute_pipeline`) requires Enterprise Edition (`enterprise_edition() == true`), and patching `database_edition` via `/v1/projects/{project_id}/databases/{database_id}` defaults `optimistic_concurrency` to match Enterprise (`true`) vs. Standard (`false`) unless `concurrency_mode` is explicitly specified.
- **Safety Lints (`[Cargo.toml:workspace.lints.rust]`)**: Workspace-wide Rust lints forbid `unsafe_code` (except where explicitly overridden inside `crates/fifo-rwlock` for `UnsafeCell` guards) and `explicit_outlives_requirements`.
- **CI vs. Local Build Features (`[Cargo.toml:features]`, `[.github/workflows/rust.yml:deploy]`)**: Local builds and installations (`cargo install`, `cargo run`, `flake.nix`) enable `default = ["ui"]` to serve the Web UI, whereas `.github/workflows/rust.yml` intentionally builds release tarballs with `--no-default-features` for headless CI runners.

## Global Side Effects
- **Process-Wide Runtime Initialization (`[src/main.rs:GLOBAL_ALLOC]`, `[src/main.rs:main]`, `[crates/emulator-tracing/src/lib.rs:DefaultTracing::start]`)**: Installs `tikv_jemallocator::Jemalloc` as the global allocator, installs `color_eyre` hooks, registers the global `tracing_subscriber` registry (and optional `console_subscriber`), and leaks `DefaultTracing` and `FirestoreProject` allocations for the lifetime of the process.
- **Network & Task IO (`[src/lib.rs:run]`, `[crates/emulator-grpc/src/lib.rs:FirestoreEmulator::batch_get_documents]`, `[crates/emulator-database/src/listener.rs:Listener::start]`)**: Binds a TCP socket with `TCP_NODELAY`, serves concurrent HTTP/1.1 and HTTP/2 streams, and spawns background Tokio tasks communicating over `mpsc` and `broadcast` channels for streaming document reads and real-time listeners.
- **In-Memory Database & Subscriber Mutation (`[crates/emulator-database/src/project.rs:FirestoreProject]`, `[crates/emulator-http/src/routes/logging.rs:TracingState]`)**: Mutates in-memory databases, collections, documents, and active transactions across gRPC and REST requests, and dynamically reloads `tracing_subscriber` layers when `/emulator/v1/logging` capture sessions are started or stopped.

## System Verification
- Run: `cargo test`
