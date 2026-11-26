# Firestore Emulator Blueprint

A local, Rust-based Google Cloud Firestore emulator built with a focus on stability, performance, and exact cloud-compliance for extensive unit testing and CI environments.

## System Overview

This repository provides a Rust alternative to the official Java-based Firebase emulator for Firestore. It acts as an in-memory backend for running large, concurrent test suites while aligning strictly with Cloud Firestore's concurrency, ordering, query, and transaction semantics. At runtime, the root binary (`src/`) binds a single TCP listener that multiplexes incoming HTTP/2 `application/grpc` requests to the Tonic gRPC service (`crates/emulator-grpc`) and all other HTTP/1.1 or HTTP/2 requests to the Axum REST and Web UI router (`crates/emulator-http` and `ui/`), both backed by shared `'static` `FirestoreProject` state (`crates/emulator-database`) and dynamic tracing (`crates/emulator-tracing`).

### Key Characteristics

- **Strict Concurrency Simulation**: Uses a zero-`unsafe` Wound-Wait transaction lock manager ([`crates/emulator-database/src/database/lock/`](./crates/emulator-database/src/database/lock/README.md)) with lazy first-lock priority, deterministic cross-document wounding, read-to-write lock upgrades with contention detection, and multi-document `WriteBatch` retries to emulate Cloud Firestore's pessimistic locking and contention behavior.
- **Driven by Need**: Exceptionally complete for core Firestore workflows (collections, documents, structured queries, aggregations, `VectorValue` embeddings and `findNearest` vector search, Enterprise Edition pipelines, real-time listeners, and pessimistic/optimistic transactions), plus versioned `.fsemu` binary state export and import via HTTP (`/emulator/v1/export`, `/emulator/v1/import`), the Web UI, and CLI startup preloading (`--import`).
- **Cloud-Compliant Behavior**: Aligns with actual Google Cloud Firestore behavior rather than Java emulator edge cases, continuously verified by the `@google-cloud/firestore` conformance suite in [`test-suite/`](./test-suite/README.md).

### Missing Features

Because the implementation is driven by consumed features, several advanced or out-of-scope features remain unimplemented. Unimplemented gRPC and database features use custom crate macros (not panicking `std::unimplemented!`) that log a warning and return structured `UNIMPLEMENTED` (`gRPC 12` / `HTTP 501`) errors:

- **Enterprise Edition pipelines (partial support)**: 8 pipeline stages remain unimplemented (`collectionGroup`, `database`, `documents`, `createFrom`, `replaceWith`, `sample`, `union`, `unnest`), along with ~70% of scalar/logic/search expressions, 6 of 12 aggregate accumulators (`countIf`, `countDistinct`, `first`, `last`, `arrayAgg`, `arrayAggDistinct`), `VariableReferenceValue`/`PipelineValue`, and `auto_commit_transaction`.
- **REST API Phase-2 endpoints (`501 UNIMPLEMENTED`)**: 7 custom verbs (`:commit`, `:batchGet`, `:batchWrite`, `:beginTransaction`, `:rollback`, `:executePipeline`, `:partitionQuery`), transactional reads/`:runQuery`/`:runAggregationQuery`, REST `findNearest`, REST `:listCollectionIds` with `readTime`, and Admin API `firestoreDataAccessMode`/`mongodbCompatibleDataAccessMode`/`realtimeUpdatesMode` fields.
- **No Firebase Web/Mobile client SDK (`firebase/firestore`) support**: The bidirectional `Write` gRPC stream is unimplemented, and `Listen` enforces a single active target (`target_id == 1`, a single document in `DocumentsTarget`, no `RemoveTarget`, and no `once`/`expected_count` options).
- **Non-goals / out-of-scope Cloud features**: No Security Rules or Authentication emulation, no automatic background on-disk persistence (state transfer uses explicit HTTP/UI/CLI `--import` `.fsemu` snapshots), and no Standard Edition composite index enforcement.
- **Other unimplemented gRPC / database options**: `Maximum` and `Minimum` field transforms (and standalone `Write.operation = Transform`), `PartitionQuery`, `explain_options`, `request_options`, `TransactionOptions.read_write.concurrency_mode`, and `ListDocuments` with `show_missing` when `collection_id` is empty.

### Usage & Development Workflow

#### Prerequisites

When building from source, initialize the `crates/googleapis/include` protobuf submodule (`git clone --recursive` or `git submodule update --init`) and install `protoc`:

```shell
brew install protobuf
```

For executing the test-suite and development tools, `just` and `node` are recommended:

```shell
brew install just node
```

#### Installation & Standalone Server

Download the prebuilt Linux x86_64 headless GitHub Release tarball (compiled with `--no-default-features` for CI runners):

```shell
curl -fsSL https://github.com/skunkteam/rust-firestore-emulator/releases/latest/download/firestore-emulator.tar.gz | tar -xz
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 ./firestore-emulator
```

Or run directly with Nix (includes the default `ui` Web UI feature):

```shell
nix run github:skunkteam/rust-firestore-emulator -- --host-port 127.0.0.1:8080
```

Or install via Cargo (requires `protoc`; includes the default `ui` feature for local use):

```shell
# From GitHub:
cargo install --git https://github.com/skunkteam/rust-firestore-emulator

# Or from a local checkout (after `git submodule update --init`):
cargo install --path .

FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 firestore-emulator
```

Run locally from source in debug or release mode (optionally preloading a saved `.fsemu` snapshot before binding the TCP socket):

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run -- --import checkpoint.fsemu
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 FIRESTORE_EMULATOR_IMPORT=checkpoint.fsemu cargo run --release
```

#### CLI Flags & Environment Variables

| CLI Flag | Environment Variable | Description |
| :--- | :--- | :--- |
| `--host-port <HOST:PORT>` | `FIRESTORE_EMULATOR_HOST` | Required `SocketAddr` to bind the multiplexed gRPC/HTTP listener (use `127.0.0.1:0` for an ephemeral port). |
| `--long-contention-timeout` | `FIRESTORE_EMULATOR_LONG_CONTENTION_TIMEOUT` | Use Cloud-like lock contention timeouts (`15s` write / `16s` read) instead of fast test defaults (`2s` write / `5s` read). |
| `--default-enterprise` | `FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE` | Default newly created databases (after startup) to Enterprise Edition (optimistic concurrency) instead of Standard Edition. |
| `--import <PATH>` | `FIRESTORE_EMULATOR_IMPORT` | Preload a `.fsemu` binary snapshot before binding the listening TCP socket (retaining each database's snapshot edition and concurrency settings). |
| `--transaction-idle-timeout-ms <MS>` | `FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS` | Optional positive override (ms) for active transaction idle timeout (defaults to `60s` unused / `120s` used minimum). |
| `--transaction-max-lifetime-ms <MS>` | `FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS` | Optional positive override (ms) for active transaction maximum lifetime (defaults to `270s`). |

Additional runtime environment variables: `RUST_LOG` controls tracing verbosity (e.g. `RUST_LOG=info`), and in debug builds with the `ui` feature enabled, `FIRESTORE_EMULATOR_UI_DIR` overrides the on-disk `ui/` asset directory.

#### Building and Testing

The full suite of CI checks (`fmt-check`, `lint` [Clippy + `sdk-lint` + `ui-check`], `test` [default features + `--no-default-features` + doctests], `ui-test` [`node --test ui/tests/*.test.mjs`], and the `test-suite/` SDK conformance suite via `sdk-test` against a freshly built Rust emulator) can be executed using:

```shell
just
```

To run `just` in watch-mode, automatically re-running on Rust file modifications (`cargo watch` ignores `test-suite/`, `tmp/`, and `lcov.info`):

```shell
just watch
```

#### Cloud Firestore Conformance Suite (`test-suite/`) & Development Rule

[`test-suite/`](./test-suite/README.md) is the authoritative conformance specification ensuring this emulator matches **production Google Cloud Firestore** across Standard Edition (pessimistic), Enterprise Edition (optimistic), and Enterprise Edition (pessimistic).

**Mandatory rule for features and bugfixes**: Every new Firestore feature (including lifting `notImplementedInRust` guards) and every behavioral bugfix must add or update conformance tests in [`test-suite/tests/`](./test-suite/tests/README.md). Assertions must reflect real Cloud Firestore behavior rather than emulator quirks.

The conformance suite supports three interchangeable targets:

1. **Google Cloud Firestore (ground truth — requires GCP authentication and unset `FIRESTORE_EMULATOR_HOST`)**:

```shell
cd test-suite/
npm test
```

2. **Rust Emulator (via `just` with build step, or directly in `test-suite/` without build step)**:

```shell
# Recommended from repository root: compiles `cargo build --no-default-features` first, then runs `npm run test:rust-emulator`
just sdk-test
```

```shell
# Direct run inside test-suite/ — NOTE: starts `../target/debug/firestore-emulator` directly WITHOUT compiling!
# Always run `cargo build` (or `npm run build:rust`) first after changing Rust code.
cd test-suite/
npm run test:rust-emulator
```

3. **Google Java-based Firebase Emulator (for behavioral comparison via `firebase-tools`)**:

```shell
cd test-suite/
npm run test:java-emulator
```

## Subdirectories & System Boundaries
- `crates`: [./crates/README.md](./crates/README.md) - Workspace crates providing the database engine and `.fsemu` snapshot codec (`emulator-database`), gRPC server (`emulator-grpc`), HTTP/REST and `/emulator/v1` snapshot control-plane server (`emulator-http`), dynamic tracing (`emulator-tracing`), compiled Firestore v1 protobufs (`googleapis`), and centralized black-box wire test suite (`emulator-tests`).
- `docs`: [./docs/designs/](./docs/designs/) - Architectural design specifications (`black-box-rust-testing-foundation.md`, `emulator-state-import-export.md`, `emulator-web-ui.md`, `query-runner-autosuggestions-and-aggregations.md`, `structured-query-filters-and-validation.md`, `value-partial-eq.md`, `vector-search.md`, and `wound-wait-transaction-locking.md`).
- `src`: [./src/README.md](./src/README.md) - Root binary (`main.rs`) and server entrypoint (`lib.rs`, `multiplex.rs`) that initialize CLI options (including pre-bind `--import` snapshot loading), Jemalloc (on Linux), `DefaultTracing`, and `'static FirestoreProject` state, and multiplex gRPC and HTTP traffic over a single TCP listener.
- `test-suite`: [./test-suite/README.md](./test-suite/README.md) - TypeScript/Jest `@google-cloud/firestore` conformance test suite verifying exact behavioral parity against live Google Cloud Firestore (`npm test`), the local Rust emulator (`just sdk-test` / `npm run test:rust-emulator`), and Google's Java-based Firebase emulator (`npm run test:java-emulator`).
- `ui`: [./ui/README.md](./ui/README.md) - Zero-build multi-file ES-module web UI (`index.html`, `styles.css`, `lit-html.js`, `lit-html.d.ts`, `app.js`, `state.js`, `router.js`, `api.js`, `values.js`, `components/` including `combobox.js`, `contention.js`, `modals.js`, `query-model.js`, and `query-suggestions.js`, plus `tests/` run via `node --test`) served by `emulator-http` (under the root crate's default `ui` feature) for browsing, creating, editing, deleting, running document/aggregation queries with standard-REST autosuggestions, exporting, importing, and inspecting live Wound-Wait lock contention (`#263`) across emulated databases via `/v1/projects/...` and `/emulator/v1`.

## Global Invariants
- **Cloud Firestore Conformance Gate (`[test-suite/package.json:scripts]`, `[test-suite/tests/utils/firestore.ts:connection]`, `[justfile:default]`, `[.github/workflows/rust.yml:jobs.compat-test]`)**: Every new feature or bugfix affecting Firestore API, query, transaction, pipeline, or listener semantics must include or ungate (`notImplementedInRust`) conformance tests in `test-suite/tests/`, validated against Google Cloud Firestore (`npm test`) as ground truth and verified against a freshly built Rust emulator (`just sdk-test`; note that `npm run test:rust-emulator` spawns `../target/debug/firestore-emulator` without running `cargo build`).
- **Static Server State (`[src/lib.rs:run]`, `[src/main.rs:main]`)**: Both `FirestoreProject` and `DefaultTracing` are leaked into `'static` memory (`Box::leak` / `OnceLock`) once at startup and shared across `emulator-grpc` and `emulator-http` services.
- **Protocol Multiplexing (`[src/lib.rs:is_not_a_grpc_request]`)**: Incoming requests on the shared TCP listener are routed to the Tonic `FirestoreServer` if and only if the `content-type` media type (before optional `;` parameters, trimmed of ASCII whitespace) matches `application/grpc` or `application/grpc+<non-empty-suffix>` ASCII case-insensitively (`application/grpc+`, `application/grpc-web`, and `application/grpcfoo` route to Axum); all other requests route to the Axum HTTP `Router`.
- **Monotonic Timestamps & MVCC (`[crates/googleapis/src/timestamp_ext.rs:Timestamp::now]`, `[crates/emulator-database/src/database/document.rs:DocumentMeta::apply_sealed_commit]`, `[crates/emulator-database/src/database/document.rs:DocumentContents::maybe_add_version]`)**: `Timestamp::now()` is strictly monotonic across the process (rolling `999_999_999` nanoseconds cleanly over to `{ seconds + 1, nanos: 0 }`), `apply_sealed_commit` stamps a single `commit_time >= lease.acquire_time()` across all physical document locks inside one critical section, and `DocumentContents::maybe_add_version` enforces monotonically non-decreasing `update_time` values.
- **Wound-Wait Locking, Commit & Transaction Lifecycle (`[crates/emulator-database/src/database/lock/table.rs:LockTable]`, `[crates/emulator-database/src/database/commit.rs:FirestoreDatabase::commit]`, `[crates/emulator-database/src/database/transaction/registry.rs:RunningTransactions]`)**: Per-document read/write locks and `(seq, arrival)` waiter queues allocate transaction priority lazily on first lock acquisition, immediately wound younger conflicting active holders/waiters across documents (`0ms`), detect read-to-write upgrade contention (`ReadObservation`) under exclusive `WriteLease`s, route both transactional and non-transactional commits through unified `commit(writes, Option<TransactionId>)`, and manage 16-byte rotated `TransactionId` tokens across a 6-state `TxnSlot` lifecycle (`Active`, `Committing`, `Wounded`, `AbortedLineage`, `Expired`, `Closed`) with 4 isolated `4,096`-entry rings (`retry_ring`, `closed_ring`, `expired_rw_ring`, `expired_ro_ring`) and activity-aware transaction TTL (`60s` unused / `120s` used min / `270s` max) via lock-order-safe reaping (`LockSession::close_expired()` and `Arc<Transaction>` drops strictly outside `RwLock<RunningTxnMap>` and `Mutex<LockTable>`).
- **Edition & Concurrency Coupling (`[crates/emulator-database/src/database/pipeline/mod.rs:execute]`, `[crates/emulator-http/src/routes/projects/admin.rs:patch_database]`)**: Pipeline execution (`execute_pipeline`) requires Enterprise Edition (`enterprise_edition() == true`), and patching `database_edition` via `/v1/projects/{project_id}/databases/{database_id}` derives `optimistic_concurrency` from the new edition (`true` for Enterprise, `false` for Standard) only when the edition actually changes and `concurrency_mode` is not explicitly updated (both gated by `updateMask`); patching `databaseEdition` to the current edition preserves existing `optimistic_concurrency`.
- **Snapshot Format, Export/Import & Live-Client Cutover (`[crates/emulator-database/src/snapshot/README.md]`, `[crates/emulator-database/src/project.rs:FirestoreProject::prepare_export]`, `[crates/emulator-database/src/project.rs:FirestoreProject::import_snapshot]`, `[crates/emulator-http/src/routes/emulator.rs]`, `[src/main.rs:load_startup_snapshot]`)**: Versioned `.fsemu` v1 containers (`FSEMUSNP`, `FORMAT_MAJOR = 1`, length-delimited Proto3 frames capped at `64 MiB`, CRC-32 + `FSEMUEOF` trailer) preserve `DatabaseEdition` / `DatabaseConcurrencyMode` settings, empty databases, zero-field documents, subcollections under missing parent documents, and all 11 storable Firestore v1 `Value` types without MVCC history or original metadata timestamps (`import_time = Timestamp::now()`). Export streams queue-free point-in-time `read_mvcc_at(snapshot_time)` reads (`404 NOT_FOUND` without lazy creation for unknown `?database=` targets); single-pass streaming import validates headers, frames, paths, writable fields/timestamps, duplicates, and checksums into detached `PreparedDatabase` structs before acquiring `databases.write().await`, rejects concurrent imports via `import_lock` (`409 Conflict` / `ABORTED`), calls `old_db.mark_cleared()` before stamping `import_time`, emits `ProjectEvent::DatabaseReplaced` / `DatabaseCreated` (plus `ContentionSseEvent::DatabaseCleared`), converges compatible gRPC listeners in one reset, and terminates active `QueryTarget` listeners with `FAILED_PRECONDITION` when `databaseEdition` changes. Both `GET /emulator/v1/export` and `POST /emulator/v1/import` compile regardless of the `ui` feature, and `POST /emulator/v1/import` enforces `Content-Type: application/octet-stream` (`415 Unsupported Media Type`) without mounting a `CorsLayer`.
- **Safety Lints (`[Cargo.toml:workspace.lints.rust]`, `[crates/googleapis/Cargo.toml:lints.rust]`)**: Workspace-wide Rust lints forbid `unsafe_code` across all crates, forbid `explicit_outlives_requirements`, and forbid `non_ascii_idents` (`crates/googleapis` opts out of `[lints] workspace = true`, keeping `unsafe_code = "forbid"` and `explicit_outlives_requirements = "forbid"` while downgrading `non_ascii_idents = "warn"`).
- **CI vs. Local Build Features (`[Cargo.toml:features]`, `[.github/workflows/rust.yml:deploy]`)**: Local builds and installations (`cargo install`, `cargo run`, `flake.nix`) enable `default = ["ui"]` to serve the Web UI, whereas `.github/workflows/rust.yml` intentionally builds release tarballs with `--no-default-features` for headless CI runners.

## Global Side Effects
- **Process-Wide Runtime Initialization (`[src/main.rs:GLOBAL_ALLOC]`, `[src/main.rs:main]`, `[crates/emulator-tracing/src/lib.rs:DefaultTracing::start]`)**: Installs `tikv_jemallocator::Jemalloc` as the global allocator on Linux (`cfg(target_os = "linux")`), idempotently registers the global `tracing_subscriber` registry via `OnceLock`, and retains `DefaultTracing` and `FirestoreProject` allocations for the lifetime of the process.
- **Network & Task IO (`[src/lib.rs:run]`, `[crates/emulator-grpc/src/lib.rs:FirestoreEmulator::batch_get_documents]`, `[crates/emulator-database/src/listener.rs:Listener::start]`)**: Binds a TCP socket with `TCP_NODELAY`, serves concurrent HTTP/1.1 and HTTP/2 streams, and spawns background Tokio tasks communicating over `mpsc` and `broadcast` channels for streaming document reads and real-time listeners.
- **In-Memory Database & Subscriber Mutation (`[crates/emulator-database/src/project.rs:FirestoreProject]`, `[crates/emulator-http/src/routes/logging.rs:TracingState]`, `[src/main.rs:load_startup_snapshot]`)**: Mutates in-memory databases, collections, documents, and active transactions across gRPC, REST, `/emulator/v1/import`, and pre-bind CLI `--import` startup loading, emits `ProjectEvent` lifecycle notifications, and dynamically reloads `tracing_subscriber` layers when `/emulator/v1/logging` capture sessions are started or stopped.
- **Conformance Suite Process & Cloud IO (`[test-suite/package.json:scripts]`, `[test-suite/tests/utils/firestore.ts]`)**: `test-suite` spawns local Rust (`127.0.0.1:8080`) or Firebase Java emulator processes—or connects directly to GCP project `firestore-test-406503` when `FIRESTORE_EMULATOR_HOST` is unset—performing live gRPC/HTTP database configurations, reads, writes, listeners, pipelines, and `recursiveDelete` teardowns.

## System Verification
- **Full CI Parity Check (Formatting, Lints/Type-Checks, Tests, Web UI Tests & Rust Emulator Conformance)**: `just` (runs `fmt-check`, `lint`, `test`, `ui-test`, and `sdk-test` — matching `.github/workflows/rust.yml`, with automatic `test-suite/node_modules` syncing via `npm ci` when missing or stale).
- **Workspace Rust Tests (Required for Cross-Crate Black-Box Suites)**: `just test` (runs `cargo nextest run --workspace`, `cargo nextest run --workspace --exclude emulator-tests --no-default-features`, and `cargo test --doc --workspace --exclude googleapis`; running `-p emulator-database` or `-p emulator-grpc` alone skips the wire-level black-box suites in `crates/emulator-tests`; also verify shared-process execution via `cargo test --workspace` and CI profile via `NEXTEST_PROFILE=ci PROPTEST_CASES=128 just test`).
- **Line & Region Coverage**: `just cov` (`cargo llvm-cov nextest --workspace`, excluding generated Protobuf/UI files and `crates/emulator-tests/`).
- **Scoped Mutation Testing**: `just mutants <crate> <file>` (runs `cargo-mutants` with the `mutants` nextest profile and writes scratch artifacts exclusively inside `./tmp/mutants`).
- **Static Checks, Test-Suite Lint & Web UI Verification**: `just fmt` (auto-format), `just fmt-check`, `just lint` (Clippy `-D warnings`, `sdk-lint` [ESLint, Prettier, and `tsc` in `test-suite/`], and `ui-check` [JSDoc `tsc` type-checking of `ui/` and `ui/tests/` plus ESLint on `ui/tests/`]), and `just ui-test` (`node --test ui/tests/*.test.mjs`).
- **Node.js SDK Conformance (Rust Emulator)**: `just sdk-test` (automatically syncs `test-suite/node_modules` if needed, builds `--no-default-features`, and runs `test-suite` against `127.0.0.1:8080` with `--maxConcurrency=3` by default, overridable via `JEST_MAX_CONCURRENCY`, matching CI).
- **Node.js SDK Conformance (Ground-Truth Google Cloud Firestore)**: `cd test-suite && npm test`
- **Node.js SDK Conformance (Google's Java-based Firebase Emulator)**: `cd test-suite && npm run test:java-emulator`
- **Other Convenience Recipes (`justfile`)**: `just build [args..]` (`cargo build`), `just run [args..]` (`cargo run`), `just doc [crate]` (`cargo doc --no-deps`), and `just clean` (`cargo clean`).
