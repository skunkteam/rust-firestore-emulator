# crates

## Overview
Workspace crates directory containing the modular Rust libraries and verification suites that power the Firestore emulator: in-memory database storage and query/pipeline execution, Tonic gRPC and Axum HTTP server interfaces, dynamic tracing and log capture, FIFO reader-writer concurrency primitives, compiled Google Firestore v1 protobuf definitions, and centralized black-box wire tests.

## Subdirectories & Boundaries
- `emulator-database`: [./emulator-database/README.md](./emulator-database/README.md) - In-memory Cloud Firestore database engine (`FirestoreProject`, `FirestoreDatabase`), MVCC document storage, optimistic and pessimistic transactions, structured/aggregation queries, Enterprise Edition pipelines, and real-time change listeners.
- `emulator-grpc`: [./emulator-grpc/README.md](./emulator-grpc/README.md) - Tonic `FirestoreServer` gRPC implementation (`service`) for `google.firestore.v1.Firestore`, stream error wrapping (`error_in_stream`), and request validation macros.
- `emulator-http`: [./emulator-http/README.md](./emulator-http/README.md) - Axum HTTP `RouterBuilder`, `/emulator/v1` REST inspection and clearing routes, `/emulator/v1/logging` dynamic log capture routes, `/v1/projects` database configuration routes, and optional embedded web UI fallback.
- `emulator-tests`: [./emulator-tests/README.md](./emulator-tests/README.md) - Single-target `#![cfg(test)]` black-box verification crate exercising `emulator-grpc`, `emulator-http`, `emulator-database`, `emulator-tracing`, and `firestore-emulator` over in-memory Tonic/Axum wire boundaries, `proptest` state-invariants, paused virtual-time contention schedules, and loopback TCP multiplexing.
- `emulator-tracing`: [./emulator-tracing/README.md](./emulator-tracing/README.md) - Idempotent global `tracing_subscriber` initialization (`DefaultTracing::start`), optional `console-subscriber` integration, and runtime-filtered in-memory log capture (`Tracing`, `DynamicSubscriber`).
- `fifo-rwlock`: [./fifo-rwlock/README.md](./fifo-rwlock/README.md) - Strictly FIFO asynchronous reader-writer lock (`FifoRwLock`) with priority-preserving read-to-write lock upgrades and atomic write-to-read lock downgrades.
- `googleapis`: [./googleapis/README.md](./googleapis/README.md) - Build-time `tonic-prost-build` protobuf compilation for Google Firestore v1 (with optional `client` feature), declarative `val!`/`fields!`/`doc!` macros, and Rust domain extensions for `Timestamp`, `TransactionOptions`, `Value`, and `LatLng`.
