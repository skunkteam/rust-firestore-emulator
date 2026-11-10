# crates

## Overview
Workspace crates directory containing the modular Rust libraries that power the Firestore emulator: in-memory database storage and query/pipeline execution, Tonic gRPC and Axum HTTP server interfaces, dynamic tracing and log capture, FIFO reader-writer concurrency primitives, and compiled Google Firestore v1 protobuf definitions.

## Subdirectories & Boundaries
- `emulator-database`: [./emulator-database/README.md](./emulator-database/README.md) - In-memory Cloud Firestore database engine (`FirestoreProject`, `FirestoreDatabase`), MVCC document storage, optimistic and pessimistic transactions, structured/aggregation queries, Enterprise Edition pipelines, and real-time change listeners.
- `emulator-grpc`: [./emulator-grpc/README.md](./emulator-grpc/README.md) - Tonic `FirestoreServer` gRPC implementation (`service`) for `google.firestore.v1.Firestore`, stream error wrapping (`error_in_stream`), and request validation macros.
- `emulator-http`: [./emulator-http/README.md](./emulator-http/README.md) - Axum HTTP `RouterBuilder`, `/emulator/v1` REST inspection and clearing routes, `/emulator/v1/logging` dynamic log capture routes, `/v1/projects` database configuration routes, and optional embedded web UI fallback.
- `emulator-tracing`: [./emulator-tracing/README.md](./emulator-tracing/README.md) - Global `tracing_subscriber` initialization (`DefaultTracing::start`), optional `console-subscriber` integration, and runtime-filtered in-memory log capture (`Tracing`, `DynamicSubscriber`).
- `fifo-rwlock`: [./fifo-rwlock/README.md](./fifo-rwlock/README.md) - Strictly FIFO asynchronous reader-writer lock (`FifoRwLock`) with priority-preserving read-to-write lock upgrades and atomic write-to-read lock downgrades.
- `googleapis`: [./googleapis/README.md](./googleapis/README.md) - Build-time `tonic-prost-build` protobuf compilation for Google Firestore v1 and Rust domain extensions for `Timestamp`, `TransactionOptions`, `Value`, and `LatLng`.
