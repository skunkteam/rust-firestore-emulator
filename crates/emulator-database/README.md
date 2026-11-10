# emulator-database

## Overview
In-memory Cloud Firestore database engine crate (`emulator-database`) providing project and database lifecycle management, MVCC document storage, optimistic and pessimistic transactions, structured and aggregation queries, Enterprise Edition pipelines, and real-time change listeners.

## Subdirectories & Boundaries
- `src`: [./src/README.md](./src/README.md) - Core library root implementing `FirestoreProject`, `FirestoreDatabase`, real-time `Listener` streams, configuration, error types, and database submodules.
