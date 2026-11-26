# emulator-database

## Overview
In-memory Cloud Firestore database engine crate (`emulator-database`) providing project and database lifecycle management, shared edition/concurrency settings, MVCC document storage, optimistic and pessimistic transactions, structured and aggregation queries, vector search (`findNearest` and distance metrics), Enterprise Edition pipelines, real-time change listeners, and the pure in-memory `.fsemu` binary snapshot export/import subsystem.

## Subdirectories & Boundaries
- `src`: [./src/README.md](./src/README.md) - Core library root implementing `FirestoreProject`, `FirestoreDatabase`, real-time `Listener` streams, configuration (`FirestoreConfig`, `DatabaseEdition`, `DatabaseConcurrencyMode`, `DatabaseSettings`), error types, database submodules, and the `snapshot/` module ([./src/snapshot/README.md](./src/snapshot/README.md)).
