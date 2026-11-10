# emulator-grpc

## Overview
Crate root for `emulator-grpc` (`[Cargo.toml:emulator-grpc]`), providing the Tonic gRPC server implementation over `emulator-database` and `googleapis` for the Firestore emulator.

## Subdirectories & Boundaries
- `src`: [./src/README.md](./src/README.md) - Tonic `FirestoreServer` implementation (`service`), `Firestore` trait handlers, stream error wrapper (`error_in_stream`), and validation macros.
