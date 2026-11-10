# googleapis

## Overview
Rust crate that compiles the upstream Google Firestore v1 Protocol Buffers (`include/google/firestore/v1/firestore.proto`) into server-side `tonic`/`prost` types at build time and layers Firestore-specific Rust domain extensions, `serde` serialization attributes, and ordering/aggregation traits on top.

## Interface
- `[Cargo.toml:googleapis]`: Rust library crate exporting compiled Google Firestore v1 and protobuf well-known types along with domain extension traits and methods.
- `[build.rs:main]`: Cargo build script invoking `tonic_prost_build` to compile `include/google/firestore/v1/firestore.proto` into `googleapis.rs`.
- Subdirectories:
  - [`./src/README.md`](./src/README.md): Crate root module (`lib.rs`) and extension implementations (`ser.rs`, `timestamp_ext.rs`, `transaction_options_ext.rs`, `value_ext.rs`).

## Invariants
- `[build.rs:main]`: Disables gRPC client generation (`.build_client(false)`), compiles protobuf well-known types (`.compile_well_known_types(true)`), and maps `bytes_value` protobuf fields to `prost::bytes::Bytes` (`.bytes("bytes_value")`).
- `[build.rs:main]`: Attaches `#[derive(serde::Serialize)]` to `.google.type.LatLng`, `.google.protobuf.Timestamp` (with `PartialOrd, Ord`), `.google.firestore.v1.Document`, `.google.firestore.v1.ArrayValue`, `.google.firestore.v1.MapValue`, `.google.firestore.v1.Function`, `.google.firestore.v1.Pipeline`, and `.google.firestore.v1.Value`, wiring `crate::ser::as_base64` to `bytes_value` and `crate::ser::as_null` to `value_type.null_value`.
- `[Cargo.toml:lints.rust]`: Forbids unsafe code (`unsafe_code = "forbid"`) and explicit outlives requirements (`explicit_outlives_requirements = "forbid"`), and disables doctests (`[lib] doctest = false`) to accommodate upstream protobuf doc comments.

## Side Effects
- `[build.rs:main]`: Reads `.proto` definitions from the `include/` directory at build time and writes generated Rust files (`googleapis.rs`) to Cargo's `OUT_DIR`.

## Verification
- Run: `cargo test -p googleapis`
