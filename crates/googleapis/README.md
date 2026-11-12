# googleapis

## Overview
Rust crate that compiles the upstream Google Firestore v1 Protocol Buffers (`include/google/firestore/v1/firestore.proto`) into server-side (and optional `client`-gated) `tonic`/`prost` types at build time and layers Firestore-specific Rust domain extensions, `From<T> for Value` conversions, declarative `val!`/`fields!`/`doc!` macros, `serde` ProtoJSON attributes, and ordering/aggregation traits on top.

## Interface
- `[Cargo.toml:googleapis]`: Rust library crate exporting compiled Google Firestore v1 and protobuf well-known types, declarative construction macros, and domain extension traits/methods. Defines optional Cargo feature `client = ["tonic/gzip"]` (`default = []`) to gate `FirestoreClient` generation for test/client consumers.
- `[build.rs:main]`: Cargo build script invoking `tonic_prost_build` to compile `include/google/firestore/v1/firestore.proto` into `googleapis.rs`.
- Subdirectories:
  - [`./src/README.md`](./src/README.md): Crate root module (`lib.rs`), declarative macros (`macros.rs`), and extension implementations (`ser.rs`, `timestamp_ext.rs`, `transaction_options_ext.rs`, `value_ext.rs`).

## Invariants
- `[build.rs:main]`: Generates gRPC client stubs only when `feature = "client"` is active (`.build_client(cfg!(feature = "client"))`), compiles protobuf well-known types (`.compile_well_known_types(true)`), and maps `bytes_value` protobuf fields to `prost::bytes::Bytes` (`.bytes("bytes_value")`).
- `[build.rs:main]`: Attaches `serde` attributes across Firestore v1 types and patches generated `Value` and `ValueType` definitions (`assert_eq!(..., 1)`) so custom `crate::ser` serializers/deserializers govern ProtoJSON wire formatting under both `--no-default-features` and `--all-features`.
- `[Cargo.toml:lints.rust]`: Forbids unsafe code (`unsafe_code = "forbid"`) and explicit outlives requirements (`explicit_outlives_requirements = "forbid"`), and disables doctests (`[lib] doctest = false`) to accommodate upstream protobuf doc comments.

## Side Effects
- `[build.rs:main]`: Reads `.proto` definitions from the `include/` directory at build time and writes generated Rust files (`googleapis.rs`) to Cargo's `OUT_DIR`.

## Verification
- Run: `cargo test -p googleapis`
