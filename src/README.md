# firestore-emulator (`src`)

## Overview
Root binary and library entry point for the `firestore-emulator` crate. Initializes process-wide allocator, tracing, CLI configuration, and `FirestoreProject` state, and runs a unified Tokio/Tonic TCP server that multiplexes incoming connections between the gRPC service (`emulator-grpc`) and the Axum HTTP/REST service (`emulator-http`) based on the request `content-type` header.

## Interface
- `[src/lib.rs:run]`: `pub async fn run(project: &'static FirestoreProject, listener: TcpListener, shutdown: impl Future<Output = ()> + Send + 'static, tracing: &'static impl Tracing) -> color_eyre::Result<()>` — Configures and runs the multiplexed HTTP/1.1 + HTTP/2 Tonic and Axum server on `listener` until `shutdown` resolves.
- `[src/main.rs:main]`: Binary entry point (`#[tokio::main] async fn main() -> color_eyre::Result<()>`) that installs `color_eyre`, starts `DefaultTracing`, parses CLI `Args`, leaks `FirestoreProject` for `'static` lifetime, binds `TcpListener`, and awaits `run` until `tokio::signal::ctrl_c`.
- `[src/main.rs:Args]`: `clap::Parser` CLI configuration struct supporting:
  - `--host-port` (`env = "FIRESTORE_EMULATOR_HOST"`): `SocketAddr` address to bind the TCP listener.
  - `--long-contention-timeout` (`env = "LONG_CONTENTION_TIMEOUT"`): `bool` flag enabling 15s lock contention timeouts instead of the default 2s.
  - `--default-enterprise` (`env = "FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE"`): `bool` flag defaulting new databases to Enterprise Edition.
- `[src/multiplex.rs:MultiplexLayer]` & `[src/multiplex.rs:MultiplexService]`: Crate-internal (`pub(crate)`) Tower `Layer` and `Service` that routes requests to an alternate service (`use_svc` / `other`) when predicate `when` returns `true`, or delegates to the inner `base` service otherwise.

## Invariants
- `[src/lib.rs:run]` & `[src/main.rs:main]`: Both `FirestoreProject` and the `Tracing` implementation must satisfy `'static` lifetimes (`main` enforces this via `Box::leak` on the allocated `FirestoreProject`).
- `[src/lib.rs:is_not_a_grpc_request]`: Requests are routed to the Tonic gRPC service if and only if the `http::header::CONTENT_TYPE` header is present and starts with `b"application/grpc"`; all other requests are routed via `MultiplexLayer` to the Axum REST router.
- `[src/multiplex.rs:MultiplexService::poll_ready]`: Both wrapped services (`self.base` and `self.other`) must be immediately ready when polled (`assert!(self.base.poll_ready(cx).is_ready())` and `assert!(self.other.poll_ready(cx).is_ready())`); `poll_ready` panics if either service is not immediately ready.

## Side Effects
- `[src/main.rs:GLOBAL_ALLOC]`: Sets `tikv_jemallocator::Jemalloc` as the process-wide `#[global_allocator]`.
- `[src/main.rs:main]`: Installs global `color_eyre` panic/error hooks (`color_eyre::install()`), initializes global tracing subscribers (`DefaultTracing::start()`), intentionally leaks `Box<FirestoreProject>` (`Box::leak`), registers an OS `ctrl_c` signal listener, and binds a TCP socket (`TcpListener::bind(host_port)`).
- `[src/lib.rs:run]`: Enables `TCP_NODELAY` on the server and incoming TCP streams (`TcpIncoming::from(listener).with_nodelay(Some(true))`), accepts HTTP/1.1 and HTTP/2 network connections, and writes startup/shutdown status to `tracing::info!` or `stderr` (`eprintln!`).

## Verification
- Run: `cargo test`
