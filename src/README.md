# firestore-emulator (`src`)

## Overview
Root binary and library entry point for the `firestore-emulator` crate. Initializes process-wide allocator, tracing, CLI configuration, and `FirestoreProject` state, and runs a unified Tokio/Hyper/Tonic TCP server that multiplexes incoming connections between the gRPC service (`emulator-grpc`) and the Axum HTTP/REST service (`emulator-http`) based on the request `content-type` header.

## Interface
- `[src/lib.rs:Result]`: `pub type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>` — Crate-level boxed error result alias returned by `run` and `run_with_shutdown_grace`.
- `[src/lib.rs:DEFAULT_SHUTDOWN_GRACE]`: `pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_millis(250)` — Default grace period given to active connections and streams to drain cleanly after `shutdown` resolves before forcing connection and stream abort.
- `[src/lib.rs:run]`: `pub async fn run(project: &'static FirestoreProject, listener: TcpListener, shutdown: impl Future<Output = ()> + Send + 'static, tracing: &'static impl Tracing) -> Result<()>` — Configures and runs the multiplexed HTTP/1.1 + HTTP/2 server on `listener` until `shutdown` resolves, delegating to `run_with_shutdown_grace` with `DEFAULT_SHUTDOWN_GRACE`.
- `[src/lib.rs:run_with_shutdown_grace]`: `pub async fn run_with_shutdown_grace(project: &'static FirestoreProject, listener: TcpListener, shutdown: impl Future<Output = ()> + Send + 'static, shutdown_grace: Duration, tracing: &'static impl Tracing) -> Result<()>` — Runs the multiplexed server via `hyper_util::server::conn::auto::Builder`, `GracefulShutdown`, and a connection `tokio::task::JoinSet`, bounding shutdown drain to `shutdown_grace`.
- `[src/lib.rs:ShutdownExecutor]`: Crate-private `hyper::rt::Executor` wrapping spawned HTTP/2 stream futures with `tokio::sync::watch` cancellation and an `mpsc::Sender<()>` drop guard so forced shutdown cancels blocked stream handlers and awaits destructor completion.
- `[src/main.rs:main]`: Binary entry point (`#[tokio::main] async fn main() -> Result<(), CliError>`) that starts `DefaultTracing`, parses CLI `Args`, leaks `FirestoreProject` for `'static` lifetime, binds `TcpListener`, and awaits `run` until `tokio::signal::ctrl_c`.
- `[src/main.rs:CliError]`: Binary-private CLI error wrapper formatting optional context, chained `Error::source` causes, and optional actionable suggestions via `Display`/`Debug`.
- `[Cargo.toml:features]`: Root crate features `default = ["ui"]` and `ui = ["emulator-http/ui"]` toggling embedded/disk Web UI serving on the HTTP service.
- `[src/main.rs:Args]`: `clap::Parser` CLI configuration struct supporting:
  - `--host-port` (`env = "FIRESTORE_EMULATOR_HOST"`): `SocketAddr` address to bind the TCP listener.
  - `--long-contention-timeout` (`env = "LONG_CONTENTION_TIMEOUT"`): `bool` flag enabling `Timeouts::CLOUD` (`15s` write / `16s` read) instead of the default `Timeouts::FAST` (`2s` write / `5s` read).
  - `--default-enterprise` (`env = "FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE"`): `bool` flag defaulting new databases to Enterprise Edition.
  - `--transaction-idle-timeout-ms` (`env = "FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS"`): optional `NonZeroU64` override (ms) for active transaction idle timeout (defaults to `None`, using Cloud's 60s idle / 120s used-minimum window).
  - `--transaction-max-lifetime-ms` (`env = "FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS"`): optional `NonZeroU64` override (ms) for active transaction maximum lifetime (defaults to `None`, using Cloud's 270s max lifetime).
- `[src/multiplex.rs:MultiplexLayer]` & `[src/multiplex.rs:MultiplexService]`: Crate-internal (`pub(crate)`) Tower `Layer` and `Service` that routes requests to an alternate service (`use_svc` / `other`) when predicate `when` returns `true`, or delegates to the inner `base` service otherwise.

## Invariants
- `[src/lib.rs:run]` & `[src/main.rs:main]`: Both `FirestoreProject` and the `Tracing` implementation must satisfy `'static` lifetimes (`main` enforces this via `Box::leak` on the allocated `FirestoreProject`).
- `[src/lib.rs:is_not_a_grpc_request]`: Requests are routed to `tonic::service::Routes` if and only if the `http::header::CONTENT_TYPE` header's media type (before optional `;` parameters, trimmed of ASCII whitespace) matches `application/grpc` or `application/grpc+<suffix>` ASCII case-insensitively; all other requests are routed via `MultiplexLayer` to the Axum REST router.
- `[src/lib.rs:run_with_shutdown_grace]`: **HTTP/2 Concurrency & Bounded Graceful Shutdown** — Sets `.http2().max_concurrent_streams(None)` on `auto::Builder` to preserve Tonic's unlimited concurrent stream limit (matching 450-listener conformance suites), drops `listener` immediately when `shutdown` resolves so reconnecting clients fail fast, exits right away when idle or once clients disconnect cleanly, clamps timers above `MAX_GRACE_TIMER` (1 year) to avoid Tokio timer overflow panics, and aborts remaining `JoinSet` connections plus `ShutdownExecutor`-tracked HTTP/2 stream tasks upon `shutdown_grace` expiry.
- `[src/multiplex.rs:MultiplexService]`: Routes requests via `tower::ServiceExt::oneshot` on a clone of the selected branch (`base` or `other`), so `poll_ready` is invoked exclusively on the chosen service and `Poll::Pending` / readiness errors propagate without panicking or holding unused readiness slots on the unchosen service.

## Side Effects
- `[src/main.rs:GLOBAL_ALLOC]`: Sets `tikv_jemallocator::Jemalloc` as the process-wide `#[global_allocator]` on Linux (`#[cfg(target_os = "linux")]`).
- `[src/main.rs:main]`: Initializes global tracing subscribers (`DefaultTracing::start()`), intentionally leaks `Box<FirestoreProject>` (`Box::leak`), registers an OS `ctrl_c` signal listener, and binds a TCP socket (`TcpListener::bind(host_port)`).
- `[src/lib.rs:run_with_shutdown_grace]`: Enables `TCP_NODELAY` on each accepted `TcpStream`, spawns per-connection tasks into `JoinSet` and per-HTTP/2-stream tasks via `ShutdownExecutor`, serves concurrent HTTP/1.1 and HTTP/2 network connections, and writes startup/shutdown status to `tracing::info!` or `stderr` (`eprintln!`).

## Verification
- Run: `cargo test`
- Run (Cloud Firestore conformance suite against built binary): `just sdk-test`
