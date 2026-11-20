#![allow(unused_crate_dependencies)]

use std::{future::Future, io, time::Duration};

use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use http::header::CONTENT_TYPE;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::{conn::auto, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinSet,
};
use tower::{ServiceBuilder, ServiceExt};
use tracing::{Level, debug, enabled, error, info};

mod multiplex;

pub type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

/// Default grace period given to active connections and streams to finish cleanly after a
/// shutdown signal before aborting remaining tasks and closing connections.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_millis(250);

/// Runs the multiplexed HTTP/1.1 + HTTP/2 Firestore emulator server until `shutdown` resolves,
/// allowing [`DEFAULT_SHUTDOWN_GRACE`] for active connections and streams to drain cleanly.
pub async fn run(
    project: &'static FirestoreProject,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    tracing: &'static impl Tracing,
) -> Result<()> {
    run_with_shutdown_grace(project, listener, shutdown, DEFAULT_SHUTDOWN_GRACE, tracing).await
}

/// Runs the multiplexed HTTP/1.1 + HTTP/2 Firestore emulator server with an explicit
/// `shutdown_grace` window before aborting remaining active connections and stream tasks.
pub async fn run_with_shutdown_grace(
    project: &'static FirestoreProject,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    shutdown_grace: Duration,
    tracing: &'static impl Tracing,
) -> Result<()> {
    let rest_router = emulator_http::RouterBuilder::new(project)
        .add_dynamic_tracing(tracing)
        .build()
        // possible slight performance improvement by doing some prepare work early:
        .with_state(())
        // convert it into a service that is compatible with tonic's GRPC services:
        .into_service()
        .map_response(|r| r.map(tonic::body::Body::new));

    let grpc_routes = tonic::service::Routes::new(emulator_grpc::service(project)).prepare();

    let service = TowerToHyperService::new(
        ServiceBuilder::new()
            .map_request(|req: http::Request<hyper::body::Incoming>| {
                req.map(tonic::body::Body::new)
            })
            // route to our Axum service if the request is not a GRPC request
            .layer(multiplex::MultiplexLayer {
                when:    is_not_a_grpc_request,
                use_svc: rest_router,
            })
            .service(grpc_routes),
    );

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (stream_guard_tx, mut stream_guard_rx) = mpsc::channel(1);
    let executor = ShutdownExecutor {
        cancel_rx,
        guard: stream_guard_tx,
    };

    let mut builder = auto::Builder::new(executor);
    builder.http1().timer(TokioTimer::new());
    // Match Tonic's unlimited concurrent streams per HTTP/2 connection (the conformance suite opens
    // 450+ concurrent Listen streams over a single channel, while hyper-util defaults to 200).
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(None);

    if enabled!(Level::INFO) {
        info!("Firestore emulator listening on {}", listener.local_addr()?);
    } else {
        eprintln!("Firestore emulator listening on {}", listener.local_addr()?);
    }

    let graceful = GracefulShutdown::new();
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            Some(res) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(err) = res && err.is_panic() {
                    error!("connection task panicked: {err}");
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(err) if is_transient_accept_error(&err) => {
                        debug!("transient accept error: {err}");
                        continue;
                    }
                    Err(err) => {
                        error!("TCP accept error: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                // simulator is only used on localhost, so TCP_NODELAY is a nobrainer
                if let Err(err) = stream.set_nodelay(true) {
                    debug!("failed to set TCP_NODELAY: {err}");
                }
                let conn = builder
                    .serve_connection(TokioIo::new(stream), service.clone())
                    .into_owned();
                let watched = graceful.watch(conn);
                tasks.spawn(async move {
                    if let Err(err) = watched.await {
                        debug!("connection terminated: {err}");
                    }
                });
            }
        }
    }

    // Stop listening immediately so reconnecting clients (e.g. Web UI EventSource) fail fast.
    drop(listener);

    let graceful_drain = async {
        graceful.shutdown().await;
        while let Some(res) = tasks.join_next().await {
            if let Err(err) = res
                && err.is_panic()
            {
                error!("connection task panicked during shutdown: {err}");
            }
        }
    };

    // Tokio's timer wheel rejects multi-decade durations; any huge grace waits cleanly without a
    // timer instead of panicking.
    const MAX_GRACE_TIMER: Duration = Duration::from_secs(365 * 86_400);
    let force_closed = if shutdown_grace <= MAX_GRACE_TIMER
        && let Some(deadline) = tokio::time::Instant::now().checked_add(shutdown_grace)
    {
        tokio::time::timeout_at(deadline, graceful_drain)
            .await
            .is_err()
    } else {
        graceful_drain.await;
        false
    };

    if force_closed {
        tasks.abort_all();
        while let Some(res) = tasks.join_next().await {
            if let Err(err) = res
                && err.is_panic()
            {
                error!("connection task panicked during forced shutdown: {err}");
            }
        }
        let _ = cancel_tx.send(true);
    }

    // Drop our local executor/service clones so `stream_guard_rx` drains once spawned HTTP/2
    // stream tasks finish unwinding and running destructors.
    drop(builder);
    drop(service);
    drop(tokio::time::timeout(Duration::from_millis(200), stream_guard_rx.recv()).await);

    if enabled!(Level::INFO) {
        info!("Firestore emulator stopped");
    } else {
        eprintln!("Firestore emulator stopped");
    }

    Ok(())
}

#[derive(Clone)]
struct ShutdownExecutor {
    cancel_rx: watch::Receiver<bool>,
    guard:     mpsc::Sender<()>,
}

impl<F> hyper::rt::Executor<F> for ShutdownExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        let mut cancel_rx = self.cancel_rx.clone();
        let _guard = self.guard.clone();
        tokio::spawn(async move {
            if *cancel_rx.borrow_and_update() {
                return;
            }
            tokio::select! {
                biased;
                res = cancel_rx.changed() => {
                    drop(res);
                }
                res = fut => {
                    drop(res);
                }
            }
        });
    }
}

fn is_transient_accept_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

fn is_not_a_grpc_request<B>(req: &http::Request<B>) -> bool {
    !is_grpc_request(req)
}

fn is_grpc_request<B>(req: &http::Request<B>) -> bool {
    let Some(content_type) = req.headers().get(CONTENT_TYPE) else {
        return false;
    };
    let bytes = content_type.as_bytes();
    let media_type = bytes
        .split(|&b| b == b';')
        .next()
        .unwrap_or(bytes)
        .trim_ascii();
    let prefix = b"application/grpc";
    if media_type.len() < prefix.len() || !media_type[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return false;
    }
    match media_type.get(prefix.len()) {
        None => true,
        Some(b'+') => media_type.len() > prefix.len() + 1,
        Some(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, Request, header::CONTENT_TYPE};

    use super::is_not_a_grpc_request;

    fn req_with_content_type(ct: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(ct) = ct {
            req.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_str(ct).unwrap());
        }
        req
    }

    #[test]
    fn detects_grpc_content_type_case_insensitively_with_delimiters() {
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "Application/Grpc"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "APPLICATION/GRPC+proto"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "Application/Grpc; charset=utf-8"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            " application/grpc+json ; charset=utf-8"
        ))));

        assert!(is_not_a_grpc_request(&req_with_content_type(None)));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(""))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc+"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc-web"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpcfoo"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/json"
        ))));
    }
}
