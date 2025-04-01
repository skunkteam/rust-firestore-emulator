use std::future::Future;

use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use http::header::CONTENT_TYPE;
use tokio::net::TcpListener;
use tonic::transport::{server::TcpIncoming, Server};
use tower::ServiceExt;
use tracing::{enabled, info, Level};

mod multiplex;

pub async fn run(
    project: &'static FirestoreProject,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    tracing: &'static impl Tracing,
) -> color_eyre::Result<()> {
    let rest_router = emulator_http::RouterBuilder::new(project)
        .add_dynamic_tracing(tracing)
        .build()
        // possible slight performance improvement by doing some prepare work early:
        .with_state(())
        // convert it into a service that is compatible with tonic's GRPC services:
        .into_service()
        .map_response(|r| r.map(tonic::body::Body::new))
        .map_err(Into::into);

    let server = Server::builder()
        .accept_http1(true)
        // simulator is only used on localhost, so TCP_NODELAY is a nobrainer
        .tcp_nodelay(true)
        // route to our Axum service if the request is not a GRPC request
        .layer(multiplex::MultiplexLayer {
            when:    is_not_a_grpc_request,
            use_svc: rest_router,
        })
        .add_service(emulator_grpc::service(project));

    if enabled!(Level::INFO) {
        info!("Firestore emulator listening on {}", listener.local_addr()?);
    } else {
        eprintln!("Firestore emulator listening on {}", listener.local_addr()?);
    }

    server
        .serve_with_incoming_shutdown(
            TcpIncoming::from(listener).with_nodelay(Some(true)),
            shutdown,
        )
        .await?;

    if enabled!(Level::INFO) {
        info!("Firestore emulator stopped");
    } else {
        eprintln!("Firestore emulator stopped");
    }

    Ok(())
}

fn is_not_a_grpc_request<B>(req: &http::Request<B>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .is_none_or(|content_type| !content_type.as_bytes().starts_with(b"application/grpc"))
}
