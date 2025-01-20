use std::{future::Future, net::SocketAddr};

use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use hybrid_axum_tonic::GrpcMultiplexLayer;
use tonic::transport::Server;
use tower::ServiceExt;
use tracing::{enabled, info, Level};

#[tokio::main]
pub async fn run(
    project: &'static FirestoreProject,
    host_port: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
    tracing: &'static impl Tracing,
) -> color_eyre::Result<()> {
    let rest_router = emulator_http::RouterBuilder::new(project)
        .add_dynamic_tracing(tracing)
        .build()
        .into_service()
        .map_response(|r| r.map(tonic::body::boxed));
    let server = Server::builder()
        .accept_http1(true)
        .tcp_nodelay(true)
        .layer(GrpcMultiplexLayer::new(rest_router))
        .add_service(emulator_grpc::service(project))
        .serve_with_shutdown(host_port, shutdown);

    if enabled!(Level::INFO) {
        info!("Firestore emulator listening on {}", host_port);
    } else {
        eprintln!("Firestore emulator listening on {}", host_port);
    }

    server.await?;

    if enabled!(Level::INFO) {
        info!("Firestore emulator stopped");
    } else {
        eprintln!("Firestore emulator stopped");
    }

    Ok(())
}
