use std::{future::Future, net::SocketAddr};

use axum::Router;
use firestore_database::FirestoreProject;
use hybrid_axum_tonic::{NestTonic, RestGrpcService};
use tracing::{enabled, info, Level};

#[tokio::main]
pub async fn run(
    project: &'static FirestoreProject,
    host_port: SocketAddr,
    shutdown: impl Future<Output = ()>,
) -> color_eyre::Result<()> {
    let rest_router = emulator_http::router(project);
    let grpc_router = Router::new().nest_tonic(emulator_grpc::service(project));
    let combined = RestGrpcService::new(rest_router, grpc_router).into_make_service();
    let server = axum::Server::bind(&host_port)
        .serve(combined)
        .with_graceful_shutdown(shutdown);

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
