use std::net::SocketAddr;

use axum::Router;
use hybrid_axum_tonic::{NestTonic, RestGrpcService};
use tracing::info;

#[tokio::main]
pub async fn run(host_port: SocketAddr) -> color_eyre::Result<()> {
    let rest_router = emulator_ui::router();
    let grpc_router = Router::new().nest_tonic(emulator_grpc::service());
    let combined = RestGrpcService::new(rest_router, grpc_router).into_make_service();
    let server = axum::Server::bind(&host_port).serve(combined);

    info!("Firestore listening on {}", host_port);

    #[cfg(not(feature = "tracing"))]
    eprintln!("Firestore listening on {}", host_port);

    server.await?;

    Ok(())
}
