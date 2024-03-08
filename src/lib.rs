use std::net::SocketAddr;

use axum::{routing::get, Router};
use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use hybrid_axum_tonic::{NestTonic, RestGrpcService};
use tonic::codec::CompressionEncoding;
use tracing::info;

mod emulator;
#[macro_use]
mod utils;

const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

#[tokio::main]
pub async fn run(host_port: SocketAddr) -> color_eyre::Result<()> {
    let emulator = FirestoreEmulator::new();
    let firestore = FirestoreServer::new(emulator)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_SIZE);

    let grpc_router = Router::new().nest_tonic(firestore);
    let rest_router = Router::new().route("/", get(|| async { "Hello world!" }));

    let service = RestGrpcService::new(rest_router, grpc_router);

    let server = axum::Server::bind(&host_port).serve(service.into_make_service());

    info!("Firestore listening on {}", host_port);

    server.await?;

    Ok(())
}
