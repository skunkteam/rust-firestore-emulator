use std::net::SocketAddr;

use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use tonic::{codec::CompressionEncoding, transport::Server};
use tracing::info;

mod database;
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

    let server = Server::builder().add_service(firestore).serve(host_port);

    info!("Firestore listening on {}", host_port);

    server.await?;

    Ok(())
}
