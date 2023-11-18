mod googleapis {
    tonic::include_proto!("googleapis");
}
mod emulator;

use clap::Parser;
use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use std::net::SocketAddr;
use tonic::{codec::CompressionEncoding, transport::Server};

#[derive(Parser, Debug)]
struct Args {
    /// The host:port to which the emulator should be bound.
    #[arg(long, env = "FIRESTORE_EMULATOR_HOST")]
    host_port: SocketAddr,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let Args { host_port } = Args::parse();

    let emulator = FirestoreEmulator::default();
    let firestore = FirestoreServer::new(emulator)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(usize::MAX);

    let server = Server::builder().add_service(firestore).serve(host_port);

    eprintln!("Firestore listening on {}", host_port);

    server.await?;

    Ok(())
}
