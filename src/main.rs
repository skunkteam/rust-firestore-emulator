mod googleapis {
    tonic::include_proto!("googleapis");
}
mod emulator;

use clap::Parser;
use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use std::net::SocketAddr;
use tonic::transport::Server;

#[derive(Parser, Debug)]
struct Args {
    /// The host:port to which the emulator should be bound.
    #[arg(long, env = "FIRESTORE_EMULATOR_HOST")]
    host_port: SocketAddr,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::parse();

    let server = Server::builder()
        .add_service(FirestoreServer::new(FirestoreEmulator::default()))
        .serve(args.host_port);

    eprintln!("Firestore Emulator started");

    server.await?;

    Ok(())
}
