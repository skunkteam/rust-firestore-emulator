mod googleapis {
    tonic::include_proto!("googleapis");
}
mod emulator;

use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use std::env;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;

    Server::builder()
        .add_service(FirestoreServer::new(FirestoreEmulator::default()))
        .serve(env::var("FIRESTORE_EMULATOR_HOST")?.parse()?)
        .await?;

    Ok(())
}
