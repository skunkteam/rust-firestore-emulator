use clap::Parser;
use emulator::FirestoreEmulator;
use googleapis::google::firestore::v1::firestore_server::FirestoreServer;
use std::net::SocketAddr;
use tonic::{codec::CompressionEncoding, transport::Server};
use tracing::info;

mod googleapis {
    tonic::include_proto!("googleapis");
}

mod database;
mod emulator;
#[macro_use]
mod utils;

const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

#[derive(Parser, Debug)]
struct Args {
    /// The host:port to which the emulator should be bound.
    #[arg(long, env = "FIRESTORE_EMULATOR_HOST")]
    host_port: SocketAddr,
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    #[cfg(any(feature = "tracing", feature = "console"))]
    {
        use tracing_subscriber::prelude::*;
        let registry = tracing_subscriber::registry();

        #[cfg(feature = "tracing")]
        let registry = registry.with({
            use time::{macros::format_description, UtcOffset};
            use tracing_subscriber::{
                fmt::{format::FmtSpan, time::OffsetTime},
                EnvFilter,
            };
            let time_offset = UtcOffset::current_local_offset()?;
            let time_format = format_description!("[hour]:[minute]:[second].[subsecond digits:6]");

            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_timer(OffsetTime::new(time_offset, time_format))
                .with_filter(EnvFilter::from_default_env())
        });

        #[cfg(feature = "console")]
        let registry = registry.with(console_subscriber::spawn());

        registry.init();
    }

    run(Args::parse())
}

#[tokio::main]
async fn run(Args { host_port }: Args) -> color_eyre::Result<()> {
    let emulator = FirestoreEmulator::default();
    let firestore = FirestoreServer::new(emulator)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_SIZE);

    let server = Server::builder().add_service(firestore).serve(host_port);

    info!("Firestore listening on {}", host_port);

    server.await?;

    Ok(())
}
