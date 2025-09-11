use std::net::SocketAddr;

use clap::Parser;
use color_eyre::{Section, eyre::Context};
use emulator_database::{FirestoreConfig, FirestoreProject};
use emulator_tracing::DefaultTracing;
use firestore_emulator::run;
use tikv_jemallocator::Jemalloc;
use tokio::{net::TcpListener, signal::ctrl_c};

#[global_allocator]
static GLOBAL_ALLOC: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The host:port to which the emulator should be bound.
    #[arg(long, env = "FIRESTORE_EMULATOR_HOST")]
    host_port: SocketAddr,

    /// Enable more accurate lock timeouts.
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. By default, in the emulator, this is reduced to 2 seconds for faster
    /// unit-tests. Enable this feature to simulate the Cloud Firestore more accurately.
    #[arg(long, env)]
    long_contention_timeout: bool,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let tracing = DefaultTracing::start();

    let Args {
        host_port,
        long_contention_timeout,
    } = Args::parse();

    // Create a new Firestore Project.
    let project = Box::new(FirestoreProject::new(FirestoreConfig {
        long_contention_timeout,
    }));
    // Make it live for the remainder of the program's life. ('static)
    let project = Box::leak(project);

    let ctrl_c_listener = async { ctrl_c().await.expect("failed to listen for ctrl-c event") };

    let listener = TcpListener::bind(host_port)
        .await
        .wrap_err("Could not start listener on provided address")
        .suggestion("Use \"--host-port 127.0.0.1:0\" to listen to a random port on localhost.")?;
    run(project, listener, ctrl_c_listener, tracing).await
}
