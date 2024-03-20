use std::net::SocketAddr;

use clap::Parser;
use firestore_database::{FirestoreConfig, FirestoreProject};
use firestore_emulator::run;
use tikv_jemallocator::Jemalloc;
use tokio::signal::ctrl_c;

#[global_allocator]
static GLOBAL_ALLOC: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
struct Args {
    /// The host:port to which the emulator should be bound.
    #[arg(long, env = "FIRESTORE_EMULATOR_HOST")]
    host_port: SocketAddr,

    /// Enable more accurate lock timeouts.
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. By default, in the emulator, this is reduced to 1 second for faster unit-tests.
    /// Enable this feature to simulate the Cloud Firestore more accurately.
    #[arg(long, env)]
    long_contention_timeout: bool,
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

    run(project, host_port, ctrl_c_listener)
}
