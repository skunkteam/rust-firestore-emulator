#![allow(unused_crate_dependencies)]

use std::{fmt, net::SocketAddr};

use clap::Parser;
use emulator_database::{FirestoreConfig, FirestoreProject};
use emulator_tracing::DefaultTracing;
use firestore_emulator::run;
use tokio::{net::TcpListener, signal::ctrl_c};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL_ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct CliError {
    context:    Option<&'static str>,
    source:     Box<dyn std::error::Error + Send + Sync>,
    suggestion: Option<&'static str>,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = self.context {
            write!(f, "{ctx}: {}", self.source)?;
        } else {
            write!(f, "{}", self.source)?;
        }
        let mut current = self.source.source();
        while let Some(cause) = current {
            write!(f, "\n  Caused by: {cause}")?;
            current = cause.source();
        }
        if let Some(suggestion) = self.suggestion {
            write!(f, "\n\nSuggestion: {suggestion}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

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

    /// Default to Enterprise Edition for all new databases.
    ///
    /// The Firestore emulator can simulate both the Standard and Enterprise editions.
    /// Enable this flag to default new databases to the Enterprise edition.
    #[arg(long, env = "FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE")]
    default_enterprise: bool,
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let tracing = DefaultTracing::start();

    let Args {
        host_port,
        long_contention_timeout,
        default_enterprise,
    } = Args::parse();

    // Create a new Firestore Project.
    let project = Box::new(FirestoreProject::new(FirestoreConfig {
        long_contention_timeout,
        default_enterprise,
    }));
    // Make it live for the remainder of the program's life. ('static)
    let project = Box::leak(project);

    let ctrl_c_listener = async { ctrl_c().await.expect("failed to listen for ctrl-c event") };

    let listener = TcpListener::bind(host_port).await.map_err(|err| CliError {
        context:    Some("Could not start listener on provided address"),
        source:     Box::new(err),
        suggestion: Some(
            "Use \"--host-port 127.0.0.1:0\" to listen to a random port on localhost.",
        ),
    })?;
    run(project, listener, ctrl_c_listener, tracing)
        .await
        .map_err(|source| CliError {
            context: None,
            source,
            suggestion: None,
        })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fmt, io};

    use clap::Parser;

    use super::{Args, CliError};

    #[derive(Debug)]
    struct OuterError(io::Error);

    impl fmt::Display for OuterError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("outer failure")
        }
    }

    impl Error for OuterError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn cli_error_formats_context_causes_and_suggestion() {
        let with_ctx = CliError {
            context:    Some("Could not start listener on provided address"),
            source:     Box::new(OuterError(io::Error::new(
                io::ErrorKind::AddrInUse,
                "address in use",
            ))),
            suggestion: Some("Use \"--host-port 127.0.0.1:0\"."),
        };
        let expected = concat!(
            "Could not start listener on provided address: outer failure\n",
            "  Caused by: address in use\n\n",
            "Suggestion: Use \"--host-port 127.0.0.1:0\"."
        );
        assert_eq!(with_ctx.to_string(), expected);
        assert_eq!(format!("{with_ctx:?}"), expected);

        let bare = CliError {
            context:    None,
            source:     Box::new(io::Error::other("server transport failed")),
            suggestion: None,
        };
        assert_eq!(bare.to_string(), "server transport failed");
        assert_eq!(format!("{bare:?}"), "server transport failed");
    }

    #[test]
    fn args_parse_cli_flags() {
        let args = Args::try_parse_from([
            "firestore-emulator",
            "--host-port",
            "127.0.0.1:8085",
            "--long-contention-timeout",
            "--default-enterprise",
        ])
        .unwrap();
        assert_eq!(args.host_port, "127.0.0.1:8085".parse().unwrap());
        assert!(args.long_contention_timeout);
        assert!(args.default_enterprise);
    }
}
