#![allow(unused_crate_dependencies)]

use std::{fmt, future::Future, net::SocketAddr, num::NonZeroU64, time::Duration};

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

    /// Optional override (in milliseconds) for the active transaction idle timeout.
    ///
    /// Defaults to Cloud Firestore's 60-second idle timeout when unset.
    #[arg(long, env = "FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS")]
    transaction_idle_timeout_ms: Option<NonZeroU64>,

    /// Optional override (in milliseconds) for the active transaction maximum lifetime.
    ///
    /// Defaults to Cloud Firestore's 270-second maximum transaction lifetime when unset.
    #[arg(long, env = "FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS")]
    transaction_max_lifetime_ms: Option<NonZeroU64>,
}

#[cfg(unix)]
fn shutdown_signal() -> Result<impl Future<Output = ()> + Send + 'static, CliError> {
    use tokio::signal::unix::{SignalKind, signal};

    // Register SIGTERM eagerly before `run(...)` prints "listening" to avoid a startup race
    // where an early SIGTERM hits the default termination disposition.
    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| CliError {
        context:    Some("Could not install SIGTERM signal handler"),
        source:     Box::new(err),
        suggestion: None,
    })?;

    Ok(async move {
        tokio::select! {
            res = ctrl_c() => {
                res.expect("failed to listen for ctrl-c event");
            }
            _ = sigterm.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl Future<Output = ()> + Send + 'static, CliError> {
    Ok(async {
        ctrl_c().await.expect("failed to listen for ctrl-c event");
    })
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let tracing = DefaultTracing::start();

    let Args {
        host_port,
        long_contention_timeout,
        default_enterprise,
        transaction_idle_timeout_ms,
        transaction_max_lifetime_ms,
    } = Args::parse();

    // Create a new Firestore Project.
    let project = Box::new(FirestoreProject::new(FirestoreConfig {
        long_contention_timeout,
        default_enterprise,
        transaction_idle_timeout: transaction_idle_timeout_ms
            .map(|ms| Duration::from_millis(ms.get())),
        transaction_max_lifetime: transaction_max_lifetime_ms
            .map(|ms| Duration::from_millis(ms.get())),
    }));
    // Make it live for the remainder of the program's life. ('static)
    let project = Box::leak(project);

    let listener = TcpListener::bind(host_port).await.map_err(|err| CliError {
        context:    Some("Could not start listener on provided address"),
        source:     Box::new(err),
        suggestion: Some(
            "Use \"--host-port 127.0.0.1:0\" to listen to a random port on localhost.",
        ),
    })?;

    let shutdown = shutdown_signal()?;

    run(project, listener, shutdown, tracing)
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
        use clap::CommandFactory;

        let default_args =
            Args::try_parse_from(["firestore-emulator", "--host-port", "127.0.0.1:8085"]).unwrap();
        assert_eq!(default_args.host_port, "127.0.0.1:8085".parse().unwrap());
        assert!(!default_args.long_contention_timeout);
        assert!(!default_args.default_enterprise);
        assert_eq!(default_args.transaction_idle_timeout_ms, None);
        assert_eq!(default_args.transaction_max_lifetime_ms, None);

        let args = Args::try_parse_from([
            "firestore-emulator",
            "--host-port",
            "127.0.0.1:8085",
            "--long-contention-timeout",
            "--default-enterprise",
            "--transaction-idle-timeout-ms",
            "2000",
            "--transaction-max-lifetime-ms",
            "10000",
        ])
        .unwrap();
        assert_eq!(args.host_port, "127.0.0.1:8085".parse().unwrap());
        assert!(args.long_contention_timeout);
        assert!(args.default_enterprise);
        assert_eq!(args.transaction_idle_timeout_ms.unwrap().get(), 2000);
        assert_eq!(args.transaction_max_lifetime_ms.unwrap().get(), 10000);

        // Zero values are rejected by NonZeroU64.
        assert!(
            Args::try_parse_from([
                "firestore-emulator",
                "--host-port",
                "127.0.0.1:8085",
                "--transaction-idle-timeout-ms",
                "0",
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from([
                "firestore-emulator",
                "--host-port",
                "127.0.0.1:8085",
                "--transaction-max-lifetime-ms",
                "0",
            ])
            .is_err()
        );

        // Verify exact env variable bindings on clap command definition.
        let cmd = Args::command();
        let idle_arg = cmd
            .get_arguments()
            .find(|a| a.get_id() == "transaction_idle_timeout_ms")
            .unwrap();
        assert_eq!(
            idle_arg.get_env().and_then(|s| s.to_str()),
            Some("FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS")
        );
        let max_arg = cmd
            .get_arguments()
            .find(|a| a.get_id() == "transaction_max_lifetime_ms")
            .unwrap();
        assert_eq!(
            max_arg.get_env().and_then(|s| s.to_str()),
            Some("FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS")
        );
    }
}
