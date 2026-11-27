#![allow(unused_crate_dependencies)]

use std::{
    borrow::Cow,
    error::Error,
    fmt,
    fs::File,
    future::Future,
    io::{self, Read},
    net::SocketAddr,
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::Duration,
};

use bytes::Bytes;
use clap::Parser;
use emulator_database::{
    FirestoreConfig, FirestoreProject,
    snapshot::{EXPORT_CHUNK_BYTES, SnapshotError},
};
use emulator_tracing::DefaultTracing;
use firestore_emulator::run;
use tokio::net::TcpListener;
use tracing::{Level, enabled, info};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL_ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct CliError {
    context:    Option<Cow<'static, str>>,
    source:     Box<dyn Error + Send + Sync>,
    suggestion: Option<&'static str>,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = &self.context {
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
    #[arg(long, env = "FIRESTORE_EMULATOR_LONG_CONTENTION_TIMEOUT")]
    long_contention_timeout: bool,

    /// Default to Enterprise Edition for new databases created after startup.
    ///
    /// By default, new databases use Standard Edition (pessimistic concurrency). Enable this flag
    /// to default new databases to Enterprise Edition (optimistic concurrency). Databases
    /// preloaded via --import retain the edition and concurrency mode saved in the snapshot.
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

    /// Optional path to a .fsemu snapshot file to preload before listening.
    ///
    /// Reads a binary .fsemu snapshot exported from /emulator/v1/export or the Web UI (Firebase
    /// Java emulator export directories are not supported). Imported databases retain their saved
    /// edition and concurrency settings regardless of --default-enterprise. Invalid, unreadable,
    /// or directory paths fail startup with a non-zero exit code before listening.
    #[arg(
        long = "import",
        value_name = "PATH",
        env = "FIRESTORE_EMULATOR_IMPORT"
    )]
    import_path: Option<PathBuf>,
}

fn cli_io_import_error(path: &Path, err: io::Error) -> CliError {
    let suggestion = if err.kind() == io::ErrorKind::IsADirectory || path.is_dir() {
        "Pass a .fsemu snapshot file exported from /emulator/v1/export or the Web UI, not a \
         directory (Firebase Java emulator export directories are not supported)."
    } else if matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
    ) {
        "Verify that the snapshot file path exists and is readable by this process."
    } else {
        "Verify that the snapshot file path points to a readable .fsemu snapshot file."
    };
    CliError {
        context:    Some(Cow::Owned(format!(
            "Could not import snapshot from '{}'",
            path.display()
        ))),
        source:     Box::new(err),
        suggestion: Some(suggestion),
    }
}

fn cli_snapshot_import_error(path: &Path, err: SnapshotError) -> CliError {
    let suggestion = match &err {
        SnapshotError::UnsupportedVersion { .. } => {
            "Re-export the snapshot using a compatible version of firestore-emulator."
        }
        SnapshotError::Stream(_) => "Verify that the snapshot file remains readable and try again.",
        SnapshotError::InvalidFormat(_)
        | SnapshotError::Truncated(_)
        | SnapshotError::FrameTooLarge { .. }
        | SnapshotError::Validation(_) => {
            "Verify that the file is a valid, uncorrupted .fsemu snapshot (or re-export it from \
             /emulator/v1/export or the Web UI)."
        }
        SnapshotError::ImportInProgress => {
            "Wait for any active snapshot import to finish and retry."
        }
    };
    CliError {
        context:    Some(Cow::Owned(format!(
            "Could not import snapshot from '{}'",
            path.display()
        ))),
        source:     Box::new(err),
        suggestion: Some(suggestion),
    }
}

async fn load_startup_snapshot(
    project: &'static FirestoreProject,
    path: &Path,
) -> Result<(), CliError> {
    let file = File::open(path).map_err(|err| cli_io_import_error(path, err))?;
    let metadata = file
        .metadata()
        .map_err(|err| cli_io_import_error(path, err))?;
    if metadata.is_dir() {
        return Err(cli_io_import_error(
            path,
            io::Error::new(
                io::ErrorKind::IsADirectory,
                "path is a directory, expected a .fsemu snapshot file",
            ),
        ));
    }

    let stream = futures_util::stream::unfold(
        Some((file, vec![0u8; EXPORT_CHUNK_BYTES])),
        |state| async move {
            let (mut file, mut buf) = state?;
            loop {
                match file.read(&mut buf) {
                    Ok(0) => return None,
                    Ok(n) => {
                        let chunk = Bytes::copy_from_slice(&buf[..n]);
                        return Some((Ok(chunk), Some((file, buf))));
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        return Some((Err(SnapshotError::Stream(err.to_string())), None));
                    }
                }
            }
        },
    );

    let summary = project
        .import_snapshot(Box::pin(stream))
        .await
        .map_err(|err| cli_snapshot_import_error(path, err))?;

    if enabled!(Level::INFO) {
        info!(
            "Loaded snapshot from {}: {} database(s), {} document(s)",
            path.display(),
            summary.total_databases(),
            summary.total_documents(),
        );
    } else {
        eprintln!(
            "Loaded snapshot from {}: {} database(s), {} document(s)",
            path.display(),
            summary.total_databases(),
            summary.total_documents(),
        );
    }

    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> Result<impl Future<Output = ()> + Send + 'static, CliError> {
    use tokio::signal::unix::{SignalKind, signal};

    // Register both SIGINT and SIGTERM eagerly before `run(...)` prints "listening" to avoid a
    // startup race where an early signal hits the default termination disposition.
    let mut sigint = signal(SignalKind::interrupt()).map_err(|err| CliError {
        context:    Some(Cow::Borrowed("Could not install SIGINT signal handler")),
        source:     Box::new(err),
        suggestion: None,
    })?;
    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| CliError {
        context:    Some(Cow::Borrowed("Could not install SIGTERM signal handler")),
        source:     Box::new(err),
        suggestion: None,
    })?;

    Ok(async move {
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl Future<Output = ()> + Send + 'static, CliError> {
    use tokio::signal::ctrl_c;

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
        import_path,
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

    if let Some(ref path) = import_path {
        load_startup_snapshot(project, path).await?;
    }
    let listener = TcpListener::bind(host_port).await.map_err(|err| CliError {
        context:    Some(Cow::Borrowed(
            "Could not start listener on provided address",
        )),
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
    use std::{borrow::Cow, error::Error, fmt, io, path::Path};

    use clap::Parser;
    use emulator_database::{GenericDatabaseError, snapshot::SnapshotError};

    use super::{Args, CliError, cli_io_import_error, cli_snapshot_import_error};

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
            context:    Some(Cow::Borrowed(
                "Could not start listener on provided address",
            )),
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

        let with_owned = CliError {
            context:    Some(Cow::Owned(
                "Could not import snapshot from 'snap.fsemu'".to_string(),
            )),
            source:     Box::new(io::Error::new(io::ErrorKind::NotFound, "missing")),
            suggestion: Some("Check the path."),
        };
        assert_eq!(
            with_owned.to_string(),
            "Could not import snapshot from 'snap.fsemu': missing\n\nSuggestion: Check the path."
        );

        let bare = CliError {
            context:    None,
            source:     Box::new(io::Error::other("server transport failed")),
            suggestion: None,
        };
        assert_eq!(bare.to_string(), "server transport failed");
        assert_eq!(format!("{bare:?}"), "server transport failed");
    }

    #[test]
    fn cli_import_errors_classify_suggestions_per_failure_class() {
        let dir_err = cli_io_import_error(
            Path::new("exports/firebase-dir"),
            io::Error::new(io::ErrorKind::IsADirectory, "is a directory"),
        )
        .to_string();
        assert!(dir_err.contains("Could not import snapshot from 'exports/firebase-dir'"));
        assert!(dir_err.contains(".fsemu"));
        assert!(dir_err.contains("directory"));

        let missing_err = cli_io_import_error(
            Path::new("missing.fsemu"),
            io::Error::new(io::ErrorKind::NotFound, "not found"),
        )
        .to_string();
        assert!(missing_err.contains("Could not import snapshot from 'missing.fsemu'"));
        assert!(missing_err.contains("exists and is readable"));

        let version_err = cli_snapshot_import_error(
            Path::new("future.fsemu"),
            SnapshotError::UnsupportedVersion {
                found_major:     99,
                supported_major: 1,
            },
        )
        .to_string();
        assert!(version_err.contains("Could not import snapshot from 'future.fsemu'"));
        assert!(version_err.contains("found major 99, supported major 1"));
        assert!(version_err.contains("compatible version"));

        for err in [
            SnapshotError::InvalidFormat("bad crc".to_string()),
            SnapshotError::Truncated("eof".to_string()),
            SnapshotError::FrameTooLarge { len: 100, max: 50 },
            SnapshotError::Validation(GenericDatabaseError::InvalidArgument("bad ts".to_string())),
        ] {
            let formatted = cli_snapshot_import_error(Path::new("bad.fsemu"), err).to_string();
            assert!(formatted.contains("Could not import snapshot from 'bad.fsemu'"));
            assert!(formatted.contains("valid, uncorrupted .fsemu snapshot"));
        }
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
        assert_eq!(default_args.import_path, None);

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
            "--import",
            "fixtures/state.fsemu",
        ])
        .unwrap();
        assert_eq!(args.host_port, "127.0.0.1:8085".parse().unwrap());
        assert!(args.long_contention_timeout);
        assert!(args.default_enterprise);
        assert_eq!(args.transaction_idle_timeout_ms.unwrap().get(), 2000);
        assert_eq!(args.transaction_max_lifetime_ms.unwrap().get(), 10000);
        assert_eq!(
            args.import_path.as_deref(),
            Some(Path::new("fixtures/state.fsemu"))
        );

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
        for (arg_id, expected_env) in [
            ("host_port", "FIRESTORE_EMULATOR_HOST"),
            (
                "long_contention_timeout",
                "FIRESTORE_EMULATOR_LONG_CONTENTION_TIMEOUT",
            ),
            (
                "default_enterprise",
                "FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE",
            ),
            (
                "transaction_idle_timeout_ms",
                "FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS",
            ),
            (
                "transaction_max_lifetime_ms",
                "FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS",
            ),
            ("import_path", "FIRESTORE_EMULATOR_IMPORT"),
        ] {
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_id() == arg_id)
                .unwrap_or_else(|| panic!("missing clap argument {arg_id}"));
            assert_eq!(
                arg.get_env().and_then(|s| s.to_str()),
                Some(expected_env),
                "unexpected env binding for {arg_id}"
            );
        }
        let import_arg = cmd
            .get_arguments()
            .find(|a| a.get_id() == "import_path")
            .unwrap();
        assert_eq!(import_arg.get_long(), Some("import"));
    }
}
