use emulator_database::{FirestoreConfig, FirestoreProject, reference::RootRef};

use super::client::TestDb;

pub(crate) const DATABASE_ROOT: &str = "projects/test-project/databases/(default)";
pub(crate) const DOCUMENTS_ROOT: &str = "projects/test-project/databases/(default)/documents";

/// Builder configuring an isolated in-memory [`FirestoreProject`] and [`TestDb`] client.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TestEnv {
    long_contention_timeout: bool,
    default_enterprise:      bool,
}

impl TestEnv {
    /// Creates a `STANDARD` edition environment with `Timeouts::FAST` (`2s` write / `5s` read).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Creates an `ENTERPRISE` edition environment with `Timeouts::FAST` (`2s` write / `5s` read).
    pub(crate) fn enterprise() -> Self {
        Self {
            long_contention_timeout: false,
            default_enterprise:      true,
        }
    }

    /// Switches lock timeouts from `Timeouts::FAST` (`2s`/`5s`) to `Timeouts::CLOUD` (`15s`/`16s`).
    pub(crate) fn cloud_timeouts(mut self) -> Self {
        self.long_contention_timeout = true;
        self
    }

    /// Leaks one isolated [`FirestoreProject`] for the test (`database_id = "(default)"`).
    pub(crate) fn build_project(self) -> &'static FirestoreProject {
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
            long_contention_timeout: self.long_contention_timeout,
            default_enterprise:      self.default_enterprise,
        })))
    }

    /// Builds an isolated [`FirestoreProject`] and returns an in-process [`TestDb`] client.
    pub(crate) fn build_db(self) -> TestDb {
        TestDb::from_project(self.build_project())
    }

    /// Returns the parsed [`RootRef`] for `projects/test-project/databases/(default)`.
    pub(crate) fn root_ref() -> RootRef {
        DATABASE_ROOT
            .parse()
            .expect("static DATABASE_ROOT must parse as RootRef")
    }
}
