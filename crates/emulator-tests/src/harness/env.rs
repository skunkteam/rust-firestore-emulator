use std::time::Duration;

use emulator_database::{FirestoreConfig, FirestoreProject, reference::RootRef};

use super::client::TestDb;

pub(crate) const DATABASE_ROOT: &str = "projects/test-project/databases/(default)";
pub(crate) const DOCUMENTS_ROOT: &str = "projects/test-project/databases/(default)/documents";

/// Builder configuring an isolated in-memory [`FirestoreProject`] and [`TestDb`] client.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TestEnv {
    long_contention_timeout: bool,
    default_enterprise: bool,
    transaction_idle_timeout: Option<Duration>,
    transaction_max_lifetime: Option<Duration>,
}

impl TestEnv {
    /// Creates a `STANDARD` edition environment with `Timeouts::FAST` (`2s` write / `5s` read).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Creates an `ENTERPRISE` edition environment with `Timeouts::FAST` (`2s` write / `5s` read).
    pub(crate) fn enterprise() -> Self {
        Self {
            default_enterprise: true,
            ..Self::default()
        }
    }

    /// Switches lock timeouts from `Timeouts::FAST` (`2s`/`5s`) to `Timeouts::CLOUD` (`15s`/`16s`).
    pub(crate) fn cloud_timeouts(mut self) -> Self {
        self.long_contention_timeout = true;
        self
    }

    /// Overrides the active transaction idle timeout (and sets post-first-read minimum window to
    /// `2 * timeout` clamped to max lifetime).
    pub(crate) fn transaction_idle_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.transaction_idle_timeout = timeout.into();
        self
    }

    /// Overrides the active transaction maximum lifetime.
    pub(crate) fn transaction_max_lifetime(
        mut self,
        lifetime: impl Into<Option<Duration>>,
    ) -> Self {
        self.transaction_max_lifetime = lifetime.into();
        self
    }

    /// Overrides both the active transaction idle timeout and maximum lifetime.
    pub(crate) fn transaction_timeouts(
        mut self,
        idle: impl Into<Option<Duration>>,
        max: impl Into<Option<Duration>>,
    ) -> Self {
        self.transaction_idle_timeout = idle.into();
        self.transaction_max_lifetime = max.into();
        self
    }

    /// Leaks one isolated [`FirestoreProject`] for the test (`database_id = "(default)"`).
    pub(crate) fn build_project(self) -> &'static FirestoreProject {
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
            long_contention_timeout: self.long_contention_timeout,
            default_enterprise: self.default_enterprise,
            transaction_idle_timeout: self.transaction_idle_timeout,
            transaction_max_lifetime: self.transaction_max_lifetime,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TestEnv;

    #[test]
    fn test_env_transaction_timeout_builders_configure_project() {
        let env_idle = TestEnv::new().transaction_idle_timeout(Duration::from_secs(3));
        assert_eq!(
            env_idle.transaction_idle_timeout,
            Some(Duration::from_secs(3))
        );
        assert_eq!(env_idle.transaction_max_lifetime, None);
        let dbg_idle = format!("{:?}", env_idle.build_project());
        assert!(dbg_idle.contains("txn_idle: 3s"), "got {dbg_idle}");

        let env_max = TestEnv::enterprise().transaction_max_lifetime(Duration::from_secs(15));
        assert_eq!(env_max.transaction_idle_timeout, None);
        assert_eq!(
            env_max.transaction_max_lifetime,
            Some(Duration::from_secs(15))
        );
        let dbg_max = format!("{:?}", env_max.build_project());
        assert!(dbg_max.contains("txn_max: 15s"), "got {dbg_max}");

        let env_both = TestEnv::new()
            .cloud_timeouts()
            .transaction_timeouts(Duration::from_secs(2), Duration::from_secs(10));
        assert_eq!(
            env_both.transaction_idle_timeout,
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            env_both.transaction_max_lifetime,
            Some(Duration::from_secs(10))
        );
        let dbg_both = format!("{:?}", env_both.build_project());
        assert!(
            dbg_both.contains("txn_idle: 2s") && dbg_both.contains("txn_max: 10s"),
            "got {dbg_both}"
        );
    }
}
