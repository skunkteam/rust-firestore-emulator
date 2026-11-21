use std::time::Duration;

#[derive(Clone, Copy, Debug, Default)]
pub struct FirestoreConfig {
    /// Enable more accurate lock timeouts.
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. By default, in the emulator, this is reduced to 2 seconds for faster
    /// unit-tests. Enable this feature to simulate the Cloud Firestore more accurately.
    pub long_contention_timeout: bool,

    /// Default to Enterprise Edition for all new databases.
    ///
    /// The Firestore emulator can simulate both the Standard and Enterprise editions.
    /// Enable this flag to default new databases to the Enterprise edition.
    pub default_enterprise: bool,

    /// Optional override for the active transaction idle timeout.
    ///
    /// When `None`, defaults to Cloud Firestore's 60-second idle timeout (and 120-second
    /// post-first-read minimum lifetime). When set, `txn_idle` is clamped to `txn_max` and the
    /// post-first-read minimum lifetime is `min(2 * idle, txn_max)`.
    pub transaction_idle_timeout: Option<Duration>,

    /// Optional override for the active transaction maximum lifetime.
    ///
    /// When `None`, defaults to Cloud Firestore's 270-second maximum transaction lifetime.
    pub transaction_max_lifetime: Option<Duration>,
}

impl FirestoreConfig {
    /// Sets both the active transaction idle timeout and maximum lifetime overrides.
    pub fn with_transaction_timeouts(
        mut self,
        idle: impl Into<Option<Duration>>,
        max: impl Into<Option<Duration>>,
    ) -> Self {
        self.transaction_idle_timeout = idle.into();
        self.transaction_max_lifetime = max.into();
        self
    }

    /// Sets the active transaction idle timeout override.
    pub fn with_transaction_idle_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.transaction_idle_timeout = timeout.into();
        self
    }

    /// Sets the active transaction maximum lifetime override.
    pub fn with_transaction_max_lifetime(mut self, lifetime: impl Into<Option<Duration>>) -> Self {
        self.transaction_max_lifetime = lifetime.into();
        self
    }
}
