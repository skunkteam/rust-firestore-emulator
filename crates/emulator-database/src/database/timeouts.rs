use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Timeouts {
    /// The duration to use for write locks.
    pub(crate) write:    Duration,
    /// Usually a slightly longer timeout for read locks as these have less chance of failing in
    /// the end.
    pub(crate) read:     Duration,
    /// Idle timeout for active transactions before expiration (`60s` Cloud default).
    pub(crate) txn_idle: Duration,
    /// Hard maximum lifetime for active transactions (`270s` Cloud default).
    pub(crate) txn_max:  Duration,
}

impl Timeouts {
    /// Minimum post-first-activity lifetime for active transactions (`120s` Cloud default).
    pub(crate) fn txn_used_min(self) -> Duration {
        self.txn_idle.saturating_mul(2).min(self.txn_max)
    }

    /// More accurate lock timeouts (than [`FAST`](Self::FAST)).
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. This simulates that behavior more accurately.
    pub(crate) const CLOUD: Timeouts = Timeouts {
        write:    Duration::from_secs(15),
        read:     Duration::from_secs(16),
        txn_idle: Duration::from_secs(60),
        txn_max:  Duration::from_secs(270),
    };

    /// Much faster lock timeouts (than [`CLOUD`](Self::CLOUD)).
    ///
    /// Reduced to 2 second write locks and 5 second read locks for faster unit-tests.
    pub(crate) const FAST: Timeouts = Timeouts {
        write:    Duration::from_secs(2),
        read:     Duration::from_secs(5),
        txn_idle: Duration::from_secs(60),
        txn_max:  Duration::from_secs(270),
    };
}
