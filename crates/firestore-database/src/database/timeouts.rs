use std::time::Duration;

#[derive(Debug)]
pub(crate) struct Timeouts {
    /// The duration to use for write locks.
    pub(crate) write: Duration,
    /// Usually a slightly longer timeout for read locks as these have less chance of failing in
    /// the end.
    pub(crate) read:  Duration,
}

impl Timeouts {
    /// More accurate lock timeouts (than [`FAST`](Self::FAST)).
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. This simulates that behavior more accurately.
    pub(crate) const CLOUD: Timeouts = Timeouts {
        write: Duration::from_secs(15),
        read:  Duration::from_secs(16),
    };

    /// Must faster lock timeouts (than [`CLOUD`](Self::CLOUD)).
    ///
    /// Reduced to 1 second write locks and 2 second read locks for faster unit-tests.
    pub(crate) const FAST: Timeouts = Timeouts {
        write: Duration::from_secs(1),
        read:  Duration::from_secs(2),
    };
}
