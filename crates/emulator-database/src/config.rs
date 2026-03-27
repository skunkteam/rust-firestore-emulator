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
}
