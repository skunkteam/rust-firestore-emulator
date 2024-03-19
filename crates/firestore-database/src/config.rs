#[derive(Debug)]
pub struct FirestoreConfig {
    /// Enable more accurate lock timeouts.
    ///
    /// In Cloud Firestore, transactions can take up to 15 seconds before aborting because of
    /// contention. By default, in the emulator, this is reduced to 2 second for faster unit-tests.
    /// Enable this feature to simulate the Cloud Firestore more accurately.
    pub long_contention_timeout: bool,
}
