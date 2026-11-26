use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DatabaseEdition {
    Standard,
    Enterprise,
}

impl DatabaseEdition {
    pub(crate) const fn from_enterprise(enterprise: bool) -> Self {
        if enterprise {
            Self::Enterprise
        } else {
            Self::Standard
        }
    }
}

impl fmt::Display for DatabaseEdition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standard => f.write_str("STANDARD"),
            Self::Enterprise => f.write_str("ENTERPRISE"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DatabaseConcurrencyMode {
    Pessimistic,
    Optimistic,
}

impl DatabaseConcurrencyMode {
    pub(crate) const fn from_optimistic(optimistic: bool) -> Self {
        if optimistic {
            Self::Optimistic
        } else {
            Self::Pessimistic
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseSettings {
    pub edition: DatabaseEdition,
    pub concurrency_mode: DatabaseConcurrencyMode,
}

impl DatabaseSettings {
    pub const fn for_edition(edition: DatabaseEdition) -> Self {
        let concurrency_mode = match edition {
            DatabaseEdition::Standard => DatabaseConcurrencyMode::Pessimistic,
            DatabaseEdition::Enterprise => DatabaseConcurrencyMode::Optimistic,
        };
        Self {
            edition,
            concurrency_mode,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_settings_for_edition_and_serde_roundtrip() {
        assert_eq!(
            DatabaseSettings::for_edition(DatabaseEdition::Standard),
            DatabaseSettings {
                edition: DatabaseEdition::Standard,
                concurrency_mode: DatabaseConcurrencyMode::Pessimistic,
            }
        );
        assert_eq!(
            DatabaseSettings::for_edition(DatabaseEdition::Enterprise),
            DatabaseSettings {
                edition: DatabaseEdition::Enterprise,
                concurrency_mode: DatabaseConcurrencyMode::Optimistic,
            }
        );

        assert_eq!(DatabaseEdition::Standard.to_string(), "STANDARD");
        assert_eq!(DatabaseEdition::Enterprise.to_string(), "ENTERPRISE");

        assert_eq!(
            serde_json::to_string(&DatabaseEdition::Standard).unwrap(),
            "\"STANDARD\""
        );
        assert_eq!(
            serde_json::to_string(&DatabaseEdition::Enterprise).unwrap(),
            "\"ENTERPRISE\""
        );
        assert_eq!(
            serde_json::from_str::<DatabaseEdition>("\"STANDARD\"").unwrap(),
            DatabaseEdition::Standard
        );
        assert_eq!(
            serde_json::from_str::<DatabaseEdition>("\"ENTERPRISE\"").unwrap(),
            DatabaseEdition::Enterprise
        );

        assert_eq!(
            serde_json::to_string(&DatabaseConcurrencyMode::Pessimistic).unwrap(),
            "\"PESSIMISTIC\""
        );
        assert_eq!(
            serde_json::to_string(&DatabaseConcurrencyMode::Optimistic).unwrap(),
            "\"OPTIMISTIC\""
        );
        assert_eq!(
            serde_json::from_str::<DatabaseConcurrencyMode>("\"PESSIMISTIC\"").unwrap(),
            DatabaseConcurrencyMode::Pessimistic
        );
        assert_eq!(
            serde_json::from_str::<DatabaseConcurrencyMode>("\"OPTIMISTIC\"").unwrap(),
            DatabaseConcurrencyMode::Optimistic
        );
    }
}
