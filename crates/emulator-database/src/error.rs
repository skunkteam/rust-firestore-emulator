use std::fmt;

use thiserror::Error;

use crate::database::transaction::TransactionId;

pub(crate) const EXPIRED_TRANSACTION_MSG: &str =
    "The referenced transaction has expired or is no longer valid.";

pub(crate) type Result<T, E = GenericDatabaseError> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AbortReason {
    Wounded {
        wounder_txn_id: Option<TransactionId>,
        wounder_seq:    u64,
    },
    UpgradeContention,
    ReadTimeout,
    WriteTimeout,
    OptimisticWriteSkew,
    TransactionExpired,
    DatabaseCleared,
    DatabaseDropped,
}

impl fmt::Display for AbortReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wounded {
                wounder_txn_id: Some(txn_id),
                wounder_seq,
            } => write!(
                f,
                "transaction aborted due to contention (wounded by {txn_id}, seq={wounder_seq})"
            ),
            Self::Wounded {
                wounder_txn_id: None,
                wounder_seq,
            } => write!(
                f,
                "transaction aborted due to contention (wounded by seq={wounder_seq})"
            ),
            Self::UpgradeContention => f.write_str("contention"),
            Self::ReadTimeout => f.write_str("timeout waiting for read lock on document"),
            Self::WriteTimeout => f.write_str("timeout waiting for write lock on document"),
            Self::OptimisticWriteSkew => {
                f.write_str("contention: optimistic concurrency violation")
            }
            Self::TransactionExpired => f.write_str(EXPIRED_TRANSACTION_MSG),
            Self::DatabaseCleared => f.write_str("database was cleared"),
            Self::DatabaseDropped => f.write_str("database was dropped"),
        }
    }
}

impl From<AbortReason> for GenericDatabaseError {
    fn from(reason: AbortReason) -> Self {
        Self::aborted(reason.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommitError {
    Aborted(AbortReason),
    Other(GenericDatabaseError),
}

impl From<AbortReason> for CommitError {
    fn from(reason: AbortReason) -> Self {
        Self::Aborted(reason)
    }
}

impl From<GenericDatabaseError> for CommitError {
    fn from(err: GenericDatabaseError) -> Self {
        Self::Other(err)
    }
}

impl From<CommitError> for GenericDatabaseError {
    fn from(err: CommitError) -> Self {
        match err {
            CommitError::Aborted(reason) => reason.into(),
            CommitError::Other(other) => other,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GenericDatabaseError {
    /// The operation was aborted, typically due to a concurrency issue like sequencer check
    /// failures, transaction aborts, etc.
    #[error("Aborted: {0}")]
    Aborted(String),
    /// Some entity that we attempted to create (e.g., file or directory) already exists.
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    /// The operation was cancelled (typically by the caller).
    #[error("Cancelled: {0}")]
    Cancelled(String),
    /// Internal errors. Means some invariants expected by underlying system have been broken.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Client specified an invalid argument. Note that this differs from FailedPrecondition.
    /// InvalidArgument indicates arguments that are problematic regardless of the state of the
    /// system (e.g., a malformed file name).
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    /// A special case of invalid argument where the provided database-/collection or
    /// document-reference is invalid.
    #[error("Invalid {0} reference: {1}")]
    InvalidReference(&'static str, String),
    /// Operation was rejected because the system is not in a state required for the operation’s
    /// execution. For example, directory to be deleted may be non-empty, an rmdir operation is
    /// applied to a non-directory, etc.
    #[error("Failed precondition: {0}")]
    FailedPrecondition(String),
    /// Some requested entity (e.g., file or directory) was not found.
    #[error("Not found: {0}")]
    NotFound(String),
    /// Operation is not implemented or not supported/enabled in this service.
    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl GenericDatabaseError {
    pub(crate) fn aborted(message: impl Into<String>) -> Self {
        Self::Aborted(message.into())
    }

    pub(crate) fn already_exists(message: impl Into<String>) -> Self {
        Self::AlreadyExists(message.into())
    }

    pub(crate) fn cancelled(message: impl Into<String>) -> Self {
        Self::Cancelled(message.into())
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub(crate) fn not_implemented(message: impl Into<String>) -> Self {
        Self::NotImplemented(message.into())
    }

    pub(crate) fn failed_precondition(message: impl Into<String>) -> Self {
        Self::FailedPrecondition(message.into())
    }

    pub fn grpc_code(&self) -> tonic::Code {
        match self {
            Self::Aborted(_) => tonic::Code::Aborted,
            Self::AlreadyExists(_) => tonic::Code::AlreadyExists,
            Self::Cancelled(_) => tonic::Code::Cancelled,
            Self::Internal(_) => tonic::Code::Internal,
            Self::InvalidArgument(_) | Self::InvalidReference(_, _) => tonic::Code::InvalidArgument,
            Self::FailedPrecondition(_) => tonic::Code::FailedPrecondition,
            Self::NotFound(_) => tonic::Code::NotFound,
            Self::NotImplemented(_) => tonic::Code::Unimplemented,
        }
    }
}

impl From<GenericDatabaseError> for tonic::Status {
    fn from(value: GenericDatabaseError) -> Self {
        match &value {
            GenericDatabaseError::Aborted(msg)
            | GenericDatabaseError::AlreadyExists(msg)
            | GenericDatabaseError::Cancelled(msg)
            | GenericDatabaseError::Internal(msg)
            | GenericDatabaseError::InvalidArgument(msg)
            | GenericDatabaseError::FailedPrecondition(msg)
            | GenericDatabaseError::NotFound(msg)
            | GenericDatabaseError::NotImplemented(msg) => Self::new(value.grpc_code(), msg),
            GenericDatabaseError::InvalidReference(_, _) => {
                Self::new(value.grpc_code(), value.to_string())
            }
        }
    }
}

impl From<GenericDatabaseError> for googleapis::google::rpc::Status {
    fn from(value: GenericDatabaseError) -> Self {
        googleapis::google::rpc::Status {
            code:    value.grpc_code() as _,
            message: value.to_string(),
            details: vec![],
        }
    }
}
