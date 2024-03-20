use thiserror::Error;

pub(crate) type Result<T, E = GenericDatabaseError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
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
    /// The operation was cancelled (typically by the caller).
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
            | GenericDatabaseError::NotImplemented(msg) => Self::new(value.grpc_code(), msg),
            GenericDatabaseError::InvalidReference(_, _) => {
                Self::new(value.grpc_code(), value.to_string())
            }
        }
    }
}
