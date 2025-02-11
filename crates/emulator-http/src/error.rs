use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use emulator_database::GenericDatabaseError;
use emulator_tracing::SetLogLevelsError;

pub(crate) type Result<T, E = RestError> = std::result::Result<T, E>;

pub(crate) struct RestError {
    status:  StatusCode,
    message: String,
}

impl RestError {
    pub(crate) fn new(status: StatusCode, message: String) -> Self {
        Self { status, message }
    }

    pub(crate) fn not_found() -> Self {
        Self {
            status:  StatusCode::NOT_FOUND,
            message: "Not found".to_string(),
        }
    }
}

impl From<GenericDatabaseError> for RestError {
    fn from(value: GenericDatabaseError) -> Self {
        let status = match value {
            GenericDatabaseError::Aborted(_) => StatusCode::INTERNAL_SERVER_ERROR,
            GenericDatabaseError::AlreadyExists(_) => StatusCode::PRECONDITION_FAILED,
            GenericDatabaseError::Cancelled(_) => StatusCode::INTERNAL_SERVER_ERROR,
            GenericDatabaseError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            GenericDatabaseError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            GenericDatabaseError::InvalidReference(_, _) => StatusCode::BAD_REQUEST,
            GenericDatabaseError::FailedPrecondition(_) => StatusCode::PRECONDITION_FAILED,
            GenericDatabaseError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
        };
        Self::new(status, value.to_string())
    }
}

impl From<SetLogLevelsError> for RestError {
    fn from(value: SetLogLevelsError) -> Self {
        match value {
            SetLogLevelsError::InvalidDirectives(parse_error) => {
                Self::new(StatusCode::BAD_REQUEST, parse_error.to_string())
            }
            SetLogLevelsError::ReloadError(error) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            }
        }
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}
