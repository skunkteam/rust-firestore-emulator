use axum::{
    Json,
    body::Bytes,
    extract::{FromRequest, Request},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use emulator_database::GenericDatabaseError;
use emulator_tracing::SetLogLevelsError;
use serde::de::DeserializeOwned;
use serde_json::json;

pub(crate) type Result<T, E = RestError> = std::result::Result<T, E>;

#[derive(Debug)]
pub(crate) struct RestError {
    status:      StatusCode,
    grpc_status: &'static str,
    message:     String,
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.status.as_u16(),
            self.grpc_status,
            self.message
        )
    }
}

impl RestError {
    pub(crate) fn new(
        status: StatusCode,
        grpc_status: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            grpc_status,
            message: message.into(),
        }
    }

    pub(crate) fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", "Not found")
    }

    pub(crate) fn not_found_msg(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_ARGUMENT", message)
    }

    pub(crate) fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "UNIMPLEMENTED", message)
    }
}

impl From<GenericDatabaseError> for RestError {
    fn from(value: GenericDatabaseError) -> Self {
        let (status, grpc_status) = match &value {
            GenericDatabaseError::Aborted(_) => (StatusCode::CONFLICT, "ABORTED"),
            GenericDatabaseError::AlreadyExists(_) => (StatusCode::CONFLICT, "ALREADY_EXISTS"),
            GenericDatabaseError::Cancelled(_) => (
                StatusCode::from_u16(499).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                "CANCELLED",
            ),
            GenericDatabaseError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
            GenericDatabaseError::InvalidArgument(_) => {
                (StatusCode::BAD_REQUEST, "INVALID_ARGUMENT")
            }
            GenericDatabaseError::InvalidReference(_, _) => {
                (StatusCode::BAD_REQUEST, "INVALID_ARGUMENT")
            }
            GenericDatabaseError::FailedPrecondition(_) => {
                (StatusCode::BAD_REQUEST, "FAILED_PRECONDITION")
            }
            GenericDatabaseError::NotImplemented(_) => {
                (StatusCode::NOT_IMPLEMENTED, "UNIMPLEMENTED")
            }
        };
        Self::new(status, grpc_status, value.to_string())
    }
}

impl From<SetLogLevelsError> for RestError {
    fn from(value: SetLogLevelsError) -> Self {
        match value {
            SetLogLevelsError::InvalidDirectives(parse_error) => Self::new(
                StatusCode::BAD_REQUEST,
                "INVALID_ARGUMENT",
                parse_error.to_string(),
            ),
            SetLogLevelsError::ReloadError(error) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                error.to_string(),
            ),
        }
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        tracing::debug!(
            http_status = self.status.as_u16(),
            grpc_status = self.grpc_status,
            message = %self.message,
            "returning REST error response"
        );
        let payload = json!({
            "error": {
                "code": self.status.as_u16(),
                "message": self.message,
                "status": self.grpc_status,
            }
        });
        (self.status, Json(payload)).into_response()
    }
}

pub(crate) fn parse_json_body<T: DeserializeOwned>(headers: &HeaderMap, body: &[u8]) -> Result<T> {
    let is_json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"));

    if !is_json {
        return Err(RestError::bad_request(
            "Content-Type must be application/json",
        ));
    }

    serde_json::from_slice::<T>(body).map_err(|err| {
        tracing::debug!(
            error = %err,
            line = err.line(),
            column = err.column(),
            "failed to deserialize JSON request body"
        );
        RestError::bad_request(format!("Invalid JSON payload: {err}"))
    })
}

#[derive(Debug)]
pub(crate) struct ApiJson<T>(pub(crate) T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = RestError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers().clone();
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|err| RestError::bad_request(format!("Failed to read request body: {err}")))?;
        let parsed = parse_json_body(&headers, &bytes)?;
        Ok(Self(parsed))
    }
}
