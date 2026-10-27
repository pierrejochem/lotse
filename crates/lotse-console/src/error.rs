//! API errors and how they map to HTTP.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::model::ErrorBody;

/// Errors returned by the console API. Each maps to one HTTP status and a
/// stable code the frontend can switch on.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The cluster, stream, queue or tenant does not exist.
    #[error("{0}")]
    NotFound(String),
    /// The request is malformed or breaks a rule.
    #[error("{0}")]
    BadRequest(String),
    /// The signed-in user's role does not allow this.
    #[error("{0}")]
    Forbidden(String),
    /// A resource with that name exists already.
    #[error("{0}")]
    Conflict(String),
    /// The cluster could not be reached.
    #[error("{0}")]
    Unavailable(String),
}

impl ApiError {
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            ApiError::Forbidden(_) => (StatusCode::FORBIDDEN, "FORBIDDEN"),
            ApiError::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
            ApiError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "CLUSTER_UNAVAILABLE"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = self.parts();
        (status, Json(ErrorBody { code: code.to_owned(), message: self.to_string() })).into_response()
    }
}

/// Result type of handlers.
pub type ApiResult<T> = Result<T, ApiError>;
