use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("rpc error: {0}")]
    RpcError(String),
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    #[error("redis pool error: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),
    #[error("redis cmd error: {0}")]
    RedisCmd(#[from] deadpool_redis::redis::RedisError),
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            AppError::RateLimitExceeded => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate limit exceeded".to_string(),
            ),
            AppError::RpcError(msg) => (StatusCode::BAD_GATEWAY, msg.clone()),
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
