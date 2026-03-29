use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use serde::Deserialize;
use tracing::warn;

use crate::{error::AppError, state::AppState};

/// Session data injected into request extensions after successful auth.
/// Downstream handlers and middleware extract this via `Extension<SessionData>`.
#[derive(Clone, Debug, Deserialize)]
pub struct SessionData {
    pub workspace_id: String,
    pub user_id: String,
    /// Unix timestamp (seconds). Validated against current time.
    pub expires_at: u64,
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = extract_bearer(req.headers())
        .ok_or_else(|| AppError::Unauthorized("missing Authorization header".to_string()))?;

    let session = lookup_session(&state, token).await?;

    let now = Utc::now().timestamp() as u64;
    if session.expires_at < now {
        warn!(workspace_id = %session.workspace_id, "session expired");
        return Err(AppError::Unauthorized("session expired".to_string()));
    }

    req.extensions_mut().insert(session);
    Ok(next.run(req).await)
}

fn extract_bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

async fn lookup_session(state: &AppState, token: &str) -> Result<SessionData, AppError> {
    let key = format!("session:{token}");

    let mut conn = state.redis_pool.get().await.map_err(AppError::RedisPool)?;

    let raw: Option<String> = conn.get(&key).await.map_err(AppError::RedisCmd)?;

    let raw = raw.ok_or_else(|| AppError::Unauthorized("invalid or expired token".to_string()))?;

    serde_json::from_str::<SessionData>(&raw)
        .map_err(|e| AppError::Internal(format!("malformed session in redis: {e}")))
}
