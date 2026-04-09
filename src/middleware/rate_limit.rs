use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use tracing::warn;

use crate::{error::AppError, middleware::auth::SessionData, state::AppState};

pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let session = req
        .extensions()
        .get::<SessionData>()
        .ok_or_else(|| AppError::Internal("SessionData missing from extensions".to_string()))?
        .clone();

    check_rate_limit(&state, &session.workspace_id, &session.user_id).await?;

    Ok(next.run(req).await)
}

async fn check_rate_limit(
    state: &AppState,
    workspace_id: &str,
    user_id: &str,
) -> Result<(), AppError> {
    // Bucket key per user per whole second. Two-second TTL gives the current
    // window and the previous one a chance to expire cleanly.
    // Per-user (not per-workspace) so one user cannot starve others.
    let window = Utc::now().timestamp();
    let key = format!("ratelimit:{workspace_id}:{user_id}:{window}");

    let mut conn = state.redis_pool.get().await.map_err(AppError::RedisPool)?;

    // INCR is atomic — safe under concurrent requests.
    let count: i64 = conn.incr(&key, 1_i64).await.map_err(AppError::RedisCmd)?;

    // Set TTL only on the first request in this window to avoid resetting it.
    if count == 1 {
        let _: bool = conn.expire(&key, 2_i64).await.map_err(AppError::RedisCmd)?;
    }

    if count > i64::from(state.config.rate_limit_rps) {
        warn!(
            workspace_id,
            user_id,
            count,
            limit = state.config.rate_limit_rps,
            "rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded);
    }

    Ok(())
}
