use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use serde::Serialize;
use serde_json::{Value, json};
use tracing::error;
use utoipa::ToSchema;

use crate::{error::AppError, state::AppState};

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    /// Always "ok" — degraded backing services are surfaced in the fields below.
    status: &'static str,
    /// "ok" | "error"
    redis: &'static str,
    /// "ok" | "disconnected"
    nats: &'static str,
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Service status", body = HealthResponse)
    )
)]
pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let redis = match ping_redis(&state).await {
        Ok(()) => "ok",
        Err(e) => {
            error!(error = %e, "redis health check failed");
            "error"
        }
    };

    let nats = if state.nats_publisher.is_connected() {
        "ok"
    } else {
        "disconnected"
    };

    Json(HealthResponse {
        status: "ok",
        redis,
        nats,
    })
}

async fn ping_redis(state: &AppState) -> Result<(), AppError> {
    let mut conn = state.redis_pool.get().await.map_err(AppError::RedisPool)?;
    let _: String = deadpool_redis::redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .map_err(AppError::RedisCmd)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// GET /admin/agents
// Returns the configured workspace→agent_id mapping from the resolver.
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/admin/agents",
    tag = "admin",
    responses(
        (status = 200, description = "Configured workspace→agent mappings")
    )
)]
pub async fn list_agents_handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "default_agent": state.agent_resolver.default_agent(),
        "workspace_overrides": state.agent_resolver.entries(),
    }))
}
