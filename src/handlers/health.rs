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
// Returns info about the GoClaw credential resolution strategy.
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/admin/agents",
    tag = "admin",
    responses(
        (status = 200, description = "GoClaw credential info")
    )
)]
pub async fn list_agents_handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "mode": "goclaw_dynamic",
        "goclaw_gateway_url": state.config.goclaw_gateway_url,
        "credential_source": "Redis ws_creds:{workspace_id}",
    }))
}
