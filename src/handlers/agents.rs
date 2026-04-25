use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};

use crate::{error::AppError, state::AppState};

// ---------------------------------------------------------------------------
// GET /api/workspaces/{ws_id}/agents
// ---------------------------------------------------------------------------
pub async fn list_agents(
    State(state): State<Arc<AppState>>,
    Path(ws_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "agents.list", json!({}))
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// POST /api/workspaces/{ws_id}/agents
// Body: agent creation object (agent_key, display_name, agent_type, provider, model, …)
// ---------------------------------------------------------------------------
pub async fn create_agent(
    State(state): State<Arc<AppState>>,
    Path(ws_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "agents.create", body)
        .await?;
    Ok((StatusCode::CREATED, Json(payload)))
}

// ---------------------------------------------------------------------------
// PATCH /api/workspaces/{ws_id}/agents/{agent_id}
// Body: partial update fields (agentId injected automatically)
// ---------------------------------------------------------------------------
pub async fn update_agent(
    State(state): State<Arc<AppState>>,
    Path((ws_id, agent_id)): Path<(String, String)>,
    Json(mut body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(obj) = body.as_object_mut() {
        obj.insert("agentId".to_owned(), json!(agent_id));
    }
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "agents.update", body)
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// DELETE /api/workspaces/{ws_id}/agents/{agent_id}
// ---------------------------------------------------------------------------
pub async fn delete_agent(
    State(state): State<Arc<AppState>>,
    Path((ws_id, agent_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    state
        .rpc_pool
        .call_workspace(&ws_id, "agents.delete", json!({ "id": agent_id }))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
