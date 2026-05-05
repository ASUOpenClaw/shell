use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::{error::AppError, state::AppState};

/// Create a new GoClaw agent in the workspace.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct CreateAgentRequest {
    /// Human-readable display name.
    pub display_name: String,
    /// Slug-style key used to address the agent (e.g. `"my-agent"`).
    pub agent_key: String,
    /// Agent type: `"open"` (per-user context files) or `"predefined"` (shared context).
    pub agent_type: Option<String>,
    /// LLM provider ID registered in GoClaw.
    pub provider: Option<String>,
    /// Model name (e.g. `"qwen3-14b"`).
    pub model: Option<String>,
}

/// Partial update for an existing agent (all fields optional).
#[derive(Serialize, Deserialize, ToSchema)]
pub struct UpdateAgentRequest {
    pub display_name: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// GET /api/workspaces/{ws_id}/agents
// ---------------------------------------------------------------------------
#[utoipa::path(
    get,
    path = "/api/workspaces/{ws_id}/agents",
    tag = "agents",
    security(("ServiceKey" = [])),
    params(("ws_id" = String, Path, description = "Workspace ID")),
    responses(
        (status = 200, description = "List of agents in the workspace"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    post,
    path = "/api/workspaces/{ws_id}/agents",
    tag = "agents",
    security(("ServiceKey" = [])),
    params(("ws_id" = String, Path, description = "Workspace ID")),
    request_body = CreateAgentRequest,
    responses(
        (status = 201, description = "Agent created"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    patch,
    path = "/api/workspaces/{ws_id}/agents/{agent_id}",
    tag = "agents",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("agent_id" = String, Path, description = "Agent ID (slug)"),
    ),
    request_body = UpdateAgentRequest,
    responses(
        (status = 200, description = "Agent updated"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    delete,
    path = "/api/workspaces/{ws_id}/agents/{agent_id}",
    tag = "agents",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("agent_id" = String, Path, description = "Agent ID (slug)"),
    ),
    responses(
        (status = 204, description = "Agent deleted"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
