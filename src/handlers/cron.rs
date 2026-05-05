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

/// Create a new cron job.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct CreateCronJobRequest {
    /// Display name for the job.
    pub name: String,
    /// Cron expression, e.g. `"0 9 * * 1-5"` (weekdays at 09:00 UTC).
    pub expression: String,
    /// GoClaw agent ID (slug) to invoke on trigger.
    pub agent_id: String,
    /// Message sent to the agent on each trigger.
    pub message: String,
    /// Optional queue lane label.
    pub lane: Option<String>,
}

/// Partial update for an existing cron job (all fields optional).
#[derive(Serialize, Deserialize, ToSchema)]
pub struct UpdateCronJobRequest {
    pub name: Option<String>,
    pub expression: Option<String>,
    pub message: Option<String>,
    pub lane: Option<String>,
}

/// Enable or disable a cron job.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct ToggleCronJobRequest {
    /// `true` to enable, `false` to disable.
    pub enabled: bool,
}

// ---------------------------------------------------------------------------
// GET /api/workspaces/{ws_id}/cron
// ---------------------------------------------------------------------------
#[utoipa::path(
    get,
    path = "/api/workspaces/{ws_id}/cron",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(("ws_id" = String, Path, description = "Workspace ID")),
    responses(
        (status = 200, description = "List of cron jobs (including disabled)"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn list_cron(
    State(state): State<Arc<AppState>>,
    Path(ws_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "cron.list", json!({ "includeDisabled": true }))
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// POST /api/workspaces/{ws_id}/cron
// Body: {"name","expression","agent_id","message","lane"?}
// ---------------------------------------------------------------------------
#[utoipa::path(
    post,
    path = "/api/workspaces/{ws_id}/cron",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(("ws_id" = String, Path, description = "Workspace ID")),
    request_body = CreateCronJobRequest,
    responses(
        (status = 201, description = "Cron job created"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn create_cron(
    State(state): State<Arc<AppState>>,
    Path(ws_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "cron.create", body)
        .await?;
    Ok((StatusCode::CREATED, Json(payload)))
}

// ---------------------------------------------------------------------------
// PATCH /api/workspaces/{ws_id}/cron/{job_id}
// Body: partial update fields
// ---------------------------------------------------------------------------
#[utoipa::path(
    patch,
    path = "/api/workspaces/{ws_id}/cron/{job_id}",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("job_id" = String, Path, description = "Cron job ID"),
    ),
    request_body = UpdateCronJobRequest,
    responses(
        (status = 200, description = "Cron job updated"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn update_cron(
    State(state): State<Arc<AppState>>,
    Path((ws_id, job_id)): Path<(String, String)>,
    Json(mut body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    // Inject jobId so the caller doesn't have to duplicate it in the body.
    if let Some(obj) = body.as_object_mut() {
        obj.insert("jobId".to_owned(), json!(job_id));
    }
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "cron.update", body)
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// DELETE /api/workspaces/{ws_id}/cron/{job_id}
// ---------------------------------------------------------------------------
#[utoipa::path(
    delete,
    path = "/api/workspaces/{ws_id}/cron/{job_id}",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("job_id" = String, Path, description = "Cron job ID"),
    ),
    responses(
        (status = 204, description = "Cron job deleted"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn delete_cron(
    State(state): State<Arc<AppState>>,
    Path((ws_id, job_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    state
        .rpc_pool
        .call_workspace(&ws_id, "cron.delete", json!({ "jobId": job_id }))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /api/workspaces/{ws_id}/cron/{job_id}/toggle
// Body: {"enabled": bool}
// ---------------------------------------------------------------------------
#[utoipa::path(
    post,
    path = "/api/workspaces/{ws_id}/cron/{job_id}/toggle",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("job_id" = String, Path, description = "Cron job ID"),
    ),
    request_body = ToggleCronJobRequest,
    responses(
        (status = 200, description = "Cron job toggled"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn toggle_cron(
    State(state): State<Arc<AppState>>,
    Path((ws_id, job_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let enabled = body["enabled"].as_bool().unwrap_or(true);
    let payload = state
        .rpc_pool
        .call_workspace(
            &ws_id,
            "cron.toggle",
            json!({ "jobId": job_id, "enabled": enabled }),
        )
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// POST /api/workspaces/{ws_id}/cron/{job_id}/run
// Trigger immediate run of a cron job.
// ---------------------------------------------------------------------------
#[utoipa::path(
    post,
    path = "/api/workspaces/{ws_id}/cron/{job_id}/run",
    tag = "cron",
    security(("ServiceKey" = [])),
    params(
        ("ws_id" = String, Path, description = "Workspace ID"),
        ("job_id" = String, Path, description = "Cron job ID"),
    ),
    responses(
        (status = 200, description = "Cron job triggered immediately"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
pub async fn run_cron(
    State(state): State<Arc<AppState>>,
    Path((ws_id, job_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "cron.run", json!({ "jobId": job_id }))
        .await?;
    Ok(Json(payload))
}
