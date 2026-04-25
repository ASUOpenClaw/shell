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
// GET /api/workspaces/{ws_id}/cron
// ---------------------------------------------------------------------------
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
