use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};

use crate::{error::AppError, middleware::auth::SessionData, state::AppState};

// ---------------------------------------------------------------------------
// User-facing — GET /v1/sessions
// Returns only the calling user's own sessions (keys prefixed "user-{user_id}").
// ---------------------------------------------------------------------------
pub async fn list_user_sessions(
    State(state): State<Arc<AppState>>,
    Extension(session): Extension<SessionData>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&session.workspace_id, "sessions.list", json!({}))
        .await?;

    let all = extract_sessions_array(&payload);
    let prefix = format!("user-{}", session.user_id);
    let own: Vec<&Value> = all
        .iter()
        .filter(|s| {
            s["key"]
                .as_str()
                .or_else(|| s["sessionKey"].as_str())
                .map(|k| k.starts_with(&prefix))
                .unwrap_or(false)
        })
        .collect();

    Ok(Json(json!({ "sessions": own })))
}

// ---------------------------------------------------------------------------
// User-facing — DELETE /v1/sessions/{key}
// Only the session owner (key starts with "user-{user_id}") may delete.
// ---------------------------------------------------------------------------
pub async fn delete_session(
    State(state): State<Arc<AppState>>,
    Extension(session): Extension<SessionData>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_own_session(&session.user_id, &key)?;
    state
        .rpc_pool
        .call_workspace(
            &session.workspace_id,
            "sessions.delete",
            json!({ "key": key }),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// User-facing — POST /v1/sessions/{key}/reset
// Only the session owner may reset.
// ---------------------------------------------------------------------------
pub async fn reset_session(
    State(state): State<Arc<AppState>>,
    Extension(session): Extension<SessionData>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_own_session(&session.user_id, &key)?;
    let result = state
        .rpc_pool
        .call_workspace(
            &session.workspace_id,
            "sessions.reset",
            json!({ "key": key }),
        )
        .await?;
    Ok(Json(result))
}

// ---------------------------------------------------------------------------
// Service-facing — GET /api/workspaces/{ws_id}/sessions
// Returns ALL sessions in the workspace (admin view). Auth: X-Shell-Service-Key.
// ---------------------------------------------------------------------------
pub async fn list_all_sessions(
    State(state): State<Arc<AppState>>,
    Path(ws_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_workspace(&ws_id, "sessions.list", json!({}))
        .await?;
    Ok(Json(
        json!({ "sessions": extract_sessions_array(&payload) }),
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ensure_own_session(user_id: &str, session_key: &str) -> Result<(), AppError> {
    let prefix = format!("user-{user_id}");
    if !session_key.starts_with(&prefix) {
        return Err(AppError::Forbidden(
            "you can only access your own sessions".to_string(),
        ));
    }
    Ok(())
}

fn extract_sessions_array(payload: &Value) -> Vec<Value> {
    if let Some(arr) = payload.as_array() {
        return arr.clone();
    }
    payload["sessions"]
        .as_array()
        .or_else(|| payload["items"].as_array())
        .cloned()
        .unwrap_or_default()
}
