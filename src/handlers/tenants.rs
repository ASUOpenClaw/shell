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
// GET /api/tenants
// ---------------------------------------------------------------------------
pub async fn list_tenants(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state.rpc_pool.call_admin("tenants.list", json!({})).await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// POST /api/tenants
// Body: {name, slug, settings?}
// ---------------------------------------------------------------------------
pub async fn create_tenant(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state.rpc_pool.call_admin("tenants.create", body).await?;
    Ok((StatusCode::CREATED, Json(payload)))
}

// ---------------------------------------------------------------------------
// PATCH /api/tenants/{id}
// Body: {name?, status?, settings?}
// ---------------------------------------------------------------------------
pub async fn update_tenant(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(mut body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_owned(), json!(id));
    }
    let payload = state.rpc_pool.call_admin("tenants.update", body).await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// GET /api/tenants/{id}/users
// ---------------------------------------------------------------------------
pub async fn list_tenant_users(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let payload = state
        .rpc_pool
        .call_admin("tenants.users.list", json!({ "tenant_id": id }))
        .await?;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------------
// POST /api/tenants/{id}/users
// Body: {user_id, role?}
// ---------------------------------------------------------------------------
pub async fn add_tenant_user(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(mut body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(obj) = body.as_object_mut() {
        obj.insert("tenant_id".to_owned(), json!(id));
    }
    let payload = state.rpc_pool.call_admin("tenants.users.add", body).await?;
    Ok((StatusCode::CREATED, Json(payload)))
}

// ---------------------------------------------------------------------------
// DELETE /api/tenants/{id}/users/{user_id}
// ---------------------------------------------------------------------------
pub async fn remove_tenant_user(
    State(state): State<Arc<AppState>>,
    Path((id, user_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    state
        .rpc_pool
        .call_admin(
            "tenants.users.remove",
            json!({ "tenant_id": id, "user_id": user_id }),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
