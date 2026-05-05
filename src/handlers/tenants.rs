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

/// Create a new GoClaw tenant.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    /// Human-readable tenant name.
    pub name: String,
    /// URL-safe slug identifier.
    pub slug: String,
}

/// Partial update for an existing tenant.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    pub name: Option<String>,
    /// Tenant status, e.g. `"active"` or `"suspended"`.
    pub status: Option<String>,
}

/// Add a user to a tenant.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct AddTenantUserRequest {
    /// User ID to add.
    pub user_id: String,
    /// Role within the tenant (optional, defaults to tenant default).
    pub role: Option<String>,
}

// ---------------------------------------------------------------------------
// GET /api/tenants
// ---------------------------------------------------------------------------
#[utoipa::path(
    get,
    path = "/api/tenants",
    tag = "tenants",
    security(("ServiceKey" = [])),
    responses(
        (status = 200, description = "List of all GoClaw tenants"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    post,
    path = "/api/tenants",
    tag = "tenants",
    security(("ServiceKey" = [])),
    request_body = CreateTenantRequest,
    responses(
        (status = 201, description = "Tenant created"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    patch,
    path = "/api/tenants/{id}",
    tag = "tenants",
    security(("ServiceKey" = [])),
    params(("id" = String, Path, description = "Tenant ID")),
    request_body = UpdateTenantRequest,
    responses(
        (status = 200, description = "Tenant updated"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    get,
    path = "/api/tenants/{id}/users",
    tag = "tenants",
    security(("ServiceKey" = [])),
    params(("id" = String, Path, description = "Tenant ID")),
    responses(
        (status = 200, description = "Users belonging to the tenant"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    post,
    path = "/api/tenants/{id}/users",
    tag = "tenants",
    security(("ServiceKey" = [])),
    params(("id" = String, Path, description = "Tenant ID")),
    request_body = AddTenantUserRequest,
    responses(
        (status = 201, description = "User added to tenant"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
#[utoipa::path(
    delete,
    path = "/api/tenants/{id}/users/{user_id}",
    tag = "tenants",
    security(("ServiceKey" = [])),
    params(
        ("id" = String, Path, description = "Tenant ID"),
        ("user_id" = String, Path, description = "User ID to remove"),
    ),
    responses(
        (status = 204, description = "User removed from tenant"),
        (status = 401, description = "Missing or invalid X-Shell-Service-Key"),
    )
)]
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
