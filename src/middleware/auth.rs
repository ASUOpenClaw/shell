use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use tracing::warn;

use crate::{error::AppError, state::AppState};

/// Session data injected into request extensions after successful auth.
/// Downstream handlers and middleware extract this via `Extension<SessionData>`.
#[derive(Clone, Debug)]
pub struct SessionData {
    pub workspace_id: String,
    pub user_id: String,
}

/// JWT claims produced by the REST API (python-jose, HS256).
/// Only the fields we care about are decoded; extras are ignored.
#[derive(Deserialize)]
struct Claims {
    /// user UUID as string
    sub: String,
    /// token type: must be "access"
    #[serde(rename = "type")]
    token_type: String,
    // exp is validated automatically by jsonwebtoken
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = extract_bearer(req.headers())
        .ok_or_else(|| AppError::Unauthorized("missing Authorization header".to_string()))?;

    let workspace_id = req
        .headers()
        .get("x-workspace-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
        .ok_or_else(|| AppError::Unauthorized("missing X-Workspace-Id header".to_string()))?;

    let user_id = validate_jwt(token, &state.config.jwt_secret)?;

    req.extensions_mut().insert(SessionData {
        workspace_id,
        user_id,
    });

    Ok(next.run(req).await)
}

fn extract_bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn validate_jwt(token: &str, secret: &str) -> Result<String, AppError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|e| {
        warn!("JWT validation failed: {e}");
        AppError::Unauthorized("invalid or expired token".to_string())
    })?;

    if data.claims.token_type != "access" {
        warn!("non-access token type: {}", data.claims.token_type);
        return Err(AppError::Unauthorized("wrong token type".to_string()));
    }

    Ok(data.claims.sub)
}
