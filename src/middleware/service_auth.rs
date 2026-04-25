use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use crate::{error::AppError, state::AppState};

/// Middleware that validates the `X-Shell-Service-Key` header for internal
/// service-to-service endpoints (REST → Shell). Injects nothing into extensions —
/// callers just need to know the request is authenticated as a service.
pub async fn service_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let key = req
        .headers()
        .get("x-shell-service-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if state.config.service_key.is_empty() || key != state.config.service_key {
        return Err(AppError::Unauthorized(
            "missing or invalid X-Shell-Service-Key".to_string(),
        ));
    }

    Ok(next.run(req).await)
}
