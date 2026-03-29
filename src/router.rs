use std::{sync::Arc, time::Duration};

use axum::http::StatusCode;
use axum::{
    Router, middleware,
    routing::{any, get},
};
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    handlers::{
        health::{health_handler, list_agents_handler},
        proxy::proxy_handler,
    },
    middleware::{
        auth::auth_middleware, logging::logging_middleware, rate_limit::rate_limit_middleware,
    },
    openapi::ApiDoc,
    state::AppState,
};

pub fn create_router(state: Arc<AppState>) -> Router {
    let timeout = Duration::from_millis(state.config.request_timeout_ms);
    let swagger_enabled = state.config.swagger_enabled;

    // Protected proxy routes: auth → rate_limit → proxy.
    // Layers are applied bottom-up in axum, so auth runs first.
    let proxy = Router::new()
        .route("/{*path}", any(proxy_handler))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            rate_limit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth_middleware,
        ));

    let admin = Router::new().route("/admin/agents", get(list_agents_handler));

    // Resolve AppState here so we end up with Router<()>.
    // This lets us merge swagger (which is also Router<()>) without a type mismatch.
    let api: Router = Router::new()
        .route("/health", get(health_handler))
        .merge(admin)
        .nest("/v1", proxy)
        .with_state(state);

    let mut app = Router::new().merge(api);

    if swagger_enabled {
        app = app
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));
    }

    // Global layers applied after merging — wrap every route including swagger.
    app.layer(middleware::from_fn(logging_middleware))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            timeout,
        ))
}
