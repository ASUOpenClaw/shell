use std::{sync::Arc, time::Duration};

use axum::http::StatusCode;
use axum::{
    Router, middleware,
    routing::{delete, get, patch, post, put},
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
        agents::{create_agent, delete_agent, list_agents, set_agent_file, update_agent},
        cron::{create_cron, delete_cron, list_cron, run_cron, toggle_cron, update_cron},
        health::{health_handler, list_agents_handler},
        proxy::proxy_handler,
        sessions::{delete_session, list_all_sessions, list_user_sessions, reset_session},
        tenants::{
            add_tenant_user, create_tenant, list_tenant_users, list_tenants, remove_tenant_user,
            update_tenant,
        },
    },
    middleware::{
        auth::auth_middleware, logging::logging_middleware, rate_limit::rate_limit_middleware,
        service_auth::service_auth_middleware,
    },
    openapi::ApiDoc,
    state::AppState,
};

pub fn create_router(state: Arc<AppState>) -> Router {
    let timeout = Duration::from_millis(state.config.request_timeout_ms);
    let swagger_enabled = state.config.swagger_enabled;

    // ------------------------------------------------------------------
    // User-facing routes: JWT + X-Workspace-Id → rate limit → handler
    // ------------------------------------------------------------------
    let user_routes = Router::new()
        .route("/chat/completions", post(proxy_handler))
        .route("/sessions", get(list_user_sessions))
        .route("/sessions/{key}", delete(delete_session))
        .route("/sessions/{key}/reset", post(reset_session))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            rate_limit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth_middleware,
        ));

    // ------------------------------------------------------------------
    // Service-facing routes: X-Shell-Service-Key
    // ------------------------------------------------------------------
    let service_routes = Router::new()
        // Sessions (admin view)
        .route("/workspaces/{ws_id}/sessions", get(list_all_sessions))
        // Cron
        .route("/workspaces/{ws_id}/cron", get(list_cron).post(create_cron))
        .route(
            "/workspaces/{ws_id}/cron/{job_id}",
            patch(update_cron).delete(delete_cron),
        )
        .route(
            "/workspaces/{ws_id}/cron/{job_id}/toggle",
            post(toggle_cron),
        )
        .route("/workspaces/{ws_id}/cron/{job_id}/run", post(run_cron))
        // Agents
        .route(
            "/workspaces/{ws_id}/agents",
            get(list_agents).post(create_agent),
        )
        .route(
            "/workspaces/{ws_id}/agents/{agent_id}",
            patch(update_agent).delete(delete_agent),
        )
        .route(
            "/workspaces/{ws_id}/agents/{agent_id}/files/{file_name}",
            put(set_agent_file),
        )
        // Tenants
        .route("/tenants", get(list_tenants).post(create_tenant))
        .route("/tenants/{id}", patch(update_tenant))
        .route(
            "/tenants/{id}/users",
            get(list_tenant_users).post(add_tenant_user),
        )
        .route("/tenants/{id}/users/{user_id}", delete(remove_tenant_user))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            service_auth_middleware,
        ));

    let api: Router = Router::new()
        .route("/health", get(health_handler))
        .route("/admin/agents", get(list_agents_handler))
        .nest("/v1", user_routes)
        .nest("/api", service_routes)
        .with_state(state);

    let mut app = Router::new().merge(api);

    if swagger_enabled {
        app = app
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));
    }

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
