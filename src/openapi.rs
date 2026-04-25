use utoipa::OpenApi;

use crate::handlers::health::HealthResponse;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Shell — OpenClaw WS Bridge",
        version = "0.2.0",
        description = "WS RPC bridge to GoClaw. Exposes chat, session management, \
                       agent CRUD, cron, and tenant APIs over HTTP."
    ),
    paths(
        crate::handlers::health::health_handler,
        crate::handlers::health::list_agents_handler,
        crate::handlers::proxy::proxy_handler,
    ),
    components(schemas(HealthResponse)),
    tags(
        (name = "health",   description = "Health and readiness"),
        (name = "admin",    description = "Admin info"),
        (name = "proxy",    description = "Chat completions (user-facing)"),
        (name = "sessions", description = "Session management"),
        (name = "cron",     description = "Cron job management"),
        (name = "agents",   description = "Agent CRUD"),
        (name = "tenants",  description = "Tenant management"),
    )
)]
pub struct ApiDoc;
