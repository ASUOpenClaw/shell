use utoipa::OpenApi;

use crate::handlers::health::HealthResponse;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Shell — OpenClaw Proxy",
        version = "0.1.0",
        description = "Lightweight proxy that validates sessions, rate-limits, \
                       routes to OpenClaw gateway agents, and publishes conversation \
                       history to NATS."
    ),
    paths(
        crate::handlers::health::health_handler,
        crate::handlers::health::list_agents_handler,
        crate::handlers::proxy::proxy_handler,
    ),
    components(schemas(HealthResponse)),
    tags(
        (name = "health",  description = "Health and readiness endpoints"),
        (name = "admin",   description = "Agent resolver configuration"),
        (name = "proxy",   description = "Proxied requests to OpenClaw gateway"),
    )
)]
pub struct ApiDoc;
