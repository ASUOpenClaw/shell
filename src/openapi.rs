use utoipa::OpenApi;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme};

use crate::handlers::{
    agents::{CreateAgentRequest, SetAgentFileRequest, UpdateAgentRequest},
    cron::{CreateCronJobRequest, ToggleCronJobRequest, UpdateCronJobRequest},
    health::HealthResponse,
    proxy::{ChatCompletionRequest, ChatMessage},
    tenants::{AddTenantUserRequest, CreateTenantRequest, UpdateTenantRequest},
};

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "BearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
        components.add_security_scheme(
            "ServiceKey",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("X-Shell-Service-Key"))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Shell — OpenClaw WS Bridge",
        version = "0.2.0",
        description = "WS RPC bridge between clients and the GoClaw agent gateway.\n\n\
                       **User-facing routes** (`/v1/*`) require:\n\
                       - `Authorization: Bearer <JWT>` (RS256, issued by REST API)\n\
                       - `X-Workspace-Id: <workspace_uuid>` header\n\n\
                       **Service-facing routes** (`/api/*`) require:\n\
                       - `X-Shell-Service-Key: <shared secret>` header"
    ),
    paths(
        // Health
        crate::handlers::health::health_handler,
        crate::handlers::health::list_agents_handler,
        // Chat proxy
        crate::handlers::proxy::proxy_handler,
        // Sessions — user-facing
        crate::handlers::sessions::list_user_sessions,
        crate::handlers::sessions::delete_session,
        crate::handlers::sessions::reset_session,
        // Sessions — service-facing
        crate::handlers::sessions::list_all_sessions,
        // Cron — service-facing
        crate::handlers::cron::list_cron,
        crate::handlers::cron::create_cron,
        crate::handlers::cron::update_cron,
        crate::handlers::cron::delete_cron,
        crate::handlers::cron::toggle_cron,
        crate::handlers::cron::run_cron,
        // Agents — service-facing
        crate::handlers::agents::list_agents,
        crate::handlers::agents::create_agent,
        crate::handlers::agents::update_agent,
        crate::handlers::agents::delete_agent,
        crate::handlers::agents::set_agent_file,
        // Tenants — service-facing
        crate::handlers::tenants::list_tenants,
        crate::handlers::tenants::create_tenant,
        crate::handlers::tenants::update_tenant,
        crate::handlers::tenants::list_tenant_users,
        crate::handlers::tenants::add_tenant_user,
        crate::handlers::tenants::remove_tenant_user,
    ),
    components(schemas(
        HealthResponse,
        ChatMessage,
        ChatCompletionRequest,
        CreateCronJobRequest,
        UpdateCronJobRequest,
        ToggleCronJobRequest,
        CreateAgentRequest,
        SetAgentFileRequest,
        UpdateAgentRequest,
        CreateTenantRequest,
        UpdateTenantRequest,
        AddTenantUserRequest,
    )),
    tags(
        (name = "health",   description = "Health and readiness"),
        (name = "admin",    description = "Admin info"),
        (name = "proxy",    description = "Chat completions — proxied to the workspace GoClaw agent (user-facing, JWT auth)"),
        (name = "sessions", description = "Session management — user-facing (JWT) and admin (service key)"),
        (name = "cron",     description = "Cron job management (service key)"),
        (name = "agents",   description = "Agent CRUD (service key)"),
        (name = "tenants",  description = "Tenant management — admin connection (service key)"),
    ),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;
