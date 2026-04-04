use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_host")]
    pub server_host: String,
    #[serde(default = "default_port")]
    pub server_port: u16,
    pub openclaw_gateway_url: String,
    /// Bearer token the proxy injects when forwarding requests to the gateway.
    /// Maps to OPENCLAW_GATEWAY_TOKEN on the gateway host.
    pub openclaw_gateway_token: String,
    /// Default OpenClaw agent ID to use when no workspace-specific mapping exists.
    #[serde(default = "default_agent")]
    pub openclaw_default_agent: String,
    /// Optional workspace→agent overrides: "ws1:agentA,ws2:agentB"
    #[serde(default)]
    pub openclaw_agent_map: String,
    /// Shared HS256 secret — must match SECRET_KEY in the REST API.
    pub jwt_secret: String,
    pub redis_url: String,
    #[serde(default = "default_pool_size")]
    pub redis_pool_size: usize,
    #[serde(default = "default_rate_limit")]
    pub rate_limit_rps: u32,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub nats_url: String,
    #[serde(default = "default_nats_stream")]
    pub nats_stream: String,
    #[serde(default = "default_nats_subject_prefix")]
    pub nats_subject_prefix: String,
    /// Serve Swagger UI at /swagger-ui. Disable in production.
    #[serde(default)]
    pub swagger_enabled: bool,
}

fn default_agent() -> String {
    "main".to_string()
}
fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}
fn default_pool_size() -> usize {
    10
}
fn default_rate_limit() -> u32 {
    60
}
fn default_timeout() -> u64 {
    30_000
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_nats_stream() -> String {
    "conversations".to_string()
}
fn default_nats_subject_prefix() -> String {
    "conversation".to_string()
}

impl Config {
    pub fn load() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            // No separator: SHELL_OPENCLAW_GATEWAY_URL -> openclaw_gateway_url (flat).
            // Using "_" as separator would split the key on every underscore,
            // turning OPENCLAW_GATEWAY_URL into nested openclaw.gateway.url.
            .add_source(config::Environment::with_prefix("SHELL"))
            .build()?
            .try_deserialize()
    }
}
