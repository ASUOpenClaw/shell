use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_host")]
    pub server_host: String,
    #[serde(default = "default_port")]
    pub server_port: u16,
    /// GoClaw gateway base URL (e.g. http://machine1:18790).
    /// Per-workspace API keys are loaded dynamically from Redis (ws_creds:{workspace_id}).
    pub goclaw_gateway_url: String,
    /// RSA public key PEM — must match the key pair used by the REST API.
    /// Shell only verifies tokens (never signs), so it needs the public key only.
    /// Env var: SHELL_JWT_PUBLIC_KEY (PEM content with literal newlines).
    pub jwt_public_key: String,
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
    #[serde(default = "default_nats_subject_prefix")]
    pub nats_subject_prefix: String,
    /// Serve Swagger UI at /swagger-ui. Disable in production.
    #[serde(default)]
    pub swagger_enabled: bool,
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
fn default_nats_subject_prefix() -> String {
    "conversation".to_string()
}

impl Config {
    pub fn load() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            // No separator: SHELL_GOCLAW_GATEWAY_URL -> goclaw_gateway_url (flat).
            // Using "_" as separator would split the key on every underscore,
            // turning GOCLAW_GATEWAY_URL into nested goclaw.gateway.url.
            .add_source(config::Environment::with_prefix("SHELL"))
            .build()?
            .try_deserialize()
    }
}
