You are going to build a Rust proxy service that routes requests to an OpenClaw gateway
and manages per-workspace session isolation. Build this step by step,
one module at a time, running and testing before moving to the next step.

## Project Overview

OpenClaw is a self-hosted AI agent framework. It runs as a single **gateway** process
that exposes OpenAI-compatible HTTP endpoints (`/v1/chat/completions`, `/v1/models`, etc.)
on its port (default 18789).

**Key facts about the OpenClaw gateway (from official docs):**

- **Agents are pre-configured**, not spawned at runtime. They live in `~/.openclaw/openclaw.json`
  under `agents.list[].id`. There is no REST API to create or destroy agents.
- **HTTP is the inference protocol.** The WebSocket interface is the control plane used by
  the CLI and the browser control UI — it is NOT needed for proxying inference requests.
- **Agent targeting** is done via the OpenAI `model` field: `openclaw/<agentId>` routes to a
  specific pre-configured agent. `openclaw/default` or `openclaw` routes to the default agent.
  Alternatively use the `x-openclaw-agent-id: <agentId>` header.
- **Session isolation** across requests is achieved via `x-openclaw-session-key: <key>`.
  Requests sharing the same session key are part of the same conversation thread.
- **Gateway authentication**: the gateway itself requires `Authorization: Bearer <OPENCLAW_GATEWAY_TOKEN>`.
  The proxy must inject this token when forwarding requests upstream.
- **Trusted-proxy auth mode**: the gateway can be configured (`gateway.auth.mode = "trusted-proxy"`)
  to trust a specific proxy IP and read user identity from a header (e.g. `x-forwarded-user`).

This Rust proxy sits between clients (FastAPI backend) and the OpenClaw gateway.
Its responsibilities are:
1. Validate client Bearer tokens against Redis sessions
2. Rate limit per workspace
3. Map workspace_id → agent_id (from config), inject `x-openclaw-session-key` for conversation continuity
4. Inject gateway auth token when forwarding upstream
5. Publish all request/response payloads to NATS JetStream for conversation history

Since this proxy is the **only architectural point through which all messages pass**,
it is also responsible for publishing conversation history to NATS JetStream. A separate
`dumper` microservice subscribes to those subjects and persists messages to Postgres
(or any other destination).

## Architecture

```
FastAPI Backend
      │
      ▼
[Rust Proxy :8080]
      │  validates Bearer → Redis session
      │  rate limits per workspace → Redis
      │  maps workspace → agent (config)
      │  injects x-openclaw-session-key
      │  injects Authorization: Bearer <OPENCLAW_GATEWAY_TOKEN>
      │  publishes req/resp → NATS JetStream
      │
      ▼
[OpenClaw Gateway :18789]          [NATS JetStream]
  POST /v1/chat/completions              │
  GET  /v1/models                  [dumper service]
  POST /v1/embeddings                    │
  POST /v1/responses               [Postgres / other]
```

## What the proxy does NOT do

- **Does NOT spawn or terminate agents** — agents are pre-configured in the gateway
- **Does NOT use WebSocket** — only the control plane (CLI/UI) uses WebSocket;
  inference is plain HTTP
- **Does NOT buffer full streaming responses** — SSE/streaming responses are teed:
  forwarded to the client while a copy is sent to NATS after the stream closes

## Tech Stack

- `tokio` — async runtime
- `axum` — HTTP server
- `reqwest` (rustls-tls) — HTTP client to OpenClaw gateway
- `deadpool-redis` — Redis connection pool
- `tower` + `tower-http` — middleware (logging, timeout, trace)
- `serde` / `serde_json` — serialization
- `tracing` + `tracing-subscriber` — structured logging
- `config` — configuration from env + TOML
- `thiserror` — error types
- `uuid` — request IDs
- `async-nats` — NATS JetStream client for conversation history publishing
- `chrono` — timestamps on published messages
- `utoipa` + `utoipa-swagger-ui` — optional Swagger UI

---

## Step 1 — Project Scaffold

Create a new Rust workspace project:

```
shell/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── config.rs
    ├── error.rs
    ├── state.rs
    ├── router.rs
    ├── agents/
    │   ├── mod.rs
    │   └── resolver.rs       ← replaces spawner.rs; workspace→agent mapping
    ├── middleware/
    │   ├── mod.rs
    │   ├── auth.rs
    │   └── rate_limit.rs
    ├── nats/
    │   ├── mod.rs
    │   └── publisher.rs
    └── handlers/
        ├── mod.rs
        ├── proxy.rs
        └── health.rs
```

Set up Cargo.toml with all dependencies. Use latest stable versions.
Build and verify it compiles before proceeding.

---

## Step 2 — Configuration (`config.rs`)

Implement a `Config` struct loaded from environment variables.
Fields:

```rust
pub struct Config {
    pub server_host: String,              // default: "0.0.0.0"
    pub server_port: u16,                 // default: 8080
    pub openclaw_gateway_url: String,     // e.g. "http://localhost:18789"
    pub openclaw_gateway_token: String,   // Bearer token for gateway auth (OPENCLAW_GATEWAY_TOKEN)
    pub openclaw_default_agent: String,   // default agent id, default: "main"
    // Workspace→agent overrides: comma-separated "ws1:agentA,ws2:agentB"
    // If a workspace_id is not in the map, openclaw_default_agent is used.
    pub openclaw_agent_map: String,       // default: ""
    pub redis_url: String,                // e.g. "redis://localhost:6379"
    pub redis_pool_size: usize,           // default: 10
    pub session_ttl_secs: u64,            // default: 3600
    pub rate_limit_rps: u32,              // requests per second per workspace
    pub request_timeout_ms: u64,          // default: 30000
    pub log_level: String,                // default: "info"
    // NATS
    pub nats_url: String,                 // e.g. "nats://localhost:4222"
    pub nats_stream: String,              // JetStream stream name, default: "conversations"
    pub nats_subject_prefix: String,      // default: "conversation"
    // Swagger UI
    pub swagger_enabled: bool,            // default: false
}
```

Use the `config` crate with env variable overrides (prefix `SHELL`).
Parse `openclaw_agent_map` into a `HashMap<String, String>` after loading.

---

## Step 3 — Error Types (`error.rs`)

Define a unified `AppError` enum using `thiserror`:

```rust
pub enum AppError {
    Unauthorized(String),
    GatewayError(String),
    RateLimitExceeded,
    RedisError(#[from] deadpool_redis::PoolError),
    NatsError(String),
    Internal(String),
}
```

Implement `IntoResponse` for `AppError`:
- `Unauthorized` → 401
- `RateLimitExceeded` → 429
- `GatewayError` → 502
- Everything else → 500

Note: NATS publish failures must NOT fail the request — log and continue.

---

## Step 4 — App State (`state.rs`)

```rust
pub struct AppState {
    pub config: Config,
    pub redis_pool: deadpool_redis::Pool,
    pub agent_resolver: AgentResolver,
    pub http_client: reqwest::Client,
    pub nats_publisher: NatsPublisher,
}
```

Initialize in `main.rs`:
- Build Redis pool with `deadpool-redis`
- Build `reqwest::Client` with connection pooling and default timeout
- Build `AgentResolver` from config
- Connect `NatsPublisher`

---

## Step 5 — Agent Resolver (`agents/resolver.rs`)

Replaces the old `AgentSpawner` + `AgentRegistry`. Agents are pre-configured in the
OpenClaw gateway — this module simply maps a `workspace_id` to the correct `agent_id`.

```rust
/// Maps workspace IDs to pre-configured OpenClaw agent IDs.
/// Falls back to the default agent if no explicit mapping exists.
pub struct AgentResolver {
    /// workspace_id → agent_id overrides loaded from config
    map: HashMap<String, String>,
    default_agent: String,
}

impl AgentResolver {
    pub fn new(map: HashMap<String, String>, default_agent: String) -> Self

    /// Returns the agent_id for a workspace.
    /// If no explicit mapping exists, returns the default agent id.
    pub fn resolve(&self, workspace_id: &str) -> &str
}
```

No HTTP calls, no async, no dynamic state. Pure config lookup.

---

## Step 6 — Auth Middleware (`middleware/auth.rs`)

Implement session validation middleware that reads from Redis:

Flow:
1. Extract `Authorization: Bearer <token>` header
2. Look up `session:{token}` key in Redis
3. If missing → 401
4. Deserialize session: `{ workspace_id, user_id, expires_at }`
5. Check expiry
6. Inject `workspace_id` and `user_id` into request extensions

```rust
pub struct SessionData {
    pub workspace_id: String,
    pub user_id: String,
    pub expires_at: u64,
}
```

Use `axum::middleware::from_fn_with_state` pattern.

---

## Step 7 — Rate Limiter (`middleware/rate_limit.rs`)

Per-workspace sliding window rate limiting using Redis INCR + EXPIRE:

```
Key: ratelimit:{workspace_id}:{window_second}
INCR key
EXPIRE key 2
If value > config.rate_limit_rps → return 429
```

The middleware reads `workspace_id` from request extensions (set by auth middleware).

---

## Step 8 — NATS Publisher (`nats/publisher.rs`)

Wraps `async-nats` JetStream context. Publish failures must never propagate — log and drop.

```rust
#[derive(Serialize)]
pub struct ConversationMessage {
    pub message_id: String,          // uuid v4
    pub workspace_id: String,
    pub user_id: String,
    pub direction: MessageDirection, // Request | Response
    pub body: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

pub struct NatsPublisher { ... }

impl NatsPublisher {
    pub async fn connect(nats_url: &str, stream: &str, subject_prefix: &str)
        -> Result<Self, AppError>

    /// Fire-and-forget. Subject: "{prefix}.{workspace_id}"
    pub fn publish(&self, msg: ConversationMessage)
}
```

---

## Step 9 — Proxy Handler (`handlers/proxy.rs`)

This is the core handler:

1. Read `workspace_id` and `user_id` from request extensions (set by auth middleware)
2. Resolve agent_id: `state.agent_resolver.resolve(&workspace_id)`
3. Build upstream URL: `{gateway_url}/v1/{tail}` where tail is the path after `/v1`
4. Build upstream headers:
   - Copy all inbound headers except hop-by-hop (`Host`, `Connection`, `Transfer-Encoding`, etc.)
   - **Replace** `Authorization` with `Bearer <config.openclaw_gateway_token>`
   - Set `x-openclaw-session-key: workspace_{workspace_id}` for session continuity
   - Set `x-openclaw-agent-id: {agent_id}` to target the correct pre-configured agent
5. Buffer the request body (cap 10 MB), publish to NATS (direction=Request)
6. Forward request to upstream gateway
7. Stream the upstream response back to the client:
   - Tee the response body: each chunk goes to client AND to an mpsc channel
   - Spawned task drains the channel; when stream closes, publishes to NATS (direction=Response)
   - SSE responses: parse and publish each `data:` event individually

```rust
pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    Extension(session): Extension<SessionData>,
    req: Request<Body>,
) -> Result<Response, AppError>
```

**Important**: Never buffer the full SSE/streaming response. Tee the stream.

---

## Step 10 — Health & Admin Handlers (`handlers/health.rs`)

```rust
// GET /health
// Returns: { status: "ok", redis: "ok"|"error", nats: "ok"|"error" }
pub async fn health_handler(State(state): State<Arc<AppState>>) -> Json<HealthResponse>

// GET /admin/agents
// Returns the resolver's agent map (workspace → agent_id)
pub async fn list_agents_handler(State(state): State<Arc<AppState>>) -> Json<serde_json::Value>
```

---

## Step 11 — Router (`router.rs`)

```rust
pub fn create_router(state: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/*path", any(proxy_handler))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit_middleware))
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    let admin = Router::new()
        .route("/admin/agents", get(list_agents_handler));

    Router::new()
        .route("/health", get(health_handler))
        .merge(admin)
        .nest("/v1", protected)
        .layer(TraceLayer::new_for_http()...)
        .layer(TimeoutLayer::new(...))
        .with_state(state)
}
```

---

## Step 12 — Main (`main.rs`)

```rust
#[tokio::main]
async fn main() {
    // 1. Load config
    // 2. Init tracing subscriber
    // 3. Connect NatsPublisher
    // 4. Build AppState (redis pool, http client, agent_resolver, nats_publisher)
    // 5. Create router
    // 6. Bind and serve with graceful shutdown on SIGTERM/SIGINT
}
```

---

## Step 13 — Integration Test

Write a basic integration test in `tests/integration_test.rs`:
- Mock OpenClaw gateway using `wiremock`
- Mock Redis using a real Redis in Docker or `redis-mock`
- Use embedded NATS server
- Test the full request flow:
  1. Valid session → agent resolved from config → request proxied (with correct
     `x-openclaw-session-key` and `x-openclaw-agent-id` headers) → response returned
     → ConversationMessage published to NATS (both Request and Response)
  2. Invalid session → 401, nothing published to NATS
  3. Rate limit exceeded → 429, nothing published
  4. NATS unavailable → request still proxied successfully

---

## Implementation Rules

1. **Build step by step** — after each step, run `cargo check` and `cargo clippy`
2. **No panics** — use `?` and `Result` everywhere, never `.unwrap()` in production paths
3. **No blocking in async context** — never use `std::thread::sleep`, use `tokio::time::sleep`
4. **Streaming is mandatory** — proxy handler must stream, never buffer full LLM responses
5. **Structured logging** — use `tracing::info!`, `tracing::error!` with field syntax
6. **Minimal Redis reads** — only auth and rate limit touch Redis
7. **Gateway token is required** — always inject `Authorization: Bearer <OPENCLAW_GATEWAY_TOKEN>`
   when forwarding to the gateway; never forward the client's session token upstream
8. **Session key isolation** — always set `x-openclaw-session-key: workspace_{workspace_id}`;
   this ensures each workspace has an isolated conversation thread on the gateway
9. **NATS failures are non-fatal** — log with `tracing::error!` and continue
10. **Request body buffering cap** — enforce 10 MB max; if exceeded, skip NATS publish and log

---

## Key Env Vars (updated)

```
SHELL_OPENCLAW_GATEWAY_URL=http://host.docker.internal:18789
SHELL_OPENCLAW_GATEWAY_TOKEN=<token from OPENCLAW_GATEWAY_TOKEN on the gateway host>
SHELL_OPENCLAW_DEFAULT_AGENT=main
SHELL_OPENCLAW_AGENT_MAP=            # optional: "ws1:agentA,ws2:agentB"
SHELL_REDIS_URL=redis://redis:6379
SHELL_NATS_URL=nats://nats:4222
SHELL_RATE_LIMIT_RPS=60
SHELL_SESSION_TTL_SECS=3600
SHELL_REQUEST_TIMEOUT_MS=30000
SHELL_LOG_LEVEL=info
SHELL_SWAGGER_ENABLED=false
```

---

## What Changed vs. Original Plan (Summary)

| Original assumption | Reality (from docs) |
|---|---|
| Agents are spawned via `POST /agents` | Agents are pre-configured in gateway config |
| Proxy URL: `{gateway}/agents/{id}/{path}` | Proxy URL: `{gateway}/v1/{path}` |
| No gateway auth needed | Gateway requires `Authorization: Bearer <token>` |
| AgentSpawner + AgentRegistry | AgentResolver (simple config map, no HTTP) |
| WebSocket needed for inference | HTTP only; WebSocket is control plane only |
| Client token forwarded upstream | Client token consumed by proxy; gateway token injected |
| Agent targeting via spawn | Agent targeting via `x-openclaw-agent-id` header |
| No session continuity | Use `x-openclaw-session-key: workspace_{id}` |
