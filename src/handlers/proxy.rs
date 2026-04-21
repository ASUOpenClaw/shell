use std::sync::Arc;

use axum::body::Bytes;
use axum::{
    Extension,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    response::Response,
};
use deadpool_redis::redis::AsyncCommands;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    agents::resolver::load_workspace_creds,
    error::AppError,
    middleware::auth::SessionData,
    nats::publisher::{ConversationMessage, MessageDirection, NatsPublisher},
    state::AppState,
};

#[utoipa::path(
    post,
    path = "/v1/{path}",
    tag = "proxy",
    params(("path" = String, Path, description = "Path forwarded to the GoClaw gateway")),
    responses(
        (status = 200, description = "Streamed response from GoClaw gateway"),
        (status = 401, description = "Unauthorized"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 502, description = "Gateway error"),
    )
)]
pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    Extension(session): Extension<SessionData>,
    req: Request<Body>,
) -> Result<Response, AppError> {
    // -----------------------------------------------------------------------
    // 1. Load per-workspace GoClaw credentials from Redis.
    // -----------------------------------------------------------------------
    let creds = load_workspace_creds(&state.redis_pool, &session.workspace_id).await?;

    // Extract or generate a request correlation ID for distributed tracing.
    // Forwarded downstream so GoClaw and MCP logs can be correlated.
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    info!(
        workspace_id = %session.workspace_id,
        agent_id = %creds.agent_id,
        request_id = %request_id,
        "routing request"
    );

    // -----------------------------------------------------------------------
    // 2. Build upstream URL.
    //    /v1/chat/completions → {gateway}/v1/chat/completions
    //    The router nests this handler under /v1, so the incoming path already
    //    has the /v1 prefix stripped by axum. Re-add it for the upstream call.
    // -----------------------------------------------------------------------
    let original_path = req.uri().path();
    // original_path here is the tail after /v1 (e.g. "/chat/completions")
    let upstream_url = format!("{}/v1{}", state.config.goclaw_gateway_url, original_path);
    let upstream_url = match req.uri().query() {
        Some(q) => format!("{upstream_url}?{q}"),
        None => upstream_url,
    };

    // -----------------------------------------------------------------------
    // 3. Capture method + headers before consuming the request.
    // -----------------------------------------------------------------------
    let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
        .map_err(|e| AppError::Internal(format!("invalid method: {e}")))?;

    let fwd_headers = build_upstream_headers(
        req.headers(),
        &creds.api_key,
        &session.user_id,
        &creds.agent_id,
        &request_id,
    );

    // -----------------------------------------------------------------------
    // 4. Buffer request body (capped at 10 MB) for NATS publish + forwarding.
    // -----------------------------------------------------------------------
    let raw_body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read request body: {e}")))?;

    // -----------------------------------------------------------------------
    // 4b. Inject MCP workspace context into the messages array and rewrite
    //     the model field to "agent:{agent_key}" so GoClaw routes correctly.
    //     Redis failure → 503: better to fail fast than silently forward
    //     a request where MCP tool calls will all fail with "invalid token".
    // -----------------------------------------------------------------------
    let body_bytes = inject_mcp_context(
        &state,
        &session.workspace_id,
        &session.user_id,
        &creds.agent_key,
        &creds.mcp_service_token,
        raw_body_bytes,
    )
    .await?;

    // Publish request to NATS — fire-and-forget.
    state.nats_publisher.publish(ConversationMessage::new(
        &session.workspace_id,
        &session.user_id,
        MessageDirection::Request,
        bytes_to_json(&body_bytes),
    ));

    // -----------------------------------------------------------------------
    // 5. Forward to upstream gateway.
    // -----------------------------------------------------------------------
    let upstream_resp = state
        .http_client
        .request(method, &upstream_url)
        .headers(fwd_headers)
        .body(body_bytes.to_vec())
        .send()
        .await
        .map_err(|e| AppError::GatewayError(e.to_string()))?;

    // -----------------------------------------------------------------------
    // 6. Map upstream status + headers into the outbound response.
    // -----------------------------------------------------------------------
    let status = StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut response_builder = Response::builder().status(status);
    let is_sse = is_sse_response(upstream_resp.headers());

    for (name, value) in upstream_resp.headers() {
        response_builder = response_builder.header(name, value);
    }

    // -----------------------------------------------------------------------
    // 7. Tee the response stream to client + NATS collector task.
    // -----------------------------------------------------------------------
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();

    let tee_stream = upstream_resp.bytes_stream().map(move |chunk| {
        if let Ok(ref bytes) = chunk {
            let _ = tx.send(bytes.clone());
        }
        chunk
    });

    spawn_nats_collector(
        Arc::clone(&state),
        session.workspace_id.clone(),
        session.user_id.clone(),
        rx,
        is_sse,
    );

    response_builder
        .body(Body::from_stream(tee_stream))
        .map_err(|e| {
            error!(error = %e, "failed to build response");
            AppError::Internal(e.to_string())
        })
}

// ---------------------------------------------------------------------------
// MCP context injection
// ---------------------------------------------------------------------------

/// Get or create a stable MCP context token for this (workspace, user) session,
/// write/refresh it in Redis, and inject a system message into the `messages`
/// array so the model knows to pass it in every tool call.
///
/// Token is stable per session: reused across requests, TTL reset on each one.
/// This prevents mid-conversation expiry — the token stays valid as long as
/// the user keeps chatting (each request resets the 300s clock).
///
/// Redis errors propagate as AppError — fail fast rather than silently forward
/// a request where every MCP tool call would fail with "invalid token".
async fn inject_mcp_context(
    state: &AppState,
    workspace_id: &str,
    user_id: &str,
    agent_key: &str,
    mcp_service_token: &str,
    body: Bytes,
) -> Result<Bytes, AppError> {
    // Only modify JSON bodies that have a "messages" field.
    let mut parsed: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return Ok(body),
    };

    // Override model to "agent:{agent_key}" so GoClaw routes to the correct agent.
    if !agent_key.is_empty() {
        parsed["model"] = Value::String(format!("agent:{agent_key}"));
    }

    let messages = match parsed.get_mut("messages").and_then(|m| m.as_array_mut()) {
        Some(arr) => arr,
        None => return Ok(body),
    };

    let mut conn = state.redis_pool.get().await.map_err(AppError::RedisPool)?;

    // Reuse existing session token if present; mint new one otherwise.
    // Either way, reset the TTL so the token stays alive while the user is active.
    let session_key = format!("mcp_session:{workspace_id}:{user_id}");
    let existing_token: Option<String> = conn
        .get::<_, Option<String>>(&session_key)
        .await
        .map_err(AppError::RedisCmd)?;
    let is_new_session = existing_token.is_none();
    let token = existing_token.unwrap_or_else(|| Uuid::new_v4().to_string());

    let ctx_json = json!({
        "workspace_id": workspace_id,
        "user_id": user_id,
    })
    .to_string();

    // Refresh both the session pointer and the ctx payload.
    let _: () = conn
        .set_ex(&session_key, &token, 300_u64)
        .await
        .map_err(AppError::RedisCmd)?;
    let _: () = conn
        .set_ex(format!("mcp_ctx:{token}"), &ctx_json, 300_u64)
        .await
        .map_err(AppError::RedisCmd)?;

    // Skills are injected only at session start — they live in conversation history
    // from that point on. GoClaw maintains history server-side, so re-injecting on
    // every message would duplicate skills in the context window every turn.
    let skills_prefix = if is_new_session {
        let skills: Option<String> = conn
            .get(format!("ws_skills:{workspace_id}"))
            .await
            .ok()
            .flatten();
        match skills {
            Some(s) if !s.is_empty() => format!("\n\n{s}"),
            _ => String::new(),
        }
    } else {
        String::new()
    };

    // Load workspace files and append to system message so the agent knows they exist.
    let files_suffix = build_files_suffix(&mut conn, workspace_id).await;

    // Build the system message content.
    let service_line = if !mcp_service_token.is_empty() {
        format!("\n[SERVICE_CTX: mcp_service={mcp_service_token}] For scheduled/automated tasks (cron, background actions) pass this token as ctx_token instead.")
    } else {
        String::new()
    };
    let ctx_msg = format!(
        "[WORKSPACE_CTX: mcp_ctx={token}] Always pass this exact token as ctx_token in every tool call. Never modify it.{service_line}{skills_prefix}{files_suffix}"
    );

    // If the first message is a system message, append to its content.
    // Otherwise prepend a new system message.
    if messages
        .first()
        .and_then(|m| m.get("role"))
        .and_then(|r| r.as_str())
        == Some("system")
    {
        if let Some(content) = messages[0]
            .get_mut("content")
            .and_then(|c| c.as_str().map(|s| s.to_owned()))
        {
            messages[0]["content"] = Value::String(format!("{content}\n{ctx_msg}"));
        }
    } else {
        messages.insert(0, json!({"role": "system", "content": ctx_msg}));
    }

    match serde_json::to_vec(&parsed) {
        Ok(modified) => Ok(Bytes::from(modified)),
        Err(e) => {
            error!("failed to re-serialize request body: {e}");
            Ok(body)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build upstream headers from the inbound request:
/// - Drop hop-by-hop headers
/// - Replace Authorization with the workspace-bound GoClaw API key
/// - Add X-GoClaw-User-Id (per-user session isolation) and X-GoClaw-Agent-Id (agent routing)
fn build_upstream_headers(
    inbound: &HeaderMap,
    api_key: &str,
    user_id: &str,
    agent_id: &str,
    request_id: &str,
) -> reqwest::header::HeaderMap {
    use axum::http::header;

    let mut out = reqwest::header::HeaderMap::new();

    for (name, value) in inbound {
        if matches!(
            name,
            &header::HOST
                | &header::CONNECTION
                | &header::TRANSFER_ENCODING
                | &header::TE
                | &header::TRAILER
                | &header::UPGRADE
                | &header::AUTHORIZATION // replaced below
                | &header::CONTENT_LENGTH // reqwest sets correct value from body
        ) {
            continue;
        }
        if let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_ref())
            && let Ok(value) = reqwest::header::HeaderValue::from_bytes(value.as_bytes())
        {
            out.insert(name, value);
        }
    }

    // Inject workspace-bound GoClaw API key (tenant resolved automatically from key).
    if let Ok(auth_value) = reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}")) {
        out.insert(reqwest::header::AUTHORIZATION, auth_value);
    }

    // Inject user-id for per-user session isolation within the tenant.
    if let Ok(v) = reqwest::header::HeaderValue::from_str(user_id) {
        out.insert(
            reqwest::header::HeaderName::from_static("x-goclaw-user-id"),
            v,
        );
    }

    // Inject agent-id to route to the workspace's pre-configured agent.
    if let Ok(v) = reqwest::header::HeaderValue::from_str(agent_id) {
        out.insert(
            reqwest::header::HeaderName::from_static("x-goclaw-agent-id"),
            v,
        );
    }

    // Propagate (or set) request correlation ID for distributed tracing.
    if let Ok(v) = reqwest::header::HeaderValue::from_str(request_id) {
        out.insert(reqwest::header::HeaderName::from_static("x-request-id"), v);
    }

    out
}

fn spawn_nats_collector(
    state: Arc<AppState>,
    workspace_id: String,
    user_id: String,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    is_sse: bool,
) {
    tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = rx.recv().await {
            buf.extend_from_slice(&chunk);
        }
        publish_response(&state.nats_publisher, &workspace_id, &user_id, &buf, is_sse);
    });
}

fn publish_response(
    publisher: &NatsPublisher,
    workspace_id: &str,
    user_id: &str,
    buf: &[u8],
    is_sse: bool,
) {
    if is_sse {
        publish_sse_events(publisher, workspace_id, user_id, buf);
    } else {
        let body = if buf.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(buf)
                .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(buf) }))
        };
        publisher.publish(ConversationMessage::new(
            workspace_id,
            user_id,
            MessageDirection::Response,
            body,
        ));
    }
}

fn publish_sse_events(publisher: &NatsPublisher, workspace_id: &str, user_id: &str, buf: &[u8]) {
    let text = String::from_utf8_lossy(buf);
    for event in text.split("\n\n") {
        let event = event.trim();
        if event.is_empty() || event.starts_with(": ") {
            continue;
        }
        let data = match event.strip_prefix("data: ") {
            Some(d) => d.trim(),
            None => continue,
        };
        if data == "[DONE]" {
            continue;
        }
        let body = serde_json::from_str(data).unwrap_or_else(|_| json!({ "raw": data }));

        publisher.publish(ConversationMessage::new(
            workspace_id,
            user_id,
            MessageDirection::Response,
            body,
        ));
    }
}

/// Read workspace files from Redis `ws_files:{ws_id}` (hash: file_id → json)
/// and return a formatted block for injection into the system message.
/// Files uploaded in the last 5 minutes are flagged as "just uploaded" with
/// a stronger prompt so the agent proactively offers to search/analyze them.
async fn build_files_suffix(conn: &mut deadpool_redis::Connection, workspace_id: &str) -> String {
    let key = format!("ws_files:{workspace_id}");
    let map: std::collections::HashMap<String, String> = match conn.hgetall(&key).await {
        Ok(m) => m,
        Err(_) => return String::new(),
    };
    if map.is_empty() {
        return String::new();
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    const RECENT_SECS: u64 = 300; // 5 minutes

    let mut recent: Vec<(String, String)> = Vec::new();
    let mut older: Vec<(String, String)> = Vec::new();

    for (file_id, meta_json) in map.iter().take(20) {
        let v = serde_json::from_str::<Value>(meta_json).unwrap_or(Value::Null);
        let name = v["name"].as_str().unwrap_or("?").to_string();
        let uploaded_at = v["uploaded_at"].as_u64().unwrap_or(0);
        if uploaded_at > 0 && now.saturating_sub(uploaded_at) < RECENT_SECS {
            recent.push((file_id.clone(), name));
        } else {
            older.push((file_id.clone(), name));
        }
    }

    let mut out = String::new();

    if !recent.is_empty() {
        out.push_str("\n\n⚠ Files JUST UPLOADED — proactively acknowledge them and offer to search or analyze:");
        for (file_id, name) in &recent {
            out.push_str(&format!("\n- {name} (file_id: {file_id}) [NEW]"));
        }
        out.push_str("\nUse ws__rag_search to find content or ws__get_download_url to share the file.");
    }

    if !older.is_empty() {
        out.push_str("\n\nOther workspace files (ws__get_file / ws__get_download_url / ws__rag_search):");
        for (file_id, name) in &older {
            out.push_str(&format!("\n- {name} (file_id: {file_id})"));
        }
    }

    out
}

fn bytes_to_json(bytes: &Bytes) -> Value {
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(bytes) }))
}

fn is_sse_response(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("text/event-stream"))
        .unwrap_or(false)
}
