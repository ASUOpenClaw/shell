use std::sync::Arc;

use axum::{
    Extension,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::Response,
};
use deadpool_redis::redis::AsyncCommands;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    agents::resolver::load_workspace_creds,
    error::AppError,
    goclaw::ws_client::{ChatParams, GoclawEvent, goclaw_chat, to_ws_url},
    middleware::auth::SessionData,
    nats::publisher::{ConversationMessage, MessageDirection},
    state::AppState,
};

/// Single message in the conversation.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct ChatMessage {
    /// Role of the message author: `system`, `user`, `assistant`, or `tool`.
    pub role: String,
    /// Text content of the message.
    pub content: String,
}

/// OpenAI-compatible chat completions request forwarded to the workspace GoClaw agent.
///
/// The `model` field is **ignored** — Shell always overrides it with
/// `agent:{agent_key}` to route to the workspace-bound agent.
/// The response includes an `X-Session-Key` header with the active session key.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct ChatCompletionRequest {
    /// Conversation history in OpenAI message format.
    pub messages: Vec<ChatMessage>,
    /// Model name — ignored, Shell routes to the workspace agent regardless.
    pub model: Option<String>,
    /// Stream the response as SSE (`data: {...}` lines ending with `data: [DONE]`).
    /// Defaults to `false`.
    #[serde(default)]
    pub stream: bool,
    /// GoClaw session key that scopes conversation continuity.
    /// Defaults to `"user-{user_id}"` when omitted.
    /// Custom keys let the same user maintain multiple independent threads.
    pub session_key: Option<String>,
}

#[utoipa::path(
    post,
    path = "/v1/chat/completions",
    tag = "proxy",
    security(("BearerAuth" = [])),
    request_body = ChatCompletionRequest,
    responses(
        (status = 200, description = "Agent response — SSE stream when `stream=true`, buffered JSON otherwise. \
                       Response header `X-Session-Key` contains the active GoClaw session key."),
        (status = 401, description = "Missing or invalid JWT / X-Workspace-Id header"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 502, description = "GoClaw gateway error"),
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
    let creds = load_workspace_creds(
        &state.redis_pool,
        &session.workspace_id,
        &state.config.rest_api_url,
        &state.config.service_key,
        &state.http_client,
    )
    .await?;

    let request_id = Uuid::new_v4().to_string();
    info!(
        workspace_id = %session.workspace_id,
        agent_id = %creds.agent_id,
        request_id = %request_id,
        "routing request via WebSocket"
    );

    // -----------------------------------------------------------------------
    // 2. Parse request body.
    // -----------------------------------------------------------------------
    let raw_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read request body: {e}")))?;

    let parsed: Value = serde_json::from_slice(&raw_bytes)
        .map_err(|e| AppError::Internal(format!("invalid JSON body: {e}")))?;

    let messages = parsed["messages"]
        .as_array()
        .ok_or_else(|| AppError::Internal("request body missing 'messages' array".to_owned()))?;

    // Extract the last user message to send to GoClaw.
    let user_message = messages
        .iter()
        .rev()
        .find(|m| m["role"].as_str() == Some("user"))
        .and_then(|m| m["content"].as_str())
        .ok_or_else(|| AppError::Internal("no user message found in messages".to_owned()))?
        .to_owned();

    let is_streaming = parsed["stream"].as_bool().unwrap_or(false);

    // Optional session key override. If absent, default to per-user key.
    let custom_session_key = parsed["session_key"].as_str().map(str::to_owned);

    // -----------------------------------------------------------------------
    // 3. Session tracking — inject workspace context on first turn.
    //    TTL: 4 h (14400 s), sliding window reset on every request.
    // -----------------------------------------------------------------------
    let mut conn = state.redis_pool.get().await.map_err(AppError::RedisPool)?;

    // Use custom session key if provided, otherwise default per-user key.
    let goclaw_session_key = custom_session_key
        .clone()
        .unwrap_or_else(|| format!("user-{}", session.user_id));

    // Track new-session state per session key (not just per user) so custom
    // sessions also get context injected on their first message.
    let session_key_redis = format!(
        "mcp_session:{}:{}",
        session.workspace_id, goclaw_session_key
    );
    let existing: Option<String> = conn
        .get::<_, Option<String>>(&session_key_redis)
        .await
        .map_err(AppError::RedisCmd)?;
    let is_new_session = existing.is_none();

    let _: () = conn
        .set_ex(&session_key_redis, "1", 14400_u64)
        .await
        .map_err(AppError::RedisCmd)?;

    // Build workspace context to inject at session start.
    let inject_content: Option<String> = if is_new_session {
        let skills: Option<String> = conn
            .get(format!("ws_skills:{}", session.workspace_id))
            .await
            .ok()
            .flatten();
        let skills_block = match skills {
            Some(s) if !s.is_empty() => format!("\n\n{s}"),
            _ => String::new(),
        };
        let files_block = build_files_suffix(&mut conn, &session.workspace_id).await;

        Some(format!(
            "[WORKSPACE_CTX: ctx_token={mcp_token}]\n\
             Use this token as the ctx_token argument for every ws__ tool call.\
             {skills_block}{files_block}",
            mcp_token = creds.mcp_service_token,
        ))
    } else {
        // On existing sessions, inject only if files were just uploaded.
        let files_block = build_files_suffix(&mut conn, &session.workspace_id).await;
        if files_block.contains("[NEW]") {
            Some(files_block)
        } else {
            None
        }
    };

    // Publish request to NATS — fire-and-forget.
    state.nats_publisher.publish(ConversationMessage::new(
        &session.workspace_id,
        &session.user_id,
        &goclaw_session_key,
        MessageDirection::Request,
        json!({"role": "user", "content": user_message}),
    ));

    // -----------------------------------------------------------------------
    // 4. Open WebSocket to GoClaw and run the chat.
    // -----------------------------------------------------------------------
    let ws_url = to_ws_url(&state.config.goclaw_gateway_url);

    let (event_tx, event_rx) = mpsc::unbounded_channel::<GoclawEvent>();

    let params_owned = OwnedChatParams {
        ws_url: ws_url.clone(),
        api_key: creds.api_key.clone(),
        user_id: session.user_id.clone(),
        agent_id: creds.agent_id.clone(),
        session_key: goclaw_session_key.clone(),
        inject_content: inject_content.clone(),
        message: user_message.clone(),
    };

    tokio::spawn(async move {
        let p = ChatParams {
            ws_url: &params_owned.ws_url,
            api_key: &params_owned.api_key,
            user_id: &params_owned.user_id,
            agent_id: &params_owned.agent_id,
            session_key: &params_owned.session_key,
            inject_content: params_owned.inject_content.as_deref(),
            message: &params_owned.message,
        };
        if let Err(e) = goclaw_chat(p, event_tx).await {
            error!(error = %e, "goclaw_chat error");
        }
    });

    // -----------------------------------------------------------------------
    // 5. Build response from GoClaw events.
    // -----------------------------------------------------------------------
    let session_key_header = goclaw_session_key.clone();

    if is_streaming {
        let stream = UnboundedReceiverStream::new(event_rx).flat_map(|event| {
            let bytes: Vec<Result<axum::body::Bytes, std::convert::Infallible>> = match event {
                GoclawEvent::Chunk(text) => {
                    let data = json!({
                        "choices": [{"delta": {"content": text}, "finish_reason": null, "index": 0}]
                    });
                    vec![Ok(axum::body::Bytes::from(format!("data: {data}\n\n")))]
                }
                GoclawEvent::Done => {
                    let done_chunk = json!({
                        "choices": [{"delta": {}, "finish_reason": "stop", "index": 0}]
                    });
                    vec![
                        Ok(axum::body::Bytes::from(format!("data: {done_chunk}\n\n"))),
                        Ok(axum::body::Bytes::from("data: [DONE]\n\n")),
                    ]
                }
                GoclawEvent::Error(err) => {
                    error!(error = %err, "GoClaw error event");
                    let data = json!({"error": {"message": err, "type": "gateway_error"}});
                    vec![Ok(axum::body::Bytes::from(format!("data: {data}\n\n")))]
                }
            };
            futures::stream::iter(bytes)
        });

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("X-Accel-Buffering", "no")
            .header("X-Session-Key", &session_key_header)
            .body(Body::from_stream(stream))
            .map_err(|e| AppError::Internal(e.to_string()))
    } else {
        // Non-streaming: collect all chunks then return JSON.
        let mut content = String::new();
        let mut finish_reason = "stop".to_owned();
        let mut rx = event_rx;

        while let Some(event) = rx.recv().await {
            match event {
                GoclawEvent::Chunk(text) => content.push_str(&text),
                GoclawEvent::Done => break,
                GoclawEvent::Error(err) => {
                    error!(error = %err, "GoClaw error during non-streaming collection");
                    finish_reason = "error".to_owned();
                    if content.is_empty() {
                        content = format!("[Error: {err}]");
                    }
                    break;
                }
            }
        }

        // Publish response to NATS.
        state.nats_publisher.publish(ConversationMessage::new(
            &session.workspace_id,
            &session.user_id,
            &session_key_header,
            MessageDirection::Response,
            json!({"role": "assistant", "content": content}),
        ));

        let response_body = json!({
            "id": format!("chatcmpl-{}", Uuid::new_v4()),
            "object": "chat.completion",
            "choices": [{
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason,
                "index": 0
            }]
        });

        let body_bytes = serde_json::to_vec(&response_body)
            .map_err(|e| AppError::Internal(format!("serialize response: {e}")))?;

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("X-Session-Key", &session_key_header)
            .body(Body::from(body_bytes))
            .map_err(|e| AppError::Internal(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Owned version of ChatParams so it can be moved into a spawned task.
struct OwnedChatParams {
    ws_url: String,
    api_key: String,
    user_id: String,
    agent_id: String,
    session_key: String,
    inject_content: Option<String>,
    message: String,
}

/// Read workspace files from Redis `ws_files:{ws_id}` and return a formatted
/// block for injection. Files uploaded in the last 5 minutes are flagged [NEW].
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
    const RECENT_SECS: u64 = 300;

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
        out.push_str(
            "\nUse ws__rag_search to find content or ws__get_download_url to share the file.",
        );
    }

    if !older.is_empty() {
        out.push_str(
            "\n\nOther workspace files (ws__get_file / ws__get_download_url / ws__rag_search):",
        );
        for (file_id, name) in &older {
            out.push_str(&format!("\n- {name} (file_id: {file_id})"));
        }
    }

    out
}
