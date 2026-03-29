use std::sync::Arc;

use axum::body::Bytes;
use axum::{
    Extension,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    response::Response,
};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{
    error::AppError,
    middleware::auth::SessionData,
    nats::publisher::{ConversationMessage, MessageDirection, NatsPublisher},
    state::AppState,
};

#[utoipa::path(
    post,
    path = "/v1/{path}",
    tag = "proxy",
    params(("path" = String, Path, description = "Path forwarded to the OpenClaw gateway")),
    responses(
        (status = 200, description = "Streamed response from OpenClaw gateway"),
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
    // 1. Resolve agent_id from workspace — pure config lookup, no HTTP calls.
    // -----------------------------------------------------------------------
    let agent_id = state
        .agent_resolver
        .resolve(&session.workspace_id)
        .to_string();

    info!(
        workspace_id = %session.workspace_id,
        agent_id = %agent_id,
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
    let upstream_url = format!("{}/v1{}", state.config.openclaw_gateway_url, original_path);
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
        &state.config.openclaw_gateway_token,
        &session.workspace_id,
        &agent_id,
    );

    // -----------------------------------------------------------------------
    // 4. Buffer request body (capped at 10 MB) for NATS publish + forwarding.
    // -----------------------------------------------------------------------
    let body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read request body: {e}")))?;

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
// Helpers
// ---------------------------------------------------------------------------

/// Build upstream headers from the inbound request:
/// - Drop hop-by-hop headers
/// - Replace Authorization with the gateway's own token
/// - Add OpenClaw session key (workspace isolation) and agent-id (routing)
fn build_upstream_headers(
    inbound: &HeaderMap,
    gateway_token: &str,
    workspace_id: &str,
    agent_id: &str,
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
        ) {
            continue;
        }
        if let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_ref())
            && let Ok(value) = reqwest::header::HeaderValue::from_bytes(value.as_bytes())
        {
            out.insert(name, value);
        }
    }

    // Inject gateway auth token.
    if let Ok(auth_value) =
        reqwest::header::HeaderValue::from_str(&format!("Bearer {gateway_token}"))
    {
        out.insert(reqwest::header::AUTHORIZATION, auth_value);
    }

    // Inject session key for conversation continuity per workspace.
    if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("workspace_{workspace_id}")) {
        out.insert(
            reqwest::header::HeaderName::from_static("x-openclaw-session-key"),
            v,
        );
    }

    // Inject agent-id to route to the correct pre-configured agent.
    if let Ok(v) = reqwest::header::HeaderValue::from_str(agent_id) {
        out.insert(
            reqwest::header::HeaderName::from_static("x-openclaw-agent-id"),
            v,
        );
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
