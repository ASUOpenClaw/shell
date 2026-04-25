use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

/// An event received from GoClaw during an agent run.
#[derive(Debug)]
pub enum GoclawEvent {
    /// A streamed text token from the agent.
    Chunk(String),
    /// The agent run completed successfully.
    Done,
    /// The agent run failed.
    Error(String),
}

/// Parameters for a single GoClaw chat request.
pub struct ChatParams<'a> {
    /// WebSocket URL, e.g. `ws://goclaw:18790/ws`
    pub ws_url: &'a str,
    /// Tenant-scoped GoClaw API key (Bearer token for `connect`).
    pub api_key: &'a str,
    /// Authenticated user UUID — used for per-user session scoping.
    pub user_id: &'a str,
    /// Agent UUID to route to.
    pub agent_id: &'a str,
    /// Stable session key for conversation continuity, e.g. `"user-{user_id}"`.
    pub session_key: &'a str,
    /// If Some, inject this content into the session before sending the message.
    /// Used on first turn to establish workspace context (token + skills + files).
    pub inject_content: Option<&'a str>,
    /// The user message to send.
    pub message: &'a str,
}

/// Connect to GoClaw over WebSocket, optionally inject context, send the user
/// message, and stream events to `tx` until the run completes or fails.
///
/// The caller is responsible for consuming `rx` and translating events into
/// the appropriate HTTP response format (SSE or buffered JSON).
pub async fn goclaw_chat(
    params: ChatParams<'_>,
    tx: mpsc::UnboundedSender<GoclawEvent>,
) -> Result<(), String> {
    // Connect
    let (ws_stream, _) = connect_async(params.ws_url)
        .await
        .map_err(|e| format!("WebSocket connect failed: {e}"))?;

    let (mut write, mut read) = ws_stream.split();

    // Authenticate
    let connect_frame = json!({
        "type": "req",
        "id": "connect",
        "method": "connect",
        "params": {
            "token": params.api_key,
            "user_id": params.user_id,
            "protocol": 3
        }
    });
    write
        .send(Message::Text(connect_frame.to_string().into()))
        .await
        .map_err(|e| format!("WS send connect failed: {e}"))?;

    // Wait for connect ack
    loop {
        match read.next().await {
            Some(Ok(Message::Text(txt))) => {
                let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
                if v["type"] == "res" && v["id"] == "connect" {
                    if v["ok"].as_bool() != Some(true) {
                        let err = v["error"]["message"]
                            .as_str()
                            .unwrap_or("connect rejected")
                            .to_owned();
                        return Err(format!("GoClaw connect rejected: {err}"));
                    }
                    break;
                }
                // ignore other frames (health pings etc.)
            }
            Some(Ok(Message::Ping(data))) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(format!("WS read error during connect: {e}")),
            None => return Err("WebSocket closed during connect".to_owned()),
        }
    }

    info!(session_key = %params.session_key, "GoClaw connected");

    // Inject workspace context on new session.
    // GoClaw chat.inject uses "message" not "content" for the injected text.
    if let Some(content) = params.inject_content {
        info!(
            session_key = %params.session_key,
            inject_len = content.len(),
            inject_preview = %&content[..content.len().min(300)],
            "injecting workspace context"
        );
        let inject_frame = json!({
            "type": "req",
            "id": "inject",
            "method": "chat.inject",
            "params": {
                "sessionKey": params.session_key,
                "message": content
            }
        });
        write
            .send(Message::Text(inject_frame.to_string().into()))
            .await
            .map_err(|e| format!("WS send inject failed: {e}"))?;

        // Wait for inject ack (non-fatal if it fails — context missing is better than no response)
        loop {
            match read.next().await {
                Some(Ok(Message::Text(txt))) => {
                    let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
                    if v["type"] == "res" && v["id"] == "inject" {
                        if v["ok"].as_bool() != Some(true) {
                            warn!(
                                error = ?v["error"],
                                "chat.inject failed — continuing without injected context"
                            );
                        } else {
                            info!(session_key = %params.session_key, "chat.inject ok");
                        }
                        break;
                    }
                    // Drop other frames while waiting
                }
                Some(Ok(Message::Ping(data))) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    warn!(error = %e, "WS error while waiting for inject ack");
                    break;
                }
                None => break,
            }
        }
    } else {
        info!("chat.inject skipped");
    }

    // Send user message
    let chat_frame = json!({
        "type": "req",
        "id": "chat",
        "method": "chat.send",
        "params": {
            "message": params.message,
            "sessionKey": params.session_key,
            "agentId": params.agent_id
        }
    });
    write
        .send(Message::Text(chat_frame.to_string().into()))
        .await
        .map_err(|e| format!("WS send chat failed: {e}"))?;

    // Stream events until run completes or fails
    loop {
        match read.next().await {
            Some(Ok(Message::Text(txt))) => {
                let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);

                match v["type"].as_str() {
                    Some("res") if v["id"] == "chat" => {
                        if v["ok"].as_bool() != Some(true) {
                            let err = v["error"]["message"]
                                .as_str()
                                .unwrap_or("chat.send rejected")
                                .to_owned();
                            let _ = tx.send(GoclawEvent::Error(err));
                            return Ok(());
                        }
                        // ok: true just means the message was accepted; events follow
                    }

                    Some("event") => {
                        // GoClaw uses the "agent" event type for all run lifecycle events.
                        // Content arrives as: payload.type="chunk", inner content at payload.payload.content
                        // Run end: payload.type="run.completed" (or run.failed/run.cancelled)
                        if v["event"].as_str() != Some("agent") {
                            // Ignore health, tick, presence, etc.
                            continue;
                        }
                        let payload = &v["payload"];

                        match payload["type"].as_str() {
                            Some("chunk") => {
                                // Streaming token: payload.payload.content
                                if let Some(text) = payload["payload"]["content"].as_str() {
                                    if !text.is_empty() {
                                        let _ = tx.send(GoclawEvent::Chunk(text.to_owned()));
                                    }
                                }
                            }
                            Some("run.completed") => {
                                let _ = tx.send(GoclawEvent::Done);
                                return Ok(());
                            }
                            Some("run.failed") | Some("run.cancelled") => {
                                let err = payload["payload"]["error"]
                                    .as_str()
                                    .or_else(|| payload["payload"]["message"].as_str())
                                    .or_else(|| payload["error"].as_str())
                                    .unwrap_or("agent run failed")
                                    .to_owned();
                                let _ = tx.send(GoclawEvent::Error(err));
                                return Ok(());
                            }
                            _ => {}
                        }
                    }

                    _ => {}
                }
            }

            Some(Ok(Message::Ping(data))) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Some(Ok(Message::Close(_))) => {
                error!("GoClaw WebSocket closed unexpectedly");
                let _ = tx.send(GoclawEvent::Error(
                    "WebSocket closed unexpectedly".to_owned(),
                ));
                return Ok(());
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                let _ = tx.send(GoclawEvent::Error(format!("WS read error: {e}")));
                return Ok(());
            }
            None => {
                let _ = tx.send(GoclawEvent::Error("WebSocket stream ended".to_owned()));
                return Ok(());
            }
        }
    }
}

/// Convert `http://host:port/...` or `https://...` base URL to `ws://...` or `wss://...`
/// and append `/ws` path.
pub fn to_ws_url(gateway_url: &str) -> String {
    let ws_base = if gateway_url.starts_with("https://") {
        gateway_url.replacen("https://", "wss://", 1)
    } else if gateway_url.starts_with("http://") {
        gateway_url.replacen("http://", "ws://", 1)
    } else {
        gateway_url.to_owned()
    };
    // Strip trailing slash then append /ws
    format!("{}/ws", ws_base.trim_end_matches('/'))
}
