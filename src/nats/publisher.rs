use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MessageDirection {
    Request,
    Response,
}

/// Envelope published to NATS for every proxied request/response pair.
/// Subject: `{prefix}.{workspace_id}` (e.g. `conversation.ws_abc`)
#[derive(Debug, Serialize, Clone)]
pub struct ConversationMessage {
    pub message_id: String,
    pub workspace_id: String,
    pub user_id: String,
    /// GoClaw session key — used by the REST subscriber to link this message
    /// to the correct Conversation row (or create one if it doesn't exist yet).
    pub session_key: String,
    pub direction: MessageDirection,
    /// Parsed JSON body when possible; falls back to a `{ "raw": "..." }` wrapper.
    pub body: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    /// Structured turn events (tool_call, tool_result, thinking, chunk).
    /// Present on direction=response only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<serde_json::Value>,
    /// Full chat.history from GoClaw fetched after run.completed.
    /// Used by the subscriber to sync local message count against GoClaw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goclaw_history: Option<serde_json::Value>,
}

impl ConversationMessage {
    pub fn new(
        workspace_id: impl Into<String>,
        user_id: impl Into<String>,
        session_key: impl Into<String>,
        direction: MessageDirection,
        body: serde_json::Value,
    ) -> Self {
        Self {
            message_id: Uuid::new_v4().to_string(),
            workspace_id: workspace_id.into(),
            user_id: user_id.into(),
            session_key: session_key.into(),
            direction,
            body,
            timestamp: Utc::now(),
            events: Vec::new(),
            goclaw_history: None,
        }
    }

    pub fn with_events(mut self, events: Vec<serde_json::Value>) -> Self {
        self.events = events;
        self
    }

    pub fn with_goclaw_history(mut self, history: serde_json::Value) -> Self {
        self.goclaw_history = Some(history);
        self
    }
}

/// Shared inner state — behind an Arc<RwLock<>> so the reconnect task can swap
/// in a live JetStream context without restarting the server.
struct NatsInner {
    js: Option<jetstream::Context>,
    subject_prefix: String,
}

#[derive(Clone)]
pub struct NatsPublisher {
    inner: Arc<RwLock<NatsInner>>,
}

impl NatsPublisher {
    /// Attempt to connect to NATS JetStream.
    ///
    /// Always returns immediately — on failure the publisher starts as a no-op
    /// and a background task retries every 5 s until the connection succeeds.
    pub async fn connect(nats_url: &str, subject_prefix: &str) -> Self {
        let inner = Arc::new(RwLock::new(NatsInner {
            js: None,
            subject_prefix: subject_prefix.to_string(),
        }));

        let publisher = Self {
            inner: inner.clone(),
        };

        // Try initial connection, then spawn background reconnect loop.
        let url = nats_url.to_string();
        let prefix = subject_prefix.to_string();
        tokio::spawn(async move {
            loop {
                match async_nats::connect(&url).await {
                    Ok(client) => {
                        info!(url = %url, "connected to NATS");
                        let js = jetstream::new(client);
                        ensure_conversations_stream(&js, &prefix).await;
                        inner.write().await.js = Some(js);
                        return; // connected — exit the retry loop
                    }
                    Err(e) => {
                        error!(url = %url, error = %e, "NATS connect failed, retrying in 5s");
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        publisher
    }

    /// Placeholder constructor for tests / local runs without NATS.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(NatsInner {
                js: None,
                subject_prefix: "conversation".to_string(),
            })),
        }
    }

    /// Fire-and-forget publish. Spawns a task so the caller is never blocked.
    /// Logs errors but never returns them.
    pub fn publish(&self, msg: ConversationMessage) {
        let inner = self.inner.clone();

        tokio::spawn(async move {
            let (js, subject) = {
                let guard = inner.read().await;
                match &guard.js {
                    Some(js) => (
                        js.clone(),
                        format!("{}.{}", guard.subject_prefix, msg.workspace_id),
                    ),
                    None => {
                        warn!(workspace_id = %msg.workspace_id, "NATS unavailable, skipping publish");
                        return;
                    }
                }
            };

            let bytes = match serde_json::to_vec(&msg) {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "failed to serialize ConversationMessage");
                    return;
                }
            };

            match js.publish(subject.clone(), bytes.into()).await {
                Ok(ack) => {
                    if let Err(e) = ack.await {
                        error!(subject, error = %e, "NATS publish ack failed");
                    }
                }
                Err(e) => {
                    error!(subject, error = %e, "NATS publish failed");
                }
            }
        });
    }

    pub fn is_connected(&self) -> bool {
        // Non-blocking best-effort check — returns false if lock is contended.
        self.inner
            .try_read()
            .map(|g| g.js.is_some())
            .unwrap_or(false)
    }
}

impl Default for NatsPublisher {
    fn default() -> Self {
        Self::new()
    }
}

/// Create the CONVERSATIONS JetStream stream if it does not already exist.
/// Idempotent — `get_or_create_stream` returns the existing stream unchanged.
async fn ensure_conversations_stream(js: &jetstream::Context, subject_prefix: &str) {
    let subject = format!("{subject_prefix}.*");
    let cfg = jetstream::stream::Config {
        name: "CONVERSATIONS".to_string(),
        subjects: vec![subject],
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    };
    match js.get_or_create_stream(cfg).await {
        Ok(_) => info!("NATS stream ready: CONVERSATIONS"),
        Err(e) => error!(error = %e, "failed to ensure CONVERSATIONS stream"),
    }
}
