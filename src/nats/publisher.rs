use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
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
        }
    }
}

pub struct NatsPublisher {
    /// None when NATS is unavailable at startup — publish becomes a no-op.
    js: Option<jetstream::Context>,
    subject_prefix: String,
}

impl NatsPublisher {
    /// Attempt to connect to NATS JetStream.
    ///
    /// Returns a fully connected publisher on success.
    /// On failure, logs an error and returns a no-op publisher so the proxy
    /// can still start and serve traffic without NATS.
    pub async fn connect(nats_url: &str, subject_prefix: &str) -> Self {
        match async_nats::connect(nats_url).await {
            Ok(client) => {
                info!(url = nats_url, "connected to NATS");
                let js = jetstream::new(client);
                // Ensure the CONVERSATIONS stream exists — REST API normally creates it,
                // but Shell may start before REST API (e.g. after VPN reconnect drops
                // containers from the Docker bridge and REST API hasn't recovered yet).
                ensure_conversations_stream(&js, subject_prefix).await;
                Self {
                    js: Some(js),
                    subject_prefix: subject_prefix.to_string(),
                }
            }
            Err(e) => {
                error!(url = nats_url, error = %e, "failed to connect to NATS — publishing disabled");
                Self {
                    js: None,
                    subject_prefix: subject_prefix.to_string(),
                }
            }
        }
    }

    /// Placeholder constructor used before Step 9 wired up in AppState::build.
    /// Kept for tests and local runs without NATS.
    pub fn new() -> Self {
        Self {
            js: None,
            subject_prefix: "conversation".to_string(),
        }
    }

    /// Fire-and-forget publish. Spawns a task so the caller is never blocked.
    /// Logs errors but never returns them.
    pub fn publish(&self, msg: ConversationMessage) {
        let js = match &self.js {
            Some(js) => js.clone(),
            None => {
                warn!(workspace_id = %msg.workspace_id, "NATS unavailable, skipping publish");
                return;
            }
        };

        let subject = format!("{}.{}", self.subject_prefix, msg.workspace_id);

        tokio::spawn(async move {
            let bytes = match serde_json::to_vec(&msg) {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "failed to serialize ConversationMessage");
                    return;
                }
            };

            match js.publish(subject.clone(), bytes.into()).await {
                Ok(ack) => {
                    // Await the server ack in the background.
                    // If the stream has no consumers yet the ack still confirms
                    // the message was persisted by the JetStream server.
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
}

impl Default for NatsPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl NatsPublisher {
    pub fn is_connected(&self) -> bool {
        self.js.is_some()
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
