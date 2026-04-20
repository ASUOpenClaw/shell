use deadpool_redis::redis::AsyncCommands;
use serde::Deserialize;

use crate::error::AppError;

/// Per-workspace GoClaw credentials cached in Redis by the REST API.
///
/// Redis key: `ws_creds:{workspace_id}`
/// Value: `{"api_key":"goclaw_sk_...","agent_id":"<uuid>","agent_key":"<slug>"}`
/// TTL: 3600 s (refreshed by REST API on workspace create/update)
#[derive(Debug, Deserialize)]
pub struct WorkspaceCreds {
    pub api_key: String,
    pub agent_id: String,
    #[serde(default)]
    pub agent_key: String,
}

/// Load workspace credentials from Redis.
/// Returns `AppError::Unauthorized` if the key is missing (workspace not provisioned).
pub async fn load_workspace_creds(
    pool: &deadpool_redis::Pool,
    workspace_id: &str,
) -> Result<WorkspaceCreds, AppError> {
    let key = format!("ws_creds:{workspace_id}");
    let mut conn = pool.get().await.map_err(AppError::RedisPool)?;
    let raw: Option<String> = conn.get(&key).await.map_err(AppError::RedisCmd)?;
    let raw = raw.ok_or_else(|| {
        AppError::Unauthorized(format!(
            "no GoClaw credentials cached for workspace {workspace_id}"
        ))
    })?;
    serde_json::from_str(&raw)
        .map_err(|e| AppError::Internal(format!("invalid ws_creds JSON: {e}")))
}
