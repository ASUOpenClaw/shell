use deadpool_redis::redis::AsyncCommands;
use serde::Deserialize;

use crate::error::AppError;

/// Per-workspace GoClaw credentials cached in Redis by the REST API.
///
/// Redis key: `ws_creds:{workspace_id}`
/// Value: `{"api_key":"goclaw_sk_...","agent_id":"<slug>",...}`
/// TTL: 86400 s — auto-refreshed from Postgres on miss via REST internal endpoint.
#[derive(Debug, Deserialize)]
pub struct WorkspaceCreds {
    pub api_key: String,
    pub agent_id: String,
    /// Permanent token (no TTL) for cron jobs and REST-triggered agent calls.
    #[serde(default)]
    pub mcp_service_token: String,
}

/// Load workspace credentials from Redis.
/// On cache miss, falls back to the REST internal endpoint (`/api/internal/workspaces/{id}/creds`)
/// which reads Postgres and re-populates Redis. Returns `AppError::Unauthorized` only if both
/// Redis and the REST fallback fail (workspace not found / not provisioned).
pub async fn load_workspace_creds(
    pool: &deadpool_redis::Pool,
    workspace_id: &str,
    rest_api_url: &str,
    service_key: &str,
    http_client: &reqwest::Client,
) -> Result<WorkspaceCreds, AppError> {
    let key = format!("ws_creds:{workspace_id}");
    let mut conn = pool.get().await.map_err(AppError::RedisPool)?;
    let raw: Option<String> = conn.get(&key).await.map_err(AppError::RedisCmd)?;

    if let Some(raw) = raw {
        return serde_json::from_str(&raw)
            .map_err(|e| AppError::Internal(format!("invalid ws_creds JSON: {e}")));
    }

    // Cache miss — try to refresh from REST API if configured
    if rest_api_url.is_empty() {
        return Err(AppError::Unauthorized(format!(
            "no GoClaw credentials cached for workspace {workspace_id}"
        )));
    }

    tracing::info!(workspace_id, "ws_creds cache miss — fetching from REST API");

    let url = format!("{rest_api_url}/api/internal/workspaces/{workspace_id}/creds");
    let resp = http_client
        .get(&url)
        .header("X-Shell-Service-Key", service_key)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("REST creds fetch failed: {e}")))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(AppError::Unauthorized(format!(
            "workspace {workspace_id} not found or not provisioned"
        )));
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(AppError::Unauthorized(format!(
            "REST creds endpoint returned {status} for workspace {workspace_id}: {body}"
        )));
    }

    let creds: WorkspaceCreds = resp
        .json()
        .await
        .map_err(|e| AppError::Internal(format!("invalid creds JSON from REST: {e}")))?;

    Ok(creds)
}
