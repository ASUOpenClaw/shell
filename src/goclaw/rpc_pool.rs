use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{agents::resolver::load_workspace_creds, error::AppError};

use super::ws_client::to_ws_url;

const RPC_TIMEOUT_SECS: u64 = 30;
const IDLE_EVICT_SECS: u64 = 1800;

type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

struct PoolEntry {
    conn: Arc<RpcConn>,
    last_used: Arc<Mutex<Instant>>,
}

struct RpcConn {
    outbound: tokio::sync::mpsc::UnboundedSender<Message>,
    pending: PendingMap,
    alive: Arc<AtomicBool>,
}

impl RpcConn {
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        if !self.alive.load(Ordering::SeqCst) {
            return Err("connection is dead".to_string());
        }
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);

        let frame = json!({
            "type": "req",
            "id": id,
            "method": method,
            "params": params,
        });
        if self
            .outbound
            .send(Message::Text(frame.to_string().into()))
            .is_err()
        {
            self.pending.lock().await.remove(&id);
            return Err("outbound channel closed".to_string());
        }

        match tokio::time::timeout(Duration::from_secs(RPC_TIMEOUT_SECS), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("oneshot dropped (connection died)".to_string()),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(format!("RPC timeout after {RPC_TIMEOUT_SECS}s"))
            }
        }
    }
}

pub struct GoclawRpcPool {
    workspace_entries: Arc<Mutex<HashMap<String, PoolEntry>>>,
    admin_entry: Arc<Mutex<Option<PoolEntry>>>,
    gateway_ws_url: String,
    gateway_token: String,
    redis_pool: deadpool_redis::Pool,
    rest_api_url: String,
    service_key: String,
    http_client: reqwest::Client,
}

impl GoclawRpcPool {
    pub fn new(
        gateway_url: &str,
        gateway_token: String,
        redis_pool: deadpool_redis::Pool,
        rest_api_url: String,
        service_key: String,
        http_client: reqwest::Client,
    ) -> Arc<Self> {
        let pool = Arc::new(Self {
            workspace_entries: Arc::new(Mutex::new(HashMap::new())),
            admin_entry: Arc::new(Mutex::new(None)),
            gateway_ws_url: to_ws_url(gateway_url),
            gateway_token,
            redis_pool,
            rest_api_url,
            service_key,
            http_client,
        });

        let pool_clone = Arc::clone(&pool);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                pool_clone.evict_idle().await;
            }
        });

        pool
    }

    async fn evict_idle(&self) {
        let mut entries = self.workspace_entries.lock().await;
        entries.retain(|ws_id, entry| {
            let elapsed = entry
                .last_used
                .try_lock()
                .map(|g| g.elapsed().as_secs())
                .unwrap_or(0);
            let keep = entry.conn.alive.load(Ordering::SeqCst) && elapsed < IDLE_EVICT_SECS;
            if !keep {
                info!(ws_id = %ws_id, "evicting idle RPC connection");
            }
            keep
        });
    }

    /// Send an RPC to a workspace's GoClaw tenant connection.
    pub async fn call_workspace(
        &self,
        ws_id: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, AppError> {
        let conn = self.get_or_connect_workspace(ws_id).await?;
        conn.call(method, params).await.map_err(AppError::RpcError)
    }

    /// Send an RPC using the master gateway token (for tenant management).
    pub async fn call_admin(&self, method: &str, params: Value) -> Result<Value, AppError> {
        let conn = self.get_or_connect_admin().await?;
        conn.call(method, params).await.map_err(AppError::RpcError)
    }

    async fn get_or_connect_workspace(&self, ws_id: &str) -> Result<Arc<RpcConn>, AppError> {
        let mut entries = self.workspace_entries.lock().await;

        if let Some(entry) = entries.get(ws_id) {
            if entry.conn.alive.load(Ordering::SeqCst) {
                *entry.last_used.lock().await = Instant::now();
                return Ok(Arc::clone(&entry.conn));
            }
        }

        let creds = load_workspace_creds(
            &self.redis_pool,
            ws_id,
            &self.rest_api_url,
            &self.service_key,
            &self.http_client,
        )
        .await?;
        info!(ws_id = %ws_id, "opening new RPC WS connection for workspace");
        let conn = self.connect(&creds.api_key, "system").await?;
        entries.insert(
            ws_id.to_owned(),
            PoolEntry {
                conn: Arc::clone(&conn),
                last_used: Arc::new(Mutex::new(Instant::now())),
            },
        );
        Ok(conn)
    }

    async fn get_or_connect_admin(&self) -> Result<Arc<RpcConn>, AppError> {
        let mut entry_opt = self.admin_entry.lock().await;

        if let Some(entry) = entry_opt.as_ref() {
            if entry.conn.alive.load(Ordering::SeqCst) {
                *entry.last_used.lock().await = Instant::now();
                return Ok(Arc::clone(&entry.conn));
            }
        }

        info!("opening new admin RPC WS connection");
        let conn = self.connect(&self.gateway_token, "system").await?;
        *entry_opt = Some(PoolEntry {
            conn: Arc::clone(&conn),
            last_used: Arc::new(Mutex::new(Instant::now())),
        });
        Ok(conn)
    }

    async fn connect(&self, token: &str, user_id: &str) -> Result<Arc<RpcConn>, AppError> {
        let (ws_stream, _) = connect_async(&self.gateway_ws_url)
            .await
            .map_err(|e| AppError::RpcError(format!("WS connect failed: {e}")))?;

        let (mut write, mut read) = ws_stream.split();

        write
            .send(Message::Text(
                json!({
                    "type": "req",
                    "id": "init",
                    "method": "connect",
                    "params": {"token": token, "user_id": user_id, "protocol": 3},
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(|e| AppError::RpcError(format!("connect frame send: {e}")))?;

        // Wait for connect ack
        loop {
            match read.next().await {
                Some(Ok(Message::Text(txt))) => {
                    let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
                    if v["type"] == "res" && v["id"] == "init" {
                        if v["ok"].as_bool() != Some(true) {
                            let err = v["error"]["message"]
                                .as_str()
                                .unwrap_or("connect rejected")
                                .to_owned();
                            return Err(AppError::RpcError(format!(
                                "GoClaw connect rejected: {err}"
                            )));
                        }
                        info!(user_id = %user_id, "RPC pool connection authenticated");
                        break;
                    }
                }
                Some(Ok(Message::Ping(data))) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(AppError::RpcError(format!("WS read during connect: {e}")));
                }
                None => {
                    return Err(AppError::RpcError("WS closed during connect".to_owned()));
                }
            }
        }

        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));

        // Writer task
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Reader task
        let pending_r = Arc::clone(&pending);
        let alive_r = Arc::clone(&alive);
        let pong_tx = outbound_tx.clone();
        tokio::spawn(async move {
            loop {
                match read.next().await {
                    Some(Ok(Message::Text(txt))) => {
                        let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
                        if v["type"] == "res" {
                            if let Some(id) = v["id"].as_str() {
                                let sender = pending_r.lock().await.remove(id);
                                if let Some(tx) = sender {
                                    let result = if v["ok"].as_bool() == Some(true) {
                                        Ok(v["payload"].clone())
                                    } else {
                                        Err(v["error"]["message"]
                                            .as_str()
                                            .unwrap_or("rpc error")
                                            .to_owned())
                                    };
                                    let _ = tx.send(result);
                                }
                            }
                        }
                        // event frames (health, tick, etc.) are silently ignored
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = pong_tx.send(Message::Pong(data));
                    }
                    Some(Ok(Message::Close(_))) => {
                        warn!("RPC pool connection closed by server");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        error!(error = %e, "RPC pool WS read error");
                        break;
                    }
                    None => {
                        warn!("RPC pool WS stream ended");
                        break;
                    }
                }
            }
            alive_r.store(false, Ordering::SeqCst);
            let mut pending_guard = pending_r.lock().await;
            for (_, tx) in pending_guard.drain() {
                let _ = tx.send(Err("connection closed".to_owned()));
            }
        });

        Ok(Arc::new(RpcConn {
            outbound: outbound_tx,
            pending,
            alive,
        }))
    }
}
