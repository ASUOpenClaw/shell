use std::sync::Arc;

use crate::{
    config::Config, error::AppError, goclaw::rpc_pool::GoclawRpcPool,
    nats::publisher::NatsPublisher,
};

pub struct AppState {
    pub config: Config,
    pub redis_pool: deadpool_redis::Pool,
    pub nats_publisher: NatsPublisher,
    pub rpc_pool: Arc<GoclawRpcPool>,
}

impl AppState {
    pub async fn build(config: Config) -> Result<Arc<Self>, AppError> {
        let redis_pool = {
            let mut cfg = deadpool_redis::Config::from_url(&config.redis_url);
            cfg.pool = Some(deadpool_redis::PoolConfig {
                max_size: config.redis_pool_size,
                ..Default::default()
            });
            cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
                .map_err(|e| AppError::Internal(format!("redis pool init: {e}")))?
        };

        let nats_publisher =
            NatsPublisher::connect(&config.nats_url, &config.nats_subject_prefix).await;

        let rpc_pool = GoclawRpcPool::new(
            &config.goclaw_gateway_url,
            config.goclaw_gateway_token.clone(),
            redis_pool.clone(),
        );

        Ok(Arc::new(Self {
            config,
            redis_pool,
            nats_publisher,
            rpc_pool,
        }))
    }
}
