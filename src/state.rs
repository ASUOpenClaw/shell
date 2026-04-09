use std::{sync::Arc, time::Duration};

use crate::{config::Config, error::AppError, nats::publisher::NatsPublisher};

pub struct AppState {
    pub config: Config,
    pub redis_pool: deadpool_redis::Pool,
    pub http_client: reqwest::Client,
    pub nats_publisher: NatsPublisher,
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

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|e| AppError::Internal(format!("http client init: {e}")))?;

        let nats_publisher =
            NatsPublisher::connect(&config.nats_url, &config.nats_subject_prefix).await;

        Ok(Arc::new(Self {
            config,
            redis_pool,
            http_client,
            nats_publisher,
        }))
    }
}
