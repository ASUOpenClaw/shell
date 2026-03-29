use std::{sync::Arc, time::Duration};

use crate::{
    agents::resolver::AgentResolver, config::Config, error::AppError,
    nats::publisher::NatsPublisher,
};

pub struct AppState {
    pub config: Config,
    pub redis_pool: deadpool_redis::Pool,
    pub agent_resolver: AgentResolver,
    pub http_client: reqwest::Client,
    pub nats_publisher: NatsPublisher,
}

impl AppState {
    pub async fn build(config: Config) -> Result<Arc<Self>, AppError> {
        let redis_pool = deadpool_redis::Config::from_url(&config.redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .map_err(|e| AppError::Internal(format!("redis pool init: {e}")))?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|e| AppError::Internal(format!("http client init: {e}")))?;

        let agent_resolver = AgentResolver::from_config(
            &config.openclaw_agent_map,
            config.openclaw_default_agent.clone(),
        );

        let nats_publisher =
            NatsPublisher::connect(&config.nats_url, &config.nats_subject_prefix).await;

        Ok(Arc::new(Self {
            config,
            redis_pool,
            agent_resolver,
            http_client,
            nats_publisher,
        }))
    }
}
