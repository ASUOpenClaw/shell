mod agents;
mod config;
mod error;
mod handlers;
mod middleware;
mod nats;
mod openapi;
mod router;
mod state;

use tracing::info;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok(); // load .env if present, ignore if missing
    let cfg = config::Config::load().expect("failed to load config");

    // Init tracing before anything else so early errors are captured.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cfg.log_level.as_str().into()),
        )
        .init();

    info!(
        host = %cfg.server_host,
        port = cfg.server_port,
        gateway = %cfg.goclaw_gateway_url,
        "shell proxy starting"
    );

    let state = state::AppState::build(cfg)
        .await
        .expect("failed to build app state");

    let addr = format!("{}:{}", state.config.server_host, state.config.server_port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    info!(addr, "listening");

    let app = router::create_router(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    info!("server shut down");
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    info!("shutdown signal received");
}
