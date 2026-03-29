use std::time::Instant;

use axum::{extract::Request, middleware::Next, response::Response};
use tracing::info;

/// Access-log middleware. Produces one log line per request:
///
/// ```text
/// INFO POST /v1/chat/completions  200  47ms
/// INFO GET  /health               200  1ms
/// INFO POST /v1/chat              401  2ms
/// ```
///
/// Sits outside the auth layer so failed auth requests are also logged.
/// workspace_id is logged separately by the proxy handler once the agent
/// is resolved.
pub async fn logging_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let start = Instant::now();

    let response = next.run(req).await;

    let status = response.status();
    let latency = start.elapsed();
    let latency_ms = latency.as_millis();

    info!(
        method = %method,
        path = %format!("{path}{query}"),
        status = status.as_u16(),
        latency_ms,
        "{method} {path}{query}  {}  {latency_ms}ms",
        status.as_u16(),
    );

    response
}
