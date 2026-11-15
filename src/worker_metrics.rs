//! A small scrape listener for worker processes that do not serve the API.
use crate::{self_metrics::SelfMetrics, shutdown::ShutdownController};
use axum::{Router, extract::State, routing::get};
use std::sync::Arc;

pub const DEFAULT_WORKER_METRICS_ADDR: &str = "0.0.0.0:9090";

/// An explicit empty address disables the listener. Bind errors are startup
/// errors, rather than silently running a worker without its configured metrics.
pub async fn spawn(
    metrics: Arc<SelfMetrics>,
    shutdown: ShutdownController,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    let address = std::env::var("RUSH_WORKER_METRICS_ADDR")
        .unwrap_or_else(|_| DEFAULT_WORKER_METRICS_ADDR.to_string());
    if address.trim().is_empty() {
        return Ok(None);
    }
    let listener = tokio::net::TcpListener::bind(address.trim()).await?;
    Ok(Some(spawn_listener(listener, metrics, shutdown)))
}

fn spawn_listener(
    listener: tokio::net::TcpListener,
    metrics: Arc<SelfMetrics>,
    shutdown: ShutdownController,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(metrics))
            .with_graceful_shutdown(async move { shutdown.wait_for_finish().await })
            .await
        {
            tracing::error!(%error, "worker metrics listener failed");
        }
    })
}

fn router(metrics: Arc<SelfMetrics>) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(|State(metrics): State<Arc<SelfMetrics>>| async move {
                (
                    [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                    metrics.render_prometheus(),
                )
            }),
        )
        .with_state(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn worker_exposes_metrics_without_serving_api_routes() {
        let metrics = Arc::new(SelfMetrics::new());
        metrics.inc_counter(
            "rush_ingest_replay_batches_total",
            &[("outcome", "committed")],
            1,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownController::new();
        let server = spawn_listener(listener, metrics, shutdown.clone());
        shutdown.request(); // metrics remain available while the worker drains

        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{address}/metrics"))
            .send()
            .await
            .unwrap();
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("rush_ingest_replay_batches_total")
        );
        assert_eq!(
            client
                .get(format!("http://{address}/api/v1/tenants"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        shutdown.finish();
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}
